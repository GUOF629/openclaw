use std::{
    collections::HashMap,
    io::Read,
    iter,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use age::secrecy::SecretString;
use axum::{
    body::Body,
    extract::{Multipart, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{fs, net::TcpListener};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tokio::io::AsyncWriteExt;

#[derive(Clone)]
struct AppState {
    data_dir: PathBuf,
    db_path: PathBuf,
    require_api_key: bool,
    api_keys: Arc<HashMap<String, ApiKey>>,
    master_key: Option<SecretString>,
}

#[derive(Clone, Debug, Deserialize)]
struct ApiKey {
    #[allow(dead_code)]
    key: String,
    tenant_id: String,
    #[allow(dead_code)]
    role: Option<String>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    tenant_id: Option<String>,
    session_id: Option<String>,
    q: Option<String>,
    mime: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct FileMeta {
    file_id: String,
    tenant_id: String,
    session_id: Option<String>,
    filename: String,
    mime: Option<String>,
    size: i64,
    sha256: String,
    created_at_ms: i64,
    source: Option<String>,
    encrypted: bool,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    ok: bool,
    items: Vec<FileMeta>,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    ok: bool,
    file_id: String,
    sha256: String,
    size: i64,
    encrypted: bool,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid_request: {0}")]
    InvalidRequest(String),
    #[error("not_found")]
    NotFound,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("db: {0}")]
    Db(String),
    #[error("crypto: {0}")]
    Crypto(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            AppError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorBody {
                    error: "unauthorized",
                    message: None,
                },
            ),
            AppError::InvalidRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorBody {
                    error: "invalid_request",
                    message: Some(msg.clone()),
                },
            ),
            AppError::NotFound => (
                StatusCode::NOT_FOUND,
                ErrorBody {
                    error: "not_found",
                    message: None,
                },
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorBody {
                    error: "internal_error",
                    message: Some(self.to_string()),
                },
            ),
        };
        (status, Json(body)).into_response()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as i64
}

fn tenant_from_auth(state: &AppState, headers: &HeaderMap, tenant_hint: Option<&str>) -> Result<String, AppError> {
    if !state.require_api_key {
        if let Some(t) = tenant_hint {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
        return Ok("default".to_string());
    }
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::Unauthorized)?;
    let entry = state.api_keys.get(&key).ok_or(AppError::Unauthorized)?;
    Ok(entry.tenant_id.clone())
}

async fn init_db(db_path: &Path) -> Result<(), AppError> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        let conn = Connection::open(db_path).map_err(|e| AppError::Db(e.to_string()))?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS files (
  file_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  session_id TEXT,
  filename TEXT NOT NULL,
  mime TEXT,
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  source TEXT,
  encrypted INTEGER NOT NULL,
  storage_path TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_tenant_created ON files(tenant_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_files_tenant_session ON files(tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_files_tenant_filename ON files(tenant_id, filename);
"#,
        )
        .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|e| AppError::Db(e.to_string()))??;
    Ok(())
}

async fn with_conn<T>(state: &AppState, f: impl FnOnce(&Connection) -> Result<T, AppError> + Send + 'static) -> Result<T, AppError>
where
    T: Send + 'static,
{
    let path = state.db_path.clone();
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(path).map_err(|e| AppError::Db(e.to_string()))?;
        f(&conn)
    })
    .await
    .map_err(|e| AppError::Db(e.to_string()))?
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "ok": true })))
}

async fn readyz(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    init_db(&state.db_path).await?;
    Ok((StatusCode::OK, Json(serde_json::json!({ "ok": true }))))
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let mut tenant_hint: Option<String> = None;
    let mut session_id: Option<String> = None;
    let mut source: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut mime: Option<String> = None;
    let mut tmp_path: Option<PathBuf> = None;
    let mut sha = Sha256::new();
    let mut size: i64 = 0;

    fs::create_dir_all(state.data_dir.join("tmp")).await?;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::InvalidRequest(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "tenant_id" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| AppError::InvalidRequest(e.to_string()))?;
                tenant_hint = Some(v);
            }
            "session_id" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| AppError::InvalidRequest(e.to_string()))?;
                let trimmed = v.trim();
                session_id = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                };
            }
            "source" => {
                let v = field
                    .text()
                    .await
                    .map_err(|e| AppError::InvalidRequest(e.to_string()))?;
                let trimmed = v.trim();
                source = if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                };
            }
            "file" => {
                filename = field.file_name().map(|s| s.to_string());
                mime = field.content_type().map(|m| m.to_string());

                let tmp = state
                    .data_dir
                    .join("tmp")
                    .join(format!("upload-{}.bin", uuid::Uuid::new_v4()));
                let mut out = fs::File::create(&tmp).await?;
                let mut stream = field;
                while let Some(chunk) = stream
                    .chunk()
                    .await
                    .map_err(|e| AppError::InvalidRequest(e.to_string()))?
                {
                    sha.update(&chunk);
                    size += chunk.len() as i64;
                    out.write_all(&chunk).await?;
                }
                out.flush().await?;
                tmp_path = Some(tmp);
            }
            _ => {
                // ignore unknown fields
            }
        }
    }

    let tenant_id = tenant_from_auth(&state, &headers, tenant_hint.as_deref())?;
    let tmp = tmp_path.ok_or_else(|| AppError::InvalidRequest("missing multipart field: file".to_string()))?;
    let filename = filename.unwrap_or_else(|| "file".to_string());
    let sha256 = hex::encode(sha.finalize());
    let file_id = sha256.clone();

    let tenant_dir = state.data_dir.join("objects").join(&tenant_id);
    fs::create_dir_all(&tenant_dir).await?;

    let created_at_ms = now_ms();
    let encrypted = state.master_key.is_some();
    let final_path_plain = tenant_dir.join(&file_id);
    let final_path = if encrypted {
        tenant_dir.join(format!("{file_id}.age"))
    } else {
        final_path_plain.clone()
    };

    // Insert-or-return existing by (tenant_id, file_id)
    let tenant_id_for_existing = tenant_id.clone();
    let file_id_for_existing = file_id.clone();
    let existing = with_conn(&state, move |conn| {
        let mut stmt = conn
            .prepare(
                "SELECT file_id, sha256, size, encrypted, storage_path FROM files WHERE tenant_id=?1 AND file_id=?2",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_for_existing, file_id_for_existing])
            .map_err(|e| AppError::Db(e.to_string()))?;
        if let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            let file_id: String = row.get(0).map_err(|e| AppError::Db(e.to_string()))?;
            let sha256: String = row.get(1).map_err(|e| AppError::Db(e.to_string()))?;
            let size: i64 = row.get(2).map_err(|e| AppError::Db(e.to_string()))?;
            let encrypted: i64 = row.get(3).map_err(|e| AppError::Db(e.to_string()))?;
            let _storage_path: String = row.get(4).map_err(|e| AppError::Db(e.to_string()))?;
            return Ok(Some((file_id, sha256, size, encrypted != 0)));
        }
        Ok(None)
    })
    .await?;

    if let Some((file_id, sha256, size, encrypted)) = existing {
        // Best-effort cleanup tmp
        let _ = fs::remove_file(&tmp).await;
        return Ok((
            StatusCode::OK,
            Json(IngestResponse {
                ok: true,
                file_id,
                sha256,
                size,
                encrypted,
            }),
        ));
    }

    // Move to final location, encrypt if configured.
    if encrypted {
        // Write plaintext to deterministic path first (temp name), then encrypt to .age and delete plaintext.
        fs::rename(&tmp, &final_path_plain).await?;

        let in_path = final_path_plain.clone();
        let out_path = final_path.clone();
        let key = state.master_key.clone().ok_or_else(|| AppError::Crypto("missing master key".to_string()))?;
        tokio::task::spawn_blocking(move || -> Result<(), AppError> {
            let input = std::fs::File::open(&in_path)?;
            let output = std::fs::File::create(&out_path)?;
            let encryptor = age::Encryptor::with_user_passphrase(key);
            let mut writer = encryptor
                .wrap_output(output)
                .map_err(|e| AppError::Crypto(e.to_string()))?;
            let mut reader = std::io::BufReader::new(input);
            std::io::copy(&mut reader, &mut writer)?;
            writer.finish().map_err(|e| AppError::Crypto(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| AppError::Crypto(e.to_string()))??;

        // Remove plaintext
        fs::remove_file(&final_path_plain).await?;
    } else {
        fs::rename(&tmp, &final_path).await?;
    }

    let storage_path = final_path
        .strip_prefix(&state.data_dir)
        .unwrap_or(&final_path)
        .to_string_lossy()
        .to_string();

    let tenant_id_for_db = tenant_id.clone();
    let session_id_for_db = session_id.clone();
    let filename_for_db = filename.clone();
    let mime_for_db = mime.clone();
    let source_for_db = source.clone();
    let sha256_for_db = sha256.clone();
    let file_id_for_db = file_id.clone();
    let encrypted_i = if encrypted { 1 } else { 0 };
    with_conn(&state, move |conn| {
        conn.execute(
            "INSERT INTO files(file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted, storage_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                file_id_for_db,
                tenant_id_for_db,
                session_id_for_db,
                filename_for_db,
                mime_for_db,
                size,
                sha256_for_db,
                created_at_ms,
                source_for_db,
                encrypted_i,
                storage_path,
            ],
        )
        .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(())
    })
    .await?;

    Ok((
        StatusCode::OK,
        Json(IngestResponse {
            ok: true,
            file_id,
            sha256,
            size,
            encrypted,
        }),
    ))
}

async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<SearchQuery>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = tenant_from_auth(&state, &headers, q.tenant_id.as_deref())?;
    let limit = q.limit.unwrap_or(50).clamp(1, 200) as i64;
    let session_id = q.session_id.clone().filter(|s| !s.trim().is_empty());
    let query_text = q.q.clone().filter(|s| !s.trim().is_empty());
    let mime = q.mime.clone().filter(|s| !s.trim().is_empty());

    let tenant_id_db = tenant_id.clone();
    let items = with_conn(&state, move |conn| -> Result<Vec<FileMeta>, AppError> {
        let mut sql = String::from(
            "SELECT file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted
             FROM files WHERE tenant_id=?1",
        );
        let mut args: Vec<rusqlite::types::Value> = vec![tenant_id_db.clone().into()];

        if let Some(sid) = &session_id {
            sql.push_str(" AND session_id=?");
            args.push(sid.clone().into());
        }
        if let Some(m) = &mime {
            sql.push_str(" AND mime=?");
            args.push(m.clone().into());
        }
        if let Some(text) = &query_text {
            sql.push_str(" AND (filename LIKE ? OR file_id LIKE ? OR sha256 LIKE ?)");
            let like = format!("%{}%", text);
            args.push(like.clone().into());
            args.push(like.clone().into());
            args.push(like.into());
        }
        sql.push_str(" ORDER BY created_at_ms DESC LIMIT ?");
        args.push(limit.into());

        let mut stmt = conn.prepare(&sql).map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(args))
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            out.push(FileMeta {
                file_id: row.get(0).map_err(|e| AppError::Db(e.to_string()))?,
                tenant_id: row.get(1).map_err(|e| AppError::Db(e.to_string()))?,
                session_id: row.get(2).map_err(|e| AppError::Db(e.to_string()))?,
                filename: row.get(3).map_err(|e| AppError::Db(e.to_string()))?,
                mime: row.get(4).map_err(|e| AppError::Db(e.to_string()))?,
                size: row.get(5).map_err(|e| AppError::Db(e.to_string()))?,
                sha256: row.get(6).map_err(|e| AppError::Db(e.to_string()))?,
                created_at_ms: row.get(7).map_err(|e| AppError::Db(e.to_string()))?,
                source: row.get(8).map_err(|e| AppError::Db(e.to_string()))?,
                encrypted: {
                    let v: i64 = row.get(9).map_err(|e| AppError::Db(e.to_string()))?;
                    v != 0
                },
            });
        }
        Ok(out)
    })
    .await?;

    Ok((StatusCode::OK, Json(SearchResponse { ok: true, items })))
}

#[derive(Debug, Deserialize)]
struct PathParams {
    file_id: String,
}

async fn meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = tenant_from_auth(&state, &headers, None)?;
    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }
    let tenant_id_db = tenant_id.clone();
    let out = with_conn(&state, move |conn| -> Result<Option<FileMeta>, AppError> {
        let mut stmt = conn
            .prepare(
                "SELECT file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted
                 FROM files WHERE tenant_id=?1 AND file_id=?2",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_db, file_id])
            .map_err(|e| AppError::Db(e.to_string()))?;
        if let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            return Ok(Some(FileMeta {
                file_id: row.get(0).map_err(|e| AppError::Db(e.to_string()))?,
                tenant_id: row.get(1).map_err(|e| AppError::Db(e.to_string()))?,
                session_id: row.get(2).map_err(|e| AppError::Db(e.to_string()))?,
                filename: row.get(3).map_err(|e| AppError::Db(e.to_string()))?,
                mime: row.get(4).map_err(|e| AppError::Db(e.to_string()))?,
                size: row.get(5).map_err(|e| AppError::Db(e.to_string()))?,
                sha256: row.get(6).map_err(|e| AppError::Db(e.to_string()))?,
                created_at_ms: row.get(7).map_err(|e| AppError::Db(e.to_string()))?,
                source: row.get(8).map_err(|e| AppError::Db(e.to_string()))?,
                encrypted: {
                    let v: i64 = row.get(9).map_err(|e| AppError::Db(e.to_string()))?;
                    v != 0
                },
            }));
        }
        Ok(None)
    })
    .await?;
    match out {
        Some(v) => Ok((StatusCode::OK, Json(v))),
        None => Err(AppError::NotFound),
    }
}

async fn download(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = tenant_from_auth(&state, &headers, None)?;
    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }

    #[derive(Debug)]
    struct Row {
        filename: String,
        mime: Option<String>,
        encrypted: bool,
        storage_path: String,
    }

    let tenant_id_db = tenant_id.clone();
    let row = with_conn(&state, move |conn| -> Result<Option<Row>, AppError> {
        let mut stmt = conn
            .prepare(
                "SELECT filename, mime, encrypted, storage_path FROM files WHERE tenant_id=?1 AND file_id=?2",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_db, file_id])
            .map_err(|e| AppError::Db(e.to_string()))?;
        if let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            let encrypted_i: i64 = row.get(2).map_err(|e| AppError::Db(e.to_string()))?;
            return Ok(Some(Row {
                filename: row.get(0).map_err(|e| AppError::Db(e.to_string()))?,
                mime: row.get(1).map_err(|e| AppError::Db(e.to_string()))?,
                encrypted: encrypted_i != 0,
                storage_path: row.get(3).map_err(|e| AppError::Db(e.to_string()))?,
            }));
        }
        Ok(None)
    })
    .await?
    .ok_or(AppError::NotFound)?;

    let abs = state.data_dir.join(row.storage_path.trim_start_matches('/'));
    if !abs.exists() {
        return Err(AppError::NotFound);
    }

    let mut headers_out = HeaderMap::new();
    headers_out.insert(
        header::CONTENT_DISPOSITION,
        format!("attachment; filename=\"{}\"", row.filename.replace('"', "_"))
            .parse()
            .unwrap(),
    );
    if let Some(ct) = row.mime.as_deref() {
        if let Ok(v) = ct.parse() {
            headers_out.insert(header::CONTENT_TYPE, v);
        }
    }

    if !row.encrypted {
        let file = fs::File::open(abs).await?;
        let body = Body::from_stream(ReaderStream::new(file));
        return Ok((StatusCode::OK, headers_out, body));
    }

    // Encrypted: decrypt on the fly (blocking reader -> async body stream).
    let key = state
        .master_key
        .clone()
        .ok_or_else(|| AppError::Crypto("encrypted file but no master key configured".to_string()))?;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::task::spawn_blocking(move || {
        let result: Result<(), std::io::Error> = (|| {
            let input = std::fs::File::open(abs)?;
            let decryptor = age::Decryptor::new(std::io::BufReader::new(input))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            let identity = age::scrypt::Identity::new(key);
            let mut reader = decryptor
                .decrypt(iter::once(&identity as &dyn age::Identity))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                if tx.blocking_send(Ok(Bytes::copy_from_slice(&buf[..n]))).is_err() {
                    break;
                }
            }
            Ok(())
        })();
        if let Err(e) = result {
            let _ = tx.blocking_send(Err(e));
        }
    });

    let body = Body::from_stream(ReceiverStream::new(rx));
    Ok((StatusCode::OK, headers_out, body))
}

fn parse_api_keys_json(raw: &str) -> HashMap<String, ApiKey> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return HashMap::new();
    }
    let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };
    let mut map = HashMap::new();
    let arr = parsed.as_array().cloned().unwrap_or_default();
    for item in arr {
        let key = item.get("key").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
        let tenant_id = item
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let role = item.get("role").and_then(|v| v.as_str()).map(|s| s.to_string());
        if key.is_empty() || tenant_id.is_empty() {
            continue;
        }
        map.insert(
            key.clone(),
            ApiKey {
                key,
                tenant_id,
                role,
            },
        );
    }
    map
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustfs=info,tower_http=warn".into()),
        )
        .init();

    let port: u16 = std::env::var("RUSTFS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8099);
    let data_dir = std::env::var("RUSTFS_DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let db_path = std::env::var("RUSTFS_DB_PATH").unwrap_or_else(|_| "/data/meta.db".to_string());
    let require_api_key = std::env::var("RUSTFS_REQUIRE_API_KEY")
        .ok()
        .map(|v| v.trim().to_lowercase() == "true" || v.trim() == "1")
        .unwrap_or(true);
    let api_keys_json = std::env::var("RUSTFS_API_KEYS_JSON").unwrap_or_default();
    let master_key_raw = std::env::var("RUSTFS_MASTER_KEY").ok().map(|v| v.trim().to_string());
    let master_key = master_key_raw
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| SecretString::from(s.to_string()));

    let state = AppState {
        data_dir: PathBuf::from(data_dir),
        db_path: PathBuf::from(db_path),
        require_api_key,
        api_keys: Arc::new(parse_api_keys_json(&api_keys_json)),
        master_key,
    };

    fs::create_dir_all(&state.data_dir).await?;
    init_db(&state.db_path).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;

    if state.require_api_key && state.api_keys.is_empty() {
        warn!("RUSTFS_REQUIRE_API_KEY=true but RUSTFS_API_KEYS_JSON is empty; all requests will be unauthorized");
    }
    info!(
        "rustfs starting: port={} data_dir={} db_path={} encryption={}",
        port,
        state.data_dir.display(),
        state.db_path.display(),
        state.master_key.is_some()
    );

    let app = Router::new()
        .route("/health", get(health))
        .route("/readyz", get(readyz))
        .route("/v1/files", post(ingest).get(search))
        .route("/v1/files/:file_id/meta", get(meta))
        .route("/v1/files/:file_id", get(download))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

