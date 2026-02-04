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
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::Bytes;
use hmac::{Hmac, Mac};
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
    signing_key: Option<Vec<u8>>,
    public_base_url: Option<String>,
    audit_log_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
struct ApiKey {
    #[allow(dead_code)]
    key: String,
    tenant_id: String,
    #[allow(dead_code)]
    role: Option<String>,
}

#[derive(Clone, Debug)]
struct AuthContext {
    tenant_id: String,
    role: String,
    key_id: String,
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
    extract_status: Option<String>,
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
    extract_status: Option<String>,
    extract_updated_at_ms: Option<i64>,
    extract_attempt: Option<i64>,
    extract_error: Option<String>,
    annotations: Option<serde_json::Value>,
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

#[derive(Debug, Deserialize)]
struct LinkRequest {
    ttl_seconds: Option<u32>,
}

#[derive(Debug, Serialize)]
struct LinkResponse {
    ok: bool,
    token: String,
    path: String,
    url: Option<String>,
    expires_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct TombstoneRequest {
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct TombstoneResponse {
    ok: bool,
    file_id: String,
    tombstoned: bool,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
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
            AppError::Forbidden => (
                StatusCode::FORBIDDEN,
                ErrorBody {
                    error: "forbidden",
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

fn normalize_role(role: Option<&str>) -> String {
    let r = role.unwrap_or("admin").trim().to_lowercase();
    match r.as_str() {
        "reader" | "writer" | "admin" => r,
        _ => "admin".to_string(),
    }
}

fn key_id_from_raw(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..8])
}

fn auth_from_headers(
    state: &AppState,
    headers: &HeaderMap,
    tenant_hint: Option<&str>,
) -> Result<AuthContext, AppError> {
    if !state.require_api_key {
        if let Some(t) = tenant_hint {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                return Ok(AuthContext {
                    tenant_id: trimmed.to_string(),
                    role: "admin".to_string(),
                    key_id: "dev".to_string(),
                });
            }
        }
        return Ok(AuthContext {
            tenant_id: "default".to_string(),
            role: "admin".to_string(),
            key_id: "dev".to_string(),
        });
    }
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or(AppError::Unauthorized)?;
    let entry = state.api_keys.get(&key).ok_or(AppError::Unauthorized)?;
    Ok(AuthContext {
        tenant_id: entry.tenant_id.clone(),
        role: normalize_role(entry.role.as_deref()),
        key_id: key_id_from_raw(&key),
    })
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
  storage_path TEXT NOT NULL,
  deleted_at_ms INTEGER,
  extract_status TEXT,
  extract_updated_at_ms INTEGER,
  extract_attempt INTEGER,
  extract_error TEXT,
  annotations_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_tenant_created ON files(tenant_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_files_tenant_session ON files(tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_files_tenant_filename ON files(tenant_id, filename);
"#,
        )
        .map_err(|e| AppError::Db(e.to_string()))?;
        // Back-compat: older DBs might not have deleted_at_ms.
        let _ = conn.execute("ALTER TABLE files ADD COLUMN deleted_at_ms INTEGER", []);
        let _ = conn.execute("ALTER TABLE files ADD COLUMN extract_status TEXT", []);
        let _ = conn.execute("ALTER TABLE files ADD COLUMN extract_updated_at_ms INTEGER", []);
        let _ = conn.execute("ALTER TABLE files ADD COLUMN extract_attempt INTEGER", []);
        let _ = conn.execute("ALTER TABLE files ADD COLUMN extract_error TEXT", []);
        let _ = conn.execute("ALTER TABLE files ADD COLUMN annotations_json TEXT", []);
        Ok(())
    })
    .await
    .map_err(|e| AppError::Db(e.to_string()))??;
    Ok(())
}

#[derive(Debug, Serialize)]
struct AuditEntry<'a> {
    id: String,
    ts_ms: i64,
    action: &'a str,
    tenant_id: &'a str,
    key_id: Option<&'a str>,
    request_id: Option<&'a str>,
    file_id: Option<&'a str>,
    extra: serde_json::Value,
}

async fn append_audit(
    state: &AppState,
    entry: AuditEntry<'_>,
) {
    let path = match state.audit_log_path.as_ref() {
        Some(p) => p,
        None => return,
    };
    let parent = match path.parent() {
        Some(p) => p,
        None => return,
    };
    if fs::create_dir_all(parent).await.is_err() {
        return;
    }
    let line = match serde_json::to_string(&entry) {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut file = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(f) => f,
        Err(_) => return,
    };
    let _ = file.write_all(format!("{line}\n").as_bytes()).await;
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

fn assert_can_read(auth: &AuthContext) -> Result<(), AppError> {
    match auth.role.as_str() {
        "reader" | "writer" | "admin" => Ok(()),
        _ => Err(AppError::Forbidden),
    }
}

fn assert_can_write(auth: &AuthContext) -> Result<(), AppError> {
    match auth.role.as_str() {
        "writer" | "admin" => Ok(()),
        _ => Err(AppError::Forbidden),
    }
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

    let request_id = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    let auth = auth_from_headers(&state, &headers, tenant_hint.as_deref())?;
    assert_can_write(&auth)?;
    let tenant_id = auth.tenant_id.clone();
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
                "SELECT file_id, sha256, size, encrypted, storage_path FROM files WHERE tenant_id=?1 AND file_id=?2 AND deleted_at_ms IS NULL",
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
        append_audit(
            &state,
            AuditEntry {
                id: uuid::Uuid::new_v4().to_string(),
                ts_ms: now_ms(),
                action: "ingest",
                tenant_id: &tenant_id,
                key_id: Some(&auth.key_id),
                request_id,
                file_id: Some(&file_id),
                extra: serde_json::json!({ "dedup": true, "size": size, "encrypted": encrypted }),
            },
        )
        .await;
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
    let extract_status_for_db = "pending".to_string();
    let extract_updated_at_for_db = now_ms();
    let extract_attempt_for_db = 0i64;
    with_conn(&state, move |conn| {
        conn.execute(
            "INSERT INTO files(file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted, storage_path, extract_status, extract_updated_at_ms, extract_attempt)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
                extract_status_for_db,
                extract_updated_at_for_db,
                extract_attempt_for_db,
            ],
        )
        .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(())
    })
    .await?;

    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "ingest",
            tenant_id: &tenant_id,
            key_id: Some(&auth.key_id),
            request_id,
            file_id: Some(&file_id),
            extra: serde_json::json!({ "dedup": false, "size": size, "encrypted": encrypted }),
        },
    )
    .await;

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
    let auth = auth_from_headers(&state, &headers, q.tenant_id.as_deref())?;
    assert_can_read(&auth)?;
    let tenant_id = auth.tenant_id;
    let limit = q.limit.unwrap_or(50).clamp(1, 200) as i64;
    let session_id = q.session_id.clone().filter(|s| !s.trim().is_empty());
    let query_text = q.q.clone().filter(|s| !s.trim().is_empty());
    let mime = q.mime.clone().filter(|s| !s.trim().is_empty());
    let extract_status = q.extract_status.clone().filter(|s| !s.trim().is_empty());

    let tenant_id_db = tenant_id.clone();
    let items = with_conn(&state, move |conn| -> Result<Vec<FileMeta>, AppError> {
        let mut sql = String::from(
            "SELECT file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted, extract_status, extract_updated_at_ms, extract_attempt, extract_error, annotations_json
             FROM files WHERE tenant_id=?1 AND deleted_at_ms IS NULL",
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
        if let Some(status) = &extract_status {
            sql.push_str(" AND extract_status=?");
            args.push(status.clone().into());
        }
        sql.push_str(" ORDER BY created_at_ms DESC LIMIT ?");
        args.push(limit.into());

        let mut stmt = conn.prepare(&sql).map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(args))
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            let annotations_raw: Option<String> = row.get(14).ok();
            let annotations = annotations_raw
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
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
                extract_status: row.get(10).ok(),
                extract_updated_at_ms: row.get(11).ok(),
                extract_attempt: row.get(12).ok(),
                extract_error: row.get(13).ok(),
                annotations,
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

#[derive(Debug, Deserialize)]
struct PendingQuery {
    tenant_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct PendingResponse {
    ok: bool,
    items: Vec<FileMeta>,
}

#[derive(Debug, Deserialize)]
struct AnnotationsRequest {
    // Keep this opaque and LLM-friendly: semantics are application-defined.
    annotations: serde_json::Value,
    source: Option<String>,
}

#[derive(Debug, Serialize)]
struct AnnotationsResponse {
    ok: bool,
    file_id: String,
    updated_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct ExtractStatusRequest {
    status: String,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ExtractStatusResponse {
    ok: bool,
    file_id: String,
    status: String,
}

async fn meta(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
) -> Result<impl IntoResponse, AppError> {
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_read(&auth)?;
    let tenant_id = auth.tenant_id;
    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }
    let tenant_id_db = tenant_id.clone();
    let out = with_conn(&state, move |conn| -> Result<Option<FileMeta>, AppError> {
        let mut stmt = conn
            .prepare(
                "SELECT file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted, extract_status, extract_updated_at_ms, extract_attempt, extract_error, annotations_json
                 FROM files WHERE tenant_id=?1 AND file_id=?2 AND deleted_at_ms IS NULL",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_db, file_id])
            .map_err(|e| AppError::Db(e.to_string()))?;
        if let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            let annotations_raw: Option<String> = row.get(14).ok();
            let annotations = annotations_raw
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
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
                extract_status: row.get(10).ok(),
                extract_updated_at_ms: row.get(11).ok(),
                extract_attempt: row.get(12).ok(),
                extract_error: row.get(13).ok(),
                annotations,
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

async fn pending_extract(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<PendingQuery>,
) -> Result<impl IntoResponse, AppError> {
    let auth = auth_from_headers(&state, &headers, q.tenant_id.as_deref())?;
    assert_can_read(&auth)?;
    let tenant_id = auth.tenant_id;
    let limit = q.limit.unwrap_or(25).clamp(1, 200) as i64;

    let tenant_id_db = tenant_id.clone();
    let items = with_conn(&state, move |conn| -> Result<Vec<FileMeta>, AppError> {
        let mut stmt = conn
            .prepare(
                "SELECT file_id, tenant_id, session_id, filename, mime, size, sha256, created_at_ms, source, encrypted, extract_status, extract_updated_at_ms, extract_attempt, extract_error, annotations_json
                 FROM files
                 WHERE tenant_id=?1 AND deleted_at_ms IS NULL AND (extract_status IS NULL OR extract_status='pending')
                 ORDER BY created_at_ms ASC
                 LIMIT ?2",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_db, limit])
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| AppError::Db(e.to_string()))? {
            let annotations_raw: Option<String> = row.get(14).ok();
            let annotations = annotations_raw
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
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
                extract_status: row.get(10).ok(),
                extract_updated_at_ms: row.get(11).ok(),
                extract_attempt: row.get(12).ok(),
                extract_error: row.get(13).ok(),
                annotations,
            });
        }
        Ok(out)
    })
    .await?;

    Ok((StatusCode::OK, Json(PendingResponse { ok: true, items })))
}

async fn upsert_annotations(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
    Json(req): Json<AnnotationsRequest>,
) -> Result<impl IntoResponse, AppError> {
    let request_id = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_write(&auth)?;

    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }
    let now = now_ms();
    let tenant_id = auth.tenant_id.clone();
    let file_id_db = file_id.clone();
    let annotations_json =
        serde_json::to_string(&req.annotations).map_err(|e| AppError::InvalidRequest(e.to_string()))?;

    let updated = with_conn(&state, move |conn| -> Result<usize, AppError> {
        let n = conn
            .execute(
                "UPDATE files SET annotations_json=?1, extract_updated_at_ms=?2 WHERE tenant_id=?3 AND file_id=?4 AND deleted_at_ms IS NULL",
                params![annotations_json, now, tenant_id, file_id_db],
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(n)
    })
    .await?;
    if updated == 0 {
        return Err(AppError::NotFound);
    }

    let source = req.source.unwrap_or_else(|| "unknown".to_string());
    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "annotations_upsert",
            tenant_id: &auth.tenant_id,
            key_id: Some(&auth.key_id),
            request_id,
            file_id: Some(&file_id),
            extra: serde_json::json!({ "source": source }),
        },
    )
    .await;

    Ok((
        StatusCode::OK,
        Json(AnnotationsResponse {
            ok: true,
            file_id,
            updated_at_ms: now,
        }),
    ))
}

async fn set_extract_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
    Json(req): Json<ExtractStatusRequest>,
) -> Result<impl IntoResponse, AppError> {
    let request_id = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_write(&auth)?;

    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }
    let status = req.status.trim().to_string();
    if status.is_empty() {
        return Err(AppError::InvalidRequest("status required".to_string()));
    }

    let now = now_ms();
    let tenant_id = auth.tenant_id.clone();
    let file_id_db = file_id.clone();
    let error = req.error.unwrap_or_default();
    let status_db = status.clone();
    let error_db = error.clone();
    let updated = with_conn(&state, move |conn| -> Result<usize, AppError> {
        let n = conn
            .execute(
                "UPDATE files
                 SET extract_status=?1,
                     extract_updated_at_ms=?2,
                     extract_attempt=COALESCE(extract_attempt, 0) + 1,
                     extract_error=?3
                 WHERE tenant_id=?4 AND file_id=?5 AND deleted_at_ms IS NULL",
                params![status_db, now, error_db, tenant_id, file_id_db],
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(n)
    })
    .await?;
    if updated == 0 {
        return Err(AppError::NotFound);
    }

    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "extract_status",
            tenant_id: &auth.tenant_id,
            key_id: Some(&auth.key_id),
            request_id,
            file_id: Some(&file_id),
            extra: serde_json::json!({ "status": status, "has_error": !error.is_empty() }),
        },
    )
    .await;

    Ok((
        StatusCode::OK,
        Json(ExtractStatusResponse {
            ok: true,
            file_id,
            status,
        }),
    ))
}

async fn download(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
) -> Result<impl IntoResponse, AppError> {
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_read(&auth)?;
    let tenant_id = auth.tenant_id;
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
                "SELECT filename, mime, encrypted, storage_path FROM files WHERE tenant_id=?1 AND file_id=?2 AND deleted_at_ms IS NULL",
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

#[derive(Debug, Serialize, Deserialize)]
struct DownloadTokenPayload {
    tenant_id: String,
    file_id: String,
    exp_ms: i64,
}

type HmacSha256 = Hmac<Sha256>;

fn sign_token(signing_key: &[u8], payload: &DownloadTokenPayload) -> Result<String, AppError> {
    let payload_json =
        serde_json::to_vec(payload).map_err(|e| AppError::InvalidRequest(e.to_string()))?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload_json);
    let mut mac = HmacSha256::new_from_slice(signing_key)
        .map_err(|_| AppError::InvalidRequest("invalid signing key".to_string()))?;
    mac.update(payload_b64.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(sig);
    Ok(format!("{payload_b64}.{sig_b64}"))
}

fn verify_token(signing_key: &[u8], token: &str) -> Result<DownloadTokenPayload, AppError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 2 {
        return Err(AppError::InvalidRequest("invalid token".to_string()));
    }
    let payload_b64 = parts[0];
    let sig_b64 = parts[1];

    let sig = URL_SAFE_NO_PAD
        .decode(sig_b64.as_bytes())
        .map_err(|_| AppError::InvalidRequest("invalid token".to_string()))?;

    let mut mac = HmacSha256::new_from_slice(signing_key)
        .map_err(|_| AppError::InvalidRequest("invalid signing key".to_string()))?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&sig)
        .map_err(|_| AppError::Unauthorized)?;

    let payload_json = URL_SAFE_NO_PAD
        .decode(payload_b64.as_bytes())
        .map_err(|_| AppError::InvalidRequest("invalid token".to_string()))?;
    let payload: DownloadTokenPayload = serde_json::from_slice(&payload_json)
        .map_err(|_| AppError::InvalidRequest("invalid token".to_string()))?;
    if payload.exp_ms <= now_ms() {
        return Err(AppError::Unauthorized);
    }
    Ok(payload)
}

#[derive(Debug, Deserialize)]
struct PublicDownloadQuery {
    token: String,
}

async fn create_link(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
    Json(req): Json<LinkRequest>,
) -> Result<impl IntoResponse, AppError> {
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_write(&auth)?;

    let signing_key = state.signing_key.as_deref().ok_or_else(|| {
        AppError::InvalidRequest("RUSTFS_SIGNING_KEY is not configured".to_string())
    })?;

    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }

    // Ensure file exists for this tenant (avoid token generation for missing / wrong tenant)
    let tenant_id_db = auth.tenant_id.clone();
    let file_id_db = file_id.clone();
    let exists = with_conn(&state, move |conn| -> Result<bool, AppError> {
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM files WHERE tenant_id=?1 AND file_id=?2 AND deleted_at_ms IS NULL")
            .map_err(|e| AppError::Db(e.to_string()))?;
        let count: i64 = stmt
            .query_row(params![tenant_id_db, file_id_db], |row| row.get(0))
            .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(count > 0)
    })
    .await?;
    if !exists {
        return Err(AppError::NotFound);
    }

    let ttl = req.ttl_seconds.unwrap_or(300).clamp(30, 3600) as i64;
    let expires_at_ms = now_ms() + ttl * 1000;
    let payload = DownloadTokenPayload {
        tenant_id: auth.tenant_id.clone(),
        file_id: file_id.clone(),
        exp_ms: expires_at_ms,
    };
    let token = sign_token(signing_key, &payload)?;
    let path = format!("/v1/public/download?token={}", token);
    let url = state.public_base_url.as_deref().map(|base| {
        let b = base.trim_end_matches('/');
        format!("{b}{path}")
    });
    let request_id = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "link_create",
            tenant_id: &auth.tenant_id,
            key_id: Some(&auth.key_id),
            request_id,
            file_id: Some(&file_id),
            extra: serde_json::json!({ "ttl_seconds": ttl }),
        },
    )
    .await;
    Ok((
        StatusCode::OK,
        Json(LinkResponse {
            ok: true,
            token,
            path,
            url,
            expires_at_ms,
        }),
    ))
}

async fn public_download(
    State(state): State<AppState>,
    Query(q): Query<PublicDownloadQuery>,
) -> Result<impl IntoResponse, AppError> {
    let signing_key = state.signing_key.as_deref().ok_or_else(|| {
        AppError::InvalidRequest("RUSTFS_SIGNING_KEY is not configured".to_string())
    })?;
    let payload = verify_token(signing_key, q.token.trim())?;

    // Reuse existing download logic by querying metadata and streaming file.
    // This endpoint bypasses API key auth but is constrained by the signed token.
    let tenant_id = payload.tenant_id;
    let file_id = payload.file_id;
    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "public_download",
            tenant_id: &tenant_id,
            key_id: None,
            request_id: None,
            file_id: Some(&file_id),
            extra: serde_json::json!({}),
        },
    )
    .await;

    #[derive(Debug)]
    struct Row {
        filename: String,
        mime: Option<String>,
        encrypted: bool,
        storage_path: String,
    }

    let tenant_id_db = tenant_id.clone();
    let file_id_db = file_id.clone();
    let row = with_conn(&state, move |conn| -> Result<Option<Row>, AppError> {
        let mut stmt = conn
            .prepare(
                "SELECT filename, mime, encrypted, storage_path FROM files WHERE tenant_id=?1 AND file_id=?2 AND deleted_at_ms IS NULL",
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        let mut rows = stmt
            .query(params![tenant_id_db, file_id_db])
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

async fn tombstone(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(path): axum::extract::Path<PathParams>,
    Json(req): Json<TombstoneRequest>,
) -> Result<impl IntoResponse, AppError> {
    let request_id = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    let auth = auth_from_headers(&state, &headers, None)?;
    assert_can_write(&auth)?;
    let file_id = path.file_id.trim().to_string();
    if file_id.is_empty() {
        return Err(AppError::InvalidRequest("file_id required".to_string()));
    }
    let tenant_id = auth.tenant_id.clone();
    let file_id_db = file_id.clone();
    let ts = now_ms();
    let updated = with_conn(&state, move |conn| -> Result<usize, AppError> {
        let n = conn
            .execute(
                "UPDATE files SET deleted_at_ms=?1 WHERE tenant_id=?2 AND file_id=?3 AND deleted_at_ms IS NULL",
                params![ts, tenant_id, file_id_db],
            )
            .map_err(|e| AppError::Db(e.to_string()))?;
        Ok(n)
    })
    .await?;
    let tombstoned = updated > 0;
    append_audit(
        &state,
        AuditEntry {
            id: uuid::Uuid::new_v4().to_string(),
            ts_ms: now_ms(),
            action: "tombstone",
            tenant_id: &auth.tenant_id,
            key_id: Some(&auth.key_id),
            request_id,
            file_id: Some(&file_id),
            extra: serde_json::json!({ "reason": req.reason, "tombstoned": tombstoned }),
        },
    )
    .await;
    Ok((
        StatusCode::OK,
        Json(TombstoneResponse {
            ok: true,
            file_id,
            tombstoned,
        }),
    ))
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
    let signing_key = std::env::var("RUSTFS_SIGNING_KEY")
        .ok()
        .map(|v| v.trim().as_bytes().to_vec())
        .filter(|v| !v.is_empty());
    let public_base_url = std::env::var("RUSTFS_PUBLIC_BASE_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let audit_log_path = std::env::var("RUSTFS_AUDIT_LOG_PATH")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from);

    let state = AppState {
        data_dir: PathBuf::from(data_dir),
        db_path: PathBuf::from(db_path),
        require_api_key,
        api_keys: Arc::new(parse_api_keys_json(&api_keys_json)),
        master_key,
        signing_key,
        public_base_url,
        audit_log_path,
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
        .route("/v1/files/pending_extract", get(pending_extract))
        .route("/v1/files/:file_id/meta", get(meta))
        .route("/v1/files/:file_id", get(download))
        .route("/v1/files/:file_id/link", post(create_link))
        .route("/v1/files/:file_id/annotations", post(upsert_annotations))
        .route("/v1/files/:file_id/extract_status", post(set_extract_status))
        .route("/v1/files/:file_id/tombstone", post(tombstone))
        .route("/v1/public/download", get(public_download))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

