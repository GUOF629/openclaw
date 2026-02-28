# RustFS (OpenClaw)

`rustfs` is a small, self-hosted file archive service intended to run alongside the OpenClaw deep-memory stack.

## Goals (MVP)

- Ingest session files/attachments into a durable volume
- Provide file listing/search by tenant + basic metadata filters
- Support large files via streaming multipart uploads
- Optional encryption at rest using an age passphrase (`RUSTFS_MASTER_KEY`)

## HTTP API

All APIs are namespaced under `/v1`.

### Upload

`POST /v1/files` (multipart form-data)

Fields:

- `file` (required): file bytes
- `tenant_id` (optional): tenant hint when auth is disabled
- `session_id` (optional)
- `source` (optional)

Response:

```json
{ "ok": true, "file_id": "<sha256>", "sha256": "<sha256>", "size": 1234, "encrypted": false }
```

### Search/list

`GET /v1/files?tenant_id=...&session_id=...&q=...&mime=...&limit=50`

Response:

```json
{ "ok": true, "items": [{ "file_id": "...", "tenant_id": "...", "filename": "...", "size": 123 }] }
```

### Extract jobs (worker)

RustFS stores a per-file extraction/indexing status (`extract_status`) and optional `annotations` used by downstream semantic retrieval.
Workers are expected to **merge** annotations (preserve `openclaw_ingest` hints) and may write a structured `schema_version=1` envelope for extraction + deep-memory traceability.

#### Claim (recommended; atomic leasing)

`POST /v1/files/claim_extract`

Body:

```json
{ "tenant_id": "default", "limit": 25, "lease_ms": 300000 }
```

Behavior:

- Atomically selects files that are pending (`extract_status` missing or `pending`) or stuck in `processing` past the lease window
- Marks them as `processing` (updates `extract_updated_at_ms`, increments `extract_attempt`)
- Returns the claimed file metadata (including `annotations` fields)

This avoids duplicate processing when multiple workers are running.

#### Pending list (legacy; non-atomic)

`GET /v1/files/pending_extract?tenant_id=...&limit=25&lease_ms=300000`

Note: this endpoint only lists candidates; it does not claim them atomically. Prefer `claim_extract`.

#### Update annotations

`POST /v1/files/:file_id/annotations`

Body:

```json
{ "annotations": { "any": "json" }, "source": "rustfs-worker" }
```

#### Update extract status

`POST /v1/files/:file_id/extract_status`

Body:

```json
{ "status": "indexed", "error": "" }
```

### Metadata

`GET /v1/files/:file_id/meta`

### Tombstone (revoke)

`POST /v1/files/:file_id/tombstone`

Body:

```json
{ "reason": "user_revoked" }
```

### Download

`GET /v1/files/:file_id`

### List tombstoned files (worker)

`GET /v1/files/tombstoned?tenant_id=...&since_ms=0&limit=100`

### Public share link (signed, short-lived)

`POST /v1/files/:file_id/link`

Body:

```json
{ "ttl_seconds": 300 }
```

Response includes:

- `path`: relative public download path
- `url`: absolute URL when `RUSTFS_PUBLIC_BASE_URL` is set

Public download:

`GET /v1/public/download?token=...`

## Configuration

- `RUSTFS_PORT` (default `8099`)
- `RUSTFS_DATA_DIR` (default `/data`)
- `RUSTFS_DB_PATH` (default `/data/meta.db`)
- `RUSTFS_REQUIRE_API_KEY` (default `true`)
- `RUSTFS_API_KEYS_JSON`: JSON array of `{ "key": "...", "tenant_id": "...", "role": "..." }`
- `RUSTFS_MASTER_KEY` (optional): enables encryption at rest for newly ingested files
- `RUSTFS_SIGNING_KEY` (required for share links): HMAC signing secret
- `RUSTFS_PUBLIC_BASE_URL` (optional): used to return absolute share URLs
- `RUSTFS_AUDIT_LOG_PATH` (optional): JSONL audit log path
