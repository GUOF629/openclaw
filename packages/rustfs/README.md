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

### Metadata

`GET /v1/files/:file_id/meta`

### Download

`GET /v1/files/:file_id`

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
