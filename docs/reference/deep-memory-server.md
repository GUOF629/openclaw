---
summary: "Reference for deep-memory-server configuration, API contract, and guardrail semantics"
read_when:
  - You are configuring deep-memory-server (env vars, guardrails, queue)
  - You need the API contract (responses, error codes, retry-after semantics)
title: "Deep memory server reference"
---

# Deep memory server reference

This page is the **configuration + API contract** reference for `deep-memory-server`.

For production operations (backups, restores, upgrades), see [Deep memory ops](/reference/deep-memory-ops).

## Configuration (environment variables)

Defaults are from `packages/deep-memory-server/src/config.ts`.

### Build metadata (optional)

- `BUILD_SHA`: git SHA (string)
- `BUILD_TIME`: build timestamp ISO string

These appear in `GET /health` under `build.sha` and `build.time`.

### Auth / security

- `REQUIRE_API_KEY` (default `false`)
- `API_KEYS_JSON` (recommended): JSON array of keys with `role` (`read|write|admin`) and `namespaces` allowlist
- Legacy single-role keys:
  - `API_KEY` (single key)
  - `API_KEYS` (comma-separated keys)

### Backends

Qdrant:

- `QDRANT_URL` (default `http://qdrant:6333`)
- `QDRANT_API_KEY` (optional)
- `QDRANT_COLLECTION` (default `openclaw_memories`)
- `VECTOR_DIMS` (default `384`)

Neo4j:

- `NEO4J_URI` (default `bolt://neo4j:7687`)
- `NEO4J_USER` (default `neo4j`)
- `NEO4J_PASSWORD` (default `openclaw`)

### Queue (durable update + forget)

- `QUEUE_DIR` (default `./data/queue`)
- `QUEUE_MAX_ATTEMPTS` (default `10`)
- `QUEUE_RETRY_BASE_MS` (default `2000`)
- `QUEUE_RETRY_MAX_MS` (default `300000`)
- `QUEUE_KEEP_DONE` (default `true`)
- `QUEUE_RETENTION_DAYS` (default `7`)
- `QUEUE_MAX_TASK_BYTES` (default `2097152`)
- `UPDATE_CONCURRENCY` (default `1`): worker concurrency for update queue

### Rate limits (fixed window, per key + route)

- `RATE_LIMIT_ENABLED` (default `false`)
- `RATE_LIMIT_WINDOW_MS` (default `60000`)
- `RATE_LIMIT_RETRIEVE_PER_WINDOW` (default `0`)
- `RATE_LIMIT_UPDATE_PER_WINDOW` (default `0`)
- `RATE_LIMIT_FORGET_PER_WINDOW` (default `0`)
- `RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW` (default `0`)

### Update ingestion guardrails (server-side)

Hard control:

- `UPDATE_DISABLED_NAMESPACES` (optional): comma/space separated namespace list that is **write-disabled**

Shaping:

- `UPDATE_MIN_INTERVAL_MS` (default `0`): minimum spacing between update requests per `namespace::session_id`
- `UPDATE_SAMPLE_RATE` (default `1`): deterministic sampling \(0..1\) for skipping some updates

Overload handling:

- `UPDATE_BACKLOG_REJECT_PENDING` (default `0`): if pending >= this, reject async updates with 503
- `UPDATE_BACKLOG_RETRY_AFTER_SECONDS` (default `30`): `retry-after` for overload responses
- `UPDATE_BACKLOG_READ_ONLY_PENDING` (default `0`): if pending >= this, enter read-only mode for async updates
- `UPDATE_BACKLOG_DELAY_PENDING` (default `0`): if pending >= this, accept but delay queue tasks
- `UPDATE_BACKLOG_DELAY_SECONDS` (default `0`): delay duration when delaying

Namespace concurrency:

- `NAMESPACE_UPDATE_CONCURRENCY` (default `0`): max concurrent update tasks per namespace (queue worker)
- `NAMESPACE_RETRIEVE_CONCURRENCY` (default `0`): max concurrent retrieve requests per namespace (API)

### Retrieve degradation (optional)

- `RETRIEVE_DEGRADE_RELATED_PENDING` (default `0`): if update queue pending >= this, retrieval degrades by skipping entity/topic hint extraction (reduces Neo4j relation work)

### Sensitive filtering (write path)

- `SENSITIVE_FILTER_ENABLED` (default `true`)
- `SENSITIVE_RULESET_VERSION` (default `builtin-v1`): audit-friendly version string
- `SENSITIVE_DENY_REGEX_JSON` (optional): JSON array of regex strings (case-insensitive) to deny
- `SENSITIVE_ALLOW_REGEX_JSON` (optional): JSON array of regex strings (case-insensitive) to allow (overrides deny)

## API contract (responses + error semantics)

### Common error semantics

- **429 rate limiting**:
  - body: `{ "error": "rate_limited" }`
  - header: `retry-after`
- **503 overload / not ready**:
  - body: `{ "error": "<code>", ...details }`
  - header: `retry-after` when a retry is appropriate

### `POST /retrieve_context`

Success (200): `RetrieveContextResponse`

Overload (503):

- `{ "error": "namespace_overloaded", "namespace": "<ns>", "active": <n>, "limit": <n> }`

### `POST /update_memory_index`

Success (200):

- processed: `{ "status": "processed", "memories_added": <n>, "memories_filtered": <n> }`
- queued: `{ "status": "queued", "memories_added": 0, "memories_filtered": 0 }`
- skipped: `{ "status": "skipped", "error": "<code>", "memories_added": 0, "memories_filtered": 0 }`

Overload (503):

- `{ "error": "queue_overloaded", "pendingApprox": <n>, "retryAfterSeconds": <n> }`
- `{ "error": "degraded_read_only", "pendingApprox": <n>, "retryAfterSeconds": <n> }`

### `POST /forget`

Success (200):

- dry run: `{ "status": "dry_run", "namespace": "<ns>", "request_id": "<id>", ... }`
- queued: `{ "status": "queued", "namespace": "<ns>", "request_id": "<id>", "task_id": "<id>", "key": "<ns::...>", ... }`
- processed: `{ "status": "processed", "namespace": "<ns>", "request_id": "<id>", "deleted": <n>, "results": { ... } }`

### Queue admin endpoints

Update queue:

- `GET /queue/stats`
- `GET /queue/failed`
- `GET /queue/failed/export`
- `POST /queue/failed/retry`

Forget queue:

- `GET /queue/forget/stats`
- `GET /queue/forget/failed`
- `GET /queue/forget/failed/export`
- `POST /queue/forget/failed/retry`
