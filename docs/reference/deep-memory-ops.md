---
summary: "Production operations for deep-memory-server: backups, restores, upgrades, and security"
read_when:
  - You are running deep-memory-server in production
  - You need backup/restore procedures or an upgrade checklist
title: "Deep memory ops"
---

# Deep memory ops

This page is a production-focused runbook for OpenClaw's deep memory stack:

- `deep-memory-server` (stateless API + durable queue directory)
- Qdrant (vector DB)
- Neo4j (graph DB)

It assumes you are using the repo root `docker-compose.yml` profiles:

- `deep-memory-prod`: built image + persistent `deepmem_data` volume
- `deep-memory`: dev-friendly profile (not recommended for production)

## Quick health checks

From the host running the services:

- `GET http://127.0.0.1:8088/health` (liveness + queue stats)
- `GET http://127.0.0.1:8088/health/details` (detailed health + schema checks; admin-only when API keys are required)
- `GET http://127.0.0.1:8088/readyz` (readiness; returns 503 if Qdrant/Neo4j are unavailable)

If you scrape metrics (recommended):

- `GET http://127.0.0.1:8088/metrics` (admin-only when API keys are required)

## Required security settings (production)

Deep memory is a powerful write/delete surface. For production:

- Set `REQUIRE_API_KEY=true`
- Configure `API_KEYS_JSON` with separate roles:
  - `read`: `/retrieve_context`
  - `write`: `/update_memory_index`
  - `admin`: `/forget`, `/queue/*`, `/metrics`

Example `API_KEYS_JSON` (use your own random secrets):

```json
[
  { "key": "<read-key>", "role": "read", "namespaces": ["teamA"] },
  { "key": "<write-key>", "role": "write", "namespaces": ["teamA"] },
  { "key": "<admin-key>", "role": "admin", "namespaces": ["teamA"] }
]
```

Notes:

- Never reuse the same key for multiple roles unless you must.
- Prefer binding keys to a namespace list instead of `"*"`.

## Backup strategy (recommended)

You need to back up three persistent stores:

- **Neo4j** data volume (`neo4j_data`)
- **Qdrant** storage volume (`qdrant_data`)
- **deep-memory-server data** volume (`deepmem_data`) — includes:
  - durable queue directory (`/data/queue`)
  - audit log (`/data/audit/audit.jsonl`)

### Option A: volume tar backups (simple, consistent)

This is the simplest approach for Docker Compose. It captures raw volumes.

1. Stop the deep memory stack (to get a consistent snapshot):

```bash
docker compose --profile deep-memory-prod stop deep-memory-server-prod qdrant neo4j
```

2. Create a backup directory:

```bash
mkdir -p backups/deep-memory
```

3. Backup volumes to tarballs:

```bash
docker run --rm -v neo4j_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'cd /data && tar -czf /backup/neo4j_data.tgz .'

docker run --rm -v qdrant_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'cd /data && tar -czf /backup/qdrant_data.tgz .'

docker run --rm -v deepmem_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'cd /data && tar -czf /backup/deepmem_data.tgz .'
```

4. Start the stack:

```bash
docker compose --profile deep-memory-prod up -d
```

5. Verify readiness:

```bash
curl -fsS http://127.0.0.1:8088/readyz
```

### Option B: DB-native snapshots (more complex, best for larger deployments)

If you run at scale, consider:

- Neo4j: `neo4j-admin database dump` / `load` with an explicit backup directory
- Qdrant: collection snapshots via Qdrant's snapshot API

This is more operationally involved but can reduce downtime and provide better controls.

## Restore procedure (Option A tar backups)

1. Stop the deep memory stack:

```bash
docker compose --profile deep-memory-prod down
```

2. Restore each volume tarball:

```bash
docker run --rm -v neo4j_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'rm -rf /data/* && cd /data && tar -xzf /backup/neo4j_data.tgz'

docker run --rm -v qdrant_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'rm -rf /data/* && cd /data && tar -xzf /backup/qdrant_data.tgz'

docker run --rm -v deepmem_data:/data -v "$PWD/backups/deep-memory:/backup" alpine \
  sh -lc 'rm -rf /data/* && cd /data && tar -xzf /backup/deepmem_data.tgz'
```

3. Start the stack:

```bash
docker compose --profile deep-memory-prod up -d
```

4. Verify:

- `GET /readyz` returns 200
- a small sample `/retrieve_context` works for a known namespace
- queue stats look sane (`/health`)

## Upgrade checklist (deep-memory-server)

When updating deep-memory-server versions:

1. Ensure you have a fresh backup (Neo4j/Qdrant/deepmem_data)
2. Confirm API keys still exist and are valid (`API_KEYS_JSON`)
3. `docker compose --profile deep-memory-prod build deep-memory-server-prod`
4. Roll the container:

```bash
docker compose --profile deep-memory-prod up -d deep-memory-server-prod
```

5. Verify:

- `GET /readyz` (200)
- `GET /health/details` shows schema checks are ok (and no migration warnings)
- `GET /metrics` (authorized admin key)
- normal OpenClaw operation (retrieval + update)

### Schema migrations (Neo4j/Qdrant)

deep-memory-server performs **startup schema checks** for Neo4j constraints/indexes and Qdrant collection schema.

Key env vars:

- `MIGRATIONS_MODE=apply|validate|off` (default `apply`)
  - `apply`: create missing safe objects (constraints/indexes/collection) but never drops data
  - `validate`: check and report missing/mismatched schema, but do not modify the DB
  - `off`: skip schema checks (not recommended)
- `MIGRATIONS_STRICT=true|false` (default `false`)
  - If `true`, the server fails startup when schema checks fail (recommended for strict production)

**Qdrant dims changes:** if you change `VECTOR_DIMS`, the existing collection cannot be resized in-place.
Recommended migration is to create a new collection (new name), run a reindex job, then switch `QDRANT_COLLECTION`.

## Key rotation (API_KEYS_JSON)

Safe rotation sequence:

1. Add new keys (keep old keys temporarily)
2. Deploy deep-memory-server with updated `API_KEYS_JSON`
3. Update OpenClaw config to use new keys
4. Remove old keys and redeploy
