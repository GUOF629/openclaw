---
summary: "Deep memory: external long-term memory service (Qdrant + Neo4j) for OpenClaw"
read_when:
  - You want cross-session memory and long-term recall beyond Markdown memory files
  - You are deploying deep-memory-server in production
title: "Deep memory"
---

# Deep memory

OpenClaw supports an optional **deep memory** backend implemented as an external service:

- **deep-memory-server**: HTTP API used by OpenClaw for retrieval + ingestion
- **Qdrant**: vector search (semantic recall)
- **Neo4j**: relationship graph (entity/topic/event links + RELATED_TO)

This is separate from the default Markdown-based memory files. See [Memory](/concepts/memory) for the built-in model.

## What it does

- **Online path** (every turn): OpenClaw calls `POST /retrieve_context` and injects a read-only memory block into the model prompt.
- **Offline path** (async): OpenClaw sends transcript batches to `POST /update_memory_index` (usually async) so deep memories can be extracted, deduped, and linked.
- **Admin operations**: `POST /forget` deletes by session or id (best-effort dual-store).

## Production deployment (Docker Compose)

The repo root `docker-compose.yml` includes two profiles:

- `deep-memory`: dev-friendly (runs from the repo, installs deps at container startup)
- `deep-memory-prod`: production-oriented (built image + persistent queue/audit volumes)

Example:

```bash
docker compose --profile deep-memory-prod up -d
```

## Observability / probes

- `GET /health`: liveness + queue stats
- `GET /readyz`: readiness (Qdrant + Neo4j)
- `GET /metrics`: Prometheus metrics (admin-only when API keys are required)

## Security checklist

- Set `REQUIRE_API_KEY=true`
- Configure `API_KEYS_JSON` and restrict:
  - `read` for `/retrieve_context`
  - `write` for `/update_memory_index`
  - `admin` for `/forget`, `/queue/*`, `/metrics`
- Run behind TLS (reverse proxy) and keep the service private to your gateway network
