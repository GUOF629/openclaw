# Deep Memory Server (OpenClaw)

This is an **optional** companion service that implements “deep memory” as an external long-term memory backend.

It exposes two HTTP endpoints used by OpenClaw:

- `POST /retrieve_context` — retrieve cross-session memory and return a formatted context block.
- `POST /update_memory_index` — ingest session transcript messages and update the long-term memory index.

## Run (Docker Compose)

The repo root `docker-compose.yml` includes an optional profile:

```bash
docker compose --profile deep-memory up
```

For production-oriented usage (built image + persistent queue/audit volumes), use:

```bash
docker compose --profile deep-memory-prod up -d
```

Then point OpenClaw at the service:

```json5
agents: {
  defaults: {
    deepMemory: {
      enabled: true,
      baseUrl: "http://127.0.0.1:8088"
    }
  }
}
```

## Endpoints

- `GET /health`: liveness + queue/config guardrail summary
- `GET /health/details`: detailed health (includes dependency address summary; admin-only when API keys are required)
- `GET /readyz`: readiness (Qdrant + Neo4j + queue stats; returns 503 on dependency failure)
- `GET /metrics`: Prometheus metrics (admin-only when API keys are required)

## Environment

See `src/config.ts` for the full env list and defaults (Neo4j/Qdrant URLs, collection name, vector dims, etc.).

Notable knobs:

- `IMPORTANCE_THRESHOLD`: minimum importance to store a memory
- `DEDUPE_SCORE`: Qdrant similarity above this reuses the existing memory id
- `DECAY_HALF_LIFE_DAYS`: time decay half-life applied during retrieval ranking
- `IMPORTANCE_BOOST` / `FREQUENCY_BOOST`: ranking boosts to model “memory growth”
- `RELATED_TOPK`: build `RELATED_TO` graph edges by linking each new memory to its nearest neighbors
- `SENSITIVE_FILTER_ENABLED`: drop likely-secret text (tokens/passwords/keys) before storage

## Production notes

- Strongly recommended: set `REQUIRE_API_KEY=true` and configure `API_KEYS_JSON` with per-role keys (`read`, `write`, `admin`).
- Keep `QUEUE_DIR` on persistent storage (container volume) so async ingestion survives restarts.
- Use `/metrics` + `/readyz` for monitoring and alerting (queue backlog, error rates, dependency availability).
