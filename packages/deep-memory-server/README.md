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

## Environment

See `src/config.ts` for the full env list and defaults (Neo4j/Qdrant URLs, collection name, vector dims, etc.).

Notable knobs:

- `IMPORTANCE_THRESHOLD`: minimum importance to store a memory
- `DEDUPE_SCORE`: Qdrant similarity above this reuses the existing memory id
- `DECAY_HALF_LIFE_DAYS`: time decay half-life applied during retrieval ranking
- `IMPORTANCE_BOOST` / `FREQUENCY_BOOST`: ranking boosts to model “memory growth”
- `RELATED_TOPK`: build `RELATED_TO` graph edges by linking each new memory to its nearest neighbors
- `SENSITIVE_FILTER_ENABLED`: drop likely-secret text (tokens/passwords/keys) before storage

