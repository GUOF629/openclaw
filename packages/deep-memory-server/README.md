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

