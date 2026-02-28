---
summary: "How to start OpenClaw, deep-memory, RustFS, and rustfs-worker (docker-compose profiles)"
read_when:
  - You are starting OpenClaw locally or on a server
  - You want to enable deep-memory and RustFS file ingestion/search
title: "Startup flow (Docker profiles)"
---

# Startup flow (Docker profiles)

This page describes the **current startup flow** for the OpenClaw stack in this repository, based on `docker-compose.yml`.

OpenClaw uses **Docker Compose profiles** so you can start components independently (recommended) or together (when needed).

## Components

- **Gateway**: `openclaw-gateway` (+ `openclaw-cli` for running commands)
- **Deep memory**:
  - dev: `deep-memory-server` + `qdrant` + `neo4j` (profile `deep-memory`)
  - prod: `deep-memory-server-prod` + `qdrant` + `neo4j` (profile `deep-memory-prod`)
- **RustFS**: `rustfs` (profile `rustfs`)
- **RustFS worker**: `rustfs-worker` (profile `rustfs-worker`)

## Compose profiles

From repo root:

- Gateway only:

```bash
docker compose up -d openclaw-gateway
```

- Deep memory (dev):

```bash
docker compose --profile deep-memory up -d
```

- Deep memory (prod):

```bash
docker compose --profile deep-memory-prod up -d
```

- RustFS:

```bash
docker compose --profile rustfs up -d
```

- RustFS + deep-memory-prod + worker (recommended for file semantic ingestion):

```bash
docker compose --profile rustfs --profile deep-memory-prod --profile rustfs-worker up -d
```

## Recommended startup order

1. Start deep-memory (prod) first (brings up Qdrant + Neo4j + deep-memory-server-prod).
2. Start RustFS.
3. Start rustfs-worker (depends on RustFS + deep-memory health checks).
4. Start the gateway.

This order ensures the worker can immediately index files and the gateway can use the stack.

## Required environment variables (high-level)

Gateway + CLI:

- `OPENCLAW_CONFIG_DIR` and `OPENCLAW_WORKSPACE_DIR` (bind mounts)
- `OPENCLAW_GATEWAY_TOKEN` (control UI token)

deep-memory-prod (recommended):

- `OPENCLAW_DEEPMEM_API_KEYS_JSON` (write/read/admin keys + namespaces allowlist)
- `OPENCLAW_NEO4J_PASSWORD`

RustFS:

- `OPENCLAW_RUSTFS_API_KEYS_JSON` (API keys bound to tenant)
- `OPENCLAW_RUSTFS_SIGNING_KEY` (required for share links)
- Optional: `OPENCLAW_RUSTFS_MASTER_KEY` (encryption at rest)

rustfs-worker:

- `OPENCLAW_RUSTFS_WORKER_API_KEY` (RustFS key with permission to claim/download/annotate)
- `OPENCLAW_DEEPMEM_WORKER_API_KEY` (deep-memory write key)
- Optional: `OPENCLAW_DEEPMEM_NAMESPACE`

Note: use a `.env` file or your host environment to provide these values; never commit secrets.

## Verification checklist

- deep-memory-prod:
  - `GET http://127.0.0.1:${OPENCLAW_DEEPMEM_PORT:-8088}/readyz`
- RustFS:
  - `GET http://127.0.0.1:${OPENCLAW_RUSTFS_PORT:-8099}/readyz`
- Gateway:
  - Open `http://127.0.0.1:${OPENCLAW_GATEWAY_PORT:-18789}/` and paste the token in Settings.

## Troubleshooting (common)

- Compose profiles didn’t start anything:
  - Confirm you passed the right `--profile ...` flags.
- Worker starts but does nothing:
  - Ensure files were ingested into RustFS and `extract_status` is `pending`.
  - Check `rustfs-worker` logs for overload/backoff or auth failures.
- deep-memory overload responses:
  - `503 queue_overloaded` or `503 degraded_read_only` indicates backlog. Increase capacity or reduce ingestion rate.
