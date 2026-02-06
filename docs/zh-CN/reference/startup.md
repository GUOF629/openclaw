---
summary: "OpenClaw + deep-memory + RustFS + rustfs-worker 的 Docker 启动流程（profiles 组合）"
read_when:
  - 你在本机或服务器上启动 OpenClaw
  - 你要启用 deep-memory 与 RustFS 文件归档/语义检索
title: "启动流程（Docker profiles）"
---

# 启动流程（Docker profiles）

本文梳理本仓库当前的启动方式：基于 `docker-compose.yml`，通过 **Docker Compose profiles** 将网关、deep-memory、RustFS、worker 进行**可选分组启动**。

## 组件一览

- **Gateway（网关）**：`openclaw-gateway`（配套 `openclaw-cli` 用于执行命令）
- **deep-memory（语义记忆）**
  - 开发模式：`deep-memory-server` + `qdrant` + `neo4j`（profile：`deep-memory`）
  - 生产模式：`deep-memory-server-prod` + `qdrant` + `neo4j`（profile：`deep-memory-prod`）
- **RustFS（文件归档）**：`rustfs`（profile：`rustfs`）
- **rustfs-worker（抽取/入库后台任务）**：`rustfs-worker`（profile：`rustfs-worker`）

## 关键点：默认是分开启动

除 `openclaw-gateway/openclaw-cli` 外，其余组件都挂在 profile 上，意味着：

- 你不显式指定 `--profile ...` 时，它们不会启动
- 你可以按需组合多个 profile，达到“一起启动”的效果

## 常用启动命令（从仓库根目录执行）

- 仅启动网关：

```bash
docker compose up -d openclaw-gateway
```

- 启动 deep-memory（开发模式）：

```bash
docker compose --profile deep-memory up -d
```

- 启动 deep-memory（生产模式，推荐）：

```bash
docker compose --profile deep-memory-prod up -d
```

- 启动 RustFS：

```bash
docker compose --profile rustfs up -d
```

- 启动 RustFS + deep-memory-prod + rustfs-worker（推荐的“文件语义入库”组合）：

```bash
docker compose --profile rustfs --profile deep-memory-prod --profile rustfs-worker up -d
```

## 推荐启动顺序（稳态）

1. 先启动 `deep-memory-prod`（会带起 Qdrant + Neo4j + deep-memory-server-prod）
2. 再启动 `rustfs`
3. 再启动 `rustfs-worker`（依赖 rustfs 与 deep-memory 的 healthcheck）
4. 最后启动 `openclaw-gateway`

这样 worker 可以立刻 claim 待抽取文件并入库，网关侧也能稳定使用相关能力。

## 环境变量（高层必需项）

网关/CLI：

- `OPENCLAW_CONFIG_DIR`、`OPENCLAW_WORKSPACE_DIR`（绑定到容器内 `~/.openclaw` 与 workspace）
- `OPENCLAW_GATEWAY_TOKEN`（控制台 UI token）

deep-memory-prod（推荐）：

- `OPENCLAW_DEEPMEM_API_KEYS_JSON`（建议用 JSON 定义 read/write/admin key，并配置 namespaces allowlist）
- `OPENCLAW_NEO4J_PASSWORD`

RustFS：

- `OPENCLAW_RUSTFS_API_KEYS_JSON`（API key 绑定 tenant）
- `OPENCLAW_RUSTFS_SIGNING_KEY`（**必需**：用于文件分享链接签名）
- 可选：`OPENCLAW_RUSTFS_MASTER_KEY`（开启新文件静态加密）

rustfs-worker：

- `OPENCLAW_RUSTFS_WORKER_API_KEY`（RustFS 权限：claim/download/annotations/status）
- `OPENCLAW_DEEPMEM_WORKER_API_KEY`（deep-memory 写权限）
- 可选：`OPENCLAW_DEEPMEM_NAMESPACE`

建议通过 `.env` 或宿主机环境变量注入；不要把任何真实 key 提交到 git。

## 启动后怎么确认“真的跑起来了”

- deep-memory-prod：
  - `GET http://127.0.0.1:${OPENCLAW_DEEPMEM_PORT:-8088}/readyz`
- RustFS：
  - `GET http://127.0.0.1:${OPENCLAW_RUSTFS_PORT:-8099}/readyz`
- Gateway：
  - 打开 `http://127.0.0.1:${OPENCLAW_GATEWAY_PORT:-18789}/`，在 Settings 粘贴 token

## 常见问题

- profile 没带导致“怎么没启动”：
  - 记得加 `--profile deep-memory-prod` / `--profile rustfs` / `--profile rustfs-worker`
- worker 一直空转：
  - 先确认 RustFS 里有文件、且 `extract_status` 为 `pending`/`processing(过期)` 可 claim
  - 查看 worker 日志是否 auth 失败、或 deep-memory 503 过载退避
