# RustFS Worker

一个最小可用的后台 worker：通过 RustFS 的原子 `claim_extract`（租约领取）接口拉取待处理文件，做基础文本抽取后：

- 回写 RustFS `annotations`
- 更新 RustFS `extract_status`（`indexed` / `skipped` / `failed`）
- 调用 deep-memory-server 的 `/update_memory_index`，将文件内容（分 chunk）纳入语义记忆系统

## 环境变量

- `RUSTFS_BASE_URL`（必填）
- `RUSTFS_API_KEY`（可选；RustFS 若要求鉴权则需要）
- `RUSTFS_TENANT_ID`（可选；仅用于调试/覆盖 tenant，正常依赖 api key 绑定的 tenant）
- `RUSTFS_POLL_INTERVAL_MS`（默认 `5000`）
- `RUSTFS_LEASE_MS`（默认 `300000`）：处理中的任务超过该时间会被重新投放为可领取
- `RUSTFS_PENDING_LIMIT`（默认 `25`）
- `RUSTFS_MAX_DOWNLOAD_BYTES`（默认 `2097152`）

- `DEEP_MEMORY_BASE_URL`（必填）
- `DEEP_MEMORY_API_KEY`（可选；deep-memory-server 若要求鉴权则需要）
- `DEEP_MEMORY_NAMESPACE`（可选）
- `DEEP_MEMORY_ASYNC`（默认 `true`）
- `DEEP_MEMORY_TIMEOUT_MS`（默认 `30000`）
- `DEEP_MEMORY_CHUNK_MAX_CHARS`（默认 `3500`）
- `DEEP_MEMORY_CHUNK_OVERLAP_CHARS`（默认 `200`）
- `DEEP_MEMORY_BACKOFF_MS`（默认 `10000`）：deep-memory 过载时退避等待

## 运行

本地开发：

```bash
pnpm -w --filter @openclaw/rustfs-worker... dev
```

生产（build 后运行）：

```bash
pnpm -w --filter @openclaw/rustfs-worker... build
pnpm -w --filter @openclaw/rustfs-worker... start
```
