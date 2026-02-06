# RustFS Worker

一个最小可用的后台 worker：通过 RustFS 的原子 `claim_extract`（租约领取）接口拉取待处理文件，做抽取/切片后：

- 回写 RustFS `annotations`
- 更新 RustFS `extract_status`（`indexed` / `skipped` / `failed`）
- 调用 deep-memory-server 的 `/update_memory_index`，将文件内容（分 segment）纳入语义记忆系统（可选返回 `memory_ids` 用于追溯）

当前支持的抽取类型（MVP）：

- 纯文本：`text/*`、`md/json/yaml/xml/toml/jsonl/txt`
- HTML：`.html/.htm`（title + heading → section segments）
- 代码：`.ts/.js/.py/.go/.rs/.java/.kt/.swift/.sh/.css`（按函数/类/结构边界做粗切分）
- PDF：`.pdf`（按 page → segments）
- DOCX：`.docx`（按段落 → segments）
- PPTX：`.pptx`（按 slide → segments）

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
- `DEEP_MEMORY_RETURN_MEMORY_IDS`（默认 `false`）：请求 deep-memory 返回本次写入/更新的 `memory_ids`（**仅在 `DEEP_MEMORY_ASYNC=false` 时生效**）
- `DEEP_MEMORY_MAX_RETURN_MEMORY_IDS`（默认 `200`，最大 `1000`）：返回 `memory_ids` 的上限
- `DEEP_MEMORY_INSPECT_ENABLED`（默认 `false`）：indexed 后调用 deep-memory `POST /session/inspect`，回写 `annotations.semantics`
- `DEEP_MEMORY_INSPECT_LIMIT`（默认 `100`）：inspect 拉取 memories 上限
- `DEEP_MEMORY_INSPECT_INCLUDE_CONTENT`（默认 `false`）：inspect 时是否包含代表性内容（用于生成 summary）

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
