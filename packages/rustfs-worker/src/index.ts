import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pino from "pino";
import { z } from "zod";

type RustFsFileMeta = {
  file_id: string;
  tenant_id: string;
  session_id?: string | null;
  filename: string;
  mime?: string | null;
  size: number;
  sha256: string;
  created_at_ms: number;
  source?: string | null;
  encrypted: boolean;
  extract_status?: string | null;
  extract_updated_at_ms?: number | null;
  extract_attempt?: number | null;
  extract_error?: string | null;
  // annotations is opaque json (optional)
  annotations?: unknown;
};

const EnvSchema = z.object({
  RUSTFS_BASE_URL: z.string().min(1),
  RUSTFS_API_KEY: z.string().optional(),
  RUSTFS_TENANT_ID: z.string().optional(),
  RUSTFS_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(5_000),
  RUSTFS_LEASE_MS: z.coerce.number().int().positive().default(300_000),
  RUSTFS_PENDING_LIMIT: z.coerce.number().int().positive().max(200).default(25),
  RUSTFS_MAX_DOWNLOAD_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(2 * 1024 * 1024),
  RUSTFS_TOMBSTONE_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
  RUSTFS_TOMBSTONE_LIMIT: z.coerce.number().int().positive().max(200).default(100),
  RUSTFS_TOMBSTONE_SINCE_MS: z.coerce.number().int().nonnegative().default(0),
  WORKER_STATE_PATH: z.string().optional(),

  DEEP_MEMORY_BASE_URL: z.string().min(1),
  DEEP_MEMORY_API_KEY: z.string().optional(),
  DEEP_MEMORY_NAMESPACE: z.string().optional(),
  DEEP_MEMORY_ASYNC: z.coerce.boolean().default(true),
  DEEP_MEMORY_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),

  // Chunking: convert extracted text into multiple messages for better semantic indexing.
  DEEP_MEMORY_CHUNK_MAX_CHARS: z.coerce.number().int().positive().default(3500),
  DEEP_MEMORY_CHUNK_OVERLAP_CHARS: z.coerce.number().int().nonnegative().default(200),

  // Backoff when deep-memory is overloaded.
  DEEP_MEMORY_BACKOFF_MS: z.coerce.number().int().positive().default(10_000),
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function isSupportedTextLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m.startsWith("text/")) {
    return true;
  }
  if (
    m === "application/json" ||
    m === "application/xml" ||
    m === "application/yaml" ||
    m === "application/x-yaml" ||
    m === "application/toml" ||
    m === "application/x-ndjson"
  ) {
    return true;
  }
  const lower = filename.toLowerCase();
  return (
    lower.endsWith(".txt") ||
    lower.endsWith(".md") ||
    lower.endsWith(".markdown") ||
    lower.endsWith(".json") ||
    lower.endsWith(".jsonl") ||
    lower.endsWith(".xml") ||
    lower.endsWith(".yaml") ||
    lower.endsWith(".yml") ||
    lower.endsWith(".toml")
  );
}

async function fetchJson<T>(params: {
  url: string;
  method: "GET" | "POST";
  apiKey?: string;
  body?: unknown;
  timeoutMs: number;
}): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), params.timeoutMs);
  try {
    const res = await fetch(params.url, {
      method: params.method,
      headers: {
        ...(params.apiKey ? { "x-api-key": params.apiKey } : {}),
        ...(params.body ? { "content-type": "application/json" } : {}),
      },
      body: params.body ? JSON.stringify(params.body) : undefined,
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
    }
    return (await res.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

function isOverloadLikeError(message: string): boolean {
  const m = message.toLowerCase();
  return (
    m.includes("http 503") ||
    m.includes("queue_overloaded") ||
    m.includes("degraded_read_only") ||
    m.includes("namespace_overloaded") ||
    m.includes("rate limit") ||
    m.includes("http 429")
  );
}

async function fetchBytes(params: {
  url: string;
  apiKey?: string;
  timeoutMs: number;
  maxBytes: number;
}): Promise<Uint8Array> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), params.timeoutMs);
  try {
    const headers: Record<string, string> = {};
    if (params.apiKey) {
      headers["x-api-key"] = params.apiKey;
    }
    const res = await fetch(params.url, {
      method: "GET",
      headers,
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
    }
    const reader = res.body?.getReader();
    if (!reader) {
      const buf = new Uint8Array(await res.arrayBuffer());
      return buf.length > params.maxBytes ? buf.slice(0, params.maxBytes) : buf;
    }
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      if (!value) {
        continue;
      }
      total += value.length;
      if (total > params.maxBytes) {
        chunks.push(value.slice(0, Math.max(0, params.maxBytes - (total - value.length))));
        break;
      }
      chunks.push(value);
    }
    const out = new Uint8Array(chunks.reduce((sum, c) => sum + c.length, 0));
    let offset = 0;
    for (const c of chunks) {
      out.set(c, offset);
      offset += c.length;
    }
    return out;
  } finally {
    clearTimeout(timer);
  }
}

function splitIntoChunks(params: {
  text: string;
  maxChars: number;
  overlapChars: number;
}): Array<{ text: string; truncated: boolean }> {
  const maxChars = Math.max(200, params.maxChars);
  const overlap = Math.max(0, Math.min(params.overlapChars, Math.max(0, maxChars - 50)));
  const t = params.text;
  if (!t.trim()) {
    return [];
  }
  const chunks: Array<{ text: string; truncated: boolean }> = [];
  let i = 0;
  while (i < t.length) {
    const end = Math.min(t.length, i + maxChars);
    const slice = t.slice(i, end);
    chunks.push({ text: slice, truncated: end < t.length });
    if (end >= t.length) {
      break;
    }
    i = Math.max(0, end - overlap);
  }
  return chunks;
}

function buildDeepMemoryMessages(params: {
  file: RustFsFileMeta;
  truncated: boolean;
  chunks: Array<{ text: string; truncated: boolean }>;
}): unknown[] {
  const header = [
    "FILE_CONTEXT",
    `file_id: ${params.file.file_id}`,
    `tenant_id: ${params.file.tenant_id}`,
    `session_id: ${params.file.session_id ?? ""}`,
    `filename: ${params.file.filename}`,
    `mime: ${params.file.mime ?? ""}`,
    `sha256: ${params.file.sha256}`,
    `size: ${params.file.size}`,
    `created_at_ms: ${params.file.created_at_ms}`,
    `truncated: ${params.truncated}`,
  ].join("\n");
  const total = params.chunks.length;
  return params.chunks.map((c, idx) => {
    const content = `${header}\nchunk: ${idx + 1}/${total}\nchunk_truncated: ${c.truncated}\n\nCONTENT_EXCERPT\n${c.text}`;
    return { role: "user", content };
  });
}

const WorkerStateSchema = z.object({
  tombstoneSinceMs: z.coerce.number().int().nonnegative().default(0),
});

type WorkerState = z.infer<typeof WorkerStateSchema>;

async function loadWorkerState(
  path: string | undefined,
  fallback: WorkerState,
): Promise<WorkerState> {
  const p = path?.trim();
  if (!p) {
    return fallback;
  }
  try {
    const raw = await readFile(p, "utf-8");
    return WorkerStateSchema.parse(JSON.parse(raw));
  } catch {
    return fallback;
  }
}

async function saveWorkerState(path: string | undefined, state: WorkerState): Promise<void> {
  const p = path?.trim();
  if (!p) {
    return;
  }
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, JSON.stringify(state, null, 2), { encoding: "utf-8" });
}

async function main(): Promise<void> {
  const env = EnvSchema.parse(process.env);
  const log = pino({ name: "rustfs-worker" });

  const rustfsBaseUrl = normalizeBaseUrl(env.RUSTFS_BASE_URL);
  const deepBaseUrl = normalizeBaseUrl(env.DEEP_MEMORY_BASE_URL);

  const loadedState = await loadWorkerState(env.WORKER_STATE_PATH, {
    tombstoneSinceMs: env.RUSTFS_TOMBSTONE_SINCE_MS,
  });
  let tombstoneSinceMs = Math.max(env.RUSTFS_TOMBSTONE_SINCE_MS, loadedState.tombstoneSinceMs);
  let lastTombstonePollAt = 0;

  log.info(
    {
      rustfsBaseUrl,
      deepBaseUrl,
      tenantHint: env.RUSTFS_TENANT_ID ?? undefined,
      pollIntervalMs: env.RUSTFS_POLL_INTERVAL_MS,
      leaseMs: env.RUSTFS_LEASE_MS,
      pendingLimit: env.RUSTFS_PENDING_LIMIT,
      maxDownloadBytes: env.RUSTFS_MAX_DOWNLOAD_BYTES,
      tombstonePollIntervalMs: env.RUSTFS_TOMBSTONE_POLL_INTERVAL_MS,
      tombstoneLimit: env.RUSTFS_TOMBSTONE_LIMIT,
      tombstoneSinceMs,
      deepNamespace: env.DEEP_MEMORY_NAMESPACE ?? undefined,
      deepAsync: env.DEEP_MEMORY_ASYNC,
    },
    "worker starting",
  );

  async function pollTombstonesIfDue(): Promise<void> {
    const now = Date.now();
    if (now - lastTombstonePollAt < env.RUSTFS_TOMBSTONE_POLL_INTERVAL_MS) {
      return;
    }
    lastTombstonePollAt = now;
    const tombUrl = new URL(`${rustfsBaseUrl}/v1/files/tombstoned`);
    if (env.RUSTFS_TENANT_ID?.trim()) {
      tombUrl.searchParams.set("tenant_id", env.RUSTFS_TENANT_ID.trim());
    }
    tombUrl.searchParams.set("since_ms", String(tombstoneSinceMs));
    tombUrl.searchParams.set("limit", String(env.RUSTFS_TOMBSTONE_LIMIT));

    const tomb = await fetchJson<{
      ok: boolean;
      items: Array<{ file_id: string; deleted_at_ms: number }>;
    }>({
      url: tombUrl.toString(),
      method: "GET",
      apiKey: env.RUSTFS_API_KEY,
      timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
    });

    const tItems = tomb.items ?? [];
    for (const item of tItems) {
      const sessionId = `rustfs:file:${item.file_id}`;
      let res: unknown;
      try {
        res = await fetchJson<unknown>({
          url: `${deepBaseUrl}/forget`,
          method: "POST",
          apiKey: env.DEEP_MEMORY_API_KEY,
          timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
          body: {
            namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
            session_id: sessionId,
            async: true,
          },
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (isOverloadLikeError(message)) {
          log.warn({ err: message }, "deep-memory overloaded during forget; backing off");
          await sleep(env.DEEP_MEMORY_BACKOFF_MS);
          // Do not advance tombstoneSinceMs so we retry later.
          throw err;
        }
        throw err;
      }
      log.info(
        { file_id: item.file_id, sessionId, deleted_at_ms: item.deleted_at_ms, res },
        "forgot tombstoned file",
      );
      tombstoneSinceMs = Math.max(tombstoneSinceMs, item.deleted_at_ms);
      await saveWorkerState(env.WORKER_STATE_PATH, { tombstoneSinceMs }).catch(() => {});
    }
  }

  while (true) {
    try {
      const pending = await fetchJson<{
        ok: boolean;
        items: RustFsFileMeta[];
        claimed_at_ms?: number;
      }>({
        url: `${rustfsBaseUrl}/v1/files/claim_extract`,
        method: "POST",
        apiKey: env.RUSTFS_API_KEY,
        timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
        body: {
          tenant_id: env.RUSTFS_TENANT_ID?.trim() || undefined,
          limit: env.RUSTFS_PENDING_LIMIT,
          lease_ms: env.RUSTFS_LEASE_MS,
        },
      });

      const items = pending.items ?? [];
      try {
        await pollTombstonesIfDue();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ err: message }, "tombstone forget poll failed");
      }

      if (items.length === 0) {
        await sleep(env.RUSTFS_POLL_INTERVAL_MS);
        continue;
      }

      for (const file of items) {
        const fileLog = log.child({ file_id: file.file_id, filename: file.filename });
        try {
          if (!isSupportedTextLike(file.mime ?? undefined, file.filename)) {
            await fetchJson({
              url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
              method: "POST",
              apiKey: env.RUSTFS_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: { status: "skipped", error: `unsupported_mime:${file.mime ?? ""}` },
            });
            fileLog.info({ mime: file.mime ?? undefined }, "skipped unsupported mime");
            continue;
          }

          const bytes = await fetchBytes({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}`,
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            maxBytes: env.RUSTFS_MAX_DOWNLOAD_BYTES,
          });
          const truncated = bytes.length >= env.RUSTFS_MAX_DOWNLOAD_BYTES;
          const text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);

          const sessionId = `rustfs:file:${file.file_id}`;
          const chunks = splitIntoChunks({
            text,
            maxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
            overlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
          });
          if (chunks.length === 0) {
            await fetchJson({
              url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
              method: "POST",
              apiKey: env.RUSTFS_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: { status: "skipped", error: "empty_text" },
            });
            fileLog.info({ sessionId }, "skipped empty text");
            continue;
          }
          const messages = buildDeepMemoryMessages({ file, truncated, chunks });
          let updateRes: unknown;
          try {
            updateRes = await fetchJson<unknown>({
              url: `${deepBaseUrl}/update_memory_index`,
              method: "POST",
              apiKey: env.DEEP_MEMORY_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: {
                namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
                session_id: sessionId,
                messages,
                async: env.DEEP_MEMORY_ASYNC,
              },
            });
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            if (isOverloadLikeError(message)) {
              const annotations = {
                rustfs_worker: {
                  version: 1,
                  indexed_at_ms: Date.now(),
                  deep_memory: {
                    base_url: deepBaseUrl,
                    namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
                    session_id: sessionId,
                    error: message,
                    overloaded: true,
                  },
                },
              };
              await fetchJson({
                url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/annotations`,
                method: "POST",
                apiKey: env.RUSTFS_API_KEY,
                timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
                body: { annotations, source: "rustfs-worker" },
              }).catch(() => {});
              fileLog.warn({ err: message }, "deep-memory overloaded; backing off");
              await sleep(env.DEEP_MEMORY_BACKOFF_MS);
            }
            throw err;
          }

          const annotations = {
            rustfs_worker: {
              version: 1,
              indexed_at_ms: Date.now(),
              deep_memory: {
                base_url: deepBaseUrl,
                namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
                session_id: sessionId,
                update_response: updateRes,
              },
              extract: {
                mime: file.mime ?? undefined,
                bytes: bytes.length,
                truncated,
                chunks: chunks.length,
                chunk_max_chars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                chunk_overlap_chars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
              },
            },
          };

          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/annotations`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { annotations, source: "rustfs-worker" },
          });
          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { status: "indexed" },
          });

          fileLog.info({ sessionId }, "indexed");
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { status: "failed", error: message },
          }).catch(() => {});
          fileLog.warn({ err: message }, "failed");
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // Best-effort global loop resilience.
      // eslint-disable-next-line no-console
      console.error(`[rustfs-worker] loop error: ${message}`);
      await sleep(env.RUSTFS_POLL_INTERVAL_MS);
    }
  }
}

await main();
