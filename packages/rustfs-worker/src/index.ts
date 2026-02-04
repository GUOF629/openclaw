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

  DEEP_MEMORY_BASE_URL: z.string().min(1),
  DEEP_MEMORY_API_KEY: z.string().optional(),
  DEEP_MEMORY_NAMESPACE: z.string().optional(),
  DEEP_MEMORY_ASYNC: z.coerce.boolean().default(true),
  DEEP_MEMORY_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),
});

type Env = z.infer<typeof EnvSchema>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function isSupportedTextLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m.startsWith("text/")) return true;
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

async function fetchBytes(params: {
  url: string;
  apiKey?: string;
  timeoutMs: number;
  maxBytes: number;
}): Promise<Uint8Array> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), params.timeoutMs);
  try {
    const res = await fetch(params.url, {
      method: "GET",
      headers: {
        ...(params.apiKey ? { "x-api-key": params.apiKey } : {}),
      },
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
      if (done) break;
      if (!value) continue;
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

function buildDeepMemoryMessages(params: {
  file: RustFsFileMeta;
  text: string;
  truncated: boolean;
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
  const content = `${header}\n\nCONTENT_EXCERPT\n${params.text}`;
  return [{ role: "user", content }];
}

async function main(): Promise<void> {
  const env = EnvSchema.parse(process.env);
  const log = pino({ name: "rustfs-worker" });

  const rustfsBaseUrl = normalizeBaseUrl(env.RUSTFS_BASE_URL);
  const deepBaseUrl = normalizeBaseUrl(env.DEEP_MEMORY_BASE_URL);

  log.info(
    {
      rustfsBaseUrl,
      deepBaseUrl,
      tenantHint: env.RUSTFS_TENANT_ID ?? undefined,
      pollIntervalMs: env.RUSTFS_POLL_INTERVAL_MS,
      leaseMs: env.RUSTFS_LEASE_MS,
      pendingLimit: env.RUSTFS_PENDING_LIMIT,
      maxDownloadBytes: env.RUSTFS_MAX_DOWNLOAD_BYTES,
      deepNamespace: env.DEEP_MEMORY_NAMESPACE ?? undefined,
      deepAsync: env.DEEP_MEMORY_ASYNC,
    },
    "worker starting",
  );

  while (true) {
    try {
      const pendingUrl = new URL(`${rustfsBaseUrl}/v1/files/pending_extract`);
      if (env.RUSTFS_TENANT_ID?.trim())
        pendingUrl.searchParams.set("tenant_id", env.RUSTFS_TENANT_ID.trim());
      pendingUrl.searchParams.set("limit", String(env.RUSTFS_PENDING_LIMIT));
      pendingUrl.searchParams.set("lease_ms", String(env.RUSTFS_LEASE_MS));

      const pending = await fetchJson<{ ok: boolean; items: RustFsFileMeta[] }>({
        url: pendingUrl.toString(),
        method: "GET",
        apiKey: env.RUSTFS_API_KEY,
        timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
      });

      const items = pending.items ?? [];
      if (items.length === 0) {
        await sleep(env.RUSTFS_POLL_INTERVAL_MS);
        continue;
      }

      for (const file of items) {
        const fileLog = log.child({ file_id: file.file_id, filename: file.filename });
        try {
          // Lease the job.
          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { status: "processing" },
          });

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
          const messages = buildDeepMemoryMessages({ file, text, truncated });
          const updateRes = await fetchJson<unknown>({
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
