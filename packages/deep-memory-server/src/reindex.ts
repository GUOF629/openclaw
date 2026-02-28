import fs from "node:fs/promises";
import process from "node:process";
import { loadConfig } from "./config.js";
import { EmbeddingModel } from "./embeddings.js";
import { createLogger } from "./logger.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore, type QdrantMemoryPayload } from "./qdrant.js";
import { DEEPMEM_SCHEMA_VERSION } from "./schema.js";

type Args = {
  namespace?: string;
  allNamespaces: boolean;
  targetCollection?: string;
  batchSize: number;
  maxItems?: number;
  dryRun: boolean;
  concurrency: number;
  maxRetries: number;
  continueOnError: boolean;
  cursorFile?: string;
  resume: boolean;
  reportFile?: string;
};

function parseArgs(argv: string[]): Args {
  const out: Args = {
    allNamespaces: false,
    batchSize: 100,
    dryRun: false,
    concurrency: 4,
    maxRetries: 3,
    continueOnError: false,
    resume: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const t = argv[i]?.trim();
    if (!t) {
      continue;
    }
    if (t === "--namespace" || t === "-n") {
      out.namespace = argv[i + 1]?.trim();
      i += 1;
      continue;
    }
    if (t === "--all-namespaces" || t === "--all") {
      out.allNamespaces = true;
      continue;
    }
    if (t === "--target-collection" || t === "--collection") {
      out.targetCollection = argv[i + 1]?.trim();
      i += 1;
      continue;
    }
    if (t === "--batch-size") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) {
        out.batchSize = Math.max(1, Math.min(1000, Math.floor(v)));
      }
      i += 1;
      continue;
    }
    if (t === "--concurrency" || t === "--parallel") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) {
        out.concurrency = Math.max(1, Math.min(32, Math.floor(v)));
      }
      i += 1;
      continue;
    }
    if (t === "--max-items") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) {
        out.maxItems = Math.max(1, Math.floor(v));
      }
      i += 1;
      continue;
    }
    if (t === "--cursor-file") {
      const v = argv[i + 1]?.trim();
      if (v) {
        out.cursorFile = v;
      }
      i += 1;
      continue;
    }
    if (t === "--resume") {
      out.resume = true;
      continue;
    }
    if (t === "--max-retries") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v >= 0) {
        out.maxRetries = Math.max(0, Math.min(10, Math.floor(v)));
      }
      i += 1;
      continue;
    }
    if (t === "--continue-on-error") {
      out.continueOnError = true;
      continue;
    }
    if (t === "--report-file") {
      const v = argv[i + 1]?.trim();
      if (v) {
        out.reportFile = v;
      }
      i += 1;
      continue;
    }
    if (t === "--dry-run") {
      out.dryRun = true;
      continue;
    }
    if (t === "--help" || t === "-h") {
      printHelpAndExit(0);
    }
  }

  if (!out.allNamespaces) {
    out.namespace = out.namespace?.trim() || undefined;
    if (!out.namespace) {
      throw new Error("missing --namespace (or use --all-namespaces)");
    }
  }
  return out;
}

function printHelpAndExit(code: number): never {
  // eslint-disable-next-line no-console
  console.log(`
Usage: pnpm --dir packages/deep-memory-server reindex -- --namespace <ns> [options]

Options:
  --namespace, -n <ns>           Reindex a single namespace (recommended)
  --all-namespaces, --all        Reindex all namespaces (large; use with care)
  --target-collection <name>     Qdrant collection to write into (default: QDRANT_COLLECTION)
  --batch-size <n>               Neo4j page size (default: 100, max: 1000)
  --concurrency, --parallel <n>  Embed+upsert concurrency (default: 4, max: 32)
  --max-items <n>                Stop after N items (for canary)
  --cursor-file <path>           Persist cursor JSON for resume (optional)
  --resume                       Resume from cursor-file if it exists
  --max-retries <n>              Retries per item on embed/upsert failure (default: 3)
  --continue-on-error            Continue past failures and write a report (default: stop)
  --report-file <path>           Write a JSON summary report (optional)
  --dry-run                      Scan only; do not embed or write
  --help, -h                     Show help
`);
  process.exit(code);
}

type CursorState = {
  afterId?: string;
  processed: number;
  written: number;
  failed: number;
  updatedAt: string;
};

async function sleep(ms: number) {
  return await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function writeCursor(file: string, state: CursorState) {
  const body = `${JSON.stringify(state, null, 2)}\n`;
  await fs.writeFile(file, body, "utf8");
}

async function readCursor(file: string): Promise<CursorState | null> {
  try {
    const raw = await fs.readFile(file, "utf8");
    const parsed = JSON.parse(raw) as Partial<CursorState>;
    if (typeof parsed !== "object" || !parsed) {
      return null;
    }
    return {
      afterId: typeof parsed.afterId === "string" ? parsed.afterId : undefined,
      processed: Number(parsed.processed ?? 0) || 0,
      written: Number(parsed.written ?? 0) || 0,
      failed: Number(parsed.failed ?? 0) || 0,
      updatedAt: typeof parsed.updatedAt === "string" ? parsed.updatedAt : new Date().toISOString(),
    };
  } catch {
    return null;
  }
}

async function withRetries<T>(params: { maxRetries: number; fn: () => Promise<T> }): Promise<T> {
  let attempt = 0;
  // maxRetries=0 means "try once"
  while (true) {
    attempt += 1;
    try {
      return await params.fn();
    } catch (err) {
      if (attempt > Math.max(1, params.maxRetries + 1)) {
        throw err;
      }
      const delay = Math.min(10_000, 250 * 2 ** Math.min(10, attempt - 1));
      await sleep(delay);
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cfg = loadConfig();
  const log = createLogger(cfg.LOG_LEVEL);

  const targetCollection = (args.targetCollection ?? cfg.QDRANT_COLLECTION).trim();
  if (!targetCollection) {
    throw new Error("empty target collection");
  }

  const neo4j = new Neo4jStore({
    uri: cfg.NEO4J_URI,
    user: cfg.NEO4J_USER,
    password: cfg.NEO4J_PASSWORD,
  });

  const qdrant = new QdrantStore({
    url: cfg.QDRANT_URL,
    apiKey: cfg.QDRANT_API_KEY,
    collection: targetCollection,
    dims: cfg.VECTOR_DIMS,
  });

  const embedder = new EmbeddingModel({ modelId: cfg.EMBEDDING_MODEL, dims: cfg.VECTOR_DIMS });

  log.info(
    {
      namespace: args.allNamespaces ? "*" : args.namespace,
      batchSize: args.batchSize,
      maxItems: args.maxItems,
      dryRun: args.dryRun,
      concurrency: args.concurrency,
      maxRetries: args.maxRetries,
      continueOnError: args.continueOnError,
      cursorFile: args.cursorFile,
      resume: args.resume,
      reportFile: args.reportFile,
      targetCollection,
      dims: cfg.VECTOR_DIMS,
      model: cfg.EMBEDDING_MODEL,
    },
    "deep-memory reindex starting",
  );

  const qdrantSchema = await qdrant.schemaStatus({
    mode: "apply",
    expectedVersion: DEEPMEM_SCHEMA_VERSION,
  });
  if (!qdrantSchema.ok) {
    throw new Error(`qdrant schema not ready: ${qdrantSchema.error ?? "unknown"}`);
  }

  const cursorFile = args.cursorFile?.trim() || undefined;
  let afterId: string | undefined;
  let processed = 0;
  let written = 0;
  let failed = 0;
  if (cursorFile) {
    const existing = await readCursor(cursorFile);
    if (existing && args.resume) {
      afterId = existing.afterId;
      processed = existing.processed;
      written = existing.written;
      failed = existing.failed;
      log.info({ cursorFile, afterId, processed, written, failed }, "reindex resumed from cursor");
    } else if (existing && !args.resume) {
      throw new Error(`cursor file exists; pass --resume to continue (${cursorFile})`);
    }
  }

  const failures: Array<{ id: string; namespace: string; error: string }> = [];

  try {
    while (true) {
      const page = await neo4j.scanMemories({
        namespace: args.allNamespaces ? undefined : args.namespace,
        afterId,
        limit: args.batchSize,
      });
      if (page.length === 0) {
        break;
      }

      const tasks = page.map((m) => async () => {
        if (args.dryRun) {
          return;
        }
        const vec = await withRetries({
          maxRetries: args.maxRetries,
          fn: async () => await embedder.embed(m.content),
        });
        const payload: QdrantMemoryPayload = {
          id: m.id,
          namespace: m.namespace,
          kind: m.kind,
          memory_key: m.memoryKey,
          subject: m.subject,
          expires_at: m.expiresAt,
          confidence: m.confidence,
          content: m.content,
          session_id: m.sessionId ?? "",
          created_at: m.createdAt,
          updated_at: new Date().toISOString(),
          importance: m.importance,
          frequency: m.frequency,
          entities: m.entities.map((e) => e.name).slice(0, 10),
          topics: m.topics.slice(0, 10),
        };
        await withRetries({
          maxRetries: args.maxRetries,
          fn: async () => await qdrant.upsertMemory({ id: m.id, vector: vec, payload }),
        });
      });

      const results: Array<
        Promise<{ ok: boolean; id: string; namespace: string; error?: string }>
      > = [];
      for (const m of page) {
        const fn = tasks[results.length];
        if (!fn) {
          break;
        }
        results.push(
          fn()
            .then(() => ({ ok: true as const, id: m.id, namespace: m.namespace }))
            .catch((err) => ({
              ok: false as const,
              id: m.id,
              namespace: m.namespace,
              error: err instanceof Error ? (err.stack ?? err.message) : String(err),
            })),
        );
      }

      // Basic concurrency throttle while preserving cursor advancement order.
      const inflight = new Set<Promise<unknown>>();
      let launched = 0;
      const launchUntil = () => {
        while (launched < results.length && inflight.size < args.concurrency) {
          const p = results[launched];
          if (!p) {
            launched += 1;
            continue;
          }
          inflight.add(p);
          void p.finally(() => inflight.delete(p));
          launched += 1;
        }
      };
      launchUntil();

      for (let i = 0; i < results.length; i += 1) {
        const r = await results[i];
        if (!r) {
          continue;
        }
        processed += 1;
        afterId = r.id;
        if (!args.dryRun && r.ok) {
          written += 1;
        }
        if (!r.ok) {
          failed += 1;
          failures.push({ id: r.id, namespace: r.namespace, error: r.error ?? "unknown" });
          log.error({ id: r.id, err: r.error }, "reindex item failed");
          if (!args.continueOnError) {
            throw new Error(`reindex failed at id=${r.id}`);
          }
        }
        if (cursorFile && (processed % 20 === 0 || i === results.length - 1)) {
          await writeCursor(cursorFile, {
            afterId,
            processed,
            written,
            failed,
            updatedAt: new Date().toISOString(),
          });
        }
        if (args.maxItems && processed >= args.maxItems) {
          break;
        }
        launchUntil();
      }

      if (args.maxItems && processed > args.maxItems) {
        break;
      }
    }
  } finally {
    await neo4j.close().catch(() => {});
  }

  const report = {
    ok: failed === 0,
    namespace: args.allNamespaces ? "*" : args.namespace,
    targetCollection,
    processed,
    written,
    failed,
    afterId,
    failures: args.continueOnError ? failures : undefined,
    finishedAt: new Date().toISOString(),
  };
  if (args.reportFile?.trim()) {
    await fs.writeFile(args.reportFile.trim(), `${JSON.stringify(report, null, 2)}\n`, "utf8");
  }
  log.info(report, "deep-memory reindex finished");
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(String(err instanceof Error ? (err.stack ?? err.message) : err));
  process.exit(1);
});
