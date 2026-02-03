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
};

function parseArgs(argv: string[]): Args {
  const out: Args = {
    allNamespaces: false,
    batchSize: 100,
    dryRun: false,
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
    if (t === "--max-items") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) {
        out.maxItems = Math.max(1, Math.floor(v));
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
  --max-items <n>                Stop after N items (for canary)
  --dry-run                      Scan only; do not embed or write
  --help, -h                     Show help
`);
  process.exit(code);
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

  let afterId: string | undefined;
  let processed = 0;
  let written = 0;

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

      for (const m of page) {
        processed += 1;
        afterId = m.id;
        if (args.maxItems && processed > args.maxItems) {
          break;
        }

        if (args.dryRun) {
          continue;
        }

        const vec = await embedder.embed(m.content);
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

        await qdrant.upsertMemory({ id: m.id, vector: vec, payload });
        written += 1;

        if (written % 50 === 0) {
          log.info({ processed, written, lastId: m.id }, "reindex progress");
        }
      }

      if (args.maxItems && processed > args.maxItems) {
        break;
      }
    }
  } finally {
    await neo4j.close().catch(() => {});
  }

  log.info({ processed, written }, "deep-memory reindex finished");
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(String(err instanceof Error ? (err.stack ?? err.message) : err));
  process.exit(1);
});
