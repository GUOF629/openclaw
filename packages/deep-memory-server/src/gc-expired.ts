import process from "node:process";
import fs from "node:fs/promises";
import { loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore } from "./qdrant.js";
import { DEEPMEM_SCHEMA_VERSION } from "./schema.js";

type Args = {
  namespace?: string;
  allNamespaces: boolean;
  batchSize: number;
  deleteBatch: number;
  maxItems?: number;
  dryRun: boolean;
  targetCollection?: string;
  outFile?: string;
};

function parseArgs(argv: string[]): Args {
  const out: Args = { allNamespaces: false, batchSize: 200, deleteBatch: 100, dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const t = argv[i]?.trim();
    if (!t) continue;
    if (t === "--namespace" || t === "-n") {
      out.namespace = argv[i + 1]?.trim();
      i += 1;
      continue;
    }
    if (t === "--all-namespaces" || t === "--all") {
      out.allNamespaces = true;
      continue;
    }
    if (t === "--batch-size") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.batchSize = Math.max(1, Math.min(1000, Math.floor(v)));
      i += 1;
      continue;
    }
    if (t === "--delete-batch") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.deleteBatch = Math.max(1, Math.min(1000, Math.floor(v)));
      i += 1;
      continue;
    }
    if (t === "--max-items") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.maxItems = Math.max(1, Math.floor(v));
      i += 1;
      continue;
    }
    if (t === "--target-collection" || t === "--collection") {
      out.targetCollection = argv[i + 1]?.trim();
      i += 1;
      continue;
    }
    if (t === "--out") {
      const v = argv[i + 1]?.trim();
      if (v) out.outFile = v;
      i += 1;
      continue;
    }
    if (t === "--dry-run") {
      out.dryRun = true;
      continue;
    }
    if (t === "--help" || t === "-h") {
      // eslint-disable-next-line no-console
      console.log(`
Usage: pnpm --dir packages/deep-memory-server gc-expired -- --namespace <ns> [options]

Options:
  --namespace, -n <ns>           GC expired memories in a single namespace
  --all-namespaces, --all        GC expired memories in all namespaces
  --batch-size <n>               Scan page size (default: 200)
  --delete-batch <n>             Delete batch size (default: 100)
  --max-items <n>                Stop after scanning N items
  --target-collection <name>     Qdrant collection (default: QDRANT_COLLECTION)
  --out <path>                   Write JSON report to file
  --dry-run                      Do not delete, only report
  --help, -h                     Show help
`);
      process.exit(0);
    }
  }
  if (!out.allNamespaces) {
    out.namespace = out.namespace?.trim() || undefined;
    if (!out.namespace) throw new Error("missing --namespace (or use --all-namespaces)");
  }
  return out;
}

function isExpired(expiresAt: string | undefined, nowMs: number): boolean {
  const t = (expiresAt ?? "").trim();
  if (!t) return false;
  const ms = Date.parse(t);
  return Number.isFinite(ms) && ms > 0 && ms <= nowMs;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cfg = loadConfig();
  const log = createLogger(cfg.LOG_LEVEL);
  const targetCollection = (args.targetCollection ?? cfg.QDRANT_COLLECTION).trim();
  if (!targetCollection) throw new Error("empty target collection");

  const neo4j = new Neo4jStore({ uri: cfg.NEO4J_URI, user: cfg.NEO4J_USER, password: cfg.NEO4J_PASSWORD });
  const qdrant = new QdrantStore({
    url: cfg.QDRANT_URL,
    apiKey: cfg.QDRANT_API_KEY,
    collection: targetCollection,
    dims: cfg.VECTOR_DIMS,
  });
  await qdrant.schemaStatus({ mode: "validate", expectedVersion: DEEPMEM_SCHEMA_VERSION });

  const namespaces = args.allNamespaces ? await neo4j.listNamespaces({ limit: 10_000 }) : [args.namespace!];

  const nowMs = Date.now();
  const report: {
    ok: boolean;
    dryRun: boolean;
    targetCollection: string;
    scanned: number;
    expiredFound: number;
    neo4jDeleted: number;
    qdrantDeleted: number;
    failedBatches: number;
    finishedAt: string;
  } = {
    ok: true,
    dryRun: args.dryRun,
    targetCollection,
    scanned: 0,
    expiredFound: 0,
    neo4jDeleted: 0,
    qdrantDeleted: 0,
    failedBatches: 0,
    finishedAt: "",
  };

  try {
    for (const ns of namespaces) {
      let afterId: string | undefined;
      const batchIds: string[] = [];
      while (true) {
        const page = await neo4j.scanMemories({ namespace: ns, afterId, limit: args.batchSize });
        if (page.length === 0) break;
        for (const m of page) {
          report.scanned += 1;
          afterId = m.id;
          if (args.maxItems && report.scanned >= args.maxItems) break;
          if (isExpired(m.expiresAt, nowMs)) {
            report.expiredFound += 1;
            batchIds.push(m.id);
          }
          if (batchIds.length >= args.deleteBatch) {
            if (!args.dryRun) {
              try {
                report.qdrantDeleted += await qdrant.deleteByIds({ ids: batchIds });
              } catch {
                report.failedBatches += 1;
                report.ok = false;
              }
              try {
                report.neo4jDeleted += await neo4j.deleteMemoriesByIds({ namespace: ns, ids: batchIds });
              } catch {
                report.failedBatches += 1;
                report.ok = false;
              }
            }
            batchIds.length = 0;
          }
        }
        if (args.maxItems && report.scanned >= args.maxItems) break;
      }
      if (batchIds.length > 0) {
        if (!args.dryRun) {
          try {
            report.qdrantDeleted += await qdrant.deleteByIds({ ids: batchIds });
          } catch {
            report.failedBatches += 1;
            report.ok = false;
          }
          try {
            report.neo4jDeleted += await neo4j.deleteMemoriesByIds({ namespace: ns, ids: batchIds });
          } catch {
            report.failedBatches += 1;
            report.ok = false;
          }
        }
      }
      log.info(
        { namespace: ns, scanned: report.scanned, expiredFound: report.expiredFound },
        "gc-expired namespace progress",
      );
    }
  } finally {
    await neo4j.close().catch(() => {});
  }

  report.finishedAt = new Date().toISOString();
  if (args.outFile?.trim()) {
    await fs.writeFile(args.outFile.trim(), `${JSON.stringify(report, null, 2)}\n`, "utf8");
  }
  // eslint-disable-next-line no-console
  console.log(
    `gc-expired: ok=${report.ok} dryRun=${report.dryRun} scanned=${report.scanned} expired=${report.expiredFound} neo4jDeleted=${report.neo4jDeleted} qdrantDeleted=${report.qdrantDeleted}`,
  );
  process.exit(report.ok ? 0 : 2);
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(String(err instanceof Error ? (err.stack ?? err.message) : err));
  process.exit(1);
});

