import fs from "node:fs/promises";
import process from "node:process";
import { loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore } from "./qdrant.js";
import { stableHash } from "./utils.js";

type Args = {
  namespace?: string;
  allNamespaces: boolean;
  targetCollection?: string;
  sampleSize: number;
  outFile?: string;
};

function parseArgs(argv: string[]): Args {
  const out: Args = { allNamespaces: false, sampleSize: 50 };
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
    if (t === "--sample-size") {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) {
        out.sampleSize = Math.max(1, Math.min(500, Math.floor(v)));
      }
      i += 1;
      continue;
    }
    if (t === "--out") {
      const v = argv[i + 1]?.trim();
      if (v) {
        out.outFile = v;
      }
      i += 1;
      continue;
    }
    if (t === "--help" || t === "-h") {
      // eslint-disable-next-line no-console
      console.log(`
Usage: pnpm --dir packages/deep-memory-server validate-reindex -- --namespace <ns> [options]

Options:
  --namespace, -n <ns>           Validate a single namespace (recommended)
  --all-namespaces, --all        Validate all namespaces (can be slow)
  --target-collection <name>     Qdrant collection to validate (default: QDRANT_COLLECTION)
  --sample-size <n>              Sample size per namespace (default: 50, max: 500)
  --out <path>                   Write JSON report to file
  --help, -h                     Show help
`);
      process.exit(0);
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

type SampleItem = { id: string; content: string; namespace: string; hash: number };

function hashToNumber(hex: string): number {
  const h = hex.slice(0, 8);
  const n = Number.parseInt(h, 16);
  return Number.isFinite(n) ? n : 0;
}

function maybeAddSample(samples: SampleItem[], item: SampleItem, limit: number) {
  if (samples.length < limit) {
    samples.push(item);
    samples.sort((a, b) => a.hash - b.hash);
    return;
  }
  const worst = samples[samples.length - 1];
  if (!worst || item.hash >= worst.hash) {
    return;
  }
  samples[samples.length - 1] = item;
  samples.sort((a, b) => a.hash - b.hash);
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

  const namespaces = args.allNamespaces
    ? await neo4j.listNamespaces({ limit: 10_000 })
    : [args.namespace!];

  const report: {
    ok: boolean;
    targetCollection: string;
    namespaces: Array<{
      namespace: string;
      neo4jCount: number;
      qdrantCount: number;
      sampleSize: number;
      sampleHits: number;
      missingIds: string[];
    }>;
    finishedAt: string;
  } = { ok: true, targetCollection, namespaces: [], finishedAt: "" };

  try {
    for (const ns of namespaces) {
      const neo4jCount = await neo4j.countMemories({ namespace: ns });
      const qdrantCount = await qdrant.countMemories({ namespace: ns });

      const samples: SampleItem[] = [];
      let afterId: string | undefined;
      while (true) {
        const page = await neo4j.scanMemories({ namespace: ns, afterId, limit: 200 });
        if (page.length === 0) {
          break;
        }
        for (const m of page) {
          afterId = m.id;
          const h = hashToNumber(stableHash(m.id));
          maybeAddSample(
            samples,
            { id: m.id, content: m.content, namespace: ns, hash: h },
            args.sampleSize,
          );
        }
      }

      const missingIds: string[] = [];
      let hits = 0;
      for (const s of samples) {
        const hit = await qdrant.getMemory(s.id);
        if (!hit?.payload) {
          missingIds.push(s.id);
          continue;
        }
        if (hit.payload.namespace !== ns) {
          missingIds.push(s.id);
          continue;
        }
        if (hit.payload.content !== s.content) {
          missingIds.push(s.id);
          continue;
        }
        hits += 1;
      }

      const ok = missingIds.length === 0;
      report.ok = report.ok && ok;
      report.namespaces.push({
        namespace: ns,
        neo4jCount,
        qdrantCount,
        sampleSize: samples.length,
        sampleHits: hits,
        missingIds: missingIds.slice(0, 50),
      });
      log.info(
        {
          namespace: ns,
          neo4jCount,
          qdrantCount,
          sample: samples.length,
          missing: missingIds.length,
        },
        "validate-reindex namespace summary",
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
  console.log(`validate-reindex: ok=${report.ok} namespaces=${report.namespaces.length}`);
  process.exit(report.ok ? 0 : 2);
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(String(err instanceof Error ? (err.stack ?? err.message) : err));
  process.exit(1);
});
