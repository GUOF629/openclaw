import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { z } from "zod";
import type { EvalDataset } from "./eval/types.js";
import { runEval } from "./eval/harness.js";
import { createRetrieverForEval } from "./eval/stubs.js";

const DatasetSchema = z.object({
  version: z.number().int().positive(),
  cases: z.array(
    z.object({
      name: z.string(),
      namespace: z.string(),
      now: z.string(),
      query: z.object({
        userInput: z.string(),
        sessionId: z.string(),
        entities: z.array(z.string()),
        topics: z.array(z.string()),
        maxMemories: z.number().int().positive(),
      }),
      qdrantHits: z.array(
        z.object({
          id: z.string(),
          score: z.number(),
          payload: z.object({
            content: z.string(),
            importance: z.number().optional(),
            frequency: z.number().optional(),
            created_at: z.string(),
            updated_at: z.string().optional(),
            kind: z.string().optional(),
            memory_key: z.string().optional(),
            subject: z.string().optional(),
            expires_at: z.string().optional(),
            confidence: z.number().optional(),
          }),
        }),
      ),
      neo4jHits: z.array(
        z.object({
          id: z.string(),
          content: z.string(),
          importance: z.number(),
          frequency: z.number(),
          lastSeenAt: z.string(),
          relationScore: z.number(),
          kind: z.string().optional(),
          memoryKey: z.string().optional(),
          subject: z.string().optional(),
          expiresAt: z.string().optional(),
          confidence: z.number().optional(),
        }),
      ),
      expect: z.object({
        includeIds: z.array(z.string()).optional(),
        excludeIds: z.array(z.string()).optional(),
        top1Id: z.string().optional(),
        uniqueByMemoryKey: z.boolean().optional(),
      }),
    }),
  ),
});

type Knobs = {
  minSemanticScore: number;
  semanticWeight: number;
  relationWeight: number;
  decayHalfLifeDays: number;
  importanceBoost: number;
  frequencyBoost: number;
};

function parseArgs(argv: string[]): { datasetPath: string; sweep: boolean } {
  let datasetPath = path.join(process.cwd(), "eval", "dataset.sample.json");
  let sweep = false;
  for (let i = 0; i < argv.length; i += 1) {
    const t = argv[i]?.trim();
    if (!t) {
      continue;
    }
    if (t === "--dataset" || t === "-d") {
      const v = argv[i + 1]?.trim();
      if (v) {
        datasetPath = v;
      }
      i += 1;
      continue;
    }
    if (t === "--sweep") {
      sweep = true;
      continue;
    }
    if (t === "--help" || t === "-h") {
      // eslint-disable-next-line no-console
      console.log(`
Usage: pnpm --dir packages/deep-memory-server eval -- [options]

Options:
  --dataset, -d <path>     Dataset JSON file (default: eval/dataset.sample.json)
  --sweep                  Run a small parameter sweep (thresholds/weights)
  --help, -h               Show help
`);
      process.exit(0);
    }
  }
  return { datasetPath, sweep };
}

async function loadDataset(datasetPath: string): Promise<EvalDataset> {
  const raw = await readFile(datasetPath, "utf-8");
  const parsed = DatasetSchema.safeParse(JSON.parse(raw) as unknown);
  if (!parsed.success) {
    throw new Error(`Invalid dataset: ${parsed.error.message}`);
  }
  return parsed.data as EvalDataset;
}

async function runOnce(dataset: EvalDataset, knobs: Knobs) {
  const results = await Promise.all(
    dataset.cases.map(async (c) => {
      const retriever = createRetrieverForEval({ evalCase: c, knobs });
      return await runEval({ retriever, cases: [c] });
    }),
  );
  const cases = results.flatMap((r) => r.cases);
  const ok = cases.every((c) => c.ok);
  const avgRecallAtK =
    cases.length === 0 ? 1 : cases.reduce((s, r) => s + r.recallAtK, 0) / cases.length;
  const avgNdcgAtK =
    cases.length === 0 ? 1 : cases.reduce((s, r) => s + r.ndcgAtK, 0) / cases.length;
  return { ok, cases, avgRecallAtK, avgNdcgAtK };
}

function formatScore(x: number): string {
  return `${(x * 100).toFixed(1)}%`;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dataset = await loadDataset(args.datasetPath);

  const base: Knobs = {
    minSemanticScore: 0.6,
    semanticWeight: 0.6,
    relationWeight: 0.4,
    decayHalfLifeDays: 90,
    importanceBoost: 0.3,
    frequencyBoost: 0.2,
  };

  if (!args.sweep) {
    const out = await runOnce(dataset, base);
    // eslint-disable-next-line no-console
    console.log(
      `eval: ok=${out.ok} cases=${out.cases.length} recall@k=${formatScore(out.avgRecallAtK)} ndcg@k=${formatScore(out.avgNdcgAtK)}`,
    );
    for (const c of out.cases) {
      if (c.ok) {
        continue;
      }
      // eslint-disable-next-line no-console
      console.log(`- FAIL: ${c.name}`);
      for (const e of c.errors) {
        // eslint-disable-next-line no-console
        console.log(`  - ${e}`);
      }
    }
    process.exit(out.ok ? 0 : 1);
  }

  const sweeps: Knobs[] = [];
  for (const minSemanticScore of [0.4, 0.5, 0.6, 0.7]) {
    for (const semanticWeight of [0.5, 0.6, 0.7]) {
      const relationWeight = 1 - semanticWeight;
      sweeps.push({ ...base, minSemanticScore, semanticWeight, relationWeight });
    }
  }

  let best = { knobs: base, score: -1, ok: false };
  for (const k of sweeps) {
    const out = await runOnce(dataset, k);
    const score = out.avgNdcgAtK * 0.7 + out.avgRecallAtK * 0.3;
    const better = score > best.score || (score === best.score && out.ok && !best.ok);
    if (better) {
      best = { knobs: k, score, ok: out.ok };
    }
  }

  // eslint-disable-next-line no-console
  console.log(
    `sweep: best ok=${best.ok} score=${best.score.toFixed(4)} knobs=${JSON.stringify(best.knobs)}`,
  );
  process.exit(best.ok ? 0 : 2);
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(String(err instanceof Error ? (err.stack ?? err.message) : err));
  process.exit(1);
});
