import { readFile } from "node:fs/promises";
import { writeFile } from "node:fs/promises";
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

type Args = {
  datasetPath: string;
  sweep: boolean;
  outPath?: string;
  comparePath?: string;
};

type EvalRunSnapshot = {
  version: 1;
  ts: string;
  datasetPath: string;
  knobs: Knobs;
  summary: { ok: boolean; cases: number; avgRecallAtK: number; avgNdcgAtK: number };
  cases: Array<{
    name: string;
    ok: boolean;
    recallAtK: number;
    ndcgAtK: number;
    top1Ok: boolean;
    errors: string[];
    expected: Record<string, unknown>;
    retrievedIds: string[];
    retrievedMemoryKeys: Array<string | undefined>;
  }>;
};

function parseArgs(argv: string[]): Args {
  let datasetPath = path.join(process.cwd(), "eval", "dataset.sample.json");
  let sweep = false;
  let outPath: string | undefined;
  let comparePath: string | undefined;
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
    if (t === "--out") {
      const v = argv[i + 1]?.trim();
      if (v) {
        outPath = v;
      }
      i += 1;
      continue;
    }
    if (t === "--compare") {
      const v = argv[i + 1]?.trim();
      if (v) {
        comparePath = v;
      }
      i += 1;
      continue;
    }
    if (t === "--help" || t === "-h") {
      // eslint-disable-next-line no-console
      console.log(`
Usage: pnpm --dir packages/deep-memory-server eval -- [options]

Options:
  --dataset, -d <path>     Dataset JSON file (default: eval/dataset.sample.json)
  --sweep                  Run a small parameter sweep (thresholds/weights)
  --out <path>             Write a JSON snapshot of results (for baseline/compare)
  --compare <path>         Compare current run vs a baseline snapshot JSON
  --help, -h               Show help
`);
      process.exit(0);
    }
  }
  return { datasetPath, sweep, outPath, comparePath };
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

function formatDelta(x: number): string {
  const pct = (x * 100).toFixed(2);
  return x >= 0 ? `+${pct}pp` : `${pct}pp`;
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
    const snapshot: EvalRunSnapshot = {
      version: 1,
      ts: new Date().toISOString(),
      datasetPath: args.datasetPath,
      knobs: base,
      summary: {
        ok: out.ok,
        cases: out.cases.length,
        avgRecallAtK: out.avgRecallAtK,
        avgNdcgAtK: out.avgNdcgAtK,
      },
      cases: out.cases.map((c) => ({
        name: c.name,
        ok: c.ok,
        recallAtK: c.recallAtK,
        ndcgAtK: c.ndcgAtK,
        top1Ok: c.top1Ok,
        errors: c.errors,
        expected: c.expected as unknown as Record<string, unknown>,
        retrievedIds: c.retrievedIds,
        retrievedMemoryKeys: c.retrievedMemoryKeys,
      })),
    };
    if (args.outPath) {
      await writeFile(args.outPath, `${JSON.stringify(snapshot, null, 2)}\n`, "utf8");
      // eslint-disable-next-line no-console
      console.log(`eval: wrote snapshot to ${args.outPath}`);
    }
    if (args.comparePath) {
      const raw = await readFile(args.comparePath, "utf8");
      const baseline = JSON.parse(raw) as EvalRunSnapshot;
      const baseByName = new Map(baseline.cases.map((c) => [c.name, c]));
      // eslint-disable-next-line no-console
      console.log(
        `eval: compare baseline=${args.comparePath} (baseline ok=${baseline.summary.ok} ndcg@k=${formatScore(baseline.summary.avgNdcgAtK)})`,
      );
      for (const c of snapshot.cases) {
        const b = baseByName.get(c.name);
        if (!b) {
          // eslint-disable-next-line no-console
          console.log(`- NEW: ${c.name}`);
          continue;
        }
        const dRecall = c.recallAtK - b.recallAtK;
        const dNdcg = c.ndcgAtK - b.ndcgAtK;
        if (dRecall === 0 && dNdcg === 0 && c.ok === b.ok) {
          continue;
        }
        // eslint-disable-next-line no-console
        console.log(
          `- ${c.name}: ok ${b.ok}→${c.ok} recall ${formatScore(b.recallAtK)}→${formatScore(c.recallAtK)} (${formatDelta(dRecall)}) ndcg ${formatScore(b.ndcgAtK)}→${formatScore(c.ndcgAtK)} (${formatDelta(dNdcg)})`,
        );
      }
    }
    for (const c of snapshot.cases) {
      if (c.ok) {
        continue;
      }
      // eslint-disable-next-line no-console
      console.log(`- FAIL: ${c.name}`);
      // eslint-disable-next-line no-console
      console.log(`  expected: ${JSON.stringify(c.expected)}`);
      // eslint-disable-next-line no-console
      console.log(`  retrievedIds: ${JSON.stringify(c.retrievedIds)}`);
      // eslint-disable-next-line no-console
      console.log(`  retrievedMemoryKeys: ${JSON.stringify(c.retrievedMemoryKeys)}`);
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
