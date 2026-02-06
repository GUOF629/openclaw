import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { EvalDataset } from "./types.js";
import { runEval } from "./harness.js";
import { createRetrieverForEval } from "./stubs.js";

const pkgRoot = path.dirname(fileURLToPath(import.meta.url));
const datasetPath = path.resolve(pkgRoot, "../../eval/dataset.sample.json");

describe("eval dataset baseline", () => {
  it("passes sample dataset with default knobs", async () => {
    const raw = await readFile(datasetPath, "utf-8");
    const dataset = JSON.parse(raw) as EvalDataset;
    const knobs = {
      minSemanticScore: 0.6,
      semanticWeight: 0.6,
      relationWeight: 0.4,
      decayHalfLifeDays: 90,
      importanceBoost: 0.3,
      frequencyBoost: 0.2,
    };
    const cases = dataset.cases.map((c) => {
      const retriever = createRetrieverForEval({ evalCase: c, knobs });
      return runEval({ retriever, cases: [c] });
    });
    const results = await Promise.all(cases);
    expect(results.every((r) => r.ok)).toBe(true);
  });

  it("meets minimum quality thresholds (avg recall@k / ndcg@k)", async () => {
    const raw = await readFile(datasetPath, "utf-8");
    const dataset = JSON.parse(raw) as EvalDataset;
    const knobs = {
      minSemanticScore: 0.6,
      semanticWeight: 0.6,
      relationWeight: 0.4,
      decayHalfLifeDays: 90,
      importanceBoost: 0.3,
      frequencyBoost: 0.2,
    };
    const summaries = await Promise.all(
      dataset.cases.map(async (c) => {
        const retriever = createRetrieverForEval({ evalCase: c, knobs });
        return await runEval({ retriever, cases: [c] });
      }),
    );
    const cases = summaries.flatMap((s) => s.cases);
    const avgRecallAtK = cases.reduce((sum, c) => sum + c.recallAtK, 0) / Math.max(1, cases.length);
    const avgNdcgAtK = cases.reduce((sum, c) => sum + c.ndcgAtK, 0) / Math.max(1, cases.length);
    expect(avgRecallAtK).toBeGreaterThanOrEqual(0.9);
    expect(avgNdcgAtK).toBeGreaterThanOrEqual(0.9);
  });
});
