import type { DeepMemoryRetriever } from "../retriever.js";
import type { RetrieveContextResponse } from "../types.js";
import type { EvalCase } from "./types.js";
import { clamp } from "../utils.js";

export type EvalCaseResult = {
  name: string;
  ok: boolean;
  errors: string[];
  retrievedIds: string[];
  retrievedMemoryKeys: Array<string | undefined>;
  recallAtK: number;
  ndcgAtK: number;
  top1Ok: boolean;
  expected: {
    includeIds?: string[];
    excludeIds?: string[];
    top1Id?: string;
    uniqueByMemoryKey?: boolean;
  };
};

export function withFixedNow<T>(isoNow: string, fn: () => T): T {
  const nowMs = Date.parse(isoNow);
  if (!Number.isFinite(nowMs)) {
    throw new Error(`Invalid ISO time for fixed now: ${isoNow}`);
  }
  const prev = Date.now;
  Date.now = () => nowMs;
  try {
    return fn();
  } finally {
    Date.now = prev;
  }
}

function dcg(relevances: number[]): number {
  let sum = 0;
  for (let i = 0; i < relevances.length; i += 1) {
    const rel = relevances[i] ?? 0;
    const denom = Math.log2(i + 2);
    sum += (Math.pow(2, rel) - 1) / denom;
  }
  return sum;
}

function ndcgAtK(params: { predicted: string[]; relevant: Set<string>; k: number }): number {
  const k = Math.max(1, params.k);
  const gains = params.predicted.slice(0, k).map((id) => (params.relevant.has(id) ? 1 : 0));
  const ideal = Array.from({ length: Math.min(k, params.relevant.size) }, () => 1);
  const denom = dcg(ideal);
  if (denom <= 0) {
    return 1;
  }
  return clamp(dcg(gains) / denom, 0, 1);
}

function recallAtK(params: { predicted: string[]; relevant: Set<string>; k: number }): number {
  const k = Math.max(1, params.k);
  if (params.relevant.size === 0) {
    return 1;
  }
  const hits = params.predicted.slice(0, k).filter((id) => params.relevant.has(id)).length;
  return clamp(hits / params.relevant.size, 0, 1);
}

function extractMemoryKeys(out: RetrieveContextResponse): Array<string | undefined> {
  return out.memories.map((m) => {
    const r = m as unknown as Record<string, unknown>;
    const key = r.memory_key;
    return typeof key === "string" && key.trim() ? key.trim() : undefined;
  });
}

export async function runEvalCase(params: {
  retriever: DeepMemoryRetriever;
  evalCase: EvalCase;
}): Promise<EvalCaseResult> {
  const errors: string[] = [];
  const c = params.evalCase;
  const relevant = new Set<string>(c.expect.includeIds ?? []);
  const k = c.query.maxMemories;

  const out = await withFixedNow(c.now, async () => {
    return await params.retriever.retrieve({
      namespace: c.namespace,
      userInput: c.query.userInput,
      sessionId: c.query.sessionId,
      maxMemories: c.query.maxMemories,
      entities: c.query.entities,
      topics: c.query.topics,
    });
  });

  const retrievedIds = out.memories.map((m) => m.id);
  const retrievedMemoryKeys = extractMemoryKeys(out);

  const top1Ok = c.expect.top1Id ? retrievedIds[0] === c.expect.top1Id : true;
  if (!top1Ok) {
    errors.push(`top1 mismatch: expected=${c.expect.top1Id} actual=${retrievedIds[0] ?? "<none>"}`);
  }

  for (const id of c.expect.includeIds ?? []) {
    if (!retrievedIds.includes(id)) {
      errors.push(`missing expected id: ${id}`);
    }
  }
  for (const id of c.expect.excludeIds ?? []) {
    if (retrievedIds.includes(id)) {
      errors.push(`unexpected id present: ${id}`);
    }
  }

  if (c.expect.uniqueByMemoryKey) {
    const seen = new Set<string>();
    for (const key of retrievedMemoryKeys) {
      if (!key) {
        continue;
      }
      if (seen.has(key)) {
        errors.push(`duplicate memory_key returned: ${key}`);
        break;
      }
      seen.add(key);
    }
  }

  return {
    name: c.name,
    ok: errors.length === 0,
    errors,
    retrievedIds,
    retrievedMemoryKeys,
    recallAtK: recallAtK({ predicted: retrievedIds, relevant, k }),
    ndcgAtK: ndcgAtK({ predicted: retrievedIds, relevant, k }),
    top1Ok,
    expected: c.expect,
  };
}

export type EvalSummary = {
  ok: boolean;
  cases: EvalCaseResult[];
  avgRecallAtK: number;
  avgNdcgAtK: number;
};

export async function runEval(params: {
  retriever: DeepMemoryRetriever;
  cases: EvalCase[];
}): Promise<EvalSummary> {
  const results: EvalCaseResult[] = [];
  for (const c of params.cases) {
    results.push(await runEvalCase({ retriever: params.retriever, evalCase: c }));
  }
  const avgRecallAtK =
    results.length === 0 ? 1 : results.reduce((s, r) => s + r.recallAtK, 0) / results.length;
  const avgNdcgAtK =
    results.length === 0 ? 1 : results.reduce((s, r) => s + r.ndcgAtK, 0) / results.length;
  return {
    ok: results.every((r) => r.ok),
    cases: results,
    avgRecallAtK,
    avgNdcgAtK,
  };
}
