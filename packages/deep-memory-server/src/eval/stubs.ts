import type { DeepMemoryRetriever } from "../retriever.js";
import type { EvalCase, EvalRelationHit, EvalSemanticHit } from "./types.js";
import { DeepMemoryRetriever as RealRetriever } from "../retriever.js";

type FakeQdrant = {
  search: (params: {
    vector: number[];
    limit: number;
    minScore: number;
    namespace?: string;
  }) => Promise<Array<{ id: string; score: number; payload?: EvalSemanticHit["payload"] }>>;
};

type FakeNeo4j = {
  queryRelatedMemories: (params: {
    namespace: string;
    entities: string[];
    topics: string[];
    limit: number;
  }) => Promise<EvalRelationHit[]>;
};

class FakeEmbedder {
  async embed(_text: string): Promise<number[]> {
    return [0, 0, 0];
  }
}

export function createRetrieverForEval(params: {
  evalCase: EvalCase;
  knobs: {
    minSemanticScore: number;
    semanticWeight: number;
    relationWeight: number;
    decayHalfLifeDays: number;
    importanceBoost: number;
    frequencyBoost: number;
  };
}): DeepMemoryRetriever {
  const qdrant: FakeQdrant = {
    search: async (p) => {
      const filtered = params.evalCase.qdrantHits
        .filter((h) => (!p.namespace ? true : h.id.startsWith(`${p.namespace}::`)))
        .filter((h) => h.score >= p.minScore)
        .slice(0, Math.max(0, p.limit));
      return filtered.map((h) => ({ id: h.id, score: h.score, payload: h.payload }));
    },
  };
  const neo4j: FakeNeo4j = {
    queryRelatedMemories: async (p) => {
      // Namespace check is handled by the dataset itself; keep deterministic.
      if (p.namespace !== params.evalCase.namespace) {
        return [];
      }
      return params.evalCase.neo4jHits.slice(0, Math.max(0, p.limit));
    },
  };

  return new RealRetriever({
    // We only need embed() to not throw; Qdrant is stubbed.
    embedder: new FakeEmbedder() as unknown as import("../embeddings.js").EmbeddingModel,
    qdrant: qdrant as unknown as import("../qdrant.js").QdrantStore,
    neo4j: neo4j as unknown as import("../neo4j.js").Neo4jStore,
    minSemanticScore: params.knobs.minSemanticScore,
    semanticWeight: params.knobs.semanticWeight,
    relationWeight: params.knobs.relationWeight,
    decayHalfLifeDays: params.knobs.decayHalfLifeDays,
    importanceBoost: params.knobs.importanceBoost,
    frequencyBoost: params.knobs.frequencyBoost,
  });
}
