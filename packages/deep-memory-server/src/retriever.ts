import type { RetrieveContextResponse } from "./types.js";
import { EmbeddingModel } from "./embeddings.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore } from "./qdrant.js";

export class DeepMemoryRetriever {
  private readonly embedder: EmbeddingModel;
  private readonly qdrant: QdrantStore;
  private readonly neo4j: Neo4jStore;
  private readonly minSemanticScore: number;
  private readonly semanticWeight: number;
  private readonly relationWeight: number;

  constructor(params: {
    embedder: EmbeddingModel;
    qdrant: QdrantStore;
    neo4j: Neo4jStore;
    minSemanticScore: number;
    semanticWeight: number;
    relationWeight: number;
  }) {
    this.embedder = params.embedder;
    this.qdrant = params.qdrant;
    this.neo4j = params.neo4j;
    this.minSemanticScore = params.minSemanticScore;
    const sum = Math.max(0, params.semanticWeight) + Math.max(0, params.relationWeight);
    this.semanticWeight = sum > 0 ? params.semanticWeight / sum : 0.6;
    this.relationWeight = sum > 0 ? params.relationWeight / sum : 0.4;
  }

  async retrieve(params: {
    userInput: string;
    sessionId: string;
    maxMemories: number;
    entities: string[];
    topics: string[];
  }): Promise<RetrieveContextResponse> {
    const semanticWeight = this.semanticWeight;
    const relationWeight = this.relationWeight;

    const candidates = Math.min(50, Math.max(10, params.maxMemories * 5));
    type Merged = {
      id: string;
      content: string;
      importance: number;
      semantic: number;
      relation: number;
      sources: Set<"qdrant" | "neo4j">;
    };
    const resultsById = new Map<string, Merged>();

    const getOrInit = (id: string, seed: { content: string; importance: number }): Merged => {
      const existing = resultsById.get(id);
      if (existing) {
        return existing;
      }
      const created: Merged = {
        id,
        content: seed.content,
        importance: seed.importance,
        semantic: 0,
        relation: 0,
        sources: new Set(),
      };
      resultsById.set(id, created);
      return created;
    };

    // Qdrant semantic retrieval (best-effort).
    try {
      const vec = await this.embedder.embed(params.userInput);
      const hits = await this.qdrant.search({
        vector: vec,
        limit: candidates,
        minScore: this.minSemanticScore,
      });
      for (const hit of hits) {
        const payload = hit.payload;
        if (!payload?.content) {
          continue;
        }
        const existing = getOrInit(hit.id, {
          content: payload.content,
          importance: payload.importance ?? 0,
        });
        existing.content = existing.content || payload.content;
        existing.importance = Math.max(existing.importance ?? 0, payload.importance ?? 0);
        existing.semantic = Math.max(existing.semantic ?? 0, hit.score ?? 0);
        existing.sources.add("qdrant");
      }
    } catch {
      // ignore; handled by fallback path
    }

    // Neo4j relation retrieval (best-effort).
    try {
      const related = await this.neo4j.queryRelatedMemories({
        entities: params.entities,
        topics: params.topics,
        limit: candidates,
      });
      for (const r of related) {
        const existing = getOrInit(r.id, { content: r.content, importance: r.importance });
        existing.content = existing.content || r.content;
        existing.importance = Math.max(existing.importance ?? 0, r.importance ?? 0);
        existing.relation = Math.max(existing.relation ?? 0, r.relationScore ?? 0);
        existing.sources.add("neo4j");
      }
    } catch {
      // ignore
    }

    const merged = Array.from(resultsById.values())
      .map((r) => {
        const semantic = r.semantic ?? 0;
        const relation = r.relation ?? 0;
        const relevance = semanticWeight * semantic + relationWeight * relation;
        return {
          id: r.id,
          content: r.content,
          importance: r.importance,
          relevance,
          semantic_score: r.semantic,
          relation_score: r.relation,
          sources: Array.from(r.sources),
        };
      })
      .sort((a, b) => b.relevance - a.relevance)
      .slice(0, params.maxMemories);

    const contextLines = merged.map((m, idx) => {
      const score = m.relevance.toFixed(2);
      const imp = m.importance.toFixed(2);
      return `${idx + 1}. (${score}, imp=${imp}) ${m.content}`;
    });

    return {
      entities: params.entities,
      topics: params.topics,
      memories: merged,
      context: contextLines.length ? `Relevant long-term memory:\n${contextLines.join("\n")}` : "",
    };
  }
}

