import type { RetrieveContextResponse } from "./types.js";
import { EmbeddingModel } from "./embeddings.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore } from "./qdrant.js";
import { clamp } from "./utils.js";

export class DeepMemoryRetriever {
  private readonly embedder: EmbeddingModel;
  private readonly qdrant: QdrantStore;
  private readonly neo4j: Neo4jStore;
  private readonly minSemanticScore: number;
  private readonly semanticWeight: number;
  private readonly relationWeight: number;
  private readonly decayHalfLifeDays: number;
  private readonly importanceBoost: number;
  private readonly frequencyBoost: number;

  constructor(params: {
    embedder: EmbeddingModel;
    qdrant: QdrantStore;
    neo4j: Neo4jStore;
    minSemanticScore: number;
    semanticWeight: number;
    relationWeight: number;
    decayHalfLifeDays: number;
    importanceBoost: number;
    frequencyBoost: number;
  }) {
    this.embedder = params.embedder;
    this.qdrant = params.qdrant;
    this.neo4j = params.neo4j;
    this.minSemanticScore = params.minSemanticScore;
    const sum = Math.max(0, params.semanticWeight) + Math.max(0, params.relationWeight);
    this.semanticWeight = sum > 0 ? params.semanticWeight / sum : 0.6;
    this.relationWeight = sum > 0 ? params.relationWeight / sum : 0.4;
    this.decayHalfLifeDays = Math.max(1, params.decayHalfLifeDays);
    this.importanceBoost = Math.max(0, params.importanceBoost);
    this.frequencyBoost = Math.max(0, params.frequencyBoost);
  }

  private scoreWithDecay(params: { relevance: number; importance: number; frequency: number; lastSeenAt?: string }): number {
    const base = Math.max(0, params.relevance);
    const imp = clamp(params.importance ?? 0, 0, 1);
    const freq = Math.max(0, params.frequency ?? 0);
    const freqNorm = clamp(Math.log1p(freq) / Math.log(10), 0, 1); // ~1 at 9+
    const boost = (1 + this.importanceBoost * imp) * (1 + this.frequencyBoost * freqNorm);

    let decay = 1;
    if (params.lastSeenAt) {
      const t = Date.parse(params.lastSeenAt);
      if (!Number.isNaN(t)) {
        const ageDays = Math.max(0, (Date.now() - t) / (24 * 3600_000));
        decay = Math.pow(0.5, ageDays / this.decayHalfLifeDays);
        decay = clamp(decay, 0.1, 1);
      }
    }
    return base * boost * decay;
  }

  async retrieve(params: {
    namespace: string;
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
      frequency: number;
      lastSeenAt?: string;
      kind?: string;
      memoryKey?: string;
      subject?: string;
      expiresAt?: string;
      confidence?: number;
      semantic: number;
      relation: number;
      sources: Set<"qdrant" | "neo4j">;
    };
    const resultsById = new Map<string, Merged>();

    const getOrInit = (
      id: string,
      seed: {
        content: string;
        importance: number;
        frequency?: number;
        lastSeenAt?: string;
        kind?: string;
        memoryKey?: string;
        subject?: string;
        expiresAt?: string;
        confidence?: number;
      },
    ): Merged => {
      const existing = resultsById.get(id);
      if (existing) {
        return existing;
      }
      const created: Merged = {
        id,
        content: seed.content,
        importance: seed.importance,
        frequency: seed.frequency ?? 0,
        lastSeenAt: seed.lastSeenAt,
        kind: seed.kind,
        memoryKey: seed.memoryKey,
        subject: seed.subject,
        expiresAt: seed.expiresAt,
        confidence: seed.confidence,
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
        namespace: params.namespace,
      });
      for (const hit of hits) {
        const payload = hit.payload;
        if (!payload?.content) {
          continue;
        }
        const existing = getOrInit(hit.id, {
          content: payload.content,
          importance: payload.importance ?? 0,
          frequency: payload.frequency ?? 0,
          lastSeenAt: payload.updated_at ?? payload.created_at,
          kind: payload.kind,
          memoryKey: payload.memory_key,
          subject: payload.subject,
          expiresAt: payload.expires_at,
          confidence: payload.confidence,
        });
        existing.content = existing.content || payload.content;
        existing.importance = Math.max(existing.importance ?? 0, payload.importance ?? 0);
        existing.frequency = Math.max(existing.frequency ?? 0, payload.frequency ?? 0);
        existing.lastSeenAt = existing.lastSeenAt ?? (payload.updated_at ?? payload.created_at);
        existing.semantic = Math.max(existing.semantic ?? 0, hit.score ?? 0);
        existing.sources.add("qdrant");
      }
    } catch {
      // ignore; handled by fallback path
    }

    // Neo4j relation retrieval (best-effort).
    try {
      const related = await this.neo4j.queryRelatedMemories({
        namespace: params.namespace,
        entities: params.entities,
        topics: params.topics,
        limit: candidates,
      });
      for (const r of related) {
        const existing = getOrInit(r.id, {
          content: r.content,
          importance: r.importance,
          frequency: r.frequency,
          lastSeenAt: r.lastSeenAt,
          kind: r.kind,
          memoryKey: r.memoryKey,
          subject: r.subject,
          expiresAt: r.expiresAt,
          confidence: r.confidence,
        });
        existing.content = existing.content || r.content;
        existing.importance = Math.max(existing.importance ?? 0, r.importance ?? 0);
        existing.frequency = Math.max(existing.frequency ?? 0, r.frequency ?? 0);
        existing.lastSeenAt = existing.lastSeenAt ?? r.lastSeenAt;
        existing.relation = Math.max(existing.relation ?? 0, r.relationScore ?? 0);
        existing.sources.add("neo4j");
      }
    } catch {
      // ignore
    }

    const now = Date.now();
    const isExpired = (expiresAt?: string) => {
      if (!expiresAt) return false;
      const t = Date.parse(expiresAt);
      return Number.isFinite(t) && t > 0 && t < now;
    };

    const merged = Array.from(resultsById.values())
      .filter((r) => !isExpired(r.expiresAt))
      .map((r) => {
        const semantic = r.semantic ?? 0;
        const relation = r.relation ?? 0;
        const relevance = semanticWeight * semantic + relationWeight * relation;
        const final = this.scoreWithDecay({
          relevance,
          importance: r.importance,
          frequency: r.frequency,
          lastSeenAt: r.lastSeenAt,
        });
        return {
          id: r.id,
          content: r.content,
          importance: r.importance,
          relevance: final,
          semantic_score: r.semantic,
          relation_score: r.relation,
          kind: r.kind,
          memory_key: r.memoryKey,
          subject: r.subject,
          sources: Array.from(r.sources),
        };
      })
      .sort((a, b) => b.relevance - a.relevance);

    // Conflict resolution: group by memory_key (slot) and pick the best per group.
    const byKey = new Map<string, (typeof merged)[number]>();
    for (const m of merged) {
      const k = m.memory_key ?? m.id;
      const prev = byKey.get(k);
      if (!prev) {
        byKey.set(k, m);
        continue;
      }
      // Prefer higher relevance; tie-break by importance.
      if (m.relevance > prev.relevance) {
        byKey.set(k, m);
      } else if (m.relevance === prev.relevance && (m.importance ?? 0) > (prev.importance ?? 0)) {
        byKey.set(k, m);
      }
    }
    const resolved = Array.from(byKey.values())
      .sort((a, b) => b.relevance - a.relevance)
      .slice(0, params.maxMemories);

    const contextLines = resolved.map((m, idx) => {
      const score = m.relevance.toFixed(2);
      const imp = m.importance.toFixed(2);
      return `${idx + 1}. (${score}, imp=${imp}) ${m.content}`;
    });

    return {
      entities: params.entities,
      topics: params.topics,
      memories: resolved.map((m) => ({
        id: m.id,
        content: m.content,
        importance: m.importance,
        relevance: m.relevance,
        semantic_score: m.semantic_score,
        relation_score: m.relation_score,
        sources: m.sources,
      })),
      context: contextLines.length ? `Relevant long-term memory:\n${contextLines.join("\n")}` : "",
    };
  }
}

