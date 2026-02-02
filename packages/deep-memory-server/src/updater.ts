import type { UpdateMemoryIndexResponse } from "./types.js";
import { SessionAnalyzer } from "./analyzer.js";
import { EmbeddingModel } from "./embeddings.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore, type QdrantMemoryPayload } from "./qdrant.js";
import { stableHash } from "./utils.js";

export class DeepMemoryUpdater {
  private readonly analyzer: SessionAnalyzer;
  private readonly embedder: EmbeddingModel;
  private readonly qdrant: QdrantStore;
  private readonly neo4j: Neo4jStore;
  private readonly minSemanticScore: number;

  constructor(params: {
    analyzer: SessionAnalyzer;
    embedder: EmbeddingModel;
    qdrant: QdrantStore;
    neo4j: Neo4jStore;
    minSemanticScore: number;
  }) {
    this.analyzer = params.analyzer;
    this.embedder = params.embedder;
    this.qdrant = params.qdrant;
    this.neo4j = params.neo4j;
    this.minSemanticScore = params.minSemanticScore;
  }

  async update(params: { sessionId: string; messages: unknown[] }): Promise<UpdateMemoryIndexResponse> {
    const analysis = this.analyzer.analyze({
      sessionId: params.sessionId,
      messages: params.messages,
      maxMemoriesPerSession: 20,
      importanceThreshold: 0.5,
    });

    // Ensure Session exists.
    await this.neo4j.upsertSession({ sessionId: params.sessionId });
    for (const t of analysis.topics) {
      await this.neo4j.upsertTopic(t);
      await this.neo4j.linkSessionTopic({ sessionId: params.sessionId, topicName: t.name });
    }
    for (const e of analysis.entities) {
      await this.neo4j.upsertEntity(e);
      for (const t of analysis.topics.slice(0, 5)) {
        await this.neo4j.linkTopicEntity({ topicName: t.name, entityId: `entity:${e.type}:${e.name}` });
      }
    }
    for (const ev of analysis.events) {
      await this.neo4j.upsertEvent(ev);
    }

    let added = 0;
    for (const mem of analysis.memories) {
      // Dedup across global store via Qdrant similarity.
      const vec = await this.embedder.embed(mem.content);
      let id = `mem_${stableHash(`${params.sessionId}:${mem.content}`)}`;
      try {
        const top = await this.qdrant.search({
          vector: vec,
          limit: 1,
          minScore: 0.9,
        });
        const best = top[0];
        if (best?.id && best.score >= 0.9) {
          // Treat as duplicate: reuse existing id and "update" importance by re-upserting.
          id = best.id;
        }
      } catch {
        // If Qdrant is unavailable, we still write Neo4j; vector upsert will be skipped by caller-level fallback.
      }

      await this.neo4j.upsertMemory({
        id,
        memory: mem,
        sessionId: params.sessionId,
      });
      for (const t of mem.topics) {
        await this.neo4j.linkMemoryTopic({ memoryId: id, topicName: t });
      }
      for (const name of mem.entities) {
        const type = "other";
        await this.neo4j.linkMemoryEntity({
          memoryId: id,
          entityId: `entity:${type}:${name}`,
          entityName: name,
          entityType: type,
        });
      }

      const payload: QdrantMemoryPayload = {
        id,
        content: mem.content,
        session_id: params.sessionId,
        created_at: mem.createdAt,
        importance: mem.importance,
        entities: mem.entities,
        topics: mem.topics,
      };
      await this.qdrant.upsertMemory({ id, vector: vec, payload });
      added += 1;
    }

    return {
      status: "processed",
      memories_added: added,
      memories_filtered: analysis.filtered.filtered,
    };
  }
}

