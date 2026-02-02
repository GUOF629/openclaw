import crypto from "node:crypto";
import type { UpdateMemoryIndexResponse } from "./types.js";
import { SessionAnalyzer } from "./analyzer.js";
import { EmbeddingModel } from "./embeddings.js";
import { computeImportance } from "./importance.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore, type QdrantMemoryPayload } from "./qdrant.js";
import { looksSensitive } from "./safety.js";
import { clamp, stableHash } from "./utils.js";

export class DeepMemoryUpdater {
  private readonly analyzer: SessionAnalyzer;
  private readonly embedder: EmbeddingModel;
  private readonly qdrant: QdrantStore;
  private readonly neo4j: Neo4jStore;
  private readonly minSemanticScore: number;
  private readonly importanceThreshold: number;
  private readonly maxMemoriesPerUpdate: number;
  private readonly dedupeScore: number;
  private readonly relatedTopK: number;
  private readonly sensitiveFilterEnabled: boolean;

  constructor(params: {
    analyzer: SessionAnalyzer;
    embedder: EmbeddingModel;
    qdrant: QdrantStore;
    neo4j: Neo4jStore;
    minSemanticScore: number;
    importanceThreshold: number;
    maxMemoriesPerUpdate: number;
    dedupeScore: number;
    relatedTopK: number;
    sensitiveFilterEnabled: boolean;
  }) {
    this.analyzer = params.analyzer;
    this.embedder = params.embedder;
    this.qdrant = params.qdrant;
    this.neo4j = params.neo4j;
    this.minSemanticScore = params.minSemanticScore;
    this.importanceThreshold = params.importanceThreshold;
    this.maxMemoriesPerUpdate = params.maxMemoriesPerUpdate;
    this.dedupeScore = params.dedupeScore;
    this.relatedTopK = Math.max(0, params.relatedTopK);
    this.sensitiveFilterEnabled = params.sensitiveFilterEnabled;
  }

  async update(params: {
    namespace: string;
    sessionId: string;
    messages: unknown[];
  }): Promise<UpdateMemoryIndexResponse> {
    const messageCount = Array.isArray(params.messages) ? params.messages.length : 0;
    const transcriptHash = crypto
      .createHash("sha256")
      .update(JSON.stringify(params.messages ?? []))
      .digest("hex");

    // Ensure Session exists (needed for idempotency meta).
    await this.neo4j.upsertSession({ namespace: params.namespace, sessionId: params.sessionId });
    try {
      const meta = await this.neo4j.getSessionIngestMeta({
        namespace: params.namespace,
        sessionId: params.sessionId,
      });
      if (meta.transcriptHash === transcriptHash) {
        return { status: "skipped", memories_added: 0, memories_filtered: 0 };
      }
    } catch {
      // If Neo4j is unavailable, we proceed best-effort (may duplicate).
    }

    const analysis = this.analyzer.analyze({
      sessionId: params.sessionId,
      messages: params.messages,
      maxMemoriesPerSession: this.maxMemoriesPerUpdate,
      importanceThreshold: this.importanceThreshold,
    });

    for (const t of analysis.topics) {
      await this.neo4j.upsertTopic({ namespace: params.namespace, topic: t });
      await this.neo4j.linkSessionTopic({ namespace: params.namespace, sessionId: params.sessionId, topicName: t.name });
    }
    for (const e of analysis.entities) {
      await this.neo4j.upsertEntity({ namespace: params.namespace, entity: e });
      for (const t of analysis.topics.slice(0, 5)) {
        await this.neo4j.linkTopicEntity({
          namespace: params.namespace,
          topicName: t.name,
          entityName: e.name,
          entityType: e.type,
        });
      }
    }
    for (const ev of analysis.events) {
      await this.neo4j.upsertEvent({ namespace: params.namespace, event: ev });
      const eventId = this.neo4j.eventId({ namespace: params.namespace, event: ev });
      await this.neo4j.linkSessionEvent({ namespace: params.namespace, sessionId: params.sessionId, eventId });
      for (const t of analysis.topics.slice(0, 3)) {
        await this.neo4j.linkEventTopic({ namespace: params.namespace, eventId, topicName: t.name });
      }
      const topEntities = analysis.entities.slice(0, 3);
      for (const ent of topEntities) {
        await this.neo4j.linkEventEntity({
          namespace: params.namespace,
          eventId,
          entityName: ent.name,
          entityType: ent.type,
        });
      }
    }

    let added = 0;
    let filtered = analysis.filtered.filtered;
    const entityTypeByName = new Map<string, string>();
    for (const e of analysis.entities) {
      entityTypeByName.set(e.name, e.type);
    }
    const nowIso = new Date().toISOString();
    for (const draft of analysis.drafts) {
      if (this.sensitiveFilterEnabled && looksSensitive(draft.content)) {
        filtered += 1;
        continue;
      }

      const vec = await this.embedder.embed(draft.content);

      // Novelty + global dedupe (best-effort). If Qdrant is down we fall back to session hash.
      let bestId: string | null = null;
      let bestScore = 0;
      try {
        const top = await this.qdrant.search({
          vector: vec,
          limit: 1,
          minScore: 0,
          namespace: params.namespace,
        });
        const best = top[0];
        if (best?.id) {
          bestId = best.id;
          bestScore = best.score ?? 0;
        }
      } catch {
        // ignore
      }

      const novelty = clamp(1 - bestScore, 0, 1);
      const importance = computeImportance({
        frequency: draft.signals.frequency,
        novelty,
        user_intent: draft.signals.user_intent,
        length: draft.signals.length,
      });
      if (importance < this.importanceThreshold) {
        filtered += 1;
        continue;
      }

      const isDuplicate = bestId && bestScore >= this.dedupeScore;
      const rawId = isDuplicate ? bestId! : `mem_${stableHash(`${params.sessionId}:${draft.content}`)}`;
      const id = rawId.includes("::") ? rawId : `${params.namespace}::${rawId}`;

      const mergedEntities = new Set(draft.entities);
      const mergedTopics = new Set(draft.topics);
      let mergedImportance = importance;
      let mergedFrequency = 1;
      if (isDuplicate) {
        try {
          const existing = await this.qdrant.getMemory(id);
          const p = existing?.payload;
          if (p?.entities) p.entities.forEach((e) => mergedEntities.add(e));
          if (p?.topics) p.topics.forEach((t) => mergedTopics.add(t));
          mergedImportance = Math.max(mergedImportance, p?.importance ?? 0);
          mergedFrequency = (p?.frequency ?? 1) + 1;
          // Preserve durable classification/slotting if present.
          if (!draft.memoryKey && p?.memory_key) {
            draft.memoryKey = p.memory_key;
          }
          if (!draft.subject && p?.subject) {
            draft.subject = p.subject;
          }
          if (!draft.kind && p?.kind) {
            draft.kind = p.kind as any;
          }
          if (!draft.expiresAt && p?.expires_at) {
            draft.expiresAt = p.expires_at;
          }
          if (typeof draft.confidence !== "number" && typeof p?.confidence === "number") {
            draft.confidence = p.confidence;
          }
        } catch {
          // ignore
        }
      }

      const mem = {
        kind: draft.kind ?? "fact",
        memoryKey: draft.memoryKey,
        subject: draft.subject,
        expiresAt: draft.expiresAt,
        confidence: draft.confidence,
        content: draft.content,
        importance: mergedImportance,
        entities: Array.from(mergedEntities).slice(0, 10),
        topics: Array.from(mergedTopics).slice(0, 10),
        createdAt: draft.createdAt,
      };

      await this.neo4j.upsertMemory({
        namespace: params.namespace,
        id,
        memory: mem,
        sessionId: params.sessionId,
      });
      for (const t of mem.topics) {
        await this.neo4j.linkMemoryTopic({ namespace: params.namespace, memoryId: id, topicName: t });
      }
      for (const name of mem.entities) {
        const type = entityTypeByName.get(name) ?? "other";
        await this.neo4j.linkMemoryEntity({
          namespace: params.namespace,
          memoryId: id,
          entityName: name,
          entityType: type,
        });
      }

      // Upsert to Qdrant (best-effort).
      try {
        const payload: QdrantMemoryPayload = {
          id,
          namespace: params.namespace,
          kind: mem.kind,
          memory_key: mem.memoryKey,
          subject: mem.subject,
          expires_at: mem.expiresAt,
          confidence: mem.confidence,
          content: mem.content,
          session_id: params.sessionId,
          source_transcript_hash: transcriptHash,
          source_message_count: messageCount,
          created_at: mem.createdAt,
          updated_at: nowIso,
          importance: mem.importance,
          frequency: mergedFrequency,
          entities: mem.entities,
          topics: mem.topics,
        };
        await this.qdrant.upsertMemory({ id, vector: vec, payload });
      } catch {
        // ignore
      }

      // Create synapse links: connect this memory to nearby ones (best-effort).
      if (this.relatedTopK > 0) {
        try {
          const hits = await this.qdrant.search({
            vector: vec,
            limit: Math.max(1, this.relatedTopK + 1),
            minScore: Math.max(this.minSemanticScore, 0.8),
            namespace: params.namespace,
          });
          for (const hit of hits) {
            if (!hit?.id || hit.id === id) continue;
            await this.neo4j.linkMemoryRelated({
              namespace: params.namespace,
              fromMemoryId: id,
              toMemoryId: hit.id,
              score: hit.score ?? 0,
            });
          }
        } catch {
          // ignore
        }
      }

      added += 1;
    }

    // Persist idempotency markers (best-effort).
    try {
      await this.neo4j.setSessionIngestMeta({
        namespace: params.namespace,
        sessionId: params.sessionId,
        transcriptHash,
        messageCount,
      });
    } catch {
      // ignore
    }

    return {
      status: "processed",
      memories_added: added,
      memories_filtered: filtered,
    };
  }
}

