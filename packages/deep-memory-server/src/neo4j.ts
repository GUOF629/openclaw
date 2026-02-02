import neo4j, { type Driver } from "neo4j-driver";
import type { CandidateMemory, ExtractedEntity, ExtractedEvent, ExtractedTopic } from "./types.js";

export class Neo4jStore {
  private readonly driver: Driver;

  constructor(params: { uri: string; user: string; password: string }) {
    this.driver = neo4j.driver(params.uri, neo4j.auth.basic(params.user, params.password), {
      // Keep defaults; the caller controls availability via retry/fallback.
    });
  }

  async close(): Promise<void> {
    await this.driver.close();
  }

  async ensureSchema(): Promise<void> {
    const session = this.driver.session();
    try {
      // Constraints for fast upserts.
      await session.run(`CREATE CONSTRAINT deepmem_session_id IF NOT EXISTS FOR (s:Session) REQUIRE s.id IS UNIQUE`);
      await session.run(`CREATE CONSTRAINT deepmem_memory_id IF NOT EXISTS FOR (m:Memory) REQUIRE m.id IS UNIQUE`);
      await session.run(`CREATE CONSTRAINT deepmem_topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE t.id IS UNIQUE`);
      await session.run(`CREATE CONSTRAINT deepmem_entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE`);
      await session.run(`CREATE CONSTRAINT deepmem_event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE`);
    } finally {
      await session.close();
    }
  }

  private prefix(namespace: string): string {
    return `${namespace}::`;
  }

  private sessionNodeId(namespace: string, sessionId: string): string {
    return `${this.prefix(namespace)}session::${sessionId}`;
  }

  private topicNodeId(namespace: string, topicName: string): string {
    return `${this.prefix(namespace)}topic::${topicName}`;
  }

  private entityNodeId(namespace: string, type: string, name: string): string {
    return `${this.prefix(namespace)}entity::${type}::${name}`;
  }

  private eventNodeId(namespace: string, event: ExtractedEvent): string {
    return `${this.prefix(namespace)}event::${event.type}::${event.timestamp}::${event.summary}`.slice(0, 240);
  }

  async upsertSession(params: {
    namespace: string;
    sessionId: string;
    startTime?: string;
    endTime?: string;
    summary?: string;
  }) {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (s:Session {id: $id})
         ON CREATE SET s.start_time = coalesce($start_time, datetime())
         SET s.end_time = $end_time,
             s.summary = $summary,
             s.namespace = $ns`,
        {
          id: this.sessionNodeId(params.namespace, params.sessionId),
          ns: params.namespace,
          start_time: params.startTime ?? null,
          end_time: params.endTime ?? null,
          summary: params.summary ?? null,
        },
      );
    } finally {
      await session.close();
    }
  }

  async getSessionIngestMeta(params: { namespace: string; sessionId: string }): Promise<{
    transcriptHash?: string;
    messageCount?: number;
  }> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        `MATCH (s:Session {id: $id})
         RETURN s.last_transcript_hash AS hash, s.last_message_count AS count`,
        { id: this.sessionNodeId(params.namespace, params.sessionId) },
      );
      const row = res.records[0];
      if (!row) {
        return {};
      }
      const hash = row.get("hash") as string | null | undefined;
      const count = row.get("count") as number | null | undefined;
      return {
        transcriptHash: typeof hash === "string" && hash.length > 0 ? hash : undefined,
        messageCount: typeof count === "number" && Number.isFinite(count) ? count : undefined,
      };
    } finally {
      await session.close();
    }
  }

  async setSessionIngestMeta(params: {
    namespace: string;
    sessionId: string;
    transcriptHash: string;
    messageCount: number;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $id})
         SET s.last_transcript_hash = $hash,
             s.last_message_count = $count,
             s.last_ingested_at = datetime()`,
        {
          id: this.sessionNodeId(params.namespace, params.sessionId),
          hash: params.transcriptHash,
          count: params.messageCount,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertTopic(params: { namespace: string; topic: ExtractedTopic }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (t:Topic {id: $id})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         SET t.frequency = coalesce(t.frequency, 0) + $frequency,
             t.importance = greatest(coalesce(t.importance, 0), $importance)`,
        {
          id: this.topicNodeId(params.namespace, params.topic.name),
          ns: params.namespace,
          name: params.topic.name,
          frequency: params.topic.frequency,
          importance: params.topic.importance,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertEntity(params: { namespace: string; entity: ExtractedEntity }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (e:Entity {id: $id})
         ON CREATE SET e.name = $name, e.type = $type, e.frequency = 0, e.namespace = $ns
         SET e.frequency = coalesce(e.frequency, 0) + $frequency`,
        {
          id: this.entityNodeId(params.namespace, params.entity.type, params.entity.name),
          ns: params.namespace,
          name: params.entity.name,
          type: params.entity.type,
          frequency: params.entity.frequency,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertEvent(params: { namespace: string; event: ExtractedEvent }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (e:Event {id: $id})
         ON CREATE SET e.type = $type, e.summary = $summary, e.timestamp = datetime($ts), e.namespace = $ns
         SET e.type = $type, e.summary = $summary`,
        {
          id: this.eventNodeId(params.namespace, params.event),
          ns: params.namespace,
          type: params.event.type,
          summary: params.event.summary,
          ts: params.event.timestamp,
        },
      );
    } finally {
      await session.close();
    }
  }

  eventId(params: { namespace: string; event: ExtractedEvent }): string {
    return this.eventNodeId(params.namespace, params.event);
  }

  async linkSessionEvent(params: { namespace: string; sessionId: string; eventId: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MATCH (e:Event {id: $eid})
         MERGE (s)-[:HAS_EVENT]->(e)`,
        { sid: this.sessionNodeId(params.namespace, params.sessionId), eid: params.eventId },
      );
    } finally {
      await session.close();
    }
  }

  async linkEventTopic(params: { namespace: string; eventId: string; topicName: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (e:Event {id: $eid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (e)-[:ABOUT_TOPIC]->(t)`,
        { eid: params.eventId, tid: this.topicNodeId(params.namespace, params.topicName), name: params.topicName, ns: params.namespace },
      );
    } finally {
      await session.close();
    }
  }

  async linkEventEntity(params: {
    namespace: string;
    eventId: string;
    entityName: string;
    entityType: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (e:Event {id: $eid})
         MERGE (x:Entity {id: $xid})
         ON CREATE SET x.name = $name, x.type = $type, x.frequency = 0, x.namespace = $ns
         MERGE (e)-[:ABOUT_ENTITY]->(x)`,
        {
          eid: params.eventId,
          xid: this.entityNodeId(params.namespace, params.entityType, params.entityName),
          name: params.entityName,
          type: params.entityType,
          ns: params.namespace,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkSessionTopic(params: { namespace: string; sessionId: string; topicName: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (s)-[:CONTAINS]->(t)`,
        { sid: this.sessionNodeId(params.namespace, params.sessionId), tid: this.topicNodeId(params.namespace, params.topicName), name: params.topicName, ns: params.namespace },
      );
    } finally {
      await session.close();
    }
  }

  async linkTopicEntity(params: { namespace: string; topicName: string; entityName: string; entityType: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (t:Topic {id: $tid})
         MATCH (e:Entity {id: $eid})
         MERGE (t)-[:MENTIONS]->(e)`,
        {
          tid: this.topicNodeId(params.namespace, params.topicName),
          eid: this.entityNodeId(params.namespace, params.entityType, params.entityName),
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertMemory(params: {
    namespace: string;
    id: string;
    memory: CandidateMemory;
    sessionId: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MERGE (m:Memory {id: $id})
         ON CREATE SET m.content = $content, m.importance = $importance, m.created_at = datetime($created_at), m.frequency = 0, m.namespace = $ns
         SET m.content = $content,
             m.importance = greatest(coalesce(m.importance, 0), $importance),
             m.frequency = coalesce(m.frequency, 0) + 1,
             m.last_seen_at = datetime()
         MERGE (m)-[:FROM_SESSION]->(s)`,
        {
          sid: this.sessionNodeId(params.namespace, params.sessionId),
          id: params.id,
          ns: params.namespace,
          content: params.memory.content,
          importance: params.memory.importance,
          created_at: params.memory.createdAt,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryTopic(params: { namespace: string; memoryId: string; topicName: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (m:Memory {id: $mid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (m)-[:ABOUT_TOPIC]->(t)`,
        { mid: params.memoryId, tid: this.topicNodeId(params.namespace, params.topicName), name: params.topicName, ns: params.namespace },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryEntity(params: { namespace: string; memoryId: string; entityName: string; entityType: string }) {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (m:Memory {id: $mid})
         MERGE (e:Entity {id: $eid})
         ON CREATE SET e.name = $name, e.type = $type, e.frequency = 0, e.namespace = $ns
         MERGE (m)-[:ABOUT_ENTITY]->(e)`,
        {
          mid: params.memoryId,
          eid: this.entityNodeId(params.namespace, params.entityType, params.entityName),
          name: params.entityName,
          type: params.entityType,
          ns: params.namespace,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryRelated(params: { namespace: string; fromMemoryId: string; toMemoryId: string; score: number }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (a:Memory {id: $a})
         MATCH (b:Memory {id: $b})
         MERGE (a)-[r:RELATED_TO]->(b)
         SET r.score = greatest(coalesce(r.score, 0), $score),
             r.updated_at = datetime()`,
        { a: params.fromMemoryId, b: params.toMemoryId, score: params.score },
      );
    } finally {
      await session.close();
    }
  }

  /**
   * Relation query: given entities/topics, return related memories and a relationScore.
   * Score is heuristic 0..1 based on matched signals.
   */
  async queryRelatedMemories(params: {
    namespace: string;
    entities: string[];
    topics: string[];
    limit: number;
  }): Promise<Array<{ id: string; content: string; importance: number; frequency: number; lastSeenAt: string; relationScore: number }>> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        `WITH $entities AS entities, $topics AS topics, $prefix AS prefix
         // Direct links: Memory -> Entity/Topic
         CALL {
           WITH entities, topics, prefix
           MATCH (m:Memory)-[:ABOUT_ENTITY]->(e:Entity)
           WHERE e.name IN entities AND m.id STARTS WITH prefix
           RETURN m, 1.0 AS score
           UNION ALL
           WITH entities, topics, prefix
           MATCH (m:Memory)-[:ABOUT_TOPIC]->(t:Topic)
           WHERE t.name IN topics AND m.id STARTS WITH prefix
           RETURN m, 0.8 AS score
         }
         WITH m, sum(score) AS directScore
         // Two-hop: Memory->Topic->Entity or Memory->Entity<-MENTIONS-Topic
         CALL {
           WITH m, entities, topics
           OPTIONAL MATCH (m:Memory)-[:ABOUT_TOPIC]->(t:Topic)-[:MENTIONS]->(e:Entity)
           WHERE e.name IN entities
           RETURN coalesce(count(e), 0) AS hopHits
         }
         WITH m, directScore, hopHits
         // RELATED_TO expansion (synapse links)
         CALL {
           WITH m
           OPTIONAL MATCH (m)-[r:RELATED_TO]->(m2:Memory)
           RETURN coalesce(max(r.score), 0.0) AS relatedBoost
         }
         WITH m,
              (directScore + toFloat(hopHits) * 0.4 + relatedBoost * 0.6) AS rawScore
         WHERE rawScore > 0
         RETURN m.id AS id,
                m.content AS content,
                coalesce(m.importance, 0.0) AS importance,
                coalesce(m.frequency, 0) AS frequency,
                toString(coalesce(m.last_seen_at, m.created_at)) AS lastSeenAt,
                least(1.0, rawScore / 2.0) AS relationScore
         ORDER BY relationScore DESC, importance DESC
         LIMIT $limit`,
        {
          entities: params.entities,
          topics: params.topics,
          prefix: this.prefix(params.namespace),
          limit: params.limit,
        },
      );
      return res.records.map((r) => ({
        id: String(r.get("id")),
        content: String(r.get("content")),
        importance: Number(r.get("importance") ?? 0),
        frequency: Number(r.get("frequency") ?? 0),
        lastSeenAt: String(r.get("lastSeenAt") ?? ""),
        relationScore: Number(r.get("relationScore") ?? 0),
      }));
    } finally {
      await session.close();
    }
  }
}

