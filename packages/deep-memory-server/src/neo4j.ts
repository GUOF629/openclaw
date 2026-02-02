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

  async upsertSession(params: { sessionId: string; startTime?: string; endTime?: string; summary?: string }) {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (s:Session {id: $id})
         ON CREATE SET s.start_time = coalesce($start_time, datetime())
         SET s.end_time = $end_time,
             s.summary = $summary`,
        {
          id: params.sessionId,
          start_time: params.startTime ?? null,
          end_time: params.endTime ?? null,
          summary: params.summary ?? null,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertTopic(topic: ExtractedTopic): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (t:Topic {id: $id})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0
         SET t.frequency = coalesce(t.frequency, 0) + $frequency,
             t.importance = greatest(coalesce(t.importance, 0), $importance)`,
        {
          id: `topic:${topic.name}`,
          name: topic.name,
          frequency: topic.frequency,
          importance: topic.importance,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertEntity(entity: ExtractedEntity): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (e:Entity {id: $id})
         ON CREATE SET e.name = $name, e.type = $type, e.frequency = 0
         SET e.frequency = coalesce(e.frequency, 0) + $frequency`,
        {
          id: `entity:${entity.type}:${entity.name}`,
          name: entity.name,
          type: entity.type,
          frequency: entity.frequency,
        },
      );
    } finally {
      await session.close();
    }
  }

  async upsertEvent(event: ExtractedEvent): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MERGE (e:Event {id: $id})
         ON CREATE SET e.type = $type, e.summary = $summary, e.timestamp = datetime($ts)
         SET e.type = $type, e.summary = $summary`,
        {
          id: `event:${event.type}:${event.timestamp}:${event.summary}`.slice(0, 200),
          type: event.type,
          summary: event.summary,
          ts: event.timestamp,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkSessionTopic(params: { sessionId: string; topicName: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0
         MERGE (s)-[:CONTAINS]->(t)`,
        { sid: params.sessionId, tid: `topic:${params.topicName}`, name: params.topicName },
      );
    } finally {
      await session.close();
    }
  }

  async linkTopicEntity(params: { topicName: string; entityId: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (t:Topic {id: $tid})
         MATCH (e:Entity {id: $eid})
         MERGE (t)-[:MENTIONS]->(e)`,
        { tid: `topic:${params.topicName}`, eid: params.entityId },
      );
    } finally {
      await session.close();
    }
  }

  async upsertMemory(params: {
    id: string;
    memory: CandidateMemory;
    sessionId: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MERGE (m:Memory {id: $id})
         ON CREATE SET m.content = $content, m.importance = $importance, m.created_at = datetime($created_at)
         SET m.content = $content,
             m.importance = greatest(coalesce(m.importance, 0), $importance)
         MERGE (m)-[:FROM_SESSION]->(s)`,
        {
          sid: params.sessionId,
          id: params.id,
          content: params.memory.content,
          importance: params.memory.importance,
          created_at: params.memory.createdAt,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryTopic(params: { memoryId: string; topicName: string }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (m:Memory {id: $mid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0
         MERGE (m)-[:ABOUT_TOPIC]->(t)`,
        { mid: params.memoryId, tid: `topic:${params.topicName}`, name: params.topicName },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryEntity(params: { memoryId: string; entityId: string; entityName: string; entityType: string }) {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (m:Memory {id: $mid})
         MERGE (e:Entity {id: $eid})
         ON CREATE SET e.name = $name, e.type = $type, e.frequency = 0
         MERGE (m)-[:ABOUT_ENTITY]->(e)`,
        { mid: params.memoryId, eid: params.entityId, name: params.entityName, type: params.entityType },
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
    entities: string[];
    topics: string[];
    limit: number;
  }): Promise<Array<{ id: string; content: string; importance: number; relationScore: number }>> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        `WITH $entities AS entities, $topics AS topics
         MATCH (m:Memory)
         OPTIONAL MATCH (m)-[:ABOUT_ENTITY]->(e:Entity)
         OPTIONAL MATCH (m)-[:ABOUT_TOPIC]->(t:Topic)
         WITH m,
              collect(DISTINCT e.name) AS enames,
              collect(DISTINCT t.name) AS tnames
         WITH m, enames, tnames,
              size([x IN enames WHERE x IN entities]) AS entityHits,
              size([x IN tnames WHERE x IN topics]) AS topicHits
         WITH m, entityHits, topicHits,
              (CASE WHEN entityHits + topicHits = 0 THEN 0.0
                    ELSE (toFloat(entityHits) * 0.7 + toFloat(topicHits) * 0.3) / toFloat(entityHits + topicHits)
               END) AS score
         WHERE score > 0
         RETURN m.id AS id, m.content AS content, coalesce(m.importance, 0.0) AS importance, score AS relationScore
         ORDER BY score DESC, importance DESC
         LIMIT $limit`,
        {
          entities: params.entities,
          topics: params.topics,
          limit: params.limit,
        },
      );
      return res.records.map((r) => ({
        id: String(r.get("id")),
        content: String(r.get("content")),
        importance: Number(r.get("importance") ?? 0),
        relationScore: Number(r.get("relationScore") ?? 0),
      }));
    } finally {
      await session.close();
    }
  }
}

