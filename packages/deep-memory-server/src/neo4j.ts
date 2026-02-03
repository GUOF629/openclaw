import neo4j, { type Driver } from "neo4j-driver";
import type { SchemaCheckResult } from "./schema.js";
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

  async healthCheck(): Promise<{ ok: true } | { ok: false; error: string }> {
    try {
      // verifyConnectivity is the lightest built-in probe.
      await this.driver.verifyConnectivity();
      return { ok: true };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }

  private async listConstraints(): Promise<
    Array<{
      name: string;
      type: string;
      labelsOrTypes: string[];
      properties: string[];
    }>
  > {
    const session = this.driver.session();
    try {
      const res = await session.run(
        "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties RETURN name, type, labelsOrTypes, properties",
      );
      return res.records.map((r) => {
        const labels = r.get("labelsOrTypes") as unknown;
        const props = r.get("properties") as unknown;
        return {
          name: String(r.get("name") ?? ""),
          type: String(r.get("type") ?? ""),
          labelsOrTypes: Array.isArray(labels) ? labels.map((x) => String(x)) : [],
          properties: Array.isArray(props) ? props.map((x) => String(x)) : [],
        };
      });
    } finally {
      await session.close();
    }
  }

  private async listIndexes(): Promise<
    Array<{
      name: string;
      type: string;
      labelsOrTypes: string[];
      properties: string[];
    }>
  > {
    const session = this.driver.session();
    try {
      const res = await session.run(
        "SHOW INDEXES YIELD name, type, labelsOrTypes, properties RETURN name, type, labelsOrTypes, properties",
      );
      return res.records.map((r) => {
        const labels = r.get("labelsOrTypes") as unknown;
        const props = r.get("properties") as unknown;
        return {
          name: String(r.get("name") ?? ""),
          type: String(r.get("type") ?? ""),
          labelsOrTypes: Array.isArray(labels) ? labels.map((x) => String(x)) : [],
          properties: Array.isArray(props) ? props.map((x) => String(x)) : [],
        };
      });
    } finally {
      await session.close();
    }
  }

  private hasUniquenessConstraint(
    constraints: Awaited<ReturnType<Neo4jStore["listConstraints"]>>,
    label: string,
    property: string,
  ): boolean {
    return constraints.some((c) => {
      const t = c.type.toUpperCase();
      return (
        t.includes("UNIQUENESS") &&
        c.labelsOrTypes.includes(label) &&
        c.properties.length === 1 &&
        c.properties[0] === property
      );
    });
  }

  private hasBtreeIndex(
    indexes: Awaited<ReturnType<Neo4jStore["listIndexes"]>>,
    label: string,
    property: string,
  ): boolean {
    return indexes.some((idx) => {
      const t = idx.type.toUpperCase();
      return (
        (t.includes("BTREE") || t.includes("RANGE") || t.includes("TEXT")) &&
        idx.labelsOrTypes.includes(label) &&
        idx.properties.length === 1 &&
        idx.properties[0] === property
      );
    });
  }

  private async getSchemaVersion(): Promise<number | undefined> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        "MATCH (m:DeepMemMeta {id: 'schema'}) RETURN m.schema_version AS v LIMIT 1",
      );
      const row = res.records[0];
      if (!row) {
        return undefined;
      }
      const v = row.get("v") as unknown;
      return typeof v === "number" && Number.isFinite(v) ? v : undefined;
    } finally {
      await session.close();
    }
  }

  private async setSchemaVersion(v: number): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        "MERGE (m:DeepMemMeta {id: 'schema'}) SET m.schema_version = $v, m.updated_at = datetime()",
        { v },
      );
    } finally {
      await session.close();
    }
  }

  private async runSchemaStatement(cypher: string): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(cypher);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // Neo4j will error if an equivalent constraint exists under another name.
      if (msg.toLowerCase().includes("already exists")) {
        return;
      }
      throw err;
    } finally {
      await session.close();
    }
  }

  async schemaStatus(params: {
    mode: SchemaCheckResult["mode"];
    expectedVersion: number;
  }): Promise<SchemaCheckResult> {
    const actions: string[] = [];
    const warnings: string[] = [];
    try {
      const currentVersion = await this.getSchemaVersion();
      if (params.mode === "off") {
        return {
          ok: true,
          mode: params.mode,
          expectedVersion: params.expectedVersion,
          currentVersion,
        };
      }

      const constraints = await this.listConstraints();
      const indexes = await this.listIndexes();

      const requiredUnique: Array<{ label: string; property: string; name: string }> = [
        { label: "Session", property: "id", name: "deepmem_session_id" },
        { label: "Memory", property: "id", name: "deepmem_memory_id" },
        { label: "Topic", property: "id", name: "deepmem_topic_id" },
        { label: "Entity", property: "id", name: "deepmem_entity_id" },
        { label: "Event", property: "id", name: "deepmem_event_id" },
      ];

      const requiredIndexes: Array<{ label: string; property: string; name: string }> = [
        { label: "Entity", property: "name", name: "deepmem_entity_name" },
        { label: "Topic", property: "name", name: "deepmem_topic_name" },
        { label: "Memory", property: "memory_key", name: "deepmem_memory_key" },
      ];

      const missingUniques = requiredUnique.filter(
        (r) => !this.hasUniquenessConstraint(constraints, r.label, r.property),
      );
      const missingIndexes = requiredIndexes.filter(
        (r) => !this.hasBtreeIndex(indexes, r.label, r.property),
      );

      if (params.mode === "apply") {
        for (const c of missingUniques) {
          await this.runSchemaStatement(
            `CREATE CONSTRAINT ${c.name} IF NOT EXISTS FOR (n:${c.label}) REQUIRE n.${c.property} IS UNIQUE`,
          );
          actions.push(`created constraint ${c.name}`);
        }
        for (const idx of missingIndexes) {
          await this.runSchemaStatement(
            `CREATE INDEX ${idx.name} IF NOT EXISTS FOR (n:${idx.label}) ON (n.${idx.property})`,
          );
          actions.push(`created index ${idx.name}`);
        }
        await this.setSchemaVersion(params.expectedVersion);
      } else {
        for (const c of missingUniques) {
          warnings.push(`missing uniqueness constraint on :${c.label}(${c.property})`);
        }
        for (const idx of missingIndexes) {
          warnings.push(`missing index on :${idx.label}(${idx.property})`);
        }
      }

      const ok =
        missingUniques.length === 0 &&
        missingIndexes.length === 0 &&
        (currentVersion == null || currentVersion <= params.expectedVersion);

      return {
        ok: params.mode === "apply" ? true : ok,
        mode: params.mode,
        expectedVersion: params.expectedVersion,
        currentVersion,
        actions: actions.length ? actions : undefined,
        warnings: warnings.length ? warnings : undefined,
      };
    } catch (err) {
      return {
        ok: false,
        mode: params.mode,
        expectedVersion: params.expectedVersion,
        error: err instanceof Error ? err.message : String(err),
        actions: actions.length ? actions : undefined,
        warnings: warnings.length ? warnings : undefined,
      };
    }
  }

  async ensureSchema(): Promise<void> {
    // Back-compat: keep behavior (create missing constraints/indexes).
    await this.schemaStatus({ mode: "apply", expectedVersion: 0 });
  }

  private prefix(namespace: string): string {
    return `${namespace}::`;
  }

  private parseSessionIdFromNodeId(nodeId: string): string | undefined {
    // Format: `${namespace}::session::<sessionId>`
    const idx = nodeId.indexOf("session::");
    if (idx < 0) {
      return undefined;
    }
    const out = nodeId.slice(idx + "session::".length);
    return out.trim() || undefined;
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
    return `${this.prefix(namespace)}event::${event.type}::${event.timestamp}::${event.summary}`.slice(
      0,
      240,
    );
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

  async linkSessionEvent(params: {
    namespace: string;
    sessionId: string;
    eventId: string;
  }): Promise<void> {
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

  async linkEventTopic(params: {
    namespace: string;
    eventId: string;
    topicName: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (e:Event {id: $eid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (e)-[:ABOUT_TOPIC]->(t)`,
        {
          eid: params.eventId,
          tid: this.topicNodeId(params.namespace, params.topicName),
          name: params.topicName,
          ns: params.namespace,
        },
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

  async linkSessionTopic(params: {
    namespace: string;
    sessionId: string;
    topicName: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (s:Session {id: $sid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (s)-[:CONTAINS]->(t)`,
        {
          sid: this.sessionNodeId(params.namespace, params.sessionId),
          tid: this.topicNodeId(params.namespace, params.topicName),
          name: params.topicName,
          ns: params.namespace,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkTopicEntity(params: {
    namespace: string;
    topicName: string;
    entityName: string;
    entityType: string;
  }): Promise<void> {
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
             m.kind = $kind,
             m.memory_key = $memory_key,
             m.subject = $subject,
             m.confidence = $confidence,
             m.expires_at = CASE WHEN $expires_at IS NULL THEN NULL ELSE datetime($expires_at) END,
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
          kind: params.memory.kind,
          memory_key: params.memory.memoryKey ?? null,
          subject: params.memory.subject ?? null,
          confidence:
            typeof params.memory.confidence === "number" ? params.memory.confidence : null,
          expires_at: params.memory.expiresAt ?? null,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryTopic(params: {
    namespace: string;
    memoryId: string;
    topicName: string;
  }): Promise<void> {
    const session = this.driver.session();
    try {
      await session.run(
        `MATCH (m:Memory {id: $mid})
         MERGE (t:Topic {id: $tid})
         ON CREATE SET t.name = $name, t.frequency = 0, t.importance = 0, t.namespace = $ns
         MERGE (m)-[:ABOUT_TOPIC]->(t)`,
        {
          mid: params.memoryId,
          tid: this.topicNodeId(params.namespace, params.topicName),
          name: params.topicName,
          ns: params.namespace,
        },
      );
    } finally {
      await session.close();
    }
  }

  async linkMemoryEntity(params: {
    namespace: string;
    memoryId: string;
    entityName: string;
    entityType: string;
  }) {
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

  async linkMemoryRelated(params: {
    namespace: string;
    fromMemoryId: string;
    toMemoryId: string;
    score: number;
  }): Promise<void> {
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
  }): Promise<
    Array<{
      id: string;
      content: string;
      importance: number;
      frequency: number;
      lastSeenAt: string;
      relationScore: number;
      kind?: string;
      memoryKey?: string;
      subject?: string;
      expiresAt?: string;
      confidence?: number;
    }>
  > {
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
                toString(coalesce(m.expires_at, "")) AS expiresAt,
                coalesce(m.kind, "") AS kind,
                coalesce(m.memory_key, "") AS memoryKey,
                coalesce(m.subject, "") AS subject,
                coalesce(m.confidence, 0.0) AS confidence,
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
        expiresAt: String(r.get("expiresAt") ?? "") || undefined,
        kind: String(r.get("kind") ?? "") || undefined,
        memoryKey: String(r.get("memoryKey") ?? "") || undefined,
        subject: String(r.get("subject") ?? "") || undefined,
        confidence: Number(r.get("confidence") ?? 0) || undefined,
        relationScore: Number(r.get("relationScore") ?? 0),
      }));
    } finally {
      await session.close();
    }
  }

  async deleteMemoriesByIds(params: { namespace: string; ids: string[] }): Promise<number> {
    const ids = params.ids.filter((id) => id.startsWith(this.prefix(params.namespace)));
    if (ids.length === 0) {
      return 0;
    }
    const session = this.driver.session();
    try {
      const res = await session.run(
        `MATCH (m:Memory)
         WHERE m.id IN $ids
         DETACH DELETE m
         RETURN count(*) AS deleted`,
        { ids },
      );
      const row = res.records[0];
      const deleted = row ? Number(row.get("deleted") ?? 0) : 0;
      return Number.isFinite(deleted) ? deleted : 0;
    } finally {
      await session.close();
    }
  }

  async deleteMemoriesBySession(params: { namespace: string; sessionId: string }): Promise<number> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        `MATCH (m:Memory)-[:FROM_SESSION]->(s:Session {id: $sid})
         DETACH DELETE m
         RETURN count(*) AS deleted`,
        { sid: this.sessionNodeId(params.namespace, params.sessionId) },
      );
      const row = res.records[0];
      const deleted = row ? Number(row.get("deleted") ?? 0) : 0;
      return Number.isFinite(deleted) ? deleted : 0;
    } finally {
      await session.close();
    }
  }

  async scanMemories(params: { namespace?: string; afterId?: string; limit: number }): Promise<
    Array<{
      id: string;
      namespace: string;
      content: string;
      kind?: string;
      memoryKey?: string;
      subject?: string;
      expiresAt?: string;
      confidence?: number;
      importance: number;
      frequency: number;
      createdAt: string;
      sessionId?: string;
      topics: string[];
      entities: Array<{ name: string; type?: string }>;
    }>
  > {
    const limit = Math.max(1, Math.min(1000, Math.floor(params.limit)));
    const session = this.driver.session();
    try {
      const res = await session.run(
        `
        MATCH (m:Memory)
        WHERE ($ns IS NULL OR m.namespace = $ns)
          AND ($afterId IS NULL OR m.id > $afterId)
        OPTIONAL MATCH (m)-[:FROM_SESSION]->(s:Session)
        OPTIONAL MATCH (m)-[:ABOUT_TOPIC]->(t:Topic)
        OPTIONAL MATCH (m)-[:ABOUT_ENTITY]->(e:Entity)
        WITH m, s,
             collect(DISTINCT t.name) AS topics,
             collect(DISTINCT {name: e.name, type: e.type}) AS entities
        RETURN
          m.id AS id,
          coalesce(m.namespace, "") AS namespace,
          coalesce(m.content, "") AS content,
          toString(coalesce(m.created_at, datetime())) AS createdAt,
          coalesce(m.kind, "") AS kind,
          coalesce(m.memory_key, "") AS memoryKey,
          coalesce(m.subject, "") AS subject,
          toString(coalesce(m.expires_at, "")) AS expiresAt,
          coalesce(m.confidence, 0.0) AS confidence,
          coalesce(m.importance, 0.0) AS importance,
          coalesce(m.frequency, 0) AS frequency,
          coalesce(s.id, "") AS sessionNodeId,
          topics AS topics,
          entities AS entities
        ORDER BY id ASC
        LIMIT $limit
        `,
        {
          ns: params.namespace ?? null,
          afterId: params.afterId ?? null,
          limit,
        },
      );

      return res.records.map((r) => {
        const sessionNodeId = String(r.get("sessionNodeId") ?? "");
        const topicsRaw = r.get("topics") as unknown;
        const entitiesRaw = r.get("entities") as unknown;

        const topics = Array.isArray(topicsRaw)
          ? topicsRaw.map((t) => String(t)).filter((t) => t.trim().length > 0)
          : [];
        const entities = Array.isArray(entitiesRaw)
          ? entitiesRaw
              .map((x) => {
                if (typeof x !== "object" || !x) {
                  return null;
                }
                const rec = x as Record<string, unknown>;
                const name = typeof rec.name === "string" ? rec.name.trim() : "";
                if (!name) {
                  return null;
                }
                const type = typeof rec.type === "string" ? rec.type.trim() : undefined;
                return { name, type };
              })
              .filter(Boolean)
          : [];

        const kindRaw = String(r.get("kind") ?? "");
        const memoryKeyRaw = String(r.get("memoryKey") ?? "");
        const subjectRaw = String(r.get("subject") ?? "");
        const expiresAtRaw = String(r.get("expiresAt") ?? "");

        const confidenceRaw = r.get("confidence") as unknown;
        const confidence =
          typeof confidenceRaw === "number" && Number.isFinite(confidenceRaw)
            ? confidenceRaw
            : undefined;

        return {
          id: String(r.get("id") ?? ""),
          namespace: String(r.get("namespace") ?? ""),
          content: String(r.get("content") ?? ""),
          createdAt: String(r.get("createdAt") ?? new Date().toISOString()),
          kind: kindRaw || undefined,
          memoryKey: memoryKeyRaw || undefined,
          subject: subjectRaw || undefined,
          expiresAt: expiresAtRaw || undefined,
          confidence,
          importance: Number(r.get("importance") ?? 0),
          frequency: Number(r.get("frequency") ?? 0),
          sessionId: sessionNodeId ? this.parseSessionIdFromNodeId(sessionNodeId) : undefined,
          topics,
          entities,
        };
      });
    } finally {
      await session.close();
    }
  }

  async countMemories(params: { namespace?: string }): Promise<number> {
    const session = this.driver.session();
    try {
      const res = await session.run(
        `
        MATCH (m:Memory)
        WHERE ($ns IS NULL OR m.namespace = $ns)
        RETURN count(m) AS cnt
        `,
        { ns: params.namespace ?? null },
      );
      const row = res.records[0];
      const cnt = row ? Number(row.get("cnt") ?? 0) : 0;
      return Number.isFinite(cnt) ? cnt : 0;
    } finally {
      await session.close();
    }
  }

  async listNamespaces(params?: { limit?: number }): Promise<string[]> {
    const limit = Math.max(1, Math.min(10_000, Math.floor(params?.limit ?? 1000)));
    const session = this.driver.session();
    try {
      const res = await session.run(
        `
        MATCH (m:Memory)
        WITH DISTINCT coalesce(m.namespace, "") AS ns
        WHERE ns <> ""
        RETURN ns
        ORDER BY ns ASC
        LIMIT $limit
        `,
        { limit },
      );
      return res.records.map((r) => String(r.get("ns") ?? "")).filter((s) => s.trim().length > 0);
    } finally {
      await session.close();
    }
  }
}
