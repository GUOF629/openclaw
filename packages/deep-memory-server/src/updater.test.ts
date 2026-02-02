import { describe, expect, it } from "vitest";
import { DeepMemoryUpdater } from "./updater.js";

describe("DeepMemoryUpdater", () => {
  it("filters sensitive + low-importance drafts and processes important ones", async () => {
    const analyzer = {
      analyze: () => ({
        entities: [],
        topics: [],
        events: [],
        drafts: [
          {
            content: "password=supersecret",
            entities: [],
            topics: [],
            createdAt: new Date().toISOString(),
            signals: { frequency: 10, user_intent: 1, length: 50 },
          },
          {
            content: "duplicate but low importance",
            entities: [],
            topics: [],
            createdAt: new Date().toISOString(),
            signals: { frequency: 1, user_intent: 0.2, length: 50 },
          },
          {
            content: "important memory that should be stored",
            entities: ["ProjectX"],
            topics: ["deep-memory"],
            createdAt: new Date().toISOString(),
            signals: { frequency: 2, user_intent: 0.9, length: 120 },
          },
          {
            content: "very important duplicate that should reuse existing id",
            entities: ["ProjectX"],
            topics: ["deep-memory"],
            createdAt: new Date().toISOString(),
            signals: { frequency: 10, user_intent: 1, length: 2000 },
          },
        ],
        filtered: { added: 4, filtered: 0 },
      }),
    };

    const embedder = { embed: async () => [0, 0, 0] };

    const qdrantUpserts: Array<{ id: string; payload: unknown }> = [];
    let searchCalls = 0;
    const qdrant = {
      search: async ({ limit }: { limit: number }) => {
        // first call per draft does (limit=1) novelty+dedupe
        // later we might do RELATED_TO search (limit>1), but this test doesn't rely on it
        if (limit !== 1) {
          return [];
        }
        searchCalls += 1;
        // Calls happen only for non-sensitive drafts.
        // 1) low importance: high similarity => low novelty
        // 2) important: low similarity => high novelty
        // 3) duplicate: very high similarity => dedupe to existing id
        if (searchCalls === 1) {
          return [{ id: "mem_existing", score: 0.95, payload: undefined }];
        }
        if (searchCalls === 2) {
          return [{ id: "mem_other", score: 0.2, payload: undefined }];
        }
        return [{ id: "mem_existing", score: 0.95, payload: undefined }];
      },
      getMemory: async () => ({
        id: "default::mem_existing",
        payload: {
          id: "default::mem_existing",
          namespace: "default",
          content: "old",
          session_id: "s0",
          created_at: "2020-01-01T00:00:00.000Z",
          updated_at: "2020-01-01T00:00:00.000Z",
          importance: 0.6,
          frequency: 3,
          entities: ["ProjectX"],
          topics: ["deep-memory"],
        },
      }),
      upsertMemory: async ({ id, payload }: { id: string; payload: unknown }) => {
        qdrantUpserts.push({ id, payload });
      },
    };

    const neo4jMemories: string[] = [];
    const neo4j = {
      upsertSession: async () => {},
      getSessionIngestMeta: async () => ({}),
      setSessionIngestMeta: async () => {},
      upsertTopic: async () => {},
      linkSessionTopic: async () => {},
      upsertEntity: async () => {},
      linkTopicEntity: async () => {},
      upsertEvent: async () => {},
      eventId: () => "e1",
      linkSessionEvent: async () => {},
      linkEventTopic: async () => {},
      linkEventEntity: async () => {},
      upsertMemory: async ({ id }: { id: string }) => {
        neo4jMemories.push(id);
      },
      linkMemoryTopic: async () => {},
      linkMemoryEntity: async () => {},
      linkMemoryRelated: async () => {},
    };

    const updater = new DeepMemoryUpdater({
      analyzer: analyzer as unknown as never,
      embedder: embedder as unknown as never,
      qdrant: qdrant as unknown as never,
      neo4j: neo4j as unknown as never,
      minSemanticScore: 0.6,
      importanceThreshold: 0.5,
      maxMemoriesPerUpdate: 20,
      dedupeScore: 0.92,
      relatedTopK: 0,
      sensitiveFilterEnabled: true,
    });

    const out = await updater.update({ namespace: "default", sessionId: "s1", messages: [] });

    // We should store 2 memories: the "important" one and the "very important duplicate".
    expect(out.memories_added).toBe(2);
    expect(neo4jMemories.length).toBe(2);

    // One of the upserts should target the deduped existing id.
    const ids = qdrantUpserts.map((u) => u.id);
    expect(ids).toContain("default::mem_existing");

    const existingPayload = qdrantUpserts.find((u) => u.id === "default::mem_existing")!
      .payload as Record<string, unknown>;
    expect(existingPayload.namespace).toBe("default");
    expect(existingPayload.frequency).toBe(4);
  });
});
