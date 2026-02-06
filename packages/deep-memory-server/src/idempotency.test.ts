import crypto from "node:crypto";
import { describe, expect, it } from "vitest";
import { DeepMemoryUpdater } from "./updater.js";

describe("DeepMemoryUpdater idempotency", () => {
  it("skips when transcriptHash already ingested", async () => {
    const analyzer = {
      analyze: () => ({
        entities: [],
        topics: [],
        events: [],
        drafts: [],
        filtered: { added: 0, filtered: 0 },
      }),
    };
    const embedder = { embed: async () => [0, 0, 0] };
    const qdrant = {
      search: async () => [],
      getMemory: async () => null,
      upsertMemory: async () => {},
    };

    const messages = [{ role: "user", content: "hello" }];
    const hash = crypto.createHash("sha256").update(JSON.stringify(messages)).digest("hex");

    let calls = 0;
    const neo4j = {
      upsertSession: async () => {},
      getSessionIngestMeta: async () => {
        calls += 1;
        // First call: no meta. Second call: meta exists and should cause skip.
        if (calls === 1) {
          return {};
        }
        return { transcriptHash: hash };
      },
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
      upsertMemory: async () => {},
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

    // First update: will set meta (best-effort), but our stub doesn't persist it.
    const a = await updater.update({ namespace: "default", sessionId: "s1", messages });
    expect(a.status).toBe("processed");

    // Second update: should skip when meta says the hash is already ingested.
    const b = await updater.update({ namespace: "default", sessionId: "s1", messages });
    expect(b.status).toBe("skipped");
    expect(calls).toBeGreaterThanOrEqual(2);
  });
});
