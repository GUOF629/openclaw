import { describe, expect, it } from "vitest";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableForgetQueue } from "./durable-forget-queue.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantMemoryPayload, QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { createApi } from "./api.js";

describe("API session inspect", () => {
  it("returns aggregated topics/entities from qdrant payloads", async () => {
    const payload = (overrides: Partial<QdrantMemoryPayload>): QdrantMemoryPayload => ({
      id: "default::mem_x",
      namespace: "default",
      content: "hello",
      session_id: "s1",
      created_at: new Date().toISOString(),
      importance: 0.5,
      entities: [],
      topics: [],
      ...overrides,
    });
    const qdrant = {
      listMemoriesBySession: async () => [
        { id: "default::m1", payload: payload({ topics: ["t1", "t2"], entities: ["e1"] }) },
        { id: "default::m2", payload: payload({ topics: ["t1"], entities: ["e1", "e2"] }) },
      ],
    } as unknown as QdrantStore;
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 10_000,
        MAX_UPDATE_BODY_BYTES: 10_000,
      } as unknown as DeepMemoryServerConfig,
      retriever: {
        retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
      } as unknown as DeepMemoryRetriever,
      updater: {
        update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      } as unknown as DeepMemoryUpdater,
      qdrant,
      neo4j: {} as unknown as Neo4jStore,
      queue: {
        enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
        runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
        cancelBySession: async () => 0,
        stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
        listFailed: async () => [],
        retryFailed: async () => ({ status: "not_found" }),
        exportFailed: async () => ({ mode: "empty" }),
        retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
      } as unknown as DurableUpdateQueue,
      forgetQueue: {
        stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
        enqueue: async () => ({ status: "queued", key: "k", taskId: "t" }),
        listFailed: async () => [],
        exportFailed: async () => ({ mode: "empty" }),
        retryFailed: async () => ({ status: "not_found" }),
        retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
      } as unknown as DurableForgetQueue,
    });

    const res = await app.request("/session/inspect", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", limit: 100 }),
    });
    expect(res.status).toBe(200);
    const json = (await res.json()) as Record<string, unknown>;
    expect(json.session_id).toBe("s1");
    const topics = json.topics as Array<{ name: string; frequency: number }>;
    const entities = json.entities as Array<{ name: string; frequency: number }>;
    expect(topics.find((t) => t.name === "t1")?.frequency).toBe(2);
    expect(entities.find((e) => e.name === "e1")?.frequency).toBe(2);
  });
});
