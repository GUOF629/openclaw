import { describe, expect, it } from "vitest";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { createApi } from "./api.js";

describe("API auth", () => {
  it("rejects update without x-api-key when API_KEY configured", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEY: "secret",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as unknown as DeepMemoryServerConfig,
      retriever: {
        retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
      } as unknown as DeepMemoryRetriever,
      updater: {
        update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      } as unknown as DeepMemoryUpdater,
      qdrant: {} as unknown as QdrantStore,
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
    });

    const res = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(res.status).toBe(401);
  });

  it("accepts update with x-api-key", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEYS: "old, secret ,new",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as unknown as DeepMemoryServerConfig,
      retriever: {
        retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
      } as unknown as DeepMemoryRetriever,
      updater: {
        update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      } as unknown as DeepMemoryUpdater,
      qdrant: {} as unknown as QdrantStore,
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
    });

    const res = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "secret" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(res.status).toBe(200);
  });

  it("accepts update with any key in API_KEYS", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEYS: "k1,k2,k3",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as unknown as DeepMemoryServerConfig,
      retriever: {
        retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
      } as unknown as DeepMemoryRetriever,
      updater: {
        update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      } as unknown as DeepMemoryUpdater,
      qdrant: {} as unknown as QdrantStore,
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
    });

    const res = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "k2" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(res.status).toBe(200);
  });

  it("enforces roles and namespace allowlist via API_KEYS_JSON", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEYS_JSON: JSON.stringify([
          { key: "r1", role: "read", namespaces: ["teamA"] },
          { key: "w1", role: "write", namespaces: ["teamA"] },
          { key: "a1", role: "admin", namespaces: ["teamA"] },
        ]),
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as unknown as DeepMemoryServerConfig,
      retriever: {
        retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
      } as unknown as DeepMemoryRetriever,
      updater: {
        update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      } as unknown as DeepMemoryUpdater,
      qdrant: {} as unknown as QdrantStore,
      neo4j: {} as unknown as Neo4jStore,
      queue: {
        enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
        runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
        cancelBySession: async () => 0,
        stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
        listFailed: async () => [],
        exportFailed: async () => ({ mode: "empty" }),
        retryFailed: async () => ({ status: "not_found" }),
        retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
      } as unknown as DurableUpdateQueue,
    });

    // read key cannot update
    const u1 = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "r1" },
      body: JSON.stringify({ namespace: "teamA", session_id: "s1", messages: [], async: true }),
    });
    expect(u1.status).toBe(403);

    // write key can update within allowed namespace
    const u2 = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "w1" },
      body: JSON.stringify({ namespace: "teamA", session_id: "s1", messages: [], async: true }),
    });
    expect(u2.status).toBe(200);

    // write key cannot access other namespace
    const u3 = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "w1" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(u3.status).toBe(403);

    // read key can retrieve within allowed namespace
    const r = await app.request("/retrieve_context", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "r1" },
      body: JSON.stringify({ namespace: "teamA", session_id: "s1", user_input: "hi" }),
    });
    expect(r.status).toBe(200);
  });
});
