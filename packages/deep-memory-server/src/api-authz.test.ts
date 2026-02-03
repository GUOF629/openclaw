import { describe, expect, it } from "vitest";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableForgetQueue } from "./durable-forget-queue.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { createApi } from "./api.js";

function createStubApi(cfg: DeepMemoryServerConfig) {
  return createApi({
    cfg,
    retriever: {
      retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
    } as unknown as DeepMemoryRetriever,
    updater: {
      update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
    } as unknown as DeepMemoryUpdater,
    qdrant: {} as unknown as QdrantStore,
    neo4j: {} as unknown as Neo4jStore,
    queue: {
      stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
      listFailed: async () => [],
      exportFailed: async () => ({ mode: "empty" }),
      retryFailed: async () => ({ status: "not_found" }),
      retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
      enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
      runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      cancelBySession: async () => 0,
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
}

describe("API authz (roles + namespaces)", () => {
  const cfgBase = {
    PORT: 0,
    HOST: "0.0.0.0",
    REQUIRE_API_KEY: true,
    MAX_BODY_BYTES: 1024,
    MAX_UPDATE_BODY_BYTES: 1024,
  };

  it("enforces role: read can retrieve but not update", async () => {
    const app = createStubApi({
      ...cfgBase,
      API_KEYS_JSON: JSON.stringify([
        { key: "readKey", role: "read", namespaces: ["ns1"] },
        { key: "writeKey", role: "write", namespaces: ["ns1"] },
      ]),
    });

    const r1 = await app.request("/retrieve_context", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "readKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", user_input: "hi" }),
    });
    expect(r1.status).toBe(200);

    const r2 = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "readKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", messages: [], async: true }),
    });
    expect(r2.status).toBe(403);
  });

  it("enforces namespace allowlist", async () => {
    const app = createStubApi({
      ...cfgBase,
      API_KEYS_JSON: JSON.stringify([{ key: "readKey", role: "read", namespaces: ["ns1"] }]),
    });

    const r = await app.request("/retrieve_context", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "readKey" },
      body: JSON.stringify({ namespace: "ns2", session_id: "s1", user_input: "hi" }),
    });
    expect(r.status).toBe(403);
    const json = await r.json();
    expect(json.error).toBe("forbidden_namespace");
  });

  it("enforces admin for forget", async () => {
    const app = createStubApi({
      ...cfgBase,
      API_KEYS_JSON: JSON.stringify([
        { key: "writeKey", role: "write", namespaces: ["ns1"] },
        { key: "adminKey", role: "admin", namespaces: ["ns1"] },
      ]),
    });

    const r1 = await app.request("/forget", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "writeKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", dry_run: true }),
    });
    expect(r1.status).toBe(403);

    const r2 = await app.request("/forget", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "adminKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", dry_run: true }),
    });
    expect(r2.status).toBe(200);
  });
});
