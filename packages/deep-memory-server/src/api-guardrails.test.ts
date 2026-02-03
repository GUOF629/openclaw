import { describe, expect, it, vi } from "vitest";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableForgetQueue } from "./durable-forget-queue.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { createApi } from "./api.js";

function createStubApi(cfg: DeepMemoryServerConfig, queueOverrides?: Partial<DurableUpdateQueue>) {
  const enqueue = vi.fn(async () => ({ status: "queued", key: "k", transcriptHash: "h" }));
  const queue = {
    stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
    listFailed: async () => [],
    exportFailed: async () => ({ mode: "empty" }),
    retryFailed: async () => ({ status: "not_found" }),
    retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
    enqueue,
    runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
    cancelBySession: async () => 0,
    ...queueOverrides,
  } as unknown as DurableUpdateQueue;

  const app = createApi({
    cfg,
    retriever: {
      retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
    } as unknown as DeepMemoryRetriever,
    updater: {
      update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
    } as unknown as DeepMemoryUpdater,
    qdrant: {} as unknown as QdrantStore,
    neo4j: {} as unknown as Neo4jStore,
    queue,
    forgetQueue: {
      stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
      enqueue: async () => ({ status: "queued", key: "k", taskId: "t" }),
      listFailed: async () => [],
      exportFailed: async () => ({ mode: "empty" }),
      retryFailed: async () => ({ status: "not_found" }),
      retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
    } as unknown as DurableForgetQueue,
  });

  return { app, enqueue };
}

describe("API guardrails", () => {
  const cfgBase: DeepMemoryServerConfig = {
    PORT: 0,
    HOST: "0.0.0.0",
    BUILD_SHA: undefined,
    BUILD_TIME: undefined,
    REQUIRE_API_KEY: true,
    API_KEYS_JSON: JSON.stringify([{ key: "readKey", role: "read", namespaces: ["ns1"] }]),
    MAX_BODY_BYTES: 1024,
    MAX_UPDATE_BODY_BYTES: 1024,
    AUDIT_LOG_PATH: undefined,
    ALLOW_UNAUTHENTICATED_METRICS: false,
    RATE_LIMIT_ENABLED: true,
    RATE_LIMIT_WINDOW_MS: 60_000,
    RATE_LIMIT_RETRIEVE_PER_WINDOW: 1,
    RATE_LIMIT_UPDATE_PER_WINDOW: 1,
    RATE_LIMIT_FORGET_PER_WINDOW: 0,
    RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW: 0,
    UPDATE_BACKLOG_REJECT_PENDING: 0,
    UPDATE_BACKLOG_RETRY_AFTER_SECONDS: 30,
    UPDATE_BACKLOG_DELAY_PENDING: 0,
    UPDATE_BACKLOG_DELAY_SECONDS: 0,
    UPDATE_BACKLOG_READ_ONLY_PENDING: 0,
    UPDATE_DISABLED_NAMESPACES: undefined,
    UPDATE_MIN_INTERVAL_MS: 0,
    UPDATE_SAMPLE_RATE: 1,
    NAMESPACE_RETRIEVE_CONCURRENCY: 0,
    NAMESPACE_UPDATE_CONCURRENCY: 0,
    RETRIEVE_DEGRADE_RELATED_PENDING: 0,
    QDRANT_URL: "http://qdrant:6333",
    QDRANT_API_KEY: undefined,
    QDRANT_COLLECTION: "openclaw_memories",
    VECTOR_DIMS: 384,
    MIN_SEMANTIC_SCORE: 0.6,
    SEMANTIC_WEIGHT: 0.6,
    RELATION_WEIGHT: 0.4,
    NEO4J_URI: "bolt://neo4j:7687",
    NEO4J_USER: "neo4j",
    NEO4J_PASSWORD: "openclaw",
    LOG_LEVEL: "info",
    RETRIEVE_CACHE_TTL_MS: 0,
    RETRIEVE_CACHE_MAX: 10,
    UPDATE_CONCURRENCY: 1,
    QUEUE_DIR: "./data/queue",
    QUEUE_MAX_ATTEMPTS: 10,
    QUEUE_RETRY_BASE_MS: 2000,
    QUEUE_RETRY_MAX_MS: 300000,
    QUEUE_KEEP_DONE: true,
    QUEUE_RETENTION_DAYS: 7,
    QUEUE_MAX_TASK_BYTES: 1024,
    IMPORTANCE_THRESHOLD: 0.5,
    MAX_MEMORIES_PER_UPDATE: 20,
    DEDUPE_SCORE: 0.92,
    DECAY_HALF_LIFE_DAYS: 90,
    IMPORTANCE_BOOST: 0.3,
    FREQUENCY_BOOST: 0.2,
    RELATED_TOPK: 5,
    SENSITIVE_FILTER_ENABLED: true,
    SENSITIVE_RULESET_VERSION: "builtin-v1",
    SENSITIVE_DENY_REGEX_JSON: undefined,
    SENSITIVE_ALLOW_REGEX_JSON: undefined,
    EMBEDDING_MODEL: "Xenova/bge-small-en-v1.5",
    API_KEY: undefined,
    API_KEYS: undefined,
    MIGRATIONS_MODE: "off",
    MIGRATIONS_STRICT: false,
  };

  it("rate limits /retrieve_context", async () => {
    const { app } = createStubApi(cfgBase);
    const headers = { "content-type": "application/json", "x-api-key": "readKey" };

    const r1 = await app.request("/retrieve_context", {
      method: "POST",
      headers,
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", user_input: "hi" }),
    });
    expect(r1.status).toBe(200);

    const r2 = await app.request("/retrieve_context", {
      method: "POST",
      headers,
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", user_input: "hi" }),
    });
    expect(r2.status).toBe(429);
  });

  it("rejects /update_memory_index when backlog is too high (async)", async () => {
    const cfg = {
      ...cfgBase,
      API_KEYS_JSON: JSON.stringify([{ key: "writeKey", role: "write", namespaces: ["ns1"] }]),
      RATE_LIMIT_UPDATE_PER_WINDOW: 0,
      UPDATE_BACKLOG_REJECT_PENDING: 1,
      UPDATE_BACKLOG_RETRY_AFTER_SECONDS: 7,
    } as DeepMemoryServerConfig;
    const { app, enqueue } = createStubApi(cfg, {
      stats: () => ({ pendingApprox: 99, active: 0, inflightKeys: 0 }),
    } as Partial<DurableUpdateQueue>);

    const r = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "writeKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", messages: [], async: true }),
    });
    expect(r.status).toBe(503);
    expect(enqueue).not.toHaveBeenCalled();
    expect(r.headers.get("retry-after")).toBe("7");
  });

  it("skips updates for disabled namespaces", async () => {
    const cfg = {
      ...cfgBase,
      API_KEYS_JSON: JSON.stringify([{ key: "writeKey", role: "write", namespaces: ["ns1"] }]),
      RATE_LIMIT_UPDATE_PER_WINDOW: 0,
      UPDATE_DISABLED_NAMESPACES: "ns1",
    } as DeepMemoryServerConfig;
    const { app, enqueue } = createStubApi(cfg);
    const r = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "writeKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", messages: [], async: true }),
    });
    expect(r.status).toBe(200);
    expect(enqueue).not.toHaveBeenCalled();
    const json = (await r.json()) as Record<string, unknown>;
    expect(json.status).toBe("skipped");
    expect(json.error).toBe("namespace_write_disabled");
  });
});
