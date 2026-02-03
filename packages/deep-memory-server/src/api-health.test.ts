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
  const queue = {
    stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
  } as unknown as DurableUpdateQueue;

  const app = createApi({
    cfg,
    retriever: {
      retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
    } as unknown as DeepMemoryRetriever,
    updater: {
      update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
    } as unknown as DeepMemoryUpdater,
    qdrant: { healthCheck: async () => ({ ok: true }) } as unknown as QdrantStore,
    neo4j: { healthCheck: async () => ({ ok: true }) } as unknown as Neo4jStore,
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
  return app;
}

describe("/health", () => {
  const cfg: DeepMemoryServerConfig = {
    PORT: 0,
    HOST: "0.0.0.0",
    API_KEY: undefined,
    API_KEYS: undefined,
    API_KEYS_JSON: JSON.stringify([{ key: "adminKey", role: "admin", namespaces: ["default"] }]),
    REQUIRE_API_KEY: true,
    MAX_BODY_BYTES: 1024,
    MAX_UPDATE_BODY_BYTES: 1024,
    AUDIT_LOG_PATH: undefined,
    ALLOW_UNAUTHENTICATED_METRICS: false,
    RATE_LIMIT_ENABLED: false,
    RATE_LIMIT_WINDOW_MS: 60_000,
    RATE_LIMIT_RETRIEVE_PER_WINDOW: 0,
    RATE_LIMIT_UPDATE_PER_WINDOW: 0,
    RATE_LIMIT_FORGET_PER_WINDOW: 0,
    RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW: 0,
    UPDATE_BACKLOG_REJECT_PENDING: 0,
    UPDATE_BACKLOG_RETRY_AFTER_SECONDS: 30,
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
    EMBEDDING_MODEL: "Xenova/bge-small-en-v1.5",
  };

  it("returns liveness without requiring auth", async () => {
    const app = createStubApi(cfg);
    const res = await app.request("/health");
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.ok).toBe(true);
    expect(body.service).toBeTruthy();
    expect(body.queue).toBeTruthy();
    expect(body.guardrails).toBeTruthy();
  });

  it("/health/details requires admin when auth is required", async () => {
    const app = createStubApi(cfg);
    const res1 = await app.request("/health/details");
    expect(res1.status).toBe(401);

    const res2 = await app.request("/health/details", {
      headers: { "x-api-key": "adminKey" },
    });
    expect(res2.status).toBe(200);
    const body = (await res2.json()) as Record<string, unknown>;
    expect(body.deps).toBeTruthy();
  });
});
