import { describe, expect, it } from "vitest";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableForgetQueue } from "./durable-forget-queue.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { createApi } from "./api.js";

function createStubApi(
  cfg: DeepMemoryServerConfig,
  overrides?: Partial<{
    qdrant: Partial<QdrantStore>;
    neo4j: Partial<Neo4jStore>;
    queue: Partial<DurableUpdateQueue>;
  }>,
) {
  return createApi({
    cfg,
    retriever: {
      retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }),
    } as unknown as DeepMemoryRetriever,
    updater: {
      update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
    } as unknown as DeepMemoryUpdater,
    qdrant: {
      deleteByIds: async ({ ids }: { ids: string[] }) => ids.length,
      deleteBySession: async () => {},
      ...overrides?.qdrant,
    } as unknown as QdrantStore,
    neo4j: {
      deleteMemoriesByIds: async ({ ids }: { ids: string[] }) => ids.length,
      deleteMemoriesBySession: async () => 0,
      ...overrides?.neo4j,
    } as unknown as Neo4jStore,
    queue: {
      stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
      listFailed: async () => [],
      exportFailed: async () => ({ mode: "empty" }),
      retryFailed: async () => ({ status: "not_found" }),
      retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
      enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
      runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
      cancelBySession: async () => 1,
      ...overrides?.queue,
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

describe("/forget proof-ish response", () => {
  const cfg: DeepMemoryServerConfig = {
    PORT: 0,
    HOST: "0.0.0.0",
    API_KEY: undefined,
    API_KEYS: undefined,
    API_KEYS_JSON: JSON.stringify([{ key: "adminKey", role: "admin", namespaces: ["ns1"] }]),
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
    UPDATE_BACKLOG_DELAY_PENDING: 0,
    UPDATE_BACKLOG_DELAY_SECONDS: 0,
    UPDATE_BACKLOG_READ_ONLY_PENDING: 0,
    UPDATE_DISABLED_NAMESPACES: undefined,
    UPDATE_MIN_INTERVAL_MS: 0,
    UPDATE_SAMPLE_RATE: 1,
    NAMESPACE_RETRIEVE_CONCURRENCY: 0,
    NAMESPACE_UPDATE_CONCURRENCY: 0,
    RETRIEVE_DEGRADE_RELATED_PENDING: 0,
    MIGRATIONS_MODE: "off",
    MIGRATIONS_STRICT: false,
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
  };

  it("includes request_id and per-backend results", async () => {
    const app = createStubApi(cfg, {
      qdrant: {
        deleteByIds: async () => {
          throw new Error("boom");
        },
      },
    });

    const res = await app.request("/forget", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "adminKey" },
      body: JSON.stringify({ namespace: "ns1", memory_ids: ["mem_1"], dry_run: false }),
    });
    expect(res.status).toBe(200);
    const json = (await res.json()) as Record<string, unknown>;
    expect(json.status).toBe("processed");
    expect(typeof json.request_id).toBe("string");
    const results = json.results as Record<string, unknown>;
    expect(results).toBeTruthy();
    expect((results.qdrant as Record<string, unknown>)?.byIds).toBeTruthy();
  });

  it("dry_run returns request_id and planned counts", async () => {
    const app = createStubApi(cfg);
    const res = await app.request("/forget", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "adminKey" },
      body: JSON.stringify({ namespace: "ns1", session_id: "s1", dry_run: true }),
    });
    expect(res.status).toBe(200);
    const json = (await res.json()) as Record<string, unknown>;
    expect(json.status).toBe("dry_run");
    expect(typeof json.request_id).toBe("string");
    expect(json.delete_session).toBe(1);
  });
});
