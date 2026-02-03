import { serve } from "@hono/node-server";
import { LRUCache } from "lru-cache";
import type { RetrieveContextResponse } from "./types.js";
import { SessionAnalyzer } from "./analyzer.js";
import { createApi } from "./api.js";
import { loadConfig } from "./config.js";
import { DurableForgetQueue } from "./durable-forget-queue.js";
import { DurableUpdateQueue } from "./durable-update-queue.js";
import { EmbeddingModel } from "./embeddings.js";
import { createLogger } from "./logger.js";
import { createMetrics } from "./metrics.js";
import { Neo4jStore } from "./neo4j.js";
import { QdrantStore } from "./qdrant.js";
import { DeepMemoryRetriever } from "./retriever.js";
import { createSensitiveFilter } from "./safety.js";
import { DEEPMEM_SCHEMA_VERSION } from "./schema.js";
import { DeepMemoryUpdater } from "./updater.js";

async function main() {
  const cfg = loadConfig();
  const log = createLogger(cfg.LOG_LEVEL);

  const embedder = new EmbeddingModel({ modelId: cfg.EMBEDDING_MODEL, dims: cfg.VECTOR_DIMS });
  const qdrant = new QdrantStore({
    url: cfg.QDRANT_URL,
    apiKey: cfg.QDRANT_API_KEY,
    collection: cfg.QDRANT_COLLECTION,
    dims: cfg.VECTOR_DIMS,
  });
  const neo4j = new Neo4jStore({
    uri: cfg.NEO4J_URI,
    user: cfg.NEO4J_USER,
    password: cfg.NEO4J_PASSWORD,
  });

  // Init (best-effort): if either backend is down, server still starts and will degrade.
  try {
    const status = await qdrant.schemaStatus({
      mode: cfg.MIGRATIONS_MODE,
      expectedVersion: DEEPMEM_SCHEMA_VERSION,
    });
    if (!status.ok) {
      log.warn({ status }, "qdrant schema not ready");
      if (cfg.MIGRATIONS_STRICT) {
        throw new Error("qdrant schema not ready (strict)");
      }
    } else {
      log.info({ status }, "qdrant schema ready");
    }
  } catch (err) {
    log.warn({ err: String(err) }, "qdrant unavailable at startup (will degrade)");
  }
  try {
    const status = await neo4j.schemaStatus({
      mode: cfg.MIGRATIONS_MODE,
      expectedVersion: DEEPMEM_SCHEMA_VERSION,
    });
    if (!status.ok) {
      log.warn({ status }, "neo4j schema not ready");
      if (cfg.MIGRATIONS_STRICT) {
        throw new Error("neo4j schema not ready (strict)");
      }
    } else {
      log.info({ status }, "neo4j schema ready");
    }
  } catch (err) {
    log.warn({ err: String(err) }, "neo4j unavailable at startup (will degrade)");
  }

  const retriever = new DeepMemoryRetriever({
    embedder,
    qdrant,
    neo4j,
    minSemanticScore: cfg.MIN_SEMANTIC_SCORE,
    semanticWeight: cfg.SEMANTIC_WEIGHT,
    relationWeight: cfg.RELATION_WEIGHT,
    decayHalfLifeDays: cfg.DECAY_HALF_LIFE_DAYS,
    importanceBoost: cfg.IMPORTANCE_BOOST,
    frequencyBoost: cfg.FREQUENCY_BOOST,
  });
  const updater = new DeepMemoryUpdater({
    analyzer: new SessionAnalyzer(),
    embedder,
    qdrant,
    neo4j,
    minSemanticScore: cfg.MIN_SEMANTIC_SCORE,
    importanceThreshold: cfg.IMPORTANCE_THRESHOLD,
    maxMemoriesPerUpdate: cfg.MAX_MEMORIES_PER_UPDATE,
    dedupeScore: cfg.DEDUPE_SCORE,
    relatedTopK: cfg.RELATED_TOPK,
    sensitiveFilterEnabled: cfg.SENSITIVE_FILTER_ENABLED,
    sensitiveFilter: createSensitiveFilter(cfg),
  });

  const updateQueue = new DurableUpdateQueue({
    log,
    updater,
    concurrency: cfg.UPDATE_CONCURRENCY,
    dir: cfg.QUEUE_DIR,
    maxAttempts: cfg.QUEUE_MAX_ATTEMPTS,
    retryBaseMs: cfg.QUEUE_RETRY_BASE_MS,
    retryMaxMs: cfg.QUEUE_RETRY_MAX_MS,
    keepDone: cfg.QUEUE_KEEP_DONE,
    retentionDays: cfg.QUEUE_RETENTION_DAYS,
    maxTaskBytes: cfg.QUEUE_MAX_TASK_BYTES,
  });
  await updateQueue.init();

  const forgetQueue = new DurableForgetQueue({
    log,
    qdrant,
    neo4j,
    updateQueue,
    concurrency: 1,
    dir: `${cfg.QUEUE_DIR.replace(/\/+$/, "")}/forget`,
    maxAttempts: cfg.QUEUE_MAX_ATTEMPTS,
    retryBaseMs: cfg.QUEUE_RETRY_BASE_MS,
    retryMaxMs: cfg.QUEUE_RETRY_MAX_MS,
    keepDone: cfg.QUEUE_KEEP_DONE,
    retentionDays: cfg.QUEUE_RETENTION_DAYS,
  });
  await forgetQueue.init();

  // Retrieve cache (server-side): best-effort; client-side cache exists too.
  const retrieveCache = new LRUCache<string, Promise<RetrieveContextResponse>>({
    max: cfg.RETRIEVE_CACHE_MAX,
    ttl: cfg.RETRIEVE_CACHE_TTL_MS,
  });

  const baseRetrieve = retriever.retrieve.bind(retriever);
  // Avoid re-embedding the same input across concurrent callers.
  retriever.retrieve = async (params: Parameters<DeepMemoryRetriever["retrieve"]>[0]) => {
    const key = `${params.namespace}::${params.sessionId}::${params.maxMemories}::${params.userInput.trim()}`;
    const cached = retrieveCache.get(key);
    if (cached) {
      return await cached;
    }
    const promise = baseRetrieve(params);
    retrieveCache.set(key, promise);
    return await promise;
  };

  const app = createApi({
    cfg,
    log,
    retriever,
    updater,
    qdrant,
    neo4j,
    queue: updateQueue,
    forgetQueue,
    metrics: createMetrics(),
  });

  const server = serve({ fetch: app.fetch, port: cfg.PORT, hostname: cfg.HOST });
  log.info({ host: cfg.HOST, port: cfg.PORT }, "deep-memory-server listening");

  const shutdown = async () => {
    try {
      server.close();
    } catch {}
    try {
      await neo4j.close();
    } catch {}
    try {
      updateQueue.stop();
    } catch {}
  };
  process.on("SIGINT", () => void shutdown().finally(() => process.exit(0)));
  process.on("SIGTERM", () => void shutdown().finally(() => process.exit(0)));
}

void main();
