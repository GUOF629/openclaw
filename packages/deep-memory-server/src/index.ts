import { serve } from "@hono/node-server";
import { LRUCache } from "lru-cache";
import { loadConfig } from "./config.js";
import { createLogger } from "./logger.js";
import { EmbeddingModel } from "./embeddings.js";
import { QdrantStore } from "./qdrant.js";
import { Neo4jStore } from "./neo4j.js";
import { DeepMemoryRetriever } from "./retriever.js";
import { DeepMemoryUpdater } from "./updater.js";
import { SessionAnalyzer } from "./analyzer.js";
import { createApi } from "./api.js";
import type { UpdateMemoryIndexResponse } from "./types.js";
import type { RetrieveContextResponse } from "./types.js";

type QueueTask = () => Promise<UpdateMemoryIndexResponse>;

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
    await qdrant.ensureCollection();
    log.info({ collection: cfg.QDRANT_COLLECTION }, "qdrant collection ready");
  } catch (err) {
    log.warn({ err: String(err) }, "qdrant unavailable at startup (will degrade)");
  }
  try {
    await neo4j.ensureSchema();
    log.info("neo4j schema ready");
  } catch (err) {
    log.warn({ err: String(err) }, "neo4j unavailable at startup (will degrade)");
  }

  const retriever = new DeepMemoryRetriever({
    embedder,
    qdrant,
    neo4j,
    minSemanticScore: cfg.MIN_SEMANTIC_SCORE,
  });
  const updater = new DeepMemoryUpdater({
    analyzer: new SessionAnalyzer(),
    embedder,
    qdrant,
    neo4j,
    minSemanticScore: cfg.MIN_SEMANTIC_SCORE,
  });

  // Simple in-process queue (serial by default) + result coalescing by session id.
  const inflightBySession = new Map<string, Promise<UpdateMemoryIndexResponse>>();
  const queue: QueueTask[] = [];
  let active = 0;

  const runNext = () => {
    if (active >= cfg.UPDATE_CONCURRENCY) return;
    const task = queue.shift();
    if (!task) return;
    active += 1;
    void task()
      .catch(() => ({ status: "error", memories_added: 0, memories_filtered: 0 } as UpdateMemoryIndexResponse))
      .finally(() => {
        active -= 1;
        runNext();
      });
  };

  // Retrieve cache (server-side): best-effort; client-side cache exists too.
  const retrieveCache = new LRUCache<string, Promise<RetrieveContextResponse>>({
    max: cfg.RETRIEVE_CACHE_MAX,
    ttl: cfg.RETRIEVE_CACHE_TTL_MS,
  });

  const app = createApi({
    cfg,
    retriever: {
      ...retriever,
      retrieve: async (params: Parameters<DeepMemoryRetriever["retrieve"]>[0]) => {
        const key = `${params.sessionId}::${params.maxMemories}::${params.userInput.trim()}`;
        const cached = retrieveCache.get(key);
        if (cached) {
          return await cached;
        }
        const promise = retriever.retrieve(params);
        retrieveCache.set(key, promise);
        return await promise;
      },
    } as unknown as DeepMemoryRetriever,
    updater,
    enqueueUpdate: async (sessionId, taskFn) => {
      const existing = inflightBySession.get(sessionId);
      if (existing) {
        return await existing;
      }
      const promise = new Promise<UpdateMemoryIndexResponse>((resolve) => {
        queue.push(async () => {
          try {
            const result = await taskFn();
            resolve(result);
            return result;
          } catch (err) {
            const result: UpdateMemoryIndexResponse = {
              status: "error",
              memories_added: 0,
              memories_filtered: 0,
              error: String(err),
            };
            resolve(result);
            return result;
          } finally {
            inflightBySession.delete(sessionId);
          }
        });
        runNext();
      });
      inflightBySession.set(sessionId, promise);
      return await promise;
    },
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
  };
  process.on("SIGINT", () => void shutdown().finally(() => process.exit(0)));
  process.on("SIGTERM", () => void shutdown().finally(() => process.exit(0)));
}

void main();

