import { z } from "zod";
import type { MigrationMode } from "./schema.js";

const EnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(8088),
  HOST: z.string().default("0.0.0.0"),
  API_KEY: z.string().optional(),
  API_KEYS: z.string().optional(),
  API_KEYS_JSON: z.string().optional(),
  BUILD_SHA: z.string().optional(),
  BUILD_TIME: z.string().optional(),
  REQUIRE_API_KEY: z.coerce.boolean().default(false),
  MAX_BODY_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(256 * 1024),
  MAX_UPDATE_BODY_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(2 * 1024 * 1024),
  AUDIT_LOG_PATH: z.string().optional(),
  ALLOW_UNAUTHENTICATED_METRICS: z.coerce.boolean().default(false),
  RATE_LIMIT_ENABLED: z.coerce.boolean().default(false),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().positive().default(60_000),
  RATE_LIMIT_RETRIEVE_PER_WINDOW: z.coerce.number().int().nonnegative().default(0),
  RATE_LIMIT_UPDATE_PER_WINDOW: z.coerce.number().int().nonnegative().default(0),
  RATE_LIMIT_FORGET_PER_WINDOW: z.coerce.number().int().nonnegative().default(0),
  RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW: z.coerce.number().int().nonnegative().default(0),
  UPDATE_BACKLOG_REJECT_PENDING: z.coerce.number().int().nonnegative().default(0),
  UPDATE_BACKLOG_RETRY_AFTER_SECONDS: z.coerce.number().int().positive().default(30),
  UPDATE_BACKLOG_DELAY_PENDING: z.coerce.number().int().nonnegative().default(0),
  UPDATE_BACKLOG_DELAY_SECONDS: z.coerce.number().int().nonnegative().default(0),
  UPDATE_BACKLOG_READ_ONLY_PENDING: z.coerce.number().int().nonnegative().default(0),
  UPDATE_DISABLED_NAMESPACES: z.string().optional(),
  UPDATE_MIN_INTERVAL_MS: z.coerce.number().int().nonnegative().default(0),
  UPDATE_SAMPLE_RATE: z.coerce.number().min(0).max(1).default(1),
  NAMESPACE_RETRIEVE_CONCURRENCY: z.coerce.number().int().nonnegative().default(0),
  NAMESPACE_UPDATE_CONCURRENCY: z.coerce.number().int().nonnegative().default(0),
  RETRIEVE_DEGRADE_RELATED_PENDING: z.coerce.number().int().nonnegative().default(0),
  MIGRATIONS_MODE: z
    .enum(["off", "validate", "apply"])
    .default("apply") satisfies z.ZodType<MigrationMode>,
  MIGRATIONS_STRICT: z.coerce.boolean().default(false),

  // Qdrant
  QDRANT_URL: z.string().default("http://qdrant:6333"),
  QDRANT_API_KEY: z.string().optional(),
  QDRANT_COLLECTION: z.string().default("openclaw_memories"),
  VECTOR_DIMS: z.coerce.number().int().positive().default(384),
  MIN_SEMANTIC_SCORE: z.coerce.number().min(0).max(1).default(0.6),
  SEMANTIC_WEIGHT: z.coerce.number().min(0).max(1).default(0.6),
  RELATION_WEIGHT: z.coerce.number().min(0).max(1).default(0.4),

  // Neo4j
  NEO4J_URI: z.string().default("bolt://neo4j:7687"),
  NEO4J_USER: z.string().default("neo4j"),
  NEO4J_PASSWORD: z.string().default("openclaw"),

  // Service behavior
  LOG_LEVEL: z.string().default("info"),
  RETRIEVE_CACHE_TTL_MS: z.coerce
    .number()
    .int()
    .nonnegative()
    .default(5 * 60_000),
  RETRIEVE_CACHE_MAX: z.coerce.number().int().positive().default(500),
  UPDATE_CONCURRENCY: z.coerce.number().int().positive().default(1),
  QUEUE_DIR: z.string().default("./data/queue"),
  QUEUE_MAX_ATTEMPTS: z.coerce.number().int().positive().default(10),
  QUEUE_RETRY_BASE_MS: z.coerce.number().int().positive().default(2_000),
  QUEUE_RETRY_MAX_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(5 * 60_000),
  QUEUE_KEEP_DONE: z.coerce.boolean().default(true),
  QUEUE_RETENTION_DAYS: z.coerce.number().int().positive().default(7),
  QUEUE_MAX_TASK_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(2 * 1024 * 1024),
  IMPORTANCE_THRESHOLD: z.coerce.number().min(0).max(1).default(0.5),
  MAX_MEMORIES_PER_UPDATE: z.coerce.number().int().positive().default(20),
  DEDUPE_SCORE: z.coerce.number().min(0).max(1).default(0.92),
  DECAY_HALF_LIFE_DAYS: z.coerce.number().int().positive().default(90),
  IMPORTANCE_BOOST: z.coerce.number().min(0).max(2).default(0.3),
  FREQUENCY_BOOST: z.coerce.number().min(0).max(2).default(0.2),
  RELATED_TOPK: z.coerce.number().int().nonnegative().default(5),
  SENSITIVE_FILTER_ENABLED: z.coerce.boolean().default(true),
  SENSITIVE_RULESET_VERSION: z.string().default("builtin-v1"),
  SENSITIVE_DENY_REGEX_JSON: z.string().optional(),
  SENSITIVE_ALLOW_REGEX_JSON: z.string().optional(),

  // Embeddings
  EMBEDDING_MODEL: z.string().default("Xenova/bge-small-en-v1.5"),
});

export type DeepMemoryServerConfig = z.infer<typeof EnvSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): DeepMemoryServerConfig {
  const parsed = EnvSchema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join(", ");
    throw new Error(`Invalid deep-memory-server env: ${issues}`);
  }
  return parsed.data;
}
