import { z } from "zod";

const EnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(8088),
  HOST: z.string().default("0.0.0.0"),

  // Qdrant
  QDRANT_URL: z.string().default("http://qdrant:6333"),
  QDRANT_API_KEY: z.string().optional(),
  QDRANT_COLLECTION: z.string().default("openclaw_memories"),
  VECTOR_DIMS: z.coerce.number().int().positive().default(384),
  MIN_SEMANTIC_SCORE: z.coerce.number().min(0).max(1).default(0.6),

  // Neo4j
  NEO4J_URI: z.string().default("bolt://neo4j:7687"),
  NEO4J_USER: z.string().default("neo4j"),
  NEO4J_PASSWORD: z.string().default("openclaw"),

  // Service behavior
  LOG_LEVEL: z.string().default("info"),
  RETRIEVE_CACHE_TTL_MS: z.coerce.number().int().nonnegative().default(5 * 60_000),
  RETRIEVE_CACHE_MAX: z.coerce.number().int().positive().default(500),
  UPDATE_CONCURRENCY: z.coerce.number().int().positive().default(1),

  // Embeddings
  EMBEDDING_MODEL: z.string().default("Xenova/bge-small-en-v1.5")
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

