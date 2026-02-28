export type EvalSemanticHit = {
  id: string;
  score: number;
  payload: {
    content: string;
    importance?: number;
    frequency?: number;
    created_at: string;
    updated_at?: string;
    kind?: string;
    memory_key?: string;
    subject?: string;
    expires_at?: string;
    confidence?: number;
  };
};

export type EvalRelationHit = {
  id: string;
  content: string;
  importance: number;
  frequency: number;
  lastSeenAt: string;
  relationScore: number;
  kind?: string;
  memoryKey?: string;
  subject?: string;
  expiresAt?: string;
  confidence?: number;
};

export type EvalCase = {
  name: string;
  namespace: string;
  now: string;
  query: {
    userInput: string;
    sessionId: string;
    entities: string[];
    topics: string[];
    maxMemories: number;
  };
  qdrantHits: EvalSemanticHit[];
  neo4jHits: EvalRelationHit[];
  expect: {
    /**
     * Expected to appear in the final retrieved list (any rank).
     * Use ids (the merged id namespace::mem_...).
     */
    includeIds?: string[];
    /** Expected to NOT appear (expired, conflict-resolved, below threshold). */
    excludeIds?: string[];
    /** If provided, expected top-1 id. */
    top1Id?: string;
    /** If provided, enforce that only one memory per memory_key is returned. */
    uniqueByMemoryKey?: boolean;
  };
};

export type EvalDataset = {
  version: number;
  cases: EvalCase[];
};
