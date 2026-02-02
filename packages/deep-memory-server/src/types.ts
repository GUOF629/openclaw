export type RetrieveContextRequest = {
  namespace?: string;
  user_input: string;
  session_id: string;
  max_memories?: number;
};

export type RetrieveContextResponse = {
  entities: string[];
  topics: string[];
  memories: Array<{
    id: string;
    content: string;
    importance: number;
    relevance: number;
    semantic_score?: number;
    relation_score?: number;
    sources: Array<"qdrant" | "neo4j">;
  }>;
  context: string;
};

export type UpdateMemoryIndexRequest = {
  namespace?: string;
  session_id: string;
  // OpenClaw sends transcript messages (best-effort). We treat as opaque and extract what we can.
  messages: unknown[];
  async?: boolean;
};

export type UpdateMemoryIndexResponse = {
  status: "queued" | "processed" | "skipped" | "error";
  memories_added: number;
  memories_filtered: number;
  error?: string;
};

export type ExtractedEntity = {
  name: string;
  type: "person" | "place" | "organization" | "project" | "concept" | "other";
  frequency: number;
};

export type ExtractedTopic = {
  name: string;
  frequency: number;
  importance: number;
};

export type ExtractedEvent = {
  type:
    | "requirement_confirmed"
    | "design_decided"
    | "implementation_started"
    | "issue_resolved"
    | "milestone_reached"
    | "other";
  summary: string;
  timestamp: string; // ISO string
};

export type MemoryKind = "rule" | "preference" | "fact" | "task" | "ephemeral";

export type CandidateMemory = {
  kind: MemoryKind;
  /** Optional canonical slot/key for conflict resolution (e.g. preference:timezone). */
  memoryKey?: string;
  /** Optional short subject label (best-effort). */
  subject?: string;
  /** Optional ISO expiration timestamp for ephemeral memories. */
  expiresAt?: string;
  /** 0..1 confidence (heuristic, best-effort). */
  confidence?: number;
  content: string;
  importance: number;
  entities: string[];
  topics: string[];
  createdAt: string; // ISO
};

export type CandidateMemoryDraft = {
  kind?: MemoryKind;
  memoryKey?: string;
  subject?: string;
  expiresAt?: string;
  confidence?: number;
  content: string;
  entities: string[];
  topics: string[];
  createdAt: string; // ISO
  signals: {
    frequency: number;
    user_intent: number; // 0..1
    length: number;
  };
};

