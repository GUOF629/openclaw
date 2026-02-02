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

export type CandidateMemory = {
  content: string;
  importance: number;
  entities: string[];
  topics: string[];
  createdAt: string; // ISO
};

export type CandidateMemoryDraft = {
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

