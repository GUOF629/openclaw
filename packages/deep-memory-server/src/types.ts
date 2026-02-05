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
  /**
   * If true and async=false, include memory ids written/updated in the response.
   * This is intended for traceability (e.g. file_id -> memory_ids).
   */
  return_memory_ids?: boolean;
  /** Maximum memory ids to return (default 200, max 1000). Only applies when return_memory_ids=true. */
  max_memory_ids?: number;
};

export type UpdateMemoryIndexResponse =
  | {
      status: "queued";
      memories_added: 0;
      memories_filtered: 0;
      degraded?: {
        mode: "delayed";
        notBeforeMs: number;
        delaySeconds: number;
      };
    }
  | {
      status: "processed";
      memories_added: number;
      memories_filtered: number;
      memory_ids?: string[];
      memory_ids_truncated?: boolean;
    }
  | {
      status: "skipped";
      memories_added: 0;
      memories_filtered: 0;
      error: "namespace_write_disabled" | "sampled_out" | "throttled";
    }
  | {
      status: "error";
      memories_added: 0;
      memories_filtered: 0;
      error: string;
    };

export type OverloadResponse =
  | {
      error: "queue_overloaded";
      pendingApprox: number;
      retryAfterSeconds: number;
    }
  | {
      error: "degraded_read_only";
      pendingApprox: number;
      retryAfterSeconds: number;
    }
  | {
      error: "namespace_overloaded";
      namespace: string;
      active: number;
      limit: number;
    };

export type ForgetRequest = {
  namespace?: string;
  memory_ids?: string[];
  session_id?: string;
  dry_run?: boolean;
  async?: boolean;
};

export type ForgetResponse =
  | {
      status: "dry_run";
      namespace: string;
      request_id?: string;
      delete_ids: number;
      delete_session: number;
    }
  | {
      status: "queued";
      namespace: string;
      request_id?: string;
      key: string;
      task_id: string;
      delete_ids: number;
      delete_session: number;
    }
  | {
      status: "processed";
      namespace: string;
      request_id?: string;
      deleted: number;
      results: unknown;
    };

export type InspectSessionRequest = {
  namespace?: string;
  session_id: string;
  limit?: number;
  include_content?: boolean;
};

export type InspectSessionResponse = {
  namespace: string;
  session_id: string;
  totals: {
    memories: number;
  };
  topics: Array<{ name: string; frequency: number }>;
  entities: Array<{ name: string; frequency: number }>;
  memories: Array<{
    id: string;
    importance?: number;
    created_at?: string;
    content?: string;
    topics?: string[];
    entities?: string[];
  }>;
  summary?: string;
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
