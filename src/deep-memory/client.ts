import { createSubsystemLogger } from "../logging/subsystem.js";

const log = createSubsystemLogger("deep-memory");

export type DeepMemoryRetrieveResponse = {
  context?: string;
  entities?: string[];
  topics?: string[];
  memories?: Array<{
    id?: string;
    content?: string;
    importance?: number;
    relevance?: number;
  }>;
};

export type DeepMemoryUpdateResponse = {
  status?: string;
  memories_added?: number;
  memories_filtered?: number;
};

type CacheEntry<T> = { value: T; expiresAt: number };

export class DeepMemoryClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly cacheEnabled: boolean;
  private readonly cacheTtlMs: number;
  private readonly cacheMaxEntries: number;
  private readonly cache = new Map<string, CacheEntry<DeepMemoryRetrieveResponse>>();
  private readonly namespace?: string;
  private readonly apiKey?: string;

  constructor(params: {
    baseUrl: string;
    timeoutMs: number;
    cache: { enabled: boolean; ttlMs: number; maxEntries: number };
    namespace?: string;
    apiKey?: string;
  }) {
    this.baseUrl = params.baseUrl.replace(/\/+$/, "");
    this.timeoutMs = params.timeoutMs;
    this.cacheEnabled = params.cache.enabled;
    this.cacheTtlMs = params.cache.ttlMs;
    this.cacheMaxEntries = params.cache.maxEntries;
    this.namespace = params.namespace?.trim() || undefined;
    this.apiKey = params.apiKey?.trim() || undefined;
  }

  private pruneCache(): void {
    if (!this.cacheEnabled) {
      return;
    }
    const now = Date.now();
    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiresAt <= now) {
        this.cache.delete(key);
      }
    }
    if (this.cache.size <= this.cacheMaxEntries) {
      return;
    }
    // Best-effort eviction: drop oldest (Map insertion order).
    const overflow = this.cache.size - this.cacheMaxEntries;
    for (let i = 0; i < overflow; i += 1) {
      const first = this.cache.keys().next().value as string | undefined;
      if (!first) {
        break;
      }
      this.cache.delete(first);
    }
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const url = `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(this.apiKey ? { "x-api-key": this.apiKey } : {}),
        },
        body: JSON.stringify(body ?? {}),
        signal: ctrl.signal,
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
      }
      return (await res.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  async retrieveContext(params: {
    userInput: string;
    sessionId: string;
    maxMemories: number;
  }): Promise<DeepMemoryRetrieveResponse> {
    const input = params.userInput.trim();
    if (!input) {
      return {};
    }
    const key = `${this.namespace ?? ""}::${params.sessionId}::${params.maxMemories}::${input}`;
    if (this.cacheEnabled && this.cacheTtlMs > 0) {
      this.pruneCache();
      const cached = this.cache.get(key);
      if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
      }
    }
    try {
      const value = await this.postJson<DeepMemoryRetrieveResponse>("/retrieve_context", {
        namespace: this.namespace,
        user_input: input,
        session_id: params.sessionId,
        max_memories: params.maxMemories,
      });
      if (this.cacheEnabled && this.cacheTtlMs > 0) {
        this.cache.set(key, { value, expiresAt: Date.now() + this.cacheTtlMs });
      }
      return value;
    } catch (err) {
      log.warn(`retrieve_context failed: ${String(err)}`);
      return {};
    }
  }

  async updateMemoryIndex(params: {
    sessionId: string;
    messages: unknown[];
    async: boolean;
  }): Promise<DeepMemoryUpdateResponse> {
    try {
      return await this.postJson<DeepMemoryUpdateResponse>("/update_memory_index", {
        namespace: this.namespace,
        session_id: params.sessionId,
        messages: params.messages,
        async: params.async,
      });
    } catch (err) {
      log.warn(`update_memory_index failed: ${String(err)}`);
      return {};
    }
  }
}
