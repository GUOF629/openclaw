import type { OpenClawConfig } from "../config/config.js";
import type { DeepMemoryConfig } from "../config/types.tools.js";
import { clampInt } from "../utils.js";
import { resolveAgentConfig } from "./agent-scope.js";

export type ResolvedDeepMemoryConfig = {
  enabled: boolean;
  namespace: string;
  baseUrl: string;
  timeoutMs: number;
  retrieve: {
    maxMemories: number;
    cache: { enabled: boolean; ttlMs: number; maxEntries: number };
  };
  inject: { maxChars: number; label: string };
  update: {
    enabled: boolean;
    thresholds: { deltaBytes: number; deltaMessages: number };
    debounceMs: number;
    nearCompaction: { enabled: boolean };
  };
};

const DEFAULT_RETRIEVE_MAX_MEMORIES = 10;
const DEFAULT_RETRIEVE_CACHE_TTL_MINUTES = 5;
const DEFAULT_RETRIEVE_CACHE_MAX_ENTRIES = 200;
const DEFAULT_INJECT_MAX_CHARS = 4000;
const DEFAULT_TIMEOUT_SECONDS = 2;
const DEFAULT_UPDATE_DEBOUNCE_MS = 5000;
const DEFAULT_UPDATE_DELTA_BYTES = 100_000;
const DEFAULT_UPDATE_DELTA_MESSAGES = 50;
const DEFAULT_NAMESPACE = "default";

function mergeConfig(
  defaults: DeepMemoryConfig | undefined,
  overrides: DeepMemoryConfig | undefined,
): DeepMemoryConfig {
  // Shallow merge is sufficient here; we normalize downstream.
  return {
    ...defaults,
    ...overrides,
    retrieve: {
      ...defaults?.retrieve,
      ...overrides?.retrieve,
      cache: {
        ...defaults?.retrieve?.cache,
        ...overrides?.retrieve?.cache,
      },
    },
    inject: {
      ...defaults?.inject,
      ...overrides?.inject,
    },
    update: {
      ...defaults?.update,
      ...overrides?.update,
      thresholds: {
        ...defaults?.update?.thresholds,
        ...overrides?.update?.thresholds,
      },
      nearCompaction: {
        ...defaults?.update?.nearCompaction,
        ...overrides?.update?.nearCompaction,
      },
    },
  };
}

export function resolveDeepMemoryConfig(
  cfg: OpenClawConfig,
  agentId: string,
): ResolvedDeepMemoryConfig | null {
  const defaults = cfg.agents?.defaults?.deepMemory;
  const overrides = resolveAgentConfig(cfg, agentId)?.deepMemory;
  const merged = mergeConfig(defaults, overrides);

  const enabled = merged.enabled ?? false;
  const namespace = (merged.namespace ?? DEFAULT_NAMESPACE).trim() || DEFAULT_NAMESPACE;
  const baseUrl = (merged.baseUrl ?? "").trim();
  if (!enabled || !baseUrl) {
    return null;
  }

  const timeoutSeconds = merged.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS;
  const timeoutMs = clampInt(Math.floor(timeoutSeconds * 1000), 100, 60_000);

  const maxMemories = clampInt(
    merged.retrieve?.maxMemories ?? DEFAULT_RETRIEVE_MAX_MEMORIES,
    1,
    50,
  );
  const cacheEnabled = merged.retrieve?.cache?.enabled ?? true;
  const ttlMinutes = merged.retrieve?.cache?.ttlMinutes ?? DEFAULT_RETRIEVE_CACHE_TTL_MINUTES;
  const ttlMs = clampInt(Math.floor(ttlMinutes * 60_000), 0, 60 * 60_000);
  const maxEntries = clampInt(
    merged.retrieve?.cache?.maxEntries ?? DEFAULT_RETRIEVE_CACHE_MAX_ENTRIES,
    1,
    5000,
  );

  const injectMaxChars = clampInt(merged.inject?.maxChars ?? DEFAULT_INJECT_MAX_CHARS, 200, 50_000);
  const label = (merged.inject?.label ?? "Deep Memory").trim() || "Deep Memory";

  const updateEnabled = merged.update?.enabled ?? true;
  const deltaBytes = clampInt(
    merged.update?.thresholds?.deltaBytes ?? DEFAULT_UPDATE_DELTA_BYTES,
    0,
    Number.MAX_SAFE_INTEGER,
  );
  const deltaMessages = clampInt(
    merged.update?.thresholds?.deltaMessages ?? DEFAULT_UPDATE_DELTA_MESSAGES,
    0,
    Number.MAX_SAFE_INTEGER,
  );
  const debounceMs = clampInt(merged.update?.debounceMs ?? DEFAULT_UPDATE_DEBOUNCE_MS, 0, 60_000);
  const nearCompactionEnabled = merged.update?.nearCompaction?.enabled ?? true;

  return {
    enabled: true,
    namespace,
    baseUrl,
    timeoutMs,
    retrieve: {
      maxMemories,
      cache: { enabled: Boolean(cacheEnabled), ttlMs, maxEntries },
    },
    inject: { maxChars: injectMaxChars, label },
    update: {
      enabled: Boolean(updateEnabled),
      thresholds: { deltaBytes, deltaMessages },
      debounceMs,
      nearCompaction: { enabled: Boolean(nearCompactionEnabled) },
    },
  };
}
