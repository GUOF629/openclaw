import type { OpenClawConfig } from "../config/config.js";
import type { RustFsConfig } from "../config/types.tools.js";
import { clampInt } from "../utils.js";
import { resolveAgentConfig } from "./agent-scope.js";

export type ResolvedRustFsConfig = {
  enabled: boolean;
  project: string;
  apiKey?: string;
  baseUrl: string;
  linkTtlSeconds: number;
  maxUploadBytes: number;
};

const DEFAULT_PROJECT = "default";
const DEFAULT_LINK_TTL_SECONDS = 300;
const DEFAULT_MAX_UPLOAD_BYTES = 500 * 1024 * 1024;

function mergeConfig(
  defaults: RustFsConfig | undefined,
  overrides: RustFsConfig | undefined,
): RustFsConfig {
  return { ...defaults, ...overrides };
}

export function resolveRustFsConfig(
  cfg: OpenClawConfig,
  agentId: string,
): ResolvedRustFsConfig | null {
  const defaults = cfg.agents?.defaults?.rustfs;
  const overrides = resolveAgentConfig(cfg, agentId)?.rustfs;
  const merged = mergeConfig(defaults, overrides);

  const enabled = merged.enabled ?? false;
  const baseUrl = (merged.baseUrl ?? "").trim();
  if (!enabled || !baseUrl) {
    return null;
  }

  const project = (merged.project ?? DEFAULT_PROJECT).trim() || DEFAULT_PROJECT;
  const apiKey = (merged.apiKey ?? "").trim() || undefined;
  const linkTtlSeconds = clampInt(merged.linkTtlSeconds ?? DEFAULT_LINK_TTL_SECONDS, 30, 3600);
  const maxUploadBytes = clampInt(
    merged.maxUploadBytes ?? DEFAULT_MAX_UPLOAD_BYTES,
    1,
    10 * 1024 * 1024 * 1024,
  );

  return {
    enabled: true,
    project,
    apiKey,
    baseUrl,
    linkTtlSeconds,
    maxUploadBytes,
  };
}
