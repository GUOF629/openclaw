import { Type } from "@sinclair/typebox";
import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import type { AnyAgentTool } from "./common.js";
import { RustFsClient } from "../../rustfs/client.js";
import { resolveSessionAgentId } from "../agent-scope.js";
import { resolveRustFsConfig } from "../rustfs.js";
import { jsonResult, readNumberParam, readStringParam } from "./common.js";

const FileSearchSchema = Type.Object({
  query: Type.Optional(Type.String()),
  sessionId: Type.Optional(Type.String()),
  mime: Type.Optional(Type.String()),
  limit: Type.Optional(Type.Number()),
});

const FileSendSchema = Type.Object({
  fileId: Type.String(),
  ttlSeconds: Type.Optional(Type.Number()),
});

const FileIngestSchema = Type.Object({
  path: Type.String(),
  sessionId: Type.Optional(Type.String()),
  source: Type.Optional(Type.String()),
  mime: Type.Optional(Type.String()),
});

function buildClient(cfg: OpenClawConfig, agentId: string): RustFsClient | null {
  const resolved = resolveRustFsConfig(cfg, agentId);
  if (!resolved) {
    return null;
  }
  return new RustFsClient({
    baseUrl: resolved.baseUrl,
    apiKey: resolved.apiKey,
    project: resolved.project,
    linkTtlSeconds: resolved.linkTtlSeconds,
    maxUploadBytes: resolved.maxUploadBytes,
  });
}

export function createFileSearchTool(options: {
  config?: OpenClawConfig;
  agentSessionKey?: string;
}): AnyAgentTool | null {
  const cfg = options.config;
  if (!cfg) {
    return null;
  }
  const agentId = resolveSessionAgentId({ sessionKey: options.agentSessionKey, config: cfg });
  const client = buildClient(cfg, agentId);
  if (!client) {
    return null;
  }
  return {
    label: "File Search",
    name: "file_search",
    description:
      "Search session files archived in RustFS by project + optional filters (query, sessionId, mime). Returns a short candidate list for the user to pick from.",
    parameters: FileSearchSchema,
    execute: async (_toolCallId, params) => {
      try {
        const query = readStringParam(params, "query", { required: false, label: "query" });
        const sessionId = readStringParam(params, "sessionId", {
          required: false,
          label: "sessionId",
        });
        const mime = readStringParam(params, "mime", { required: false, label: "mime" });
        const limit = readNumberParam(params, "limit", { integer: true });
        const out = await client.search({ query, sessionId, mime, limit });
        return jsonResult(out);
      } catch (err) {
        return jsonResult({ ok: false, error: err instanceof Error ? err.message : String(err) });
      }
    },
  };
}

export function createFileSendTool(options: {
  config?: OpenClawConfig;
  agentSessionKey?: string;
}): AnyAgentTool | null {
  const cfg = options.config;
  if (!cfg) {
    return null;
  }
  const agentId = resolveSessionAgentId({ sessionKey: options.agentSessionKey, config: cfg });
  const client = buildClient(cfg, agentId);
  if (!client) {
    return null;
  }
  return {
    label: "File Send (Link)",
    name: "file_send",
    description:
      "Create a short-lived public download link for a RustFS fileId. Use this to share files with users (default behavior: send link).",
    parameters: FileSendSchema,
    execute: async (_toolCallId, params) => {
      try {
        const fileId = readStringParam(params, "fileId", { required: true, label: "fileId" });
        const ttlSeconds = readNumberParam(params, "ttlSeconds", { integer: true });
        const out = await client.createLink({
          fileId,
          ttlSeconds: ttlSeconds ?? undefined,
        });
        return jsonResult(out);
      } catch (err) {
        return jsonResult({ ok: false, error: err instanceof Error ? err.message : String(err) });
      }
    },
  };
}

export function createFileIngestTool(options: {
  config?: OpenClawConfig;
  agentSessionKey?: string;
  workspaceDir?: string;
}): AnyAgentTool | null {
  const cfg = options.config;
  const workspaceDir = options.workspaceDir?.trim();
  if (!cfg || !workspaceDir) {
    return null;
  }
  const agentId = resolveSessionAgentId({ sessionKey: options.agentSessionKey, config: cfg });
  const client = buildClient(cfg, agentId);
  if (!client) {
    return null;
  }
  return {
    label: "File Ingest",
    name: "file_ingest",
    description:
      "Upload a local workspace file into RustFS (streaming) so it can be searched/shared later. Path must be within the workspace directory.",
    parameters: FileIngestSchema,
    execute: async (_toolCallId, params) => {
      try {
        const rel = readStringParam(params, "path", { required: true, label: "path" });
        const sessionId = readStringParam(params, "sessionId", {
          required: false,
          label: "sessionId",
        });
        const source = readStringParam(params, "source", { required: false, label: "source" });
        const mime = readStringParam(params, "mime", { required: false, label: "mime" });

        const absPath = path.isAbsolute(rel) ? path.resolve(rel) : path.resolve(workspaceDir, rel);
        const relToWorkspace = path.relative(workspaceDir, absPath).replace(/\\/g, "/");
        const inWorkspace =
          relToWorkspace.length > 0 &&
          !relToWorkspace.startsWith("..") &&
          !path.isAbsolute(relToWorkspace);
        if (!inWorkspace) {
          return jsonResult({ ok: false, error: "path must be within the workspace" });
        }

        const stat = await fs.stat(absPath).catch(() => null);
        if (!stat || !stat.isFile()) {
          return jsonResult({ ok: false, error: "file not found" });
        }

        const out = await client.ingestFile({
          absPath,
          sessionId: sessionId ?? undefined,
          source: source ?? undefined,
          mime: mime ?? undefined,
        });
        return jsonResult(out);
      } catch (err) {
        return jsonResult({ ok: false, error: err instanceof Error ? err.message : String(err) });
      }
    },
  };
}
