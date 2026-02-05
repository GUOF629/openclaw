import { Type } from "@sinclair/typebox";
import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";
import type { AnyAgentTool } from "./common.js";
import { DeepMemoryClient } from "../../deep-memory/client.js";
import { RustFsClient } from "../../rustfs/client.js";
import { resolveSessionAgentId } from "../agent-scope.js";
import { resolveDeepMemoryConfig } from "../deep-memory.js";
import { resolveRustFsConfig } from "../rustfs.js";
import { jsonResult, readNumberParam, readStringParam } from "./common.js";

type FileSearchCandidate = {
  n: number;
  fileId: string;
  filename: string;
  mime?: string;
  size?: number;
  createdAtMs?: number;
  extractStatus?: string;
  tags?: string[];
  kind?: string;
  hint?: string;
  semanticScore?: number;
  semanticTopics?: string[];
  semanticEntities?: string[];
  semanticContext?: string;
  semanticSummary?: string;
};

function buildClarify(params: { candidates: FileSearchCandidate[]; includeSemantic: boolean }):
  | {
      required: boolean;
      reasons: string[];
      questions: string[];
    }
  | undefined {
  const c = params.candidates;
  if (c.length === 0) {
    return {
      required: false,
      reasons: ["no_candidates"],
      questions: ["没找到匹配文件。你能补充文件类型、时间范围、或更具体的关键词吗？"],
    };
  }

  const reasons: string[] = [];
  const questions: string[] = [];

  // Same filename ambiguity.
  const byName = new Map<string, FileSearchCandidate[]>();
  for (const it of c) {
    const key = (it.filename ?? "").trim().toLowerCase();
    if (!key) {
      continue;
    }
    const arr = byName.get(key) ?? [];
    arr.push(it);
    byName.set(key, arr);
  }
  for (const [name, arr] of byName.entries()) {
    if (arr.length >= 2) {
      reasons.push("duplicate_filename");
      questions.push(
        `发现多个同名文件 \`${name}\`。你要哪一个（按 n 选择），或提供更具体的会话/时间线索？`,
      );
      break;
    }
  }

  if (params.includeSemantic && c.length >= 2) {
    const top = c[0];
    const second = c[1];
    const s1 = typeof top?.semanticScore === "number" ? top.semanticScore : 0;
    const s2 = typeof second?.semanticScore === "number" ? second.semanticScore : 0;
    if (s1 <= 0) {
      reasons.push("weak_semantic_evidence");
      questions.push("语义证据较弱。你能提供更明确的项目/主题/关键词，或文件的大致名称吗？");
    } else if (s2 > 0 && s1 - s2 <= Math.max(0.15 * s1, 0.5)) {
      reasons.push("close_semantic_scores");
      const tTopics = (top?.semanticTopics ?? []).slice(0, 3).join("、");
      const sTopics = (second?.semanticTopics ?? []).slice(0, 3).join("、");
      if (tTopics || sTopics) {
        questions.push(
          `前两个候选相关性接近。候选1主题：${tTopics || "（无）"}；候选2主题：${sTopics || "（无）"}。你更倾向哪一个（按 n 选择）？`,
        );
      } else {
        questions.push(
          "前两个候选相关性接近。你更倾向哪一个（按 n 选择），或说出你要的具体内容点？",
        );
      }
    }
  }

  // If multiple candidates and no disambiguation info, encourage choosing by n.
  if (c.length >= 2) {
    reasons.push("multiple_candidates");
    questions.push("请从 candidates 里回复 n（或 fileId）确认要发送的文件。");
  }

  if (reasons.length === 0) {
    return undefined;
  }
  return {
    required: true,
    reasons: Array.from(new Set(reasons)),
    questions: Array.from(new Set(questions)).slice(0, 5),
  };
}

function readSemanticsFromAnnotations(annotations: unknown): {
  summary?: string;
  topics?: string[];
  entities?: string[];
} {
  const root =
    annotations && typeof annotations === "object"
      ? (annotations as Record<string, unknown>)
      : null;
  const sem =
    root && root.semantics && typeof root.semantics === "object"
      ? (root.semantics as Record<string, unknown>)
      : null;
  if (!sem) {
    return {};
  }
  const summary = typeof sem.summary === "string" ? sem.summary.slice(0, 400) : undefined;
  const topics =
    Array.isArray(sem.topics) && sem.topics.every((t) => typeof t === "string")
      ? sem.topics.slice(0, 10)
      : undefined;
  const entities =
    Array.isArray(sem.entities) && sem.entities.every((t) => typeof t === "string")
      ? sem.entities.slice(0, 10)
      : undefined;
  return {
    summary: summary && summary.trim() ? summary.trim() : undefined,
    topics: topics && topics.length > 0 ? topics : undefined,
    entities: entities && entities.length > 0 ? entities : undefined,
  };
}

function readIngestHintsFromAnnotations(annotations: unknown): {
  tags?: string[];
  kind?: string;
  hint?: string;
} {
  const root =
    annotations && typeof annotations === "object"
      ? (annotations as Record<string, unknown>)
      : null;
  const ingestRaw =
    root && root.openclaw_ingest && typeof root.openclaw_ingest === "object"
      ? (root.openclaw_ingest as Record<string, unknown>)
      : null;
  const clsRaw =
    root && root.classification && typeof root.classification === "object"
      ? (root.classification as Record<string, unknown>)
      : null;
  const ingest = ingestRaw ?? clsRaw;
  if (!ingest) {
    return {};
  }
  const kind = typeof ingest.kind === "string" ? ingest.kind.trim() : "";
  const hint = typeof ingest.hint === "string" ? ingest.hint.trim() : "";
  const tags =
    Array.isArray(ingest.tags) && ingest.tags.every((t) => typeof t === "string")
      ? ingest.tags
          .map((t) => t.trim())
          .filter(Boolean)
          .slice(0, 20)
      : undefined;
  return {
    kind: kind || undefined,
    hint: hint || undefined,
    tags: tags && tags.length > 0 ? tags : undefined,
  };
}

function buildCandidates(items: Array<Record<string, unknown>>): FileSearchCandidate[] {
  return items.slice(0, 50).map((raw, idx) => {
    const fileId = typeof raw.file_id === "string" ? raw.file_id : "";
    const filename = typeof raw.filename === "string" ? raw.filename : "";
    const mime = typeof raw.mime === "string" ? raw.mime : undefined;
    const size = typeof raw.size === "number" ? raw.size : undefined;
    const createdAtMs = typeof raw.created_at_ms === "number" ? raw.created_at_ms : undefined;
    const extractStatus = typeof raw.extract_status === "string" ? raw.extract_status : undefined;

    const { tags, kind, hint } = readIngestHintsFromAnnotations(raw.annotations);
    const sem = readSemanticsFromAnnotations(raw.annotations);

    const semantic =
      raw.semantic && typeof raw.semantic === "object"
        ? (raw.semantic as Record<string, unknown>)
        : null;
    const semanticScore =
      semantic && typeof semantic.score === "number" ? semantic.score : undefined;
    const semanticTopics =
      (semantic && Array.isArray(semantic.topics)
        ? (semantic.topics as string[]).filter((t) => typeof t === "string").slice(0, 10)
        : undefined) ?? sem.topics;
    const semanticEntities =
      (semantic && Array.isArray(semantic.entities)
        ? (semantic.entities as string[]).filter((t) => typeof t === "string").slice(0, 10)
        : undefined) ?? sem.entities;
    const semanticContext =
      semantic && typeof semantic.context === "string" ? semantic.context.slice(0, 400) : undefined;
    const semanticSummary =
      (semantic && typeof semantic.summary === "string"
        ? semantic.summary.slice(0, 400)
        : undefined) ?? sem.summary;

    return {
      n: idx + 1,
      fileId,
      filename,
      mime,
      size,
      createdAtMs,
      extractStatus,
      tags,
      kind,
      hint,
      semanticScore,
      semanticTopics,
      semanticEntities,
      semanticContext,
      semanticSummary,
    };
  });
}

const FileSearchSchema = Type.Object({
  query: Type.Optional(Type.String()),
  semanticQuery: Type.Optional(Type.String()),
  sessionId: Type.Optional(Type.String()),
  mime: Type.Optional(Type.String()),
  extractStatus: Type.Optional(Type.String()),
  limit: Type.Optional(Type.Number()),
  includeSemantic: Type.Optional(Type.Boolean()),
  semanticMaxFiles: Type.Optional(Type.Number()),
  semanticMaxMemories: Type.Optional(Type.Number()),
  semanticMaxChars: Type.Optional(Type.Number()),
  rerank: Type.Optional(Type.Boolean()),
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
  kind: Type.Optional(Type.String()),
  tags: Type.Optional(Type.Array(Type.String())),
  hint: Type.Optional(Type.String()),
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

function buildDeepMemoryClient(cfg: OpenClawConfig, agentId: string): DeepMemoryClient | null {
  const resolved = resolveDeepMemoryConfig(cfg, agentId);
  if (!resolved) {
    return null;
  }
  return new DeepMemoryClient({
    baseUrl: resolved.baseUrl,
    timeoutMs: resolved.timeoutMs,
    cache: resolved.retrieve.cache,
    namespace: resolved.namespace,
    apiKey: resolved.apiKey,
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
  const deep = buildDeepMemoryClient(cfg, agentId);
  const semanticCache = new Map<string, { expiresAt: number; value: Record<string, unknown> }>();
  const semanticCacheTtlMs = 30_000;
  const semanticMaxConcurrent = 4;

  async function mapWithConcurrency<T, R>(
    items: T[],
    concurrency: number,
    fn: (item: T) => Promise<R>,
  ): Promise<R[]> {
    const limit = Math.max(1, Math.min(16, Math.trunc(concurrency)));
    const out: R[] = [];
    let i = 0;
    async function worker(): Promise<void> {
      for (;;) {
        const idx = i;
        i += 1;
        const item = items[idx];
        if (!item) {
          return;
        }
        out[idx] = await fn(item);
      }
    }
    const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
    await Promise.all(workers);
    return out;
  }
  return {
    label: "File Search",
    name: "file_search",
    description:
      "Search session files archived in RustFS by project + optional filters (query, sessionId, mime). Always ask the user to confirm a candidate (by n or fileId) before calling file_send.",
    parameters: FileSearchSchema,
    execute: async (_toolCallId, params) => {
      try {
        const query = readStringParam(params, "query", { required: false, label: "query" });
        const semanticQuery = readStringParam(params, "semanticQuery", {
          required: false,
          label: "semanticQuery",
        });
        const sessionId = readStringParam(params, "sessionId", {
          required: false,
          label: "sessionId",
        });
        const mime = readStringParam(params, "mime", { required: false, label: "mime" });
        const extractStatus = readStringParam(params, "extractStatus", {
          required: false,
          label: "extractStatus",
        });
        const limit = readNumberParam(params, "limit", { integer: true });
        const includeSemantic = Boolean((params as Record<string, unknown>).includeSemantic);
        const rerank = Boolean((params as Record<string, unknown>).rerank);
        const semanticMaxFiles =
          readNumberParam(params, "semanticMaxFiles", { integer: true }) ?? 10;
        const semanticMaxMemories =
          readNumberParam(params, "semanticMaxMemories", { integer: true }) ?? 8;
        const semanticMaxChars =
          readNumberParam(params, "semanticMaxChars", { integer: true }) ?? 800;

        const out = await client.search({
          query,
          sessionId,
          mime,
          extractStatus,
          limit,
        });
        if (!out.ok) {
          return jsonResult(out);
        }
        if (!includeSemantic) {
          const items = out.items as unknown as Array<Record<string, unknown>>;
          return jsonResult({
            ok: true,
            items,
            candidates: buildCandidates(items),
            selection: {
              required: true,
              message:
                "请让用户从 candidates 里选择（回复 n 或 fileId），确认后再调用 file_send。避免在未确认时直接发送文件。",
            },
          });
        }

        if (!deep) {
          const items = out.items as unknown as Array<Record<string, unknown>>;
          return jsonResult({
            ok: true,
            items,
            candidates: buildCandidates(items),
            semantic: { ok: false, error: "deep-memory not configured for this agent" },
            selection: {
              required: true,
              message:
                "deep-memory 未配置；请让用户从 candidates 里选择（回复 n 或 fileId），确认后再调用 file_send。",
            },
          });
        }

        const userInput = (semanticQuery ?? query ?? "").trim();
        if (!userInput) {
          return jsonResult({
            ...out,
            semantic: {
              ok: false,
              error: "semanticQuery or query required for includeSemantic=true",
            },
          });
        }

        const semanticItems = out.items.slice(0, Math.max(1, Math.min(50, semanticMaxFiles)));
        const semanticIds = new Set(semanticItems.map((i) => i.file_id));
        const scoreOf = (v: unknown): number => {
          const score = (v as { semantic?: { score?: unknown } } | null | undefined)?.semantic
            ?.score;
          return typeof score === "number" ? score : 0;
        };
        const now = Date.now();
        for (const [key, entry] of semanticCache.entries()) {
          if (entry.expiresAt <= now) {
            semanticCache.delete(key);
          }
        }

        const semanticLookup = await mapWithConcurrency(
          semanticItems,
          semanticMaxConcurrent,
          async (item) => {
            const dmSessionId = `rustfs:file:${item.file_id}`;
            const retrieveKey = `retrieve::${userInput}::${semanticMaxMemories}::${dmSessionId}`;
            const inspectKey = `inspect::${dmSessionId}`;

            const cachedRetrieve = semanticCache.get(retrieveKey);
            const cachedInspect = semanticCache.get(inspectKey);

            const retrievePromise =
              cachedRetrieve && cachedRetrieve.expiresAt > Date.now()
                ? Promise.resolve(cachedRetrieve.value)
                : deep
                    .retrieveContext({
                      userInput,
                      sessionId: dmSessionId,
                      maxMemories: Math.max(1, Math.min(50, semanticMaxMemories)),
                    })
                    .then((retrieved) => {
                      const context = (retrieved.context ?? "").trim();
                      const contextClamped =
                        context.length > semanticMaxChars
                          ? context.slice(0, semanticMaxChars)
                          : context;
                      const memories = Array.isArray(retrieved.memories) ? retrieved.memories : [];
                      const score = memories.reduce(
                        (acc, m) => acc + (typeof m.relevance === "number" ? m.relevance : 0),
                        0,
                      );
                      const value = {
                        score,
                        context: contextClamped,
                        memories: memories
                          .slice(0, Math.max(1, Math.min(50, semanticMaxMemories)))
                          .map((m) => ({
                            id: m.id,
                            relevance: m.relevance,
                            importance: m.importance,
                            content:
                              typeof m.content === "string" ? m.content.slice(0, 400) : undefined,
                          })),
                      } satisfies Record<string, unknown>;
                      semanticCache.set(retrieveKey, {
                        value,
                        expiresAt: Date.now() + semanticCacheTtlMs,
                      });
                      return value;
                    });

            const inspectPromise =
              cachedInspect && cachedInspect.expiresAt > Date.now()
                ? Promise.resolve(cachedInspect.value)
                : deep
                    .inspectSession({
                      sessionId: dmSessionId,
                      limit: 100,
                      includeContent: false,
                    })
                    .then((inspected) => {
                      const topics =
                        Array.isArray(inspected.topics) && inspected.topics.length > 0
                          ? inspected.topics
                              .map((t) => (typeof t?.name === "string" ? t.name.trim() : ""))
                              .filter(Boolean)
                              .slice(0, 20)
                          : [];
                      const entities =
                        Array.isArray(inspected.entities) && inspected.entities.length > 0
                          ? inspected.entities
                              .map((e) => (typeof e?.name === "string" ? e.name.trim() : ""))
                              .filter(Boolean)
                              .slice(0, 20)
                          : [];
                      const summary =
                        typeof inspected.summary === "string"
                          ? inspected.summary.slice(0, 800)
                          : undefined;
                      const value = {
                        topics,
                        entities,
                        summary,
                      } satisfies Record<string, unknown>;
                      semanticCache.set(inspectKey, {
                        value,
                        expiresAt: Date.now() + semanticCacheTtlMs,
                      });
                      return value;
                    });

            const [retrieveEvidence, inspectEvidence] = await Promise.all([
              retrievePromise,
              inspectPromise,
            ]);

            const semantic = {
              deepMemorySessionId: dmSessionId,
              ...inspectEvidence,
              ...retrieveEvidence,
            } satisfies Record<string, unknown>;

            return { fileId: item.file_id, semantic };
          },
        );

        const semanticMap = new Map<string, Record<string, unknown>>();
        for (const entry of semanticLookup) {
          if (entry && entry.fileId && entry.semantic) {
            semanticMap.set(entry.fileId, entry.semantic);
          }
        }

        const decorated: Array<Record<string, unknown>> = out.items.map((item) => {
          if (!semanticIds.has(item.file_id)) {
            return item as unknown as Record<string, unknown>;
          }
          const semantic = semanticMap.get(item.file_id);
          return semantic
            ? ({ ...(item as unknown as Record<string, unknown>), semantic } as Record<
                string,
                unknown
              >)
            : (item as unknown as Record<string, unknown>);
        });

        const finalItems = rerank
          ? decorated.toSorted((a, b) => scoreOf(b) - scoreOf(a))
          : decorated;

        const candidates = buildCandidates(finalItems);
        const clarify = buildClarify({ candidates, includeSemantic });

        return jsonResult({
          ok: true,
          items: finalItems,
          candidates,
          semantic: { ok: true },
          clarify,
          selection: {
            required: true,
            message:
              "请让用户从 candidates 里选择（回复 n 或 fileId），确认后再调用 file_send。若用户目标不明确，先追问澄清再发送。",
          },
        });
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
      "Create a short-lived public download link for a RustFS fileId. Only call this after the user explicitly confirms which file to send (from file_search candidates).",
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
        const kind = readStringParam(params, "kind", { required: false, label: "kind" });
        const hint = readStringParam(params, "hint", { required: false, label: "hint" });
        const tagsRaw = (params as Record<string, unknown>).tags;
        const tags = Array.isArray(tagsRaw)
          ? tagsRaw
              .filter((t): t is string => typeof t === "string")
              .map((t) => t.trim())
              .filter((t) => t.length > 0)
              .slice(0, 50)
          : undefined;

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
        if (out.ok && (kind || hint || (tags && tags.length > 0))) {
          await client.upsertAnnotations({
            fileId: out.file_id,
            source: "openclaw",
            annotations: {
              openclaw_ingest: {
                kind: kind ?? undefined,
                hint: hint ?? undefined,
                tags: tags && tags.length > 0 ? tags : undefined,
                session_id: sessionId ?? undefined,
                source: source ?? undefined,
                mime: mime ?? undefined,
              },
            },
          });
        }
        return jsonResult(out);
      } catch (err) {
        return jsonResult({ ok: false, error: err instanceof Error ? err.message : String(err) });
      }
    },
  };
}

export const __test__ = {
  buildClarify,
  buildCandidates,
  readIngestHintsFromAnnotations,
  readSemanticsFromAnnotations,
};
