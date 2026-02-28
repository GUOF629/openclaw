import { resolveSessionAgentId } from "../../agents/agent-scope.js";
import { resolveDeepMemoryConfig } from "../../agents/deep-memory.js";
import { formatCliCommand } from "../../cli/command-format.js";
import { DeepMemoryClient } from "../../deep-memory/client.js";
import { logVerbose } from "../../globals.js";
import type { CommandHandler } from "./commands-types.js";

type ParsedForgetCommand =
  | {
      ok: true;
      dryRun: boolean;
      confirm: boolean;
      sessionId?: string;
      memoryIds?: string[];
    }
  | { ok: false; error: string };

const COMMAND = "/forget";

function parseForgetCommand(raw: string): ParsedForgetCommand | null {
  const trimmed = raw.trim();
  if (!trimmed.toLowerCase().startsWith(COMMAND)) {
    return null;
  }
  const rest = trimmed.slice(COMMAND.length).trim();
  if (!rest) {
    return { ok: true, dryRun: true, confirm: false };
  }
  const tokens = rest.split(/\s+/).filter(Boolean);
  let confirm = false;
  let dryRunExplicit: boolean | undefined;
  let sessionId: string | undefined;
  let memoryIds: string[] | undefined;

  const readRemainderList = (startIndex: number): string[] => {
    const remainder = tokens.slice(startIndex).join(" ").trim();
    if (!remainder) {
      return [];
    }
    return remainder
      .split(/[,\s]+/g)
      .map((s) => s.trim())
      .filter(Boolean);
  };

  for (let i = 0; i < tokens.length; i += 1) {
    const t = tokens[i]?.trim();
    if (!t) {
      continue;
    }
    const lowered = t.toLowerCase();
    if (
      lowered === "confirm" ||
      lowered === "--confirm" ||
      lowered === "yes" ||
      lowered === "--yes" ||
      lowered === "execute" ||
      lowered === "--execute"
    ) {
      confirm = true;
      continue;
    }
    if (
      lowered === "dry-run" ||
      lowered === "--dry-run" ||
      lowered === "preview" ||
      lowered === "--preview"
    ) {
      dryRunExplicit = true;
      continue;
    }
    if (lowered === "apply" || lowered === "--apply" || lowered === "--no-dry-run") {
      dryRunExplicit = false;
      continue;
    }
    if (lowered === "session" || lowered === "--session") {
      const value = tokens[i + 1]?.trim();
      if (!value) {
        return { ok: false, error: "Usage: /forget session <sessionId> [confirm]" };
      }
      sessionId = value;
      i += 1;
      continue;
    }
    if (lowered === "id" || lowered === "--id") {
      const value = tokens[i + 1]?.trim();
      if (!value) {
        return { ok: false, error: "Usage: /forget id <memoryId> [confirm]" };
      }
      memoryIds = [value];
      i += 1;
      continue;
    }
    if (lowered === "ids" || lowered === "--ids") {
      const ids = readRemainderList(i + 1);
      if (ids.length === 0) {
        return { ok: false, error: "Usage: /forget ids <id1,id2,...> [confirm]" };
      }
      memoryIds = ids;
      break;
    }
  }

  const dryRun = dryRunExplicit ?? !confirm;
  return { ok: true, dryRun, confirm, sessionId, memoryIds };
}

function formatElevatedRequired(params: Parameters<CommandHandler>[0]): string {
  const lines: string[] = [];
  lines.push("⚠️ /forget 需要 elevated 权限（并且 sender 在 allowFrom 中）。");
  lines.push("用法：");
  lines.push("- /forget            （默认 dry-run，预览删除当前 session 的 deep memory）");
  lines.push("- /forget confirm    （确认执行删除）");
  lines.push("- /forget id <id> [confirm]");
  lines.push("- /forget ids <id1,id2,...> [confirm]");
  lines.push("- /forget session <sessionId> [confirm]");
  if (params.elevated.failures.length > 0) {
    lines.push(
      `Failing gates: ${params.elevated.failures.map((f) => `${f.gate} (${f.key})`).join(", ")}`,
    );
  }
  if (params.sessionKey) {
    lines.push(
      `See: ${formatCliCommand(`openclaw sandbox explain --session ${params.sessionKey}`)}`,
    );
  }
  return lines.join("\n");
}

export const handleForgetCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) {
    return null;
  }
  const normalized = params.command.commandBodyNormalized;
  const parsed = parseForgetCommand(normalized);
  if (!parsed) {
    return null;
  }
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /forget from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }
  if (!params.elevated.enabled || !params.elevated.allowed) {
    return { shouldContinue: false, reply: { text: formatElevatedRequired(params) } };
  }
  if (!parsed.ok) {
    return { shouldContinue: false, reply: { text: parsed.error } };
  }

  const agentId =
    params.agentId ??
    resolveSessionAgentId({
      sessionKey: params.sessionKey,
      config: params.cfg,
    });
  if (!agentId) {
    return { shouldContinue: false, reply: { text: "⚠️ /forget unavailable (missing agent id)." } };
  }

  const deep = resolveDeepMemoryConfig(params.cfg, agentId);
  if (!deep) {
    return {
      shouldContinue: false,
      reply: { text: "⚠️ Deep memory 未启用（agents.*.deepMemory.enabled/baseUrl）。" },
    };
  }

  const sessionIdFromContext = params.sessionEntry?.sessionId;
  const sessionId = parsed.sessionId ?? sessionIdFromContext;
  if (!sessionId && (!parsed.memoryIds || parsed.memoryIds.length === 0)) {
    return {
      shouldContinue: false,
      reply: {
        text: "⚠️ /forget 需要 sessionId（当前会话缺少 session id），或显式提供 memory id(s)。",
      },
    };
  }

  const client = new DeepMemoryClient({
    baseUrl: deep.baseUrl,
    timeoutMs: deep.timeoutMs,
    cache: { enabled: false, ttlMs: 0, maxEntries: 1 },
    namespace: deep.namespace,
    apiKey: deep.apiKey,
  });

  const result = await client.forget({
    sessionId: parsed.memoryIds?.length ? undefined : sessionId,
    memoryIds: parsed.memoryIds,
    dryRun: parsed.dryRun,
  });
  if (!result.ok) {
    return {
      shouldContinue: false,
      reply: { text: `❌ /forget 调用 deep-memory-server 失败：${result.error}` },
    };
  }

  const r = result.value;
  if (r.status === "dry_run") {
    const sessionLabel = sessionId ? `session=${sessionId}` : "";
    const idsLabel = r.delete_ids != null ? `ids=${r.delete_ids}` : "";
    const targets = [sessionLabel, idsLabel].filter(Boolean).join(", ") || "(none)";
    return {
      shouldContinue: false,
      reply: {
        text: [
          `🧪 /forget dry-run: ${targets}`,
          `namespace=${r.namespace ?? deep.namespace}`,
          "要确认执行：/forget confirm",
        ].join("\n"),
      },
    };
  }

  return {
    shouldContinue: false,
    reply: {
      text: `✅ /forget processed. namespace=${r.namespace ?? deep.namespace} deleted=${r.deleted ?? 0}`,
    },
  };
};
