import type { CommandHandler } from "./commands-types.js";
import { resolveSessionAgentId } from "../../agents/agent-scope.js";
import { resolveDeepMemoryConfig } from "../../agents/deep-memory.js";
import { formatCliCommand } from "../../cli/command-format.js";
import { DeepMemoryClient } from "../../deep-memory/client.js";
import { logVerbose } from "../../globals.js";

const COMMAND = "/deepmemory";

type Parsed = { ok: true; details: boolean; queue: boolean } | { ok: false; error: string } | null;

function parseDeepMemoryCommand(raw: string): Parsed {
  const trimmed = raw.trim();
  if (!trimmed.toLowerCase().startsWith(COMMAND)) {
    return null;
  }
  const rest = trimmed.slice(COMMAND.length).trim();
  if (!rest) {
    return { ok: true, details: false, queue: false };
  }
  const tokens = rest
    .split(/\s+/)
    .filter(Boolean)
    .map((t) => t.trim());
  const first = tokens[0]?.toLowerCase();
  if (first && first !== "status") {
    return { ok: false, error: "Usage: /deepmemory status [details] [queue]" };
  }
  const details = tokens.some((t) => {
    const v = t.toLowerCase();
    return v === "details" || v === "--details";
  });
  const queue = tokens.some((t) => {
    const v = t.toLowerCase();
    return v === "queue" || v === "--queue";
  });
  return { ok: true, details, queue };
}

function formatElevatedRequired(params: Parameters<CommandHandler>[0]): string {
  const lines: string[] = [];
  lines.push("⚠️ /deepmemory status 需要 elevated 权限（并且 sender 在 allowFrom 中）。");
  lines.push("用法：");
  lines.push("- /deepmemory status");
  lines.push("- /deepmemory status details   （尝试调用 /health/details；需要 admin key）");
  lines.push("- /deepmemory status queue     （尝试调用 /queue/stats；需要 admin key）");
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

function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === "object" && v ? (v as Record<string, unknown>) : null;
}

function pickString(obj: Record<string, unknown> | null, key: string): string | undefined {
  const v = obj?.[key];
  return typeof v === "string" && v.trim() ? v.trim() : undefined;
}

function pickNumber(obj: Record<string, unknown> | null, key: string): number | undefined {
  const v = obj?.[key];
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

export const handleDeepMemoryStatusCommand: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) {
    return null;
  }
  const parsed = parseDeepMemoryCommand(params.command.commandBodyNormalized);
  if (!parsed) {
    return null;
  }
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring /deepmemory from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
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
    return {
      shouldContinue: false,
      reply: { text: "⚠️ /deepmemory unavailable (missing agent id)." },
    };
  }
  const deep = resolveDeepMemoryConfig(params.cfg, agentId);
  if (!deep) {
    return {
      shouldContinue: false,
      reply: { text: "⚠️ Deep memory 未启用（agents.*.deepMemory.enabled/baseUrl）。" },
    };
  }

  const client = new DeepMemoryClient({
    baseUrl: deep.baseUrl,
    timeoutMs: deep.timeoutMs,
    cache: { enabled: false, ttlMs: 0, maxEntries: 1 },
    namespace: deep.namespace,
    apiKey: deep.apiKey,
  });

  const lines: string[] = [];
  lines.push("Deep memory status");
  lines.push(`- baseUrl: ${deep.baseUrl}`);
  lines.push(`- namespace: ${deep.namespace}`);

  const health = await client.health({ details: parsed.details });
  if (!health.ok) {
    lines.push(`- health: ❌ ${health.error}`);
  } else {
    const body = asRecord(health.value);
    const service = asRecord(body?.service);
    const version = pickString(service, "version");
    const ok = body?.ok === true;
    lines.push(`- health: ${ok ? "✅ ok" : "⚠️ not_ok"}${version ? ` (v${version})` : ""}`);
    const guardrails = asRecord(body?.guardrails);
    const rate = asRecord(guardrails?.rateLimit);
    const enabled = rate?.enabled === true;
    const retrieve = pickNumber(rate, "retrievePerWindow");
    const update = pickNumber(rate, "updatePerWindow");
    if (enabled) {
      lines.push(
        `- rateLimit: enabled retrieve=${retrieve ?? "?"}/window update=${update ?? "?"}/window`,
      );
    } else {
      lines.push("- rateLimit: disabled");
    }
    const backlog = asRecord(guardrails?.updateBacklog);
    const rejectPending = pickNumber(backlog, "rejectPending");
    if (rejectPending && rejectPending > 0) {
      lines.push(`- updateBacklog: reject when pending>=${rejectPending}`);
    } else {
      lines.push("- updateBacklog: disabled");
    }
  }

  const readyz = await client.readyz();
  if (!readyz.ok) {
    lines.push(`- readyz: ❌ ${readyz.error}`);
  } else {
    const ok = asRecord(readyz.value)?.ok === true;
    lines.push(`- readyz: ${ok ? "✅ ok" : "⚠️ not_ok"}`);
  }

  if (parsed.queue) {
    const q = await client.queueStats();
    if (!q.ok) {
      lines.push(`- queue: ❌ ${q.error}`);
    } else {
      const r = asRecord(q.value);
      const pending = pickNumber(r, "pendingApprox");
      const active = pickNumber(r, "active");
      lines.push(
        `- queue: pendingApprox=${pending ?? "?"} active=${active ?? "?"} inflightKeys=${pickNumber(r, "inflightKeys") ?? "?"}`,
      );
    }
  }

  return { shouldContinue: false, reply: { text: lines.join("\n") } };
};
