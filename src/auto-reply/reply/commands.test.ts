import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../../config/config.js";
import type { MsgContext } from "../templating.js";
import {
  addSubagentRunForTests,
  resetSubagentRegistryForTests,
} from "../../agents/subagent-registry.js";
import * as internalHooks from "../../hooks/internal-hooks.js";
import { clearPluginCommands, registerPluginCommand } from "../../plugins/commands.js";
import { resetBashChatCommandForTests } from "./bash-command.js";
import { buildCommandContext, handleCommands } from "./commands.js";
import { parseInlineDirectives } from "./directive-handling.js";

// Avoid expensive workspace scans during /context tests.
vi.mock("./commands-context-report.js", () => ({
  buildContextReply: async (params: { command: { commandBodyNormalized: string } }) => {
    const normalized = params.command.commandBodyNormalized;
    if (normalized === "/context list") {
      return { text: "Injected workspace files:\n- AGENTS.md" };
    }
    if (normalized === "/context detail") {
      return { text: "Context breakdown (detailed)\nTop tools (schema size):" };
    }
    return { text: "/context\n- /context list\nInline shortcut" };
  },
}));

let testWorkspaceDir = os.tmpdir();

beforeAll(async () => {
  testWorkspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-commands-"));
  await fs.writeFile(path.join(testWorkspaceDir, "AGENTS.md"), "# Agents\n", "utf-8");
});

afterAll(async () => {
  await fs.rm(testWorkspaceDir, { recursive: true, force: true });
});

function buildParams(commandBody: string, cfg: OpenClawConfig, ctxOverrides?: Partial<MsgContext>) {
  const ctx = {
    Body: commandBody,
    CommandBody: commandBody,
    CommandSource: "text",
    CommandAuthorized: true,
    Provider: "whatsapp",
    Surface: "whatsapp",
    ...ctxOverrides,
  } as MsgContext;

  const command = buildCommandContext({
    ctx,
    cfg,
    isGroup: false,
    triggerBodyNormalized: commandBody.trim().toLowerCase(),
    commandAuthorized: true,
  });

  return {
    ctx,
    cfg,
    command,
    directives: parseInlineDirectives(commandBody),
    elevated: { enabled: true, allowed: true, failures: [] },
    sessionKey: "agent:main:main",
    workspaceDir: testWorkspaceDir,
    defaultGroupActivation: () => "mention",
    resolvedVerboseLevel: "off" as const,
    resolvedReasoningLevel: "off" as const,
    resolveDefaultThinkingLevel: async () => undefined,
    provider: "whatsapp",
    model: "test-model",
    contextTokens: 0,
    isGroup: false,
  };
}

describe("handleCommands gating", () => {
  it("blocks /bash when disabled", async () => {
    resetBashChatCommandForTests();
    const cfg = {
      commands: { bash: false, text: true },
      whatsapp: { allowFrom: ["*"] },
    } as OpenClawConfig;
    const params = buildParams("/bash echo hi", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("bash is disabled");
  });

  it("blocks /bash when elevated is not allowlisted", async () => {
    resetBashChatCommandForTests();
    const cfg = {
      commands: { bash: true, text: true },
      whatsapp: { allowFrom: ["*"] },
    } as OpenClawConfig;
    const params = buildParams("/bash echo hi", cfg);
    params.elevated = {
      enabled: true,
      allowed: false,
      failures: [{ gate: "allowFrom", key: "tools.elevated.allowFrom.whatsapp" }],
    };
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("elevated is not available");
  });

  it("blocks /config when disabled", async () => {
    const cfg = {
      commands: { config: false, debug: false, text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/config show", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/config is disabled");
  });

  it("blocks /debug when disabled", async () => {
    const cfg = {
      commands: { config: false, debug: false, text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/debug show", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/debug is disabled");
  });
});

describe("handleCommands /forget", () => {
  it("returns elevated-required message when elevated is blocked", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      agents: {
        defaults: {
          deepMemory: { enabled: true, baseUrl: "http://deep-memory.test", timeoutSeconds: 2 },
        },
      },
    } as OpenClawConfig;
    const params = buildParams("/forget", cfg);
    params.agentId = "main";
    params.elevated = {
      enabled: true,
      allowed: false,
      failures: [{ gate: "allowFrom", key: "tools.elevated.allowFrom.whatsapp" }],
    };
    // oxlint-disable-next-line typescript/no-explicit-any
    params.sessionEntry = { sessionId: "sess-1", updatedAt: Date.now() } as any;
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/forget 需要 elevated");
  });

  it("calls deep-memory-server /forget in dry-run mode by default", async () => {
    let seenBody: Record<string, unknown> | null = null;
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : "";
      const bodyText =
        typeof init?.body === "string"
          ? init.body
          : init?.body instanceof Uint8Array
            ? new TextDecoder().decode(init.body)
            : "";
      if (url.includes("/forget")) {
        seenBody = bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : null;
      }
      return new Response(
        JSON.stringify({
          status: "dry_run",
          namespace: "default",
          delete_ids: 0,
          delete_session: 1,
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      );
    });

    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      agents: {
        defaults: {
          deepMemory: { enabled: true, baseUrl: "http://deep-memory.test", timeoutSeconds: 2 },
        },
      },
    } as OpenClawConfig;
    const params = buildParams("/forget", cfg);
    params.agentId = "main";
    // oxlint-disable-next-line typescript/no-explicit-any
    params.sessionEntry = { sessionId: "sess-1", updatedAt: Date.now() } as any;

    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("dry-run");
    expect(seenBody).toEqual(
      expect.objectContaining({
        namespace: "default",
        session_id: "sess-1",
        dry_run: true,
      }),
    );
  });

  it("executes /forget when confirm is provided", async () => {
    let seenBody: Record<string, unknown> | null = null;
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : "";
      const bodyText =
        typeof init?.body === "string"
          ? init.body
          : init?.body instanceof Uint8Array
            ? new TextDecoder().decode(init.body)
            : "";
      if (url.includes("/forget")) {
        seenBody = bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : null;
      }
      return new Response(
        JSON.stringify({ status: "processed", namespace: "default", deleted: 3 }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      );
    });

    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      agents: {
        defaults: {
          deepMemory: { enabled: true, baseUrl: "http://deep-memory.test", timeoutSeconds: 2 },
        },
      },
    } as OpenClawConfig;
    const params = buildParams("/forget confirm", cfg);
    params.agentId = "main";
    // oxlint-disable-next-line typescript/no-explicit-any
    params.sessionEntry = { sessionId: "sess-1", updatedAt: Date.now() } as any;

    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("processed");
    expect(seenBody).toEqual(
      expect.objectContaining({
        session_id: "sess-1",
        dry_run: false,
      }),
    );
  });
});

describe("handleCommands /deepmemory status", () => {
  it("returns elevated-required message when elevated is blocked", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      agents: {
        defaults: {
          deepMemory: { enabled: true, baseUrl: "http://deep-memory.test", timeoutSeconds: 2 },
        },
      },
    } as OpenClawConfig;
    const params = buildParams("/deepmemory status", cfg);
    params.agentId = "main";
    params.elevated = {
      enabled: true,
      allowed: false,
      failures: [{ gate: "allowFrom", key: "tools.elevated.allowFrom.whatsapp" }],
    };
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/deepmemory status 需要 elevated");
  });

  it("calls deep-memory-server /health and /readyz", async () => {
    const seen: Array<{ url: string; method: string }> = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : "";
      const method = init?.method ?? (input instanceof Request ? input.method : "GET");
      seen.push({ url, method });
      if (url.includes("/health")) {
        return new Response(
          JSON.stringify({
            ok: true,
            service: { name: "deep-memory-server", version: "0.1.0" },
            guardrails: { rateLimit: { enabled: false }, updateBacklog: { rejectPending: 0 } },
            queue: { pendingApprox: 0, active: 0, inflightKeys: 0 },
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      if (url.includes("/readyz")) {
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      agents: {
        defaults: {
          deepMemory: { enabled: true, baseUrl: "http://deep-memory.test", timeoutSeconds: 2 },
        },
      },
    } as OpenClawConfig;
    const params = buildParams("/deepmemory status", cfg);
    params.agentId = "main";
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Deep memory status");
    expect(seen.some((x) => x.url.includes("/health") && x.method === "GET")).toBe(true);
    expect(seen.some((x) => x.url.includes("/readyz") && x.method === "GET")).toBe(true);
  });
});

describe("handleCommands bash alias", () => {
  it("routes !poll through the /bash handler", async () => {
    resetBashChatCommandForTests();
    const cfg = {
      commands: { bash: true, text: true },
      whatsapp: { allowFrom: ["*"] },
    } as OpenClawConfig;
    const params = buildParams("!poll", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("No active bash job");
  });

  it("routes !stop through the /bash handler", async () => {
    resetBashChatCommandForTests();
    const cfg = {
      commands: { bash: true, text: true },
      whatsapp: { allowFrom: ["*"] },
    } as OpenClawConfig;
    const params = buildParams("!stop", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("No active bash job");
  });
});

describe("handleCommands plugin commands", () => {
  it("dispatches registered plugin commands", async () => {
    clearPluginCommands();
    const result = registerPluginCommand("test-plugin", {
      name: "card",
      description: "Test card",
      handler: async () => ({ text: "from plugin" }),
    });
    expect(result.ok).toBe(true);

    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/card", cfg);
    const commandResult = await handleCommands(params);

    expect(commandResult.shouldContinue).toBe(false);
    expect(commandResult.reply?.text).toBe("from plugin");
    clearPluginCommands();
  });
});

describe("handleCommands identity", () => {
  it("returns sender details for /whoami", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/whoami", cfg, {
      SenderId: "12345",
      SenderUsername: "TestUser",
      ChatType: "direct",
    });
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Channel: whatsapp");
    expect(result.reply?.text).toContain("User id: 12345");
    expect(result.reply?.text).toContain("Username: @TestUser");
    expect(result.reply?.text).toContain("AllowFrom: 12345");
  });
});

describe("handleCommands hooks", () => {
  it("triggers hooks for /new with arguments", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/new take notes", cfg);
    const spy = vi.spyOn(internalHooks, "triggerInternalHook").mockResolvedValue();

    await handleCommands(params);

    expect(spy).toHaveBeenCalledWith(expect.objectContaining({ type: "command", action: "new" }));
    spy.mockRestore();
  });
});

describe("handleCommands context", () => {
  it("returns context help for /context", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/context", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/context list");
    expect(result.reply?.text).toContain("Inline shortcut");
  });

  it("returns a per-file breakdown for /context list", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/context list", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Injected workspace files:");
    expect(result.reply?.text).toContain("AGENTS.md");
  });

  it("returns a detailed breakdown for /context detail", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/context detail", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Context breakdown (detailed)");
    expect(result.reply?.text).toContain("Top tools (schema size):");
  });
});

describe("handleCommands subagents", () => {
  it("lists subagents when none exist", async () => {
    resetSubagentRegistryForTests();
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/subagents list", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Subagents: none");
  });

  it("lists subagents for the current command session over the target session", async () => {
    resetSubagentRegistryForTests();
    addSubagentRunForTests({
      runId: "run-1",
      childSessionKey: "agent:main:subagent:abc",
      requesterSessionKey: "agent:main:slack:slash:u1",
      requesterDisplayKey: "agent:main:slack:slash:u1",
      task: "do thing",
      cleanup: "keep",
      createdAt: 1000,
      startedAt: 1000,
    });
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/subagents list", cfg, {
      CommandSource: "native",
      CommandTargetSessionKey: "agent:main:main",
    });
    params.sessionKey = "agent:main:slack:slash:u1";
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Subagents (current session)");
    expect(result.reply?.text).toContain("agent:main:subagent:abc");
  });

  it("omits subagent status line when none exist", async () => {
    resetSubagentRegistryForTests();
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      session: { mainKey: "main", scope: "per-sender" },
    } as OpenClawConfig;
    const params = buildParams("/status", cfg);
    params.resolvedVerboseLevel = "on";
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).not.toContain("Subagents:");
  });

  it("returns help for unknown subagents action", async () => {
    resetSubagentRegistryForTests();
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/subagents foo", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/subagents");
  });

  it("returns usage for subagents info without target", async () => {
    resetSubagentRegistryForTests();
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const params = buildParams("/subagents info", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("/subagents info");
  });

  it("includes subagent count in /status when active", async () => {
    resetSubagentRegistryForTests();
    addSubagentRunForTests({
      runId: "run-1",
      childSessionKey: "agent:main:subagent:abc",
      requesterSessionKey: "agent:main:main",
      requesterDisplayKey: "main",
      task: "do thing",
      cleanup: "keep",
      createdAt: 1000,
      startedAt: 1000,
    });
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      session: { mainKey: "main", scope: "per-sender" },
    } as OpenClawConfig;
    const params = buildParams("/status", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("🤖 Subagents: 1 active");
  });

  it("includes subagent details in /status when verbose", async () => {
    resetSubagentRegistryForTests();
    addSubagentRunForTests({
      runId: "run-1",
      childSessionKey: "agent:main:subagent:abc",
      requesterSessionKey: "agent:main:main",
      requesterDisplayKey: "main",
      task: "do thing",
      cleanup: "keep",
      createdAt: 1000,
      startedAt: 1000,
    });
    addSubagentRunForTests({
      runId: "run-2",
      childSessionKey: "agent:main:subagent:def",
      requesterSessionKey: "agent:main:main",
      requesterDisplayKey: "main",
      task: "finished task",
      cleanup: "keep",
      createdAt: 900,
      startedAt: 900,
      endedAt: 1200,
      outcome: { status: "ok" },
    });
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      session: { mainKey: "main", scope: "per-sender" },
    } as OpenClawConfig;
    const params = buildParams("/status", cfg);
    params.resolvedVerboseLevel = "on";
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("🤖 Subagents: 1 active");
    expect(result.reply?.text).toContain("· 1 done");
  });

  it("returns info for a subagent", async () => {
    resetSubagentRegistryForTests();
    addSubagentRunForTests({
      runId: "run-1",
      childSessionKey: "agent:main:subagent:abc",
      requesterSessionKey: "agent:main:main",
      requesterDisplayKey: "main",
      task: "do thing",
      cleanup: "keep",
      createdAt: 1000,
      startedAt: 1000,
      endedAt: 2000,
      outcome: { status: "ok" },
    });
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      session: { mainKey: "main", scope: "per-sender" },
    } as OpenClawConfig;
    const params = buildParams("/subagents info 1", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Subagent info");
    expect(result.reply?.text).toContain("Run: run-1");
    expect(result.reply?.text).toContain("Status: done");
  });
});

describe("handleCommands /tts", () => {
  it("returns status for bare /tts on text command surfaces", async () => {
    const cfg = {
      commands: { text: true },
      channels: { whatsapp: { allowFrom: ["*"] } },
      messages: { tts: { prefsPath: path.join(testWorkspaceDir, "tts.json") } },
    } as OpenClawConfig;
    const params = buildParams("/tts", cfg);
    const result = await handleCommands(params);
    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("TTS status");
  });
});
