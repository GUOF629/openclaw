import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../config/config.js";
import type { SessionEntry } from "../config/sessions.js";
import {
  considerDeepMemoryUpdateForTranscriptDelta,
  considerDeepMemoryUpdateNearCompaction,
} from "./update-scheduler.js";

function buildCfg(overrides?: Partial<OpenClawConfig>): OpenClawConfig {
  return {
    agents: {
      defaults: {
        deepMemory: {
          enabled: true,
          baseUrl: "http://deep-memory.test",
          timeoutSeconds: 2,
          retrieve: { maxMemories: 10, cache: { enabled: false, ttlMinutes: 0, maxEntries: 10 } },
          inject: { maxChars: 2000, label: "Deep Memory" },
          update: {
            enabled: true,
            thresholds: { deltaBytes: 1, deltaMessages: 0 },
            debounceMs: 0,
            nearCompaction: { enabled: true },
          },
        },
      },
    },
    ...overrides,
  };
}

async function writeTranscript(tmpFile: string, lines: unknown[]) {
  const payload = lines.map((l) => JSON.stringify(l)).join("\n") + "\n";
  await fs.writeFile(tmpFile, payload, "utf-8");
}

describe("deep-memory update scheduler", () => {
  it("enqueues update on transcript delta threshold (bytes)", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-deepmem-"));
    const sessionFile = path.join(tmpDir, "s.jsonl");
    await writeTranscript(sessionFile, [{ message: { role: "user", content: "hi" } }]);

    const fetchCalls: string[] = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : "";
      fetchCalls.push(url);
      return new Response(JSON.stringify({ status: "ok" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const cfg = buildCfg();
    const sessionEntry: SessionEntry = {
      sessionId: "sess-1",
      updatedAt: Date.now(),
      sessionFile,
      deepMemoryTranscriptSize: 0,
      deepMemoryTranscriptLines: 0,
    };

    await considerDeepMemoryUpdateForTranscriptDelta({
      cfg,
      agentId: "test",
      sessionKey: "agent:test:main",
      sessionId: "sess-1",
      sessionFile,
      sessionEntry,
      storePath: undefined,
    });

    // Debounce is 0ms; allow microtasks to flush.
    await new Promise((r) => setTimeout(r, 10));

    expect(fetchCalls.some((u) => u.includes("/update_memory_index"))).toBe(true);
  });

  it("enqueues near-compaction update once per compaction cycle", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-deepmem-"));
    const sessionFile = path.join(tmpDir, "s.jsonl");
    await writeTranscript(sessionFile, [{ message: { role: "user", content: "hi" } }]);

    const fetchCalls: string[] = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : "";
      fetchCalls.push(url);
      return new Response(JSON.stringify({ status: "ok" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });

    const cfg = buildCfg();

    considerDeepMemoryUpdateNearCompaction({
      cfg,
      agentId: "test",
      sessionKey: "agent:test:main",
      sessionId: "sess-1",
      sessionFile,
      storePath: undefined,
      compactionCount: 3,
      deepMemoryNearCompactionCount: undefined,
    });

    // allow async to run
    await new Promise((r) => setTimeout(r, 10));
    const firstCount = fetchCalls.filter((u) => u.includes("/update_memory_index")).length;
    expect(firstCount).toBe(1);

    // same cycle should be ignored by caller-provided marker
    considerDeepMemoryUpdateNearCompaction({
      cfg,
      agentId: "test",
      sessionKey: "agent:test:main",
      sessionId: "sess-1",
      sessionFile,
      storePath: undefined,
      compactionCount: 3,
      deepMemoryNearCompactionCount: 3,
    });
    await new Promise((r) => setTimeout(r, 10));
    const secondCount = fetchCalls.filter((u) => u.includes("/update_memory_index")).length;
    expect(secondCount).toBe(1);
  });
});
