import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import pino from "pino";
import { DurableUpdateQueue } from "./durable-update-queue.js";

describe("DurableUpdateQueue", () => {
  it("persists tasks and processes them", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "deepmem-queue-"));
    const log = pino({ level: "silent" });

    let processed = 0;
    const updater = {
      update: async () => {
        processed += 1;
        return { status: "processed", memories_added: 1, memories_filtered: 0 };
      },
    };

    const queue = new DurableUpdateQueue({
      log,
      updater: updater as any,
      concurrency: 1,
      dir,
      maxAttempts: 3,
      retryBaseMs: 10,
      retryMaxMs: 50,
      keepDone: true,
      retentionDays: 1,
      maxTaskBytes: 1024 * 1024,
    });
    await queue.init();

    await queue.enqueue({ namespace: "default", sessionId: "s1", messages: [{ role: "user", content: "hi" }] });
    const ok = await queue.onIdle({ timeoutMs: 5_000 });
    expect(ok).toBe(true);
    expect(processed).toBe(1);

    const doneDir = path.join(dir, "done");
    const done = await fs.readdir(doneDir);
    expect(done.length).toBeGreaterThanOrEqual(1);
  });

  it("recovers inflight tasks on restart", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "deepmem-queue-"));
    const log = pino({ level: "silent" });

    // First queue: enqueue and simulate an inflight crash by moving file ourselves.
    const updater = { update: async () => ({ status: "processed", memories_added: 1, memories_filtered: 0 }) };
    const q1 = new DurableUpdateQueue({
      log,
      updater: updater as any,
      concurrency: 1,
      dir,
      maxAttempts: 3,
      retryBaseMs: 10,
      retryMaxMs: 50,
      keepDone: false,
      retentionDays: 1,
      maxTaskBytes: 1024 * 1024,
    });
    await q1.init();
    await q1.enqueue({ namespace: "default", sessionId: "s1", messages: [{ role: "user", content: "hi" }] });
    await q1.onIdle({ timeoutMs: 5_000 });

    // Now create an inflight file to be recovered.
    const pendingDir = path.join(dir, "pending");
    const inflightDir = path.join(dir, "inflight");
    const fakeFile = path.join(inflightDir, "fake.json");
    await fs.writeFile(
      fakeFile,
      JSON.stringify({
        kind: "update",
        id: "x",
        key: "default::s2",
        namespace: "default",
        sessionId: "s2",
        transcriptHash: "h",
        messageCount: 1,
        createdAt: new Date().toISOString(),
        attempt: 0,
        nextRunAt: Date.now(),
        messages: [{ role: "user", content: "yo" }],
      }),
      "utf8",
    );

    // Restart: should move inflight back to pending.
    const q2 = new DurableUpdateQueue({
      log,
      updater: updater as any,
      concurrency: 1,
      dir,
      maxAttempts: 3,
      retryBaseMs: 10,
      retryMaxMs: 50,
      keepDone: false,
      retentionDays: 1,
      maxTaskBytes: 1024 * 1024,
    });
    await q2.init();
    const pending = await fs.readdir(pendingDir);
    expect(pending.some((n) => n === "fake.json")).toBe(true);
  });
});

