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

  it("exports failed tasks without messages", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "deepmem-queue-"));
    const log = pino({ level: "silent" });
    const updater = { update: async () => ({ status: "processed", memories_added: 1, memories_filtered: 0 }) };
    const q = new DurableUpdateQueue({
      log,
      updater: updater as any,
      concurrency: 1,
      dir,
      maxAttempts: 1,
      retryBaseMs: 10,
      retryMaxMs: 50,
      keepDone: false,
      retentionDays: 1,
      maxTaskBytes: 1024 * 1024,
    });
    await q.init();

    const failedDir = path.join(dir, "failed");
    await fs.writeFile(
      path.join(failedDir, "x.json"),
      JSON.stringify({
        kind: "update",
        id: "x",
        key: "default::s1",
        namespace: "default",
        sessionId: "s1",
        transcriptHash: "h",
        messageCount: 1,
        createdAt: new Date().toISOString(),
        attempt: 10,
        nextRunAt: Date.now(),
        lastError: "boom",
        messages_gzip_base64: "H4sIAAAAAAAAAwMAAAAAAAAAAAA=", // gzipped empty payload placeholder
      }),
      "utf8",
    );

    const out = await q.exportFailed({ limit: 10 });
    expect(out.mode).toBe("list");
    if (out.mode === "list") {
      expect(out.items[0]!.file).toBe("x.json");
      expect((out.items[0] as any).messages).toBeUndefined();
      expect(out.items[0]!.lastError).toBe("boom");
    }
  });

  it("retries failed tasks by key with limit", async () => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "deepmem-queue-"));
    const log = pino({ level: "silent" });
    const updater = { update: async () => ({ status: "processed", memories_added: 1, memories_filtered: 0 }) };
    const q = new DurableUpdateQueue({
      log,
      updater: updater as any,
      concurrency: 1,
      dir,
      maxAttempts: 1,
      retryBaseMs: 10,
      retryMaxMs: 50,
      keepDone: false,
      retentionDays: 1,
      maxTaskBytes: 1024 * 1024,
    });
    await q.init();

    const failedDir = path.join(dir, "failed");
    const pendingDir = path.join(dir, "pending");
    const now = Date.now();
    await fs.writeFile(
      path.join(failedDir, "a.json"),
      JSON.stringify({
        kind: "update",
        id: "a",
        key: "default::s1",
        namespace: "default",
        sessionId: "s1",
        transcriptHash: "h1",
        messageCount: 1,
        createdAt: new Date().toISOString(),
        attempt: 10,
        nextRunAt: now,
        lastError: "boom",
        messages_gzip_base64: "H4sIAAAAAAAAAwMAAAAAAAAAAAA=",
      }),
      "utf8",
    );
    await fs.writeFile(
      path.join(failedDir, "b.json"),
      JSON.stringify({
        kind: "update",
        id: "b",
        key: "default::s1",
        namespace: "default",
        sessionId: "s1",
        transcriptHash: "h2",
        messageCount: 1,
        createdAt: new Date().toISOString(),
        attempt: 10,
        nextRunAt: now,
        lastError: "boom",
        messages_gzip_base64: "H4sIAAAAAAAAAwMAAAAAAAAAAAA=",
      }),
      "utf8",
    );

    const out = await q.retryFailedByKey({ key: "default::s1", limit: 1, dryRun: false });
    expect(out.matched).toBeGreaterThanOrEqual(1);
    expect(out.retried).toBe(1);

    const pending = await fs.readdir(pendingDir);
    expect(pending.length).toBe(1);
  });
});

