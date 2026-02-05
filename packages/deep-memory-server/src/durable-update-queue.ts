import type { Logger } from "pino";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";
import type { UpdateMemoryIndexResponse } from "./types.js";
import type { DeepMemoryUpdater } from "./updater.js";

type UpdateRequest = {
  namespace: string;
  sessionId: string;
  messages: unknown[];
  notBeforeMs?: number;
  returnMemoryIds?: {
    max: number;
  };
};

type PersistedUpdateTask = {
  kind: "update";
  id: string; // unique
  key: string; // namespace::sessionId
  namespace: string;
  sessionId: string;
  transcriptHash: string;
  messageCount: number;
  createdAt: string; // ISO
  attempt: number;
  nextRunAt: number; // epoch ms
  lastError?: string;
  // New format: compressed to reduce disk usage.
  messages_gzip_base64?: string;
  // Legacy format (back-compat).
  messages?: unknown[];
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function stableTranscriptHash(messages: unknown[]): { hash: string; count: number } {
  const count = Array.isArray(messages) ? messages.length : 0;
  const hash = crypto
    .createHash("sha256")
    .update(JSON.stringify(messages ?? []))
    .digest("hex");
  return { hash, count };
}

function encodeMessages(messages: unknown[]): { b64: string; bytes: number } {
  const json = JSON.stringify(messages ?? []);
  const gz = gzipSync(Buffer.from(json, "utf8"), { level: 9 });
  return { b64: gz.toString("base64"), bytes: gz.length };
}

function decodeMessages(task: PersistedUpdateTask): unknown[] {
  if (task.messages_gzip_base64) {
    const buf = Buffer.from(task.messages_gzip_base64, "base64");
    const raw = gunzipSync(buf).toString("utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  }
  return Array.isArray(task.messages) ? task.messages : [];
}

function backoffMs(params: { baseMs: number; maxMs: number; attempt: number }): number {
  // attempt starts at 1. Exponential backoff with jitter.
  const exp = Math.min(20, Math.max(0, params.attempt - 1));
  const raw = Math.min(params.maxMs, params.baseMs * 2 ** exp);
  const jitter = Math.floor(Math.random() * Math.min(250, Math.max(10, raw / 10)));
  return Math.min(params.maxMs, raw + jitter);
}

async function ensureDir(dir: string) {
  await fs.mkdir(dir, { recursive: true });
}

async function atomicWriteJson(filePath: string, value: unknown) {
  const dir = path.dirname(filePath);
  await ensureDir(dir);
  const tmp = `${filePath}.tmp-${crypto.randomUUID()}`;
  const data = JSON.stringify(value);
  const handle = await fs.open(tmp, "w");
  try {
    await handle.writeFile(data, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fs.rename(tmp, filePath);
}

async function readJson<T>(filePath: string): Promise<T> {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw) as T;
}

function sanitizeFailedTask(task: PersistedUpdateTask, file: string) {
  return {
    file,
    key: task.key,
    namespace: task.namespace,
    sessionId: task.sessionId,
    transcriptHash: task.transcriptHash,
    messageCount: task.messageCount,
    createdAt: task.createdAt,
    attempt: task.attempt ?? 0,
    nextRunAt: task.nextRunAt,
    lastError: task.lastError,
  };
}

export class DurableUpdateQueue {
  private readonly log: Logger;
  private readonly updater: DeepMemoryUpdater;
  private readonly concurrency: number;
  private readonly namespaceConcurrency: number;
  private readonly baseDir: string;
  private readonly pendingDir: string;
  private readonly inflightDir: string;
  private readonly doneDir: string;
  private readonly failedDir: string;
  private readonly maxAttempts: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;
  private readonly keepDone: boolean;
  private readonly retentionDays: number;
  private readonly maxTaskBytes: number;

  private active = 0;
  private stopped = false;
  private readonly inflightKeys = new Set<string>();
  private readonly inflightNamespaces = new Map<string, number>();
  private readonly pendingFilesByKey = new Map<string, string>(); // best-effort "latest"

  private idleWaiters: Array<() => void> = [];

  constructor(params: {
    log: Logger;
    updater: DeepMemoryUpdater;
    concurrency: number;
    namespaceConcurrency?: number;
    dir: string;
    maxAttempts: number;
    retryBaseMs: number;
    retryMaxMs: number;
    keepDone: boolean;
    retentionDays: number;
    maxTaskBytes: number;
  }) {
    this.log = params.log;
    this.updater = params.updater;
    this.concurrency = Math.max(1, params.concurrency);
    this.namespaceConcurrency = Math.max(0, params.namespaceConcurrency ?? 0);
    this.baseDir = params.dir;
    this.pendingDir = path.join(this.baseDir, "pending");
    this.inflightDir = path.join(this.baseDir, "inflight");
    this.doneDir = path.join(this.baseDir, "done");
    this.failedDir = path.join(this.baseDir, "failed");
    this.maxAttempts = Math.max(1, params.maxAttempts);
    this.retryBaseMs = Math.max(1, params.retryBaseMs);
    this.retryMaxMs = Math.max(this.retryBaseMs, params.retryMaxMs);
    this.keepDone = params.keepDone;
    this.retentionDays = Math.max(1, params.retentionDays);
    this.maxTaskBytes = Math.max(1, params.maxTaskBytes);
  }

  async init(): Promise<void> {
    await Promise.all([
      ensureDir(this.pendingDir),
      ensureDir(this.inflightDir),
      ensureDir(this.doneDir),
      ensureDir(this.failedDir),
    ]);

    // Crash recovery: anything in inflight goes back to pending with a backoff.
    const inflight = await fs.readdir(this.inflightDir).catch(() => []);
    for (const name of inflight) {
      const from = path.join(this.inflightDir, name);
      const to = path.join(this.pendingDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(from);
        const attempt = Math.max(1, (task.attempt ?? 0) + 1);
        task.attempt = attempt;
        task.nextRunAt =
          Date.now() + backoffMs({ baseMs: this.retryBaseMs, maxMs: this.retryMaxMs, attempt });
        await atomicWriteJson(to, task);
        await fs.rm(from, { force: true });
      } catch (err) {
        this.log.warn({ err: String(err), file: from }, "failed to recover inflight task");
        // Leave it for manual inspection.
      }
    }

    // Build best-effort index so we can replace older per-key tasks.
    const pending = await fs.readdir(this.pendingDir).catch(() => []);
    for (const name of pending) {
      const file = path.join(this.pendingDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(file);
        if (task?.kind !== "update" || !task.key) {
          continue;
        }
        const prev = this.pendingFilesByKey.get(task.key);
        if (!prev) {
          this.pendingFilesByKey.set(task.key, file);
        } else {
          // Keep the newest by nextRunAt/createdAt (best-effort).
          const prevTask = await readJson<PersistedUpdateTask>(prev);
          const prevScore = Number(prevTask.nextRunAt ?? 0);
          const nextScore = Number(task.nextRunAt ?? 0);
          if (nextScore >= prevScore) {
            this.pendingFilesByKey.set(task.key, file);
          }
        }
      } catch {
        // ignore
      }
    }

    // Periodic cleanup of done tasks.
    void this.cleanupLoop();

    // Start workers.
    void this.pump();
  }

  stop(): void {
    this.stopped = true;
  }

  stats(): { pendingApprox: number; active: number; inflightKeys: number } {
    return {
      pendingApprox: this.pendingFilesByKey.size,
      active: this.active,
      inflightKeys: this.inflightKeys.size,
    };
  }

  async listFailed(params: { limit: number }): Promise<
    Array<{
      file: string;
      key: string;
      attempt: number;
      lastError?: string;
      createdAt: string;
      nextRunAt: number;
    }>
  > {
    const names = await fs.readdir(this.failedDir).catch(() => []);
    const out: Array<{
      file: string;
      key: string;
      attempt: number;
      lastError?: string;
      createdAt: string;
      nextRunAt: number;
    }> = [];
    for (const name of names.slice(0, Math.max(1, params.limit))) {
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(filePath);
        if (!task || task.kind !== "update") {
          continue;
        }
        out.push({
          file: name,
          key: task.key,
          attempt: task.attempt ?? 0,
          lastError: task.lastError,
          createdAt: task.createdAt,
          nextRunAt: task.nextRunAt,
        });
      } catch {
        // ignore
      }
    }
    return out;
  }

  async exportFailed(params: {
    file?: string;
    key?: string;
    limit: number;
  }): Promise<
    | { mode: "file"; item: ReturnType<typeof sanitizeFailedTask> }
    | { mode: "list"; items: Array<ReturnType<typeof sanitizeFailedTask>> }
    | { mode: "empty" }
  > {
    const file = params.file?.trim();
    if (file) {
      const name = path.basename(file);
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(filePath);
        if (!task || task.kind !== "update") {
          return { mode: "empty" };
        }
        return { mode: "file", item: sanitizeFailedTask(task, name) };
      } catch {
        return { mode: "empty" };
      }
    }
    const key = params.key?.trim();
    const names = await fs.readdir(this.failedDir).catch(() => []);
    const items: Array<ReturnType<typeof sanitizeFailedTask>> = [];
    for (const name of names) {
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(filePath);
        if (!task || task.kind !== "update") {
          continue;
        }
        if (key && task.key !== key) {
          continue;
        }
        items.push(sanitizeFailedTask(task, name));
        if (items.length >= Math.max(1, params.limit)) {
          break;
        }
      } catch {
        // ignore
      }
    }
    return { mode: "list", items };
  }

  async retryFailed(params: { file: string }): Promise<{ status: "requeued" | "not_found" }> {
    const name = path.basename(params.file);
    const from = path.join(this.failedDir, name);
    try {
      const task = await readJson<PersistedUpdateTask>(from);
      if (!task || task.kind !== "update") {
        return { status: "not_found" };
      }
      // Reset schedule; keep attempt counter for visibility.
      task.nextRunAt = Date.now();
      task.lastError = undefined;
      const to = path.join(this.pendingDir, name);
      await atomicWriteJson(to, task);
      await fs.rm(from, { force: true });

      const prev = this.pendingFilesByKey.get(task.key);
      this.pendingFilesByKey.set(task.key, to);
      if (prev && prev !== to) {
        void fs.rm(prev, { force: true }).catch(() => {});
      }
      void this.pump();
      return { status: "requeued" };
    } catch {
      return { status: "not_found" };
    }
  }

  async retryFailedByKey(params: {
    key: string;
    limit: number;
    dryRun: boolean;
  }): Promise<{ status: "ok"; matched: number; retried: number }> {
    const key = params.key.trim();
    if (!key) {
      return { status: "ok", matched: 0, retried: 0 };
    }
    const names = await fs.readdir(this.failedDir).catch(() => []);
    let matched = 0;
    let retried = 0;
    for (const name of names) {
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedUpdateTask>(filePath);
        if (!task || task.kind !== "update") {
          continue;
        }
        if (task.key !== key) {
          continue;
        }
        matched += 1;
        if (params.dryRun) {
          continue;
        }
        const out = await this.retryFailed({ file: name });
        if (out.status === "requeued") {
          retried += 1;
        }
        if (retried >= Math.max(1, params.limit)) {
          break;
        }
      } catch {
        // ignore
      }
    }
    return { status: "ok", matched, retried };
  }

  async cancelBySession(params: { namespace: string; sessionId: string }): Promise<number> {
    const key = `${params.namespace}::${params.sessionId}`;
    const file = this.pendingFilesByKey.get(key);
    if (!file) {
      return 0;
    }
    this.pendingFilesByKey.delete(key);
    try {
      await fs.rm(file, { force: true });
      return 1;
    } catch {
      return 0;
    }
  }

  async enqueue(
    req: UpdateRequest,
  ): Promise<{ status: "queued"; key: string; transcriptHash: string }> {
    const namespace = req.namespace?.trim() || "default";
    const key = `${namespace}::${req.sessionId}`;
    const { hash, count } = stableTranscriptHash(req.messages);

    const existingPath = this.pendingFilesByKey.get(key);
    if (existingPath) {
      try {
        const existing = await readJson<PersistedUpdateTask>(existingPath);
        if (existing?.transcriptHash === hash) {
          return { status: "queued", key, transcriptHash: hash };
        }
      } catch {
        // ignore
      }
    }

    const now = new Date();
    const encoded = encodeMessages(Array.isArray(req.messages) ? req.messages : []);
    if (encoded.bytes > this.maxTaskBytes) {
      throw new Error(
        `queue task too large (${encoded.bytes} bytes gzipped > ${this.maxTaskBytes})`,
      );
    }
    const task: PersistedUpdateTask = {
      kind: "update",
      id: crypto.randomUUID(),
      key,
      namespace,
      sessionId: req.sessionId,
      transcriptHash: hash,
      messageCount: count,
      createdAt: now.toISOString(),
      attempt: 0,
      nextRunAt: Math.max(Date.now(), Number(req.notBeforeMs ?? Date.now()) || Date.now()),
      messages_gzip_base64: encoded.b64,
    };

    // File name includes key hash so it is filesystem safe and stable.
    const keyHash = crypto.createHash("sha256").update(key).digest("hex").slice(0, 16);
    const fileName = `${keyHash}-${Date.now()}-${task.id}.json`;
    const filePath = path.join(this.pendingDir, fileName);

    await atomicWriteJson(filePath, task);

    // Best-effort: keep only latest pending task per key (debounce/coalesce).
    const prev = this.pendingFilesByKey.get(key);
    this.pendingFilesByKey.set(key, filePath);
    if (prev && prev !== filePath) {
      void fs.rm(prev, { force: true }).catch(() => {});
    }

    void this.pump();
    return { status: "queued", key, transcriptHash: hash };
  }

  async runNow(req: UpdateRequest): Promise<UpdateMemoryIndexResponse> {
    // Synchronous path still needs per-key mutual exclusion.
    const key = `${req.namespace}::${req.sessionId}`;
    while (this.inflightKeys.has(key)) {
      await sleep(50);
    }
    this.inflightKeys.add(key);
    try {
      return await this.updater.update(req);
    } finally {
      this.inflightKeys.delete(key);
      this.signalIdleIfNeeded();
      void this.pump();
    }
  }

  async onIdle(params?: { timeoutMs?: number }): Promise<boolean> {
    const timeoutMs = params?.timeoutMs ?? 10_000;
    if (this.active === 0 && this.pendingFilesByKey.size === 0) {
      return true;
    }
    let timer: NodeJS.Timeout | null = null;
    return await new Promise<boolean>((resolve) => {
      const done = () => {
        if (timer) {
          clearTimeout(timer);
        }
        resolve(true);
      };
      this.idleWaiters.push(done);
      timer = setTimeout(() => {
        this.idleWaiters = this.idleWaiters.filter((w) => w !== done);
        resolve(false);
      }, timeoutMs);
    });
  }

  private signalIdleIfNeeded() {
    if (this.active !== 0 || this.pendingFilesByKey.size !== 0) {
      return;
    }
    const waiters = this.idleWaiters;
    this.idleWaiters = [];
    for (const w of waiters) {
      w();
    }
  }

  private async cleanupLoop() {
    while (!this.stopped) {
      await sleep(30_000);
      if (!this.keepDone) {
        continue;
      }
      const cutoff = Date.now() - this.retentionDays * 24 * 3600_000;
      const entries = await fs.readdir(this.doneDir).catch(() => []);
      for (const name of entries) {
        const file = path.join(this.doneDir, name);
        try {
          const stat = await fs.stat(file);
          if (stat.mtimeMs < cutoff) {
            await fs.rm(file, { force: true });
          }
        } catch {
          // ignore
        }
      }
    }
  }

  private async pump() {
    if (this.stopped) {
      return;
    }
    while (this.active < this.concurrency) {
      const next = await this.pickRunnableTask();
      if (!next) {
        this.signalIdleIfNeeded();
        return;
      }
      this.active += 1;
      void this.runTask(next)
        .catch((err) => {
          this.log.warn(
            { err: String(err), file: next.filePath },
            "task execution failed (unexpected)",
          );
        })
        .finally(() => {
          this.active -= 1;
          this.signalIdleIfNeeded();
          void this.pump();
        });
    }
  }

  private async pickRunnableTask(): Promise<{
    filePath: string;
    task: PersistedUpdateTask;
  } | null> {
    // Best-effort: pick first runnable among latest-by-key.
    const now = Date.now();
    for (const [key, filePath] of this.pendingFilesByKey.entries()) {
      if (this.inflightKeys.has(key)) {
        continue;
      }
      try {
        const task = await readJson<PersistedUpdateTask>(filePath);
        if (!task || task.kind !== "update") {
          this.pendingFilesByKey.delete(key);
          continue;
        }
        if ((task.nextRunAt ?? 0) > now) {
          continue;
        }
        if (this.namespaceConcurrency > 0) {
          const activeNs = this.inflightNamespaces.get(task.namespace) ?? 0;
          if (activeNs >= this.namespaceConcurrency) {
            continue;
          }
        }
        // Move to inflight atomically by rename to avoid duplicates across crashes.
        const inflightPath = path.join(this.inflightDir, path.basename(filePath));
        await fs.rename(filePath, inflightPath);
        this.pendingFilesByKey.delete(key);
        task.attempt = Math.max(0, task.attempt ?? 0);
        return { filePath: inflightPath, task };
      } catch {
        // If file disappeared or unreadable, drop it.
        this.pendingFilesByKey.delete(key);
      }
    }
    return null;
  }

  private async runTask(params: { filePath: string; task: PersistedUpdateTask }): Promise<void> {
    const task = params.task;
    const key = task.key;
    const ns = task.namespace;
    if (this.inflightKeys.has(key)) {
      // Another runner took it (shouldn't happen). Put back.
      await fs.rename(params.filePath, path.join(this.pendingDir, path.basename(params.filePath)));
      this.pendingFilesByKey.set(key, path.join(this.pendingDir, path.basename(params.filePath)));
      return;
    }
    this.inflightKeys.add(key);
    if (this.namespaceConcurrency > 0) {
      this.inflightNamespaces.set(ns, (this.inflightNamespaces.get(ns) ?? 0) + 1);
    }
    try {
      const attempt = Math.max(1, (task.attempt ?? 0) + 1);
      task.attempt = attempt;
      const messages = decodeMessages(task);
      const result = await this.updater.update({
        namespace: task.namespace,
        sessionId: task.sessionId,
        messages,
      });
      if (result.status === "error") {
        throw new Error(result.error ?? "update returned error");
      }

      if (this.keepDone) {
        await fs.rename(params.filePath, path.join(this.doneDir, path.basename(params.filePath)));
      } else {
        await fs.rm(params.filePath, { force: true });
      }
    } catch (err) {
      const attempt = Math.max(1, task.attempt ?? 1);
      const lastError = String(err);
      if (attempt >= this.maxAttempts) {
        const failed: PersistedUpdateTask = {
          ...task,
          lastError,
          nextRunAt: Date.now(),
        };
        await atomicWriteJson(path.join(this.failedDir, path.basename(params.filePath)), failed);
        await fs.rm(params.filePath, { force: true });
        this.log.error({ key, attempt, lastError }, "task moved to failed");
        return;
      }

      const delay = backoffMs({ baseMs: this.retryBaseMs, maxMs: this.retryMaxMs, attempt });
      const retryTask: PersistedUpdateTask = {
        ...task,
        lastError,
        nextRunAt: Date.now() + delay,
      };
      const pendingPath = path.join(this.pendingDir, path.basename(params.filePath));
      await atomicWriteJson(pendingPath, retryTask);
      await fs.rm(params.filePath, { force: true });
      this.pendingFilesByKey.set(key, pendingPath);
      this.log.warn({ key, attempt, delay, lastError }, "task retry scheduled");
    } finally {
      this.inflightKeys.delete(key);
      if (this.namespaceConcurrency > 0) {
        const next = (this.inflightNamespaces.get(ns) ?? 1) - 1;
        if (next <= 0) {
          this.inflightNamespaces.delete(ns);
        } else {
          this.inflightNamespaces.set(ns, next);
        }
      }
    }
  }
}
