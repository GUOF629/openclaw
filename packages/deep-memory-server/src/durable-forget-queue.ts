import type { Logger } from "pino";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";

type ForgetRequest = {
  namespace: string;
  sessionId?: string;
  memoryIds?: string[];
};

type PersistedForgetTask = {
  kind: "forget";
  id: string;
  key: string; // namespace::(sessionId|ids::<hash>)
  namespace: string;
  sessionId?: string;
  memoryIds?: string[];
  createdAt: string;
  attempt: number;
  nextRunAt: number;
  lastError?: string;
  result?: {
    deletedNeo4j?: number;
    qdrant?: {
      bySession?: { ok: boolean; error?: string };
      byIds?: { ok: boolean; deleted?: number; error?: string };
    };
    neo4j?: {
      bySession?: { ok: boolean; deleted?: number; error?: string };
      byIds?: { ok: boolean; deleted?: number; error?: string };
    };
    queue?: { ok: boolean; cancelled?: number; error?: string };
  };
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function stableHash(s: string): string {
  return crypto.createHash("sha256").update(s).digest("hex").slice(0, 16);
}

function backoffMs(params: { baseMs: number; maxMs: number; attempt: number }): number {
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

function sanitizeFailedTask(task: PersistedForgetTask, file: string) {
  return {
    file,
    key: task.key,
    namespace: task.namespace,
    sessionId: task.sessionId,
    memoryIdsCount: Array.isArray(task.memoryIds) ? task.memoryIds.length : 0,
    createdAt: task.createdAt,
    attempt: task.attempt ?? 0,
    nextRunAt: task.nextRunAt,
    lastError: task.lastError,
  };
}

export class DurableForgetQueue {
  private readonly log: Logger;
  private readonly qdrant: QdrantStore;
  private readonly neo4j: Neo4jStore;
  private readonly updateQueue: DurableUpdateQueue;
  private readonly concurrency: number;
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

  private active = 0;
  private stopped = false;
  private readonly inflightKeys = new Set<string>();
  private readonly pendingFilesByKey = new Map<string, string>();
  private idleWaiters: Array<() => void> = [];

  constructor(params: {
    log: Logger;
    qdrant: QdrantStore;
    neo4j: Neo4jStore;
    updateQueue: DurableUpdateQueue;
    concurrency: number;
    dir: string;
    maxAttempts: number;
    retryBaseMs: number;
    retryMaxMs: number;
    keepDone: boolean;
    retentionDays: number;
  }) {
    this.log = params.log;
    this.qdrant = params.qdrant;
    this.neo4j = params.neo4j;
    this.updateQueue = params.updateQueue;
    this.concurrency = Math.max(1, params.concurrency);
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
  }

  async init(): Promise<void> {
    await Promise.all([
      ensureDir(this.pendingDir),
      ensureDir(this.inflightDir),
      ensureDir(this.doneDir),
      ensureDir(this.failedDir),
    ]);

    // Crash recovery.
    const inflight = await fs.readdir(this.inflightDir).catch(() => []);
    for (const name of inflight) {
      const from = path.join(this.inflightDir, name);
      const to = path.join(this.pendingDir, name);
      try {
        const task = await readJson<PersistedForgetTask>(from);
        const attempt = Math.max(1, (task.attempt ?? 0) + 1);
        task.attempt = attempt;
        task.nextRunAt =
          Date.now() + backoffMs({ baseMs: this.retryBaseMs, maxMs: this.retryMaxMs, attempt });
        await atomicWriteJson(to, task);
        await fs.rm(from, { force: true });
      } catch (err) {
        this.log.warn({ err: String(err), file: from }, "failed to recover inflight forget task");
      }
    }

    const pending = await fs.readdir(this.pendingDir).catch(() => []);
    for (const name of pending) {
      const file = path.join(this.pendingDir, name);
      try {
        const task = await readJson<PersistedForgetTask>(file);
        if (task?.kind !== "forget" || !task.key) {
          continue;
        }
        const prev = this.pendingFilesByKey.get(task.key);
        if (!prev) {
          this.pendingFilesByKey.set(task.key, file);
        } else {
          const prevTask = await readJson<PersistedForgetTask>(prev);
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

    void this.cleanupLoop();
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

  private notifyIdle() {
    if (this.active !== 0) {
      return;
    }
    const waiters = this.idleWaiters;
    this.idleWaiters = [];
    waiters.forEach((w) => w());
  }

  async waitIdle(): Promise<void> {
    if (this.active === 0) {
      return;
    }
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  async enqueue(req: ForgetRequest): Promise<{ status: "queued"; key: string; taskId: string }> {
    const namespace = req.namespace?.trim() || "default";
    const sessionId = req.sessionId?.trim() || undefined;
    const memoryIds = Array.isArray(req.memoryIds)
      ? req.memoryIds.map((x) => x.trim()).filter(Boolean)
      : undefined;
    const key = sessionId
      ? `${namespace}::${sessionId}`
      : `${namespace}::ids::${stableHash(JSON.stringify(memoryIds ?? []))}`;

    const now = new Date();
    const task: PersistedForgetTask = {
      kind: "forget",
      id: crypto.randomUUID(),
      key,
      namespace,
      sessionId,
      memoryIds,
      createdAt: now.toISOString(),
      attempt: 0,
      nextRunAt: Date.now(),
    };

    const fileName = `${now.toISOString().replace(/[:.]/g, "-")}-${task.id}.json`;
    const filePath = path.join(this.pendingDir, fileName);
    await atomicWriteJson(filePath, task);
    this.pendingFilesByKey.set(key, filePath);
    return { status: "queued", key, taskId: task.id };
  }

  async listFailed(params: { limit: number }) {
    const names = await fs.readdir(this.failedDir).catch(() => []);
    const sorted = names.toSorted();
    const out: Array<ReturnType<typeof sanitizeFailedTask>> = [];
    for (const name of sorted) {
      if (out.length >= Math.max(1, params.limit)) {
        break;
      }
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedForgetTask>(filePath);
        if (!task || task.kind !== "forget") {
          continue;
        }
        out.push(sanitizeFailedTask(task, name));
      } catch {
        // ignore
      }
    }
    return out;
  }

  async exportFailed(params: { file?: string; key?: string; limit: number }) {
    if (params.file) {
      const filePath = path.join(this.failedDir, params.file);
      const task = await readJson<PersistedForgetTask>(filePath);
      return { mode: "file" as const, item: sanitizeFailedTask(task, params.file) };
    }
    if (params.key) {
      const items = await this.listFailed({ limit: params.limit });
      return {
        mode: "key" as const,
        key: params.key,
        items: items.filter((i) => i.key === params.key),
      };
    }
    return { mode: "empty" as const };
  }

  async retryFailed(params: { file: string }): Promise<{ status: "requeued" } | { status: "not_found" }> {
    const from = path.join(this.failedDir, params.file);
    const exists = await fs.stat(from).then(() => true).catch(() => false);
    if (!exists) {
      return { status: "not_found" };
    }
    const task = await readJson<PersistedForgetTask>(from);
    task.lastError = undefined;
    task.nextRunAt = Date.now();
    const to = path.join(this.pendingDir, params.file);
    await atomicWriteJson(to, task);
    await fs.rm(from, { force: true });
    this.pendingFilesByKey.set(task.key, to);
    return { status: "requeued" };
  }

  async retryFailedByKey(params: { key: string; limit: number; dryRun: boolean }) {
    const names = await fs.readdir(this.failedDir).catch(() => []);
    let matched = 0;
    let retried = 0;
    for (const name of names.toSorted()) {
      if (retried >= Math.max(1, params.limit)) {
        break;
      }
      const filePath = path.join(this.failedDir, name);
      try {
        const task = await readJson<PersistedForgetTask>(filePath);
        if (!task || task.kind !== "forget") {
          continue;
        }
        if (task.key !== params.key) {
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
      } catch {
        // ignore
      }
    }
    return { status: "ok" as const, matched, retried };
  }

  private async cleanupLoop() {
    const retentionMs = this.retentionDays * 24 * 3600_000;
    while (!this.stopped) {
      await sleep(60_000);
      if (!this.keepDone) {
        continue;
      }
      const now = Date.now();
      const names = await fs.readdir(this.doneDir).catch(() => []);
      for (const name of names) {
        const filePath = path.join(this.doneDir, name);
        try {
          const st = await fs.stat(filePath);
          if (now - st.mtimeMs > retentionMs) {
            await fs.rm(filePath, { force: true });
          }
        } catch {
          // ignore
        }
      }
    }
  }

  private async pump() {
    while (!this.stopped) {
      if (this.active >= this.concurrency) {
        await sleep(50);
        continue;
      }
      const due = await this.pickDueTask();
      if (!due) {
        await sleep(100);
        continue;
      }
      void this.runTask(due).finally(() => {
        this.active = Math.max(0, this.active - 1);
        this.notifyIdle();
      });
    }
  }

  private async pickDueTask(): Promise<{ file: string; task: PersistedForgetTask } | null> {
    const entries = Array.from(this.pendingFilesByKey.entries());
    const now = Date.now();
    for (const [key, file] of entries) {
      if (this.inflightKeys.has(key)) {
        continue;
      }
      try {
        const task = await readJson<PersistedForgetTask>(file);
        if (!task || task.kind !== "forget") {
          continue;
        }
        if ((task.nextRunAt ?? 0) > now) {
          continue;
        }
        return { file, task };
      } catch {
        // ignore
      }
    }
    return null;
  }

  private async runTask(picked: { file: string; task: PersistedForgetTask }) {
    this.active += 1;
    this.inflightKeys.add(picked.task.key);
    const fileName = path.basename(picked.file);
    const inflightPath = path.join(this.inflightDir, fileName);
    try {
      await fs.rename(picked.file, inflightPath);
    } catch {
      this.inflightKeys.delete(picked.task.key);
      return;
    }

    const task = await readJson<PersistedForgetTask>(inflightPath).catch(() => picked.task);
    try {
      task.result = await this.processForget(task);
      const donePath = path.join(this.doneDir, fileName);
      await atomicWriteJson(donePath, task);
      await fs.rm(inflightPath, { force: true });
      this.pendingFilesByKey.delete(task.key);
    } catch (err) {
      const attempt = Math.max(1, (task.attempt ?? 0) + 1);
      task.attempt = attempt;
      task.lastError = err instanceof Error ? err.message : String(err);
      if (attempt >= this.maxAttempts) {
        const failedPath = path.join(this.failedDir, fileName);
        await atomicWriteJson(failedPath, task);
        await fs.rm(inflightPath, { force: true });
        this.pendingFilesByKey.delete(task.key);
      } else {
        task.nextRunAt =
          Date.now() + backoffMs({ baseMs: this.retryBaseMs, maxMs: this.retryMaxMs, attempt });
        const pendingPath = path.join(this.pendingDir, fileName);
        await atomicWriteJson(pendingPath, task);
        await fs.rm(inflightPath, { force: true });
        this.pendingFilesByKey.set(task.key, pendingPath);
      }
    } finally {
      this.inflightKeys.delete(task.key);
    }
  }

  private async processForget(task: PersistedForgetTask): Promise<NonNullable<PersistedForgetTask["result"]>> {
    const out: NonNullable<PersistedForgetTask["result"]> = {
      qdrant: {},
      neo4j: {},
      queue: { ok: true, cancelled: 0 },
      deletedNeo4j: 0,
    };

    if (task.sessionId) {
      try {
        await this.qdrant.deleteBySession({ namespace: task.namespace, sessionId: task.sessionId });
        out.qdrant!.bySession = { ok: true };
      } catch (err) {
        out.qdrant!.bySession = { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
      try {
        const d = await this.neo4j.deleteMemoriesBySession({
          namespace: task.namespace,
          sessionId: task.sessionId,
        });
        out.deletedNeo4j = (out.deletedNeo4j ?? 0) + d;
        out.neo4j!.bySession = { ok: true, deleted: d };
      } catch (err) {
        out.neo4j!.bySession = { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
      try {
        const cancelled = await this.updateQueue.cancelBySession({
          namespace: task.namespace,
          sessionId: task.sessionId,
        });
        out.queue = { ok: true, cancelled };
      } catch (err) {
        out.queue = { ok: false, cancelled: 0, error: err instanceof Error ? err.message : String(err) };
      }
    }

    const ids = Array.isArray(task.memoryIds) ? task.memoryIds : [];
    if (ids.length > 0) {
      try {
        const d = await this.qdrant.deleteByIds({ ids });
        out.qdrant!.byIds = { ok: true, deleted: d };
      } catch (err) {
        out.qdrant!.byIds = { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
      try {
        const d = await this.neo4j.deleteMemoriesByIds({ namespace: task.namespace, ids });
        out.deletedNeo4j = (out.deletedNeo4j ?? 0) + d;
        out.neo4j!.byIds = { ok: true, deleted: d };
      } catch (err) {
        out.neo4j!.byIds = { ok: false, error: err instanceof Error ? err.message : String(err) };
      }
    }

    return out;
  }
}

