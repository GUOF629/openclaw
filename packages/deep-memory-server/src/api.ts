import type { Logger } from "pino";
import { Hono } from "hono";
import crypto from "node:crypto";
import { readFileSync } from "node:fs";
import { z } from "zod";
import type { DeepMemoryServerConfig } from "./config.js";
import type { DurableForgetQueue } from "./durable-forget-queue.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";
import type { DeepMemoryMetrics } from "./metrics.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { RetrieveContextResponse, UpdateMemoryIndexResponse } from "./types.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { extractHintsFromText } from "./analyzer.js";
import { appendAuditLog } from "./audit-log.js";
import { createAuthz } from "./authz.js";
import { enforceBodySize, readJsonWithLimit } from "./body-limit.js";
import { FixedWindowRateLimiter } from "./rate-limit.js";
import { DEEPMEM_SCHEMA_VERSION } from "./schema.js";
import { stableHash } from "./utils.js";

type PackageJson = { version?: unknown; name?: unknown };

let cachedServiceVersion: string | null = null;

function getServiceVersion(): string {
  if (cachedServiceVersion !== null) {
    return cachedServiceVersion;
  }
  try {
    const raw = readFileSync(new URL("../package.json", import.meta.url), "utf-8");
    const parsed = JSON.parse(raw) as unknown;
    if (typeof parsed === "object" && parsed) {
      const pkg = parsed as PackageJson;
      if (typeof pkg.version === "string" && pkg.version.trim()) {
        cachedServiceVersion = pkg.version.trim();
        return cachedServiceVersion;
      }
    }
  } catch {
    // ignore: best-effort only (e.g. tests or packaged runtime)
  }
  cachedServiceVersion = "unknown";
  return cachedServiceVersion;
}

function getBuildInfo(): { sha?: string; ref?: string; time?: string } | undefined {
  const sha = (process.env.GIT_SHA ?? process.env.BUILD_SHA ?? "").trim() || undefined;
  const ref = (process.env.GIT_REF ?? process.env.BUILD_REF ?? "").trim() || undefined;
  const time = (process.env.BUILD_TIME ?? process.env.BUILD_DATE ?? "").trim() || undefined;
  if (!sha && !ref && !time) {
    return undefined;
  }
  return { sha, ref, time };
}

const RetrieveSchema = z.object({
  namespace: z.string().optional(),
  user_input: z.string(),
  session_id: z.string(),
  max_memories: z.number().int().positive().optional(),
});

const UpdateSchema = z.object({
  namespace: z.string().optional(),
  session_id: z.string(),
  messages: z.array(z.unknown()),
  async: z.boolean().optional(),
});

const ForgetSchema = z
  .object({
    namespace: z.string().optional(),
    memory_ids: z.array(z.string()).optional(),
    session_id: z.string().optional(),
    dry_run: z.boolean().optional(),
    async: z.boolean().optional(),
  })
  .strict()
  .superRefine((val, ctx) => {
    if ((!val.memory_ids || val.memory_ids.length === 0) && !val.session_id) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "must provide memory_ids or session_id",
      });
    }
  });

export function createApi(params: {
  cfg: DeepMemoryServerConfig;
  log?: Logger;
  retriever: DeepMemoryRetriever;
  updater: DeepMemoryUpdater;
  qdrant: QdrantStore;
  neo4j: Neo4jStore;
  queue: DurableUpdateQueue;
  forgetQueue: DurableForgetQueue;
  metrics?: DeepMemoryMetrics;
}) {
  const app = new Hono();
  const authz = createAuthz(params.cfg);
  const limiter = new FixedWindowRateLimiter({ windowMs: params.cfg.RATE_LIMIT_WINDOW_MS });
  const lastUpdateAtByKey = new Map<string, number>();
  const activeRetrieveByNamespace = new Map<string, number>();
  const updateDisabledNamespaces = new Set(
    (params.cfg.UPDATE_DISABLED_NAMESPACES ?? "")
      .split(/[,\s]+/g)
      .map((s) => s.trim())
      .filter(Boolean),
  );
  const serviceVersion = getServiceVersion();
  const build = getBuildInfo();

  const withTimeout = async <T>(
    name: string,
    ms: number,
    fn: () => Promise<T>,
  ): Promise<{ ok: true; value: T } | { ok: false; error: string }> => {
    let timer: NodeJS.Timeout | undefined;
    try {
      const value = await new Promise<T>((resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`${name} timeout after ${ms}ms`)), ms);
        fn().then(resolve, reject);
      });
      return { ok: true, value };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  };

  app.use("*", enforceBodySize(params.cfg));

  // Request logger: stable request id + latency + auth keyId (never log raw API key).
  app.use("*", async (c, next) => {
    const incoming = c.req.header("x-request-id")?.trim() || "";
    const requestId =
      incoming.length > 0 && incoming.length <= 128 ? incoming : crypto.randomUUID();
    c.header("x-request-id", requestId);
    const start = performance.now();
    try {
      await next();
    } catch (err) {
      params.log?.error(
        {
          requestId,
          method: c.req.method,
          path: c.req.path,
          keyId: authz.getAuth(c)?.keyId,
          err: err instanceof Error ? err.message : String(err),
        },
        "request error",
      );
      throw err;
    } finally {
      const status = c.res?.status ?? 500;
      const ms = Math.max(0, Math.round(performance.now() - start));
      params.log?.info(
        {
          requestId,
          method: c.req.method,
          path: c.req.path,
          status,
          ms,
          keyId: authz.getAuth(c)?.keyId,
        },
        "request",
      );
    }
  });
  // Permission matrix:
  // - retrieve_context: read
  // - update_memory_index: write
  // - forget: admin
  // - queue/*: admin
  app.use("*", authz.requirePrefix("/queue", "admin"));
  app.use("/forget", authz.requireRole("admin"));
  app.use("/update_memory_index", authz.requireRole("write"));
  if (authz.required) {
    app.use("/retrieve_context", authz.requireRole("read"));
  }

  const checkRateLimit = (c: import("hono").Context, route: string, limit: number) => {
    if (!params.cfg.RATE_LIMIT_ENABLED) {
      return { ok: true as const };
    }
    if (limit <= 0) {
      return { ok: true as const };
    }
    const keyId = authz.getAuth(c)?.keyId ?? "anon";
    const out = limiter.take({ key: `${keyId}::${route}`, limit });
    if (!out.ok) {
      const retryAfterSeconds = Math.max(1, Math.ceil((out.resetAtMs - Date.now()) / 1000));
      c.header("retry-after", String(retryAfterSeconds));
      return { ok: false as const };
    }
    return { ok: true as const };
  };

  // Metrics endpoint: protected if auth is required (recommended).
  if (params.metrics) {
    if (authz.required) {
      app.use("/metrics", authz.requireRole("admin"));
    }
    app.get("/metrics", async (c) => {
      if (!authz.required && !params.cfg.ALLOW_UNAUTHENTICATED_METRICS) {
        return c.text("not found", 404);
      }
      const stats = params.queue.stats();
      params.metrics!.queuePending.set(stats.pendingApprox);
      params.metrics!.queueActive.set(stats.active);
      params.metrics!.queueInflightKeys.set(stats.inflightKeys);
      c.header("content-type", params.metrics!.registry.contentType);
      return c.text(await params.metrics!.registry.metrics());
    });
  }

  // Request-level metrics: best-effort, route key uses path (stable).
  app.use("*", async (c, next) => {
    const start = performance.now();
    try {
      await next();
    } finally {
      const metrics = params.metrics;
      if (metrics) {
        const route = c.req.path;
        const method = c.req.method;
        const status = String(c.res.status ?? 200);
        const seconds = Math.max(0, (performance.now() - start) / 1000);
        metrics.httpRequestsTotal.labels(route, method, status).inc();
        metrics.httpRequestDurationSeconds.labels(route, method, status).observe(seconds);
      }
    }
  });

  const buildHealthBody = (details: boolean) => {
    const queueStats = params.queue.stats();
    return {
      ok: true,
      service: {
        name: "deep-memory-server",
        version: serviceVersion,
        build: details ? build : undefined,
      },
      runtime: {
        node: process.version,
        pid: process.pid,
        platform: process.platform,
        arch: process.arch,
        uptimeSec: Math.max(0, Math.floor(process.uptime())),
      },
      auth: {
        required: authz.required,
      },
      deps: details
        ? {
            qdrant: {
              url: params.cfg.QDRANT_URL,
              collection: params.cfg.QDRANT_COLLECTION,
              vectorDims: params.cfg.VECTOR_DIMS,
            },
            neo4j: {
              uri: params.cfg.NEO4J_URI,
              user: params.cfg.NEO4J_USER,
            },
          }
        : undefined,
      queue: {
        ...queueStats,
        dir: params.cfg.QUEUE_DIR,
        keepDone: params.cfg.QUEUE_KEEP_DONE,
        retentionDays: params.cfg.QUEUE_RETENTION_DAYS,
        maxAttempts: params.cfg.QUEUE_MAX_ATTEMPTS,
        updateConcurrency: params.cfg.UPDATE_CONCURRENCY,
      },
      guardrails: {
        rateLimit: {
          enabled: params.cfg.RATE_LIMIT_ENABLED,
          windowMs: params.cfg.RATE_LIMIT_WINDOW_MS,
          retrievePerWindow: params.cfg.RATE_LIMIT_RETRIEVE_PER_WINDOW,
          updatePerWindow: params.cfg.RATE_LIMIT_UPDATE_PER_WINDOW,
          forgetPerWindow: params.cfg.RATE_LIMIT_FORGET_PER_WINDOW,
          queueAdminPerWindow: params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW,
        },
        updateBacklog: {
          rejectPending: params.cfg.UPDATE_BACKLOG_REJECT_PENDING,
          retryAfterSeconds: params.cfg.UPDATE_BACKLOG_RETRY_AFTER_SECONDS,
          delayPending: params.cfg.UPDATE_BACKLOG_DELAY_PENDING,
          delaySeconds: params.cfg.UPDATE_BACKLOG_DELAY_SECONDS,
          readOnlyPending: params.cfg.UPDATE_BACKLOG_READ_ONLY_PENDING,
        },
        updateIngest: {
          disabledNamespaces: Array.from(updateDisabledNamespaces.values()),
          minIntervalMs: params.cfg.UPDATE_MIN_INTERVAL_MS,
          sampleRate: params.cfg.UPDATE_SAMPLE_RATE,
        },
        namespaceConcurrency: {
          retrieve: params.cfg.NAMESPACE_RETRIEVE_CONCURRENCY,
          update: params.cfg.NAMESPACE_UPDATE_CONCURRENCY,
        },
        retrieveDegrade: {
          relatedPending: params.cfg.RETRIEVE_DEGRADE_RELATED_PENDING,
        },
        sensitiveFilter: {
          enabled: params.cfg.SENSITIVE_FILTER_ENABLED,
          rulesetVersion: params.cfg.SENSITIVE_RULESET_VERSION,
          customAllow: Boolean(params.cfg.SENSITIVE_ALLOW_REGEX_JSON),
          customDeny: Boolean(params.cfg.SENSITIVE_DENY_REGEX_JSON),
        },
      },
      schema: details
        ? {
            expectedVersion: DEEPMEM_SCHEMA_VERSION,
            mode: params.cfg.MIGRATIONS_MODE,
          }
        : undefined,
      now: new Date().toISOString(),
    };
  };

  app.get("/health", (c) => c.json(buildHealthBody(false)));

  // Detailed health is useful for ops; protect it when API keys are required.
  if (authz.required) {
    app.use("/health/details", authz.requireRole("admin"));
  }
  app.get("/health/details", async (c) => {
    const timeoutMs = 1500;
    const qdrant = await withTimeout("qdrant_schema", timeoutMs, async () =>
      params.qdrant.schemaStatus({
        mode: params.cfg.MIGRATIONS_MODE,
        expectedVersion: DEEPMEM_SCHEMA_VERSION,
      }),
    );
    const neo4j = await withTimeout("neo4j_schema", timeoutMs, async () =>
      params.neo4j.schemaStatus({
        mode: params.cfg.MIGRATIONS_MODE,
        expectedVersion: DEEPMEM_SCHEMA_VERSION,
      }),
    );
    return c.json({
      ...buildHealthBody(true),
      schema: {
        expectedVersion: DEEPMEM_SCHEMA_VERSION,
        mode: params.cfg.MIGRATIONS_MODE,
        qdrant: qdrant.ok ? qdrant.value : { ok: false, error: qdrant.error },
        neo4j: neo4j.ok ? neo4j.value : { ok: false, error: neo4j.error },
      },
    });
  });

  // Readiness probe: verifies dependencies are reachable within a tight timeout.
  app.get("/readyz", async (c) => {
    const timeoutMs = 1500;
    const qdrant = await withTimeout("qdrant", timeoutMs, async () => params.qdrant.healthCheck());
    const neo4j = await withTimeout("neo4j", timeoutMs, async () => params.neo4j.healthCheck());
    const queue = params.queue.stats();

    const qdrantResult = qdrant.ok ? qdrant.value : { ok: false as const, error: qdrant.error };
    const neo4jResult = neo4j.ok ? neo4j.value : { ok: false as const, error: neo4j.error };
    const ok = qdrantResult.ok && neo4jResult.ok;

    return c.json(
      {
        ok,
        qdrant: qdrantResult,
        neo4j: neo4jResult,
        queue,
      },
      ok ? 200 : 503,
    );
  });

  app.post("/retrieve_context", async (c) => {
    const rl = checkRateLimit(c, "/retrieve_context", params.cfg.RATE_LIMIT_RETRIEVE_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const body = await readJsonWithLimit<Record<string, unknown>>(c, {
      limitBytes: params.cfg.MAX_BODY_BYTES,
      fallback: {},
    });
    if (typeof body === "object" && body && "error" in body) {
      return c.json(body, body.error === "payload_too_large" ? 413 : 400);
    }
    const parsed = RetrieveSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const nsCheck = authz.assertNamespace(c, namespace);
    if (!nsCheck.ok) {
      return c.json(nsCheck.body, nsCheck.status);
    }

    const nsRetrieveLimit = params.cfg.NAMESPACE_RETRIEVE_CONCURRENCY;
    if (nsRetrieveLimit > 0) {
      const active = activeRetrieveByNamespace.get(namespace) ?? 0;
      if (active >= nsRetrieveLimit) {
        c.header("retry-after", "1");
        return c.json(
          { error: "namespace_overloaded", namespace, active, limit: nsRetrieveLimit },
          503,
        );
      }
      activeRetrieveByNamespace.set(namespace, active + 1);
    }
    const maxMemories = req.max_memories ?? 10;
    const degradeRelatedPending = params.cfg.RETRIEVE_DEGRADE_RELATED_PENDING;
    const pendingApprox = params.queue.stats().pendingApprox;
    const degradeRelated = degradeRelatedPending > 0 && pendingApprox >= degradeRelatedPending;
    const hints = degradeRelated
      ? { entities: [] as string[], topics: [] as string[] }
      : extractHintsFromText(req.user_input);
    try {
      const out: RetrieveContextResponse = await params.retriever.retrieve({
        namespace,
        userInput: req.user_input,
        sessionId: req.session_id,
        maxMemories,
        entities: hints.entities,
        topics: hints.topics,
      });
      params.metrics?.retrieveReturnedMemoriesTotal
        .labels("200")
        .inc(Array.isArray(out.memories) ? out.memories.length : 0);
      return c.json(out);
    } finally {
      if (nsRetrieveLimit > 0) {
        const next = (activeRetrieveByNamespace.get(namespace) ?? 1) - 1;
        if (next <= 0) {
          activeRetrieveByNamespace.delete(namespace);
        } else {
          activeRetrieveByNamespace.set(namespace, next);
        }
      }
    }
  });

  app.post("/update_memory_index", async (c) => {
    const rl = checkRateLimit(c, "/update_memory_index", params.cfg.RATE_LIMIT_UPDATE_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const body = await readJsonWithLimit<Record<string, unknown>>(c, {
      limitBytes: params.cfg.MAX_UPDATE_BODY_BYTES,
      fallback: {},
    });
    if (typeof body === "object" && body && "error" in body) {
      return c.json(body, body.error === "payload_too_large" ? 413 : 400);
    }
    const parsed = UpdateSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const nsCheck = authz.assertNamespace(c, namespace);
    if (!nsCheck.ok) {
      return c.json(nsCheck.body, nsCheck.status);
    }

    if (updateDisabledNamespaces.has(namespace)) {
      return c.json({
        status: "skipped",
        memories_added: 0,
        memories_filtered: 0,
        error: "namespace_write_disabled",
      });
    }

    const sampleRate = params.cfg.UPDATE_SAMPLE_RATE;
    if (sampleRate < 1) {
      const msgCount = Array.isArray(req.messages) ? req.messages.length : 0;
      const h = stableHash(`${namespace}::${req.session_id}::${msgCount}`);
      const bucket = Number.parseInt(h.slice(0, 8), 16);
      const p = (bucket % 10_000) / 10_000;
      if (p >= sampleRate) {
        return c.json({
          status: "skipped",
          memories_added: 0,
          memories_filtered: 0,
          error: "sampled_out",
        });
      }
    }

    const minIntervalMs = params.cfg.UPDATE_MIN_INTERVAL_MS;
    if (minIntervalMs > 0) {
      const key = `${namespace}::${req.session_id}`;
      const last = lastUpdateAtByKey.get(key) ?? 0;
      const now = Date.now();
      const waitMs = last > 0 ? minIntervalMs - (now - last) : 0;
      if (waitMs > 0) {
        c.header("retry-after", String(Math.max(1, Math.ceil(waitMs / 1000))));
        return c.json({
          status: "skipped",
          memories_added: 0,
          memories_filtered: 0,
          error: "throttled",
        });
      }
      lastUpdateAtByKey.set(key, now);
    }
    const runAsync = req.async ?? true;

    if (runAsync) {
      const stats = params.queue.stats();
      const readOnlyPending = params.cfg.UPDATE_BACKLOG_READ_ONLY_PENDING;
      if (readOnlyPending > 0 && stats.pendingApprox >= readOnlyPending) {
        const retryAfter = params.cfg.UPDATE_BACKLOG_RETRY_AFTER_SECONDS;
        c.header("retry-after", String(retryAfter));
        return c.json({
          status: "skipped",
          memories_added: 0,
          memories_filtered: 0,
          error: "degraded_read_only",
          pendingApprox: stats.pendingApprox,
          retryAfterSeconds: retryAfter,
        });
      }
      const rejectPending = params.cfg.UPDATE_BACKLOG_REJECT_PENDING;
      if (rejectPending > 0) {
        if (stats.pendingApprox >= rejectPending) {
          const retryAfter = params.cfg.UPDATE_BACKLOG_RETRY_AFTER_SECONDS;
          c.header("retry-after", String(retryAfter));
          return c.json(
            {
              error: "queue_overloaded",
              pendingApprox: stats.pendingApprox,
              retryAfterSeconds: retryAfter,
            },
            503,
          );
        }
      }
      const delayPending = params.cfg.UPDATE_BACKLOG_DELAY_PENDING;
      const delaySeconds = params.cfg.UPDATE_BACKLOG_DELAY_SECONDS;
      const notBeforeMs =
        delayPending > 0 && delaySeconds > 0 && stats.pendingApprox >= delayPending
          ? Date.now() + delaySeconds * 1000
          : undefined;
      void params.queue.enqueue({
        namespace,
        sessionId: req.session_id,
        messages: req.messages,
        notBeforeMs,
      });
      const resp: UpdateMemoryIndexResponse = {
        status: "queued",
        memories_added: 0,
        memories_filtered: 0,
      };
      if (notBeforeMs) {
        (resp as unknown as Record<string, unknown>).degraded = {
          mode: "delayed",
          notBeforeMs,
          delaySeconds,
        };
      }
      return c.json(resp);
    }

    const result = await params.queue.runNow({
      namespace,
      sessionId: req.session_id,
      messages: req.messages,
    });
    params.metrics?.updateMemoriesAddedTotal.labels("200").inc(result.memories_added ?? 0);
    params.metrics?.updateMemoriesFilteredTotal.labels("200").inc(result.memories_filtered ?? 0);
    return c.json(result);
  });

  // Minimal “forget” API: delete by explicit ids or by session.
  // This is intentionally best-effort and should be protected at the network layer.
  app.post("/forget", async (c) => {
    const rl = checkRateLimit(c, "/forget", params.cfg.RATE_LIMIT_FORGET_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const body = await readJsonWithLimit<Record<string, unknown>>(c, {
      limitBytes: params.cfg.MAX_BODY_BYTES,
      fallback: {},
    });
    if (typeof body === "object" && body && "error" in body) {
      return c.json(body, body.error === "payload_too_large" ? 413 : 400);
    }
    const parsed = ForgetSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const nsCheck = authz.assertNamespace(c, namespace);
    if (!nsCheck.ok) {
      return c.json(nsCheck.body, nsCheck.status);
    }
    const dryRun = req.dry_run ?? false;
    const runAsync = req.async ?? false;

    const ids = (req.memory_ids ?? []).map((id) => id.trim()).filter(Boolean);
    const normalizedIds = ids.map((id) => (id.includes("::") ? id : `${namespace}::${id}`));
    const requestId = c.res.headers.get("x-request-id") ?? undefined;

    if (dryRun) {
      await appendAuditLog(params.cfg, {
        action: "forget",
        namespace,
        requestId,
        dryRun: true,
        sessionId: req.session_id ?? undefined,
        memoryIdsCount: normalizedIds.length,
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({
        status: "dry_run",
        namespace,
        request_id: requestId,
        delete_ids: normalizedIds.length,
        delete_session: req.session_id ? 1 : 0,
      });
    }

    if (runAsync) {
      const out = await params.forgetQueue.enqueue({
        namespace,
        sessionId: req.session_id ?? undefined,
        memoryIds: normalizedIds,
      });
      await appendAuditLog(params.cfg, {
        action: "forget",
        namespace,
        requestId,
        dryRun: false,
        sessionId: req.session_id ?? undefined,
        memoryIdsCount: normalizedIds.length,
        deletedReported: 0,
        results: {
          queue: { ok: true, cancelled: 0 },
        },
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({
        status: "queued",
        namespace,
        request_id: requestId,
        key: out.key,
        task_id: out.taskId,
        delete_ids: normalizedIds.length,
        delete_session: req.session_id ? 1 : 0,
      });
    }

    let deleted = 0;
    const results: NonNullable<
      Parameters<typeof appendAuditLog>[1] & { action: "forget" }
    >["results"] = {
      qdrant: {},
      neo4j: {},
      queue: { ok: true, cancelled: 0 },
    };
    if (req.session_id) {
      // Best-effort dual delete.
      try {
        await params.qdrant.deleteBySession({ namespace, sessionId: req.session_id });
        results.qdrant!.bySession = { ok: true };
      } catch {
        results.qdrant!.bySession = { ok: false, error: "qdrant deleteBySession failed" };
      }
      try {
        const d = await params.neo4j.deleteMemoriesBySession({
          namespace,
          sessionId: req.session_id,
        });
        deleted += d;
        results.neo4j!.bySession = { ok: true, deleted: d };
      } catch {
        results.neo4j!.bySession = { ok: false, error: "neo4j deleteMemoriesBySession failed" };
      }
      try {
        const cancelled = await params.queue.cancelBySession({
          namespace,
          sessionId: req.session_id,
        });
        results.queue = { ok: true, cancelled };
      } catch {
        results.queue = { ok: false, cancelled: 0, error: "queue cancelBySession failed" };
      }
    }
    if (normalizedIds.length > 0) {
      try {
        const d = await params.qdrant.deleteByIds({ ids: normalizedIds });
        results.qdrant!.byIds = { ok: true, deleted: d };
      } catch {
        results.qdrant!.byIds = { ok: false, error: "qdrant deleteByIds failed" };
      }
      try {
        const d = await params.neo4j.deleteMemoriesByIds({ namespace, ids: normalizedIds });
        deleted += d;
        results.neo4j!.byIds = { ok: true, deleted: d };
      } catch {
        results.neo4j!.byIds = { ok: false, error: "neo4j deleteMemoriesByIds failed" };
      }
    }
    await appendAuditLog(params.cfg, {
      action: "forget",
      namespace,
      requestId,
      dryRun: false,
      sessionId: req.session_id ?? undefined,
      memoryIdsCount: normalizedIds.length,
      deletedReported: deleted,
      results,
      requester: {
        ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
        userAgent: c.req.header("user-agent") ?? undefined,
        keyId: authz.getAuth(c)?.keyId,
      },
    }).catch(() => {});
    params.metrics?.forgetDeletedTotal.labels("200").inc(deleted);
    return c.json({ status: "processed", namespace, request_id: requestId, deleted, results });
  });

  app.get("/queue/stats", (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    return c.json({ ok: true, ...params.queue.stats() });
  });

  app.get("/queue/forget/stats", (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    return c.json({ ok: true, ...params.forgetQueue.stats() });
  });

  app.get("/queue/forget/failed", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const limitRaw = c.req.query("limit");
    const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));
    const items = await params.forgetQueue.listFailed({ limit });
    return c.json({ ok: true, items });
  });

  app.get("/queue/forget/failed/export", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const file = c.req.query("file") ?? undefined;
    const key = c.req.query("key") ?? undefined;
    const limitRaw = c.req.query("limit");
    const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));

    const ns = key ? authz.extractNamespaceFromKey(key) : null;
    if (ns) {
      const nsCheck = authz.assertNamespace(c, ns);
      if (!nsCheck.ok) {
        return c.json(nsCheck.body, nsCheck.status);
      }
    }
    const out = await params.forgetQueue.exportFailed({ file, key, limit });
    await appendAuditLog(params.cfg, {
      action: "queue_failed_export",
      file: file?.trim() || undefined,
      key: key?.trim() || undefined,
      limit,
      requester: {
        ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
        userAgent: c.req.header("user-agent") ?? undefined,
        keyId: authz.getAuth(c)?.keyId,
      },
    }).catch(() => {});
    return c.json({ ok: true, ...out });
  });

  app.post("/queue/forget/failed/retry", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const body = await readJsonWithLimit<Record<string, unknown>>(c, {
      limitBytes: params.cfg.MAX_BODY_BYTES,
      fallback: {},
    });
    if (typeof body === "object" && body && "error" in body) {
      return c.json(body, body.error === "payload_too_large" ? 413 : 400);
    }
    const file = typeof body.file === "string" ? body.file : "";
    const key = typeof body.key === "string" ? body.key : "";
    const dryRun = body.dry_run === true || body.dry_run === "true";
    const limitRaw =
      typeof body.limit === "number"
        ? body.limit
        : typeof body.limit === "string"
          ? Number(body.limit)
          : 50;
    const limit = Math.max(1, Math.min(200, Number(limitRaw) || 50));
    if (file) {
      if (dryRun) {
        await appendAuditLog(params.cfg, {
          action: "queue_failed_retry",
          dryRun: true,
          file,
          requester: {
            ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
            userAgent: c.req.header("user-agent") ?? undefined,
            keyId: authz.getAuth(c)?.keyId,
          },
        }).catch(() => {});
        return c.json({ ok: true, mode: "file", status: "dry_run" });
      }
      const out = await params.forgetQueue.retryFailed({ file });
      await appendAuditLog(params.cfg, {
        action: "queue_failed_retry",
        dryRun: false,
        file,
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({ ok: true, mode: "file", ...out });
    }
    if (key) {
      const ns = authz.extractNamespaceFromKey(key);
      if (ns) {
        const nsCheck = authz.assertNamespace(c, ns);
        if (!nsCheck.ok) {
          return c.json(nsCheck.body, nsCheck.status);
        }
      }
      const out = await params.forgetQueue.retryFailedByKey({ key, limit, dryRun });
      await appendAuditLog(params.cfg, {
        action: "queue_failed_retry",
        dryRun,
        key,
        limit,
        retried: out.retried,
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({ ok: true, mode: "key", ...out });
    }
    return c.json({ error: "invalid_request" }, 400);
  });

  app.get("/queue/failed", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const limitRaw = c.req.query("limit");
    const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));
    const items = await params.queue.listFailed({ limit });
    return c.json({ ok: true, items });
  });

  app.get("/queue/failed/export", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const file = c.req.query("file") ?? undefined;
    const key = c.req.query("key") ?? undefined;
    const limitRaw = c.req.query("limit");
    const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));

    const ns = key ? authz.extractNamespaceFromKey(key) : null;
    if (ns) {
      const nsCheck = authz.assertNamespace(c, ns);
      if (!nsCheck.ok) {
        return c.json(nsCheck.body, nsCheck.status);
      }
    }
    const out = await params.queue.exportFailed({ file, key, limit });
    await appendAuditLog(params.cfg, {
      action: "queue_failed_export",
      file: file?.trim() || undefined,
      key: key?.trim() || undefined,
      limit,
      requester: {
        ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
        userAgent: c.req.header("user-agent") ?? undefined,
        keyId: authz.getAuth(c)?.keyId,
      },
    }).catch(() => {});
    return c.json({ ok: true, ...out });
  });

  app.post("/queue/failed/retry", async (c) => {
    const rl = checkRateLimit(c, "/queue", params.cfg.RATE_LIMIT_QUEUE_ADMIN_PER_WINDOW);
    if (!rl.ok) {
      return c.json({ error: "rate_limited" }, 429);
    }
    const body = await readJsonWithLimit<Record<string, unknown>>(c, {
      limitBytes: params.cfg.MAX_BODY_BYTES,
      fallback: {},
    });
    if (typeof body === "object" && body && "error" in body) {
      return c.json(body, body.error === "payload_too_large" ? 413 : 400);
    }
    const file = typeof body.file === "string" ? body.file : "";
    const key = typeof body.key === "string" ? body.key : "";
    const dryRun = body.dry_run === true || body.dry_run === "true";
    const limitRaw =
      typeof body.limit === "number"
        ? body.limit
        : typeof body.limit === "string"
          ? Number(body.limit)
          : 50;
    const limit = Math.max(1, Math.min(200, Number(limitRaw) || 50));
    if (file) {
      // Namespace binding for file retry: peek meta via export first.
      const meta = await params.queue.exportFailed({ file, limit: 1 });
      const ns = meta.mode === "file" ? meta.item.namespace : null;
      if (ns) {
        const nsCheck = authz.assertNamespace(c, ns);
        if (!nsCheck.ok) {
          return c.json(nsCheck.body, nsCheck.status);
        }
      }
      if (dryRun) {
        await appendAuditLog(params.cfg, {
          action: "queue_failed_retry",
          dryRun: true,
          file,
          requester: {
            ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
            userAgent: c.req.header("user-agent") ?? undefined,
            keyId: authz.getAuth(c)?.keyId,
          },
        }).catch(() => {});
        return c.json({ ok: true, mode: "file", status: "dry_run" });
      }
      const out = await params.queue.retryFailed({ file });
      await appendAuditLog(params.cfg, {
        action: "queue_failed_retry",
        dryRun: false,
        file,
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({ ok: true, mode: "file", ...out });
    }
    if (key) {
      const ns = authz.extractNamespaceFromKey(key);
      if (ns) {
        const nsCheck = authz.assertNamespace(c, ns);
        if (!nsCheck.ok) {
          return c.json(nsCheck.body, nsCheck.status);
        }
      }
      const out = await params.queue.retryFailedByKey({ key, limit, dryRun });
      await appendAuditLog(params.cfg, {
        action: "queue_failed_retry",
        dryRun,
        key,
        limit,
        retried: out.retried,
        requester: {
          ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
          userAgent: c.req.header("user-agent") ?? undefined,
          keyId: authz.getAuth(c)?.keyId,
        },
      }).catch(() => {});
      return c.json({ ok: true, mode: "key", ...out });
    }
    return c.json({ error: "invalid_request" }, 400);
  });

  return app;
}
