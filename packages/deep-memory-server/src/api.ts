import { Hono } from "hono";
import { z } from "zod";
import type { DeepMemoryServerConfig } from "./config.js";
import type { RetrieveContextResponse, UpdateMemoryIndexResponse } from "./types.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";
import { extractHintsFromText } from "./analyzer.js";
import type { QdrantStore } from "./qdrant.js";
import type { Neo4jStore } from "./neo4j.js";
import type { DurableUpdateQueue } from "./durable-update-queue.js";

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
  retriever: DeepMemoryRetriever;
  updater: DeepMemoryUpdater;
  qdrant: QdrantStore;
  neo4j: Neo4jStore;
  queue: DurableUpdateQueue;
}) {
  const app = new Hono();

  app.get("/health", (c) => c.json({ ok: true }));

  app.post("/retrieve_context", async (c) => {
    const body = await c.req.json().catch(() => ({}));
    const parsed = RetrieveSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const maxMemories = req.max_memories ?? 10;
    const hints = extractHintsFromText(req.user_input);
    const out: RetrieveContextResponse = await params.retriever.retrieve({
      namespace,
      userInput: req.user_input,
      sessionId: req.session_id,
      maxMemories,
      entities: hints.entities,
      topics: hints.topics,
    });
    return c.json(out);
  });

  app.post("/update_memory_index", async (c) => {
    const body = await c.req.json().catch(() => ({}));
    const parsed = UpdateSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const runAsync = req.async ?? true;

    if (runAsync) {
      void params.queue.enqueue({ namespace, sessionId: req.session_id, messages: req.messages });
      const resp: UpdateMemoryIndexResponse = {
        status: "queued",
        memories_added: 0,
        memories_filtered: 0,
      };
      return c.json(resp);
    }

    const result = await params.queue.runNow({ namespace, sessionId: req.session_id, messages: req.messages });
    return c.json(result);
  });

  // Minimal “forget” API: delete by explicit ids or by session.
  // This is intentionally best-effort and should be protected at the network layer.
  app.post("/forget", async (c) => {
    const body = await c.req.json().catch(() => ({}));
    const parsed = ForgetSchema.safeParse(body);
    if (!parsed.success) {
      return c.json({ error: "invalid_request", details: parsed.error.issues }, 400);
    }
    const req = parsed.data;
    const namespace = req.namespace?.trim() || "default";
    const dryRun = req.dry_run ?? false;

    const ids = (req.memory_ids ?? []).map((id) => id.trim()).filter(Boolean);
    const normalizedIds = ids.map((id) => (id.includes("::") ? id : `${namespace}::${id}`));

    if (dryRun) {
      return c.json({
        status: "dry_run",
        namespace,
        delete_ids: normalizedIds.length,
        delete_session: req.session_id ? 1 : 0,
      });
    }

    let deleted = 0;
    if (req.session_id) {
      // Best-effort dual delete.
      try {
        await params.qdrant.deleteBySession({ namespace, sessionId: req.session_id });
      } catch {}
      try {
        deleted += await params.neo4j.deleteMemoriesBySession({ namespace, sessionId: req.session_id });
      } catch {}
      try {
        await params.queue.cancelBySession({ namespace, sessionId: req.session_id });
      } catch {}
    }
    if (normalizedIds.length > 0) {
      try {
        await params.qdrant.deleteByIds({ ids: normalizedIds });
      } catch {}
      try {
        deleted += await params.neo4j.deleteMemoriesByIds({ namespace, ids: normalizedIds });
      } catch {}
    }
    return c.json({ status: "processed", namespace, deleted });
  });

  app.get("/queue/stats", (c) => {
    return c.json({ ok: true, ...params.queue.stats() });
  });

  return app;
}

