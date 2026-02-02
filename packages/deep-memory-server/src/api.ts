import { Hono } from "hono";
import { z } from "zod";
import type { DeepMemoryServerConfig } from "./config.js";
import type { RetrieveContextResponse, UpdateMemoryIndexResponse } from "./types.js";
import type { DeepMemoryRetriever } from "./retriever.js";
import type { DeepMemoryUpdater } from "./updater.js";

const RetrieveSchema = z.object({
  user_input: z.string(),
  session_id: z.string(),
  max_memories: z.number().int().positive().optional(),
});

const UpdateSchema = z.object({
  session_id: z.string(),
  messages: z.array(z.unknown()),
  async: z.boolean().optional(),
});

export function createApi(params: {
  cfg: DeepMemoryServerConfig;
  retriever: DeepMemoryRetriever;
  updater: DeepMemoryUpdater;
  enqueueUpdate: (
    sessionId: string,
    task: () => Promise<UpdateMemoryIndexResponse>,
  ) => Promise<UpdateMemoryIndexResponse>;
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
    const maxMemories = req.max_memories ?? 10;
    // Extraction in server is lightweight; we use simple tokens as both entities/topics hints.
    const raw = req.user_input.trim();
    const entities = raw
      .split(/[^0-9A-Za-z\u4e00-\u9fff]+/g)
      .map((t) => t.trim())
      .filter((t) => t.length >= 2)
      .slice(0, 10);
    const topics = entities.slice(0, 10);
    const out: RetrieveContextResponse = await params.retriever.retrieve({
      userInput: req.user_input,
      sessionId: req.session_id,
      maxMemories,
      entities,
      topics,
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
    const runAsync = req.async ?? true;

    if (runAsync) {
      void params.enqueueUpdate(req.session_id, async () =>
        await params.updater.update({ sessionId: req.session_id, messages: req.messages }),
      );
      const resp: UpdateMemoryIndexResponse = {
        status: "queued",
        memories_added: 0,
        memories_filtered: 0,
      };
      return c.json(resp);
    }

    const result = await params.enqueueUpdate(req.session_id, async () =>
      await params.updater.update({ sessionId: req.session_id, messages: req.messages }),
    );
    return c.json(result);
  });

  return app;
}

