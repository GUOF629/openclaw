import { Hono } from "hono";
import { z } from "zod";
import { extractHintsFromText } from "./analyzer.js";
import { createAuthz } from "./authz.js";
import { enforceBodySize, readJsonWithLimit } from "./body-limit.js";
import { appendAuditLog } from "./audit-log.js";
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
export function createApi(params) {
    const app = new Hono();
    const authz = createAuthz(params.cfg);
    app.use("*", enforceBodySize(params.cfg));
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
    app.get("/health", (c) => c.json({ ok: true }));
    app.post("/retrieve_context", async (c) => {
        const body = await readJsonWithLimit(c, {
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
        const maxMemories = req.max_memories ?? 10;
        const hints = extractHintsFromText(req.user_input);
        const out = await params.retriever.retrieve({
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
        const body = await readJsonWithLimit(c, {
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
        const runAsync = req.async ?? true;
        if (runAsync) {
            void params.queue.enqueue({ namespace, sessionId: req.session_id, messages: req.messages });
            const resp = {
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
        const body = await readJsonWithLimit(c, {
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
        const ids = (req.memory_ids ?? []).map((id) => id.trim()).filter(Boolean);
        const normalizedIds = ids.map((id) => (id.includes("::") ? id : `${namespace}::${id}`));
        if (dryRun) {
            await appendAuditLog(params.cfg, {
                action: "forget",
                namespace,
                dryRun: true,
                sessionId: req.session_id ?? undefined,
                memoryIdsCount: normalizedIds.length,
                requester: {
                    ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
                    userAgent: c.req.header("user-agent") ?? undefined,
                    keyId: authz.getAuth(c)?.keyId,
                },
            }).catch(() => { });
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
            }
            catch { }
            try {
                deleted += await params.neo4j.deleteMemoriesBySession({ namespace, sessionId: req.session_id });
            }
            catch { }
            try {
                await params.queue.cancelBySession({ namespace, sessionId: req.session_id });
            }
            catch { }
        }
        if (normalizedIds.length > 0) {
            try {
                await params.qdrant.deleteByIds({ ids: normalizedIds });
            }
            catch { }
            try {
                deleted += await params.neo4j.deleteMemoriesByIds({ namespace, ids: normalizedIds });
            }
            catch { }
        }
        await appendAuditLog(params.cfg, {
            action: "forget",
            namespace,
            dryRun: false,
            sessionId: req.session_id ?? undefined,
            memoryIdsCount: normalizedIds.length,
            deletedReported: deleted,
            requester: {
                ip: c.req.header("x-forwarded-for") ?? c.req.header("x-real-ip") ?? undefined,
                userAgent: c.req.header("user-agent") ?? undefined,
                keyId: authz.getAuth(c)?.keyId,
            },
        }).catch(() => { });
        return c.json({ status: "processed", namespace, deleted });
    });
    app.get("/queue/stats", (c) => {
        return c.json({ ok: true, ...params.queue.stats() });
    });
    app.get("/queue/failed", async (c) => {
        const limitRaw = c.req.query("limit");
        const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));
        const items = await params.queue.listFailed({ limit });
        return c.json({ ok: true, items });
    });
    app.get("/queue/failed/export", async (c) => {
        const file = c.req.query("file") ?? undefined;
        const key = c.req.query("key") ?? undefined;
        const limitRaw = c.req.query("limit");
        const limit = Math.max(1, Math.min(200, Number(limitRaw ?? 50) || 50));
        const ns = key ? authz.extractNamespaceFromKey(key) : null;
        if (ns) {
            const nsCheck = authz.assertNamespace(c, ns);
            if (!nsCheck.ok)
                return c.json(nsCheck.body, nsCheck.status);
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
        }).catch(() => { });
        return c.json({ ok: true, ...out });
    });
    app.post("/queue/failed/retry", async (c) => {
        const body = await readJsonWithLimit(c, {
            limitBytes: params.cfg.MAX_BODY_BYTES,
            fallback: {},
        });
        if (typeof body === "object" && body && "error" in body) {
            return c.json(body, body.error === "payload_too_large" ? 413 : 400);
        }
        const file = typeof body.file === "string" ? body.file : "";
        const key = typeof body.key === "string" ? body.key : "";
        const dryRun = Boolean(body.dry_run);
        const limit = Math.max(1, Math.min(200, Number(body.limit ?? 50) || 50));
        if (file) {
            // Namespace binding for file retry: peek meta via export first.
            const meta = await params.queue.exportFailed({ file, limit: 1 });
            const ns = meta.mode === "file" ? meta.item.namespace : null;
            if (ns) {
                const nsCheck = authz.assertNamespace(c, ns);
                if (!nsCheck.ok)
                    return c.json(nsCheck.body, nsCheck.status);
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
                }).catch(() => { });
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
            }).catch(() => { });
            return c.json({ ok: true, mode: "file", ...out });
        }
        if (key) {
            const ns = authz.extractNamespaceFromKey(key);
            if (ns) {
                const nsCheck = authz.assertNamespace(c, ns);
                if (!nsCheck.ok)
                    return c.json(nsCheck.body, nsCheck.status);
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
            }).catch(() => { });
            return c.json({ ok: true, mode: "key", ...out });
        }
        return c.json({ error: "invalid_request" }, 400);
    });
    return app;
}
//# sourceMappingURL=api.js.map