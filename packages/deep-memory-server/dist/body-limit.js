function parseContentLength(raw) {
    if (!raw)
        return null;
    const n = Number(raw);
    return Number.isFinite(n) && n >= 0 ? n : null;
}
export function enforceBodySize(cfg) {
    return async (c, next) => {
        const path = c.req.path;
        // Conservative: retrieve should be small; update may be large.
        const limit = path === "/update_memory_index" ? cfg.MAX_UPDATE_BODY_BYTES : cfg.MAX_BODY_BYTES;
        const contentLength = parseContentLength(c.req.header("content-length"));
        if (typeof limit === "number" && limit > 0 && contentLength !== null && contentLength > limit) {
            return c.json({ error: "payload_too_large", limitBytes: limit }, 413);
        }
        return await next();
    };
}
export async function readJsonWithLimit(c, params) {
    try {
        const buf = Buffer.from(await c.req.arrayBuffer());
        if (buf.length > params.limitBytes) {
            return { error: "payload_too_large", limitBytes: params.limitBytes };
        }
        if (buf.length === 0) {
            return params.fallback;
        }
        return JSON.parse(buf.toString("utf8"));
    }
    catch {
        return { error: "invalid_json" };
    }
}
//# sourceMappingURL=body-limit.js.map