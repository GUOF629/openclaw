import crypto from "node:crypto";
function timingSafeEqual(a, b) {
    // Avoid leaking length differences.
    const ab = Buffer.from(a);
    const bb = Buffer.from(b);
    const len = Math.max(ab.length, bb.length);
    const ap = Buffer.concat([ab, Buffer.alloc(Math.max(0, len - ab.length))]);
    const bp = Buffer.concat([bb, Buffer.alloc(Math.max(0, len - bb.length))]);
    return crypto.timingSafeEqual(ap, bp) && a.length === b.length;
}
export function requireApiKey(cfg) {
    const required = cfg.REQUIRE_API_KEY || Boolean(cfg.API_KEY) || Boolean(cfg.API_KEYS);
    const expected = [];
    if (cfg.API_KEY) {
        expected.push(cfg.API_KEY);
    }
    if (cfg.API_KEYS) {
        for (const part of cfg.API_KEYS.split(",")) {
            const k = part.trim();
            if (k)
                expected.push(k);
        }
    }
    return async (c, next) => {
        if (!required) {
            return await next();
        }
        const header = c.req.header("x-api-key") ?? "";
        const ok = Boolean(header) && expected.some((k) => timingSafeEqual(header, k));
        if (!ok) {
            return c.json({ error: "unauthorized" }, 401);
        }
        return await next();
    };
}
export function requireApiKeyForPaths(cfg, opts) {
    const guard = requireApiKey(cfg);
    return async (c, next) => {
        const path = c.req.path;
        if (path.startsWith(opts.prefix)) {
            return await guard(c, next);
        }
        return await next();
    };
}
//# sourceMappingURL=auth.js.map