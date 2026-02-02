import crypto from "node:crypto";
import { z } from "zod";
const ApiKeyRulesSchema = z.array(z
    .object({
    key: z.string().min(1),
    role: z.union([z.literal("read"), z.literal("write"), z.literal("admin")]),
    namespaces: z.array(z.string()).optional(),
})
    .strict());
function roleRank(role) {
    return role === "read" ? 1 : role === "write" ? 2 : 3;
}
function timingSafeEqual(a, b) {
    const ab = Buffer.from(a);
    const bb = Buffer.from(b);
    const len = Math.max(ab.length, bb.length);
    const ap = Buffer.concat([ab, Buffer.alloc(Math.max(0, len - ab.length))]);
    const bp = Buffer.concat([bb, Buffer.alloc(Math.max(0, len - bb.length))]);
    return crypto.timingSafeEqual(ap, bp) && a.length === b.length;
}
function keyFingerprint(key) {
    return crypto.createHash("sha256").update(key).digest("hex").slice(0, 12);
}
function parseRules(cfg) {
    if (cfg.API_KEYS_JSON) {
        const raw = cfg.API_KEYS_JSON.trim();
        if (raw) {
            const parsed = ApiKeyRulesSchema.safeParse(JSON.parse(raw));
            if (!parsed.success) {
                throw new Error("Invalid API_KEYS_JSON");
            }
            return parsed.data;
        }
    }
    const keys = [];
    if (cfg.API_KEY?.trim())
        keys.push(cfg.API_KEY.trim());
    if (cfg.API_KEYS?.trim()) {
        for (const part of cfg.API_KEYS.split(",")) {
            const k = part.trim();
            if (k)
                keys.push(k);
        }
    }
    // Back-compat: if configured via API_KEY/API_KEYS, treat as admin over all namespaces.
    return keys.map((k) => ({ key: k, role: "admin", namespaces: ["*"] }));
}
function normalizeNamespaces(list) {
    if (!list || list.length === 0)
        return ["*"];
    const out = list.map((s) => s.trim()).filter(Boolean);
    return out.length === 0 ? ["*"] : out;
}
function namespaceAllowed(auth, namespace) {
    if (auth.namespaces.includes("*"))
        return true;
    return auth.namespaces.includes(namespace);
}
export function createAuthz(cfg) {
    const rules = parseRules(cfg).map((r) => ({
        key: r.key,
        role: r.role,
        namespaces: normalizeNamespaces(r.namespaces),
        keyId: keyFingerprint(r.key),
    }));
    const required = cfg.REQUIRE_API_KEY || rules.length > 0;
    const resolveAuth = (provided) => {
        const header = provided.trim();
        if (!header)
            return null;
        for (const r of rules) {
            if (timingSafeEqual(header, r.key)) {
                return { role: r.role, namespaces: r.namespaces, keyId: r.keyId };
            }
        }
        return null;
    };
    const getAuth = (c) => {
        return c.get("auth") ?? null;
    };
    const requireRole = (minRole) => {
        return async (c, next) => {
            if (!required) {
                // No keys configured and not required => open access.
                return await next();
            }
            const header = c.req.header("x-api-key") ?? "";
            const auth = resolveAuth(header);
            if (!auth) {
                return c.json({ error: "unauthorized" }, 401);
            }
            if (roleRank(auth.role) < roleRank(minRole)) {
                return c.json({ error: "forbidden" }, 403);
            }
            c.set("auth", auth);
            return await next();
        };
    };
    const requirePrefix = (prefix, minRole) => {
        const guard = requireRole(minRole);
        return async (c, next) => {
            if (c.req.path.startsWith(prefix)) {
                return await guard(c, next);
            }
            return await next();
        };
    };
    const assertNamespace = (c, namespace) => {
        if (!required)
            return { ok: true };
        const auth = getAuth(c);
        if (!auth) {
            return { ok: false, status: 401, body: { error: "unauthorized" } };
        }
        if (!namespaceAllowed(auth, namespace)) {
            return {
                ok: false,
                status: 403,
                body: { error: "forbidden_namespace" },
            };
        }
        return { ok: true };
    };
    const extractNamespaceFromKey = (key) => {
        const idx = key.indexOf("::");
        if (idx <= 0)
            return null;
        return key.slice(0, idx);
    };
    return {
        required,
        requireRole,
        requirePrefix,
        assertNamespace,
        extractNamespaceFromKey,
        getAuth,
    };
}
//# sourceMappingURL=authz.js.map