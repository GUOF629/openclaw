import type { Context, Next } from "hono";
import crypto from "node:crypto";
import { z } from "zod";
import type { DeepMemoryServerConfig } from "./config.js";

export type AuthRole = "read" | "write" | "admin";

export type AuthInfo = {
  role: AuthRole;
  namespaces: string[]; // ["*"] means all
  keyId: string; // short fingerprint for auditing
};

type ApiKeyRule = {
  key: string;
  role: AuthRole;
  namespaces?: string[];
};

const ApiKeyRulesSchema = z.array(
  z
    .object({
      key: z.string().min(1),
      role: z.union([z.literal("read"), z.literal("write"), z.literal("admin")]),
      namespaces: z.array(z.string()).optional(),
    })
    .strict(),
);

function roleRank(role: AuthRole): number {
  return role === "read" ? 1 : role === "write" ? 2 : 3;
}

function timingSafeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  const len = Math.max(ab.length, bb.length);
  const ap = Buffer.concat([ab, Buffer.alloc(Math.max(0, len - ab.length))]);
  const bp = Buffer.concat([bb, Buffer.alloc(Math.max(0, len - bb.length))]);
  return crypto.timingSafeEqual(ap, bp) && a.length === b.length;
}

function keyFingerprint(key: string): string {
  return crypto.createHash("sha256").update(key).digest("hex").slice(0, 12);
}

function parseRules(cfg: DeepMemoryServerConfig): ApiKeyRule[] {
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

  const keys: string[] = [];
  if (cfg.API_KEY?.trim()) {
    keys.push(cfg.API_KEY.trim());
  }
  if (cfg.API_KEYS?.trim()) {
    for (const part of cfg.API_KEYS.split(",")) {
      const k = part.trim();
      if (k) {
        keys.push(k);
      }
    }
  }
  // Back-compat: if configured via API_KEY/API_KEYS, treat as admin over all namespaces.
  return keys.map((k) => ({ key: k, role: "admin", namespaces: ["*"] }));
}

function normalizeNamespaces(list?: string[]): string[] {
  if (!list || list.length === 0) {
    return ["*"];
  }
  const out = list.map((s) => s.trim()).filter(Boolean);
  return out.length === 0 ? ["*"] : out;
}

function namespaceAllowed(auth: AuthInfo, namespace: string): boolean {
  if (auth.namespaces.includes("*")) {
    return true;
  }
  return auth.namespaces.includes(namespace);
}

export function createAuthz(cfg: DeepMemoryServerConfig) {
  const rules = parseRules(cfg).map((r) => ({
    key: r.key,
    role: r.role,
    namespaces: normalizeNamespaces(r.namespaces),
    keyId: keyFingerprint(r.key),
  }));
  const required = cfg.REQUIRE_API_KEY || rules.length > 0;
  if (cfg.REQUIRE_API_KEY && rules.length === 0) {
    throw new Error("REQUIRE_API_KEY is set but no API keys are configured");
  }

  const resolveAuth = (provided: string): AuthInfo | null => {
    const header = provided.trim();
    if (!header) {
      return null;
    }
    for (const r of rules) {
      if (timingSafeEqual(header, r.key)) {
        return { role: r.role, namespaces: r.namespaces, keyId: r.keyId };
      }
    }
    return null;
  };

  const getAuth = (c: Context): AuthInfo | null => {
    return (c.get("auth") as AuthInfo | undefined) ?? null;
  };

  const requireRole = (minRole: AuthRole) => {
    return async (c: Context, next: Next) => {
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

  const requirePrefix = (prefix: string, minRole: AuthRole) => {
    const guard = requireRole(minRole);
    return async (c: Context, next: Next) => {
      if (c.req.path.startsWith(prefix)) {
        return await guard(c, next);
      }
      return await next();
    };
  };

  const assertNamespace = (c: Context, namespace: string) => {
    if (!required) {
      return { ok: true as const };
    }
    const auth = getAuth(c);
    if (!auth) {
      return { ok: false as const, status: 401 as const, body: { error: "unauthorized" as const } };
    }
    if (!namespaceAllowed(auth, namespace)) {
      return {
        ok: false as const,
        status: 403 as const,
        body: { error: "forbidden_namespace" as const },
      };
    }
    return { ok: true as const };
  };

  const extractNamespaceFromKey = (key: string): string | null => {
    const idx = key.indexOf("::");
    if (idx <= 0) {
      return null;
    }
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
