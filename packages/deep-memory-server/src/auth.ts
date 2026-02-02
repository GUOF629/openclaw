import crypto from "node:crypto";
import type { Context, Next } from "hono";
import type { DeepMemoryServerConfig } from "./config.js";

function timingSafeEqual(a: string, b: string): boolean {
  // Avoid leaking length differences.
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  const len = Math.max(ab.length, bb.length);
  const ap = Buffer.concat([ab, Buffer.alloc(Math.max(0, len - ab.length))]);
  const bp = Buffer.concat([bb, Buffer.alloc(Math.max(0, len - bb.length))]);
  return crypto.timingSafeEqual(ap, bp) && a.length === b.length;
}

export function requireApiKey(cfg: DeepMemoryServerConfig) {
  const required = cfg.REQUIRE_API_KEY || Boolean(cfg.API_KEY);
  const expected = cfg.API_KEY ?? "";
  return async (c: Context, next: Next) => {
    if (!required) {
      return await next();
    }
    const header = c.req.header("x-api-key") ?? "";
    const ok = expected && header && timingSafeEqual(header, expected);
    if (!ok) {
      return c.json({ error: "unauthorized" }, 401);
    }
    return await next();
  };
}

export function requireApiKeyForPaths(cfg: DeepMemoryServerConfig, opts: { prefix: string }) {
  const guard = requireApiKey(cfg);
  return async (c: Context, next: Next) => {
    const path = c.req.path;
    if (path.startsWith(opts.prefix)) {
      return await guard(c, next);
    }
    return await next();
  };
}

