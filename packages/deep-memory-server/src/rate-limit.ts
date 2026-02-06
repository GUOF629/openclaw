type Bucket = {
  windowStartMs: number;
  count: number;
};

export class FixedWindowRateLimiter {
  private readonly windowMs: number;
  private readonly buckets = new Map<string, Bucket>();

  constructor(params: { windowMs: number }) {
    this.windowMs = Math.max(1000, Math.floor(params.windowMs));
  }

  take(params: {
    key: string;
    limit: number;
    nowMs?: number;
  }): { ok: true; remaining: number; resetAtMs: number } | { ok: false; resetAtMs: number } {
    const limit = Math.max(0, Math.floor(params.limit));
    const now = params.nowMs ?? Date.now();
    const windowStart = Math.floor(now / this.windowMs) * this.windowMs;
    const resetAt = windowStart + this.windowMs;
    if (limit === 0) {
      return { ok: true, remaining: 0, resetAtMs: resetAt };
    }

    const existing = this.buckets.get(params.key);
    const b =
      existing && existing.windowStartMs === windowStart
        ? existing
        : { windowStartMs: windowStart, count: 0 };

    if (b.count >= limit) {
      this.buckets.set(params.key, b);
      return { ok: false, resetAtMs: resetAt };
    }
    b.count += 1;
    this.buckets.set(params.key, b);
    return { ok: true, remaining: Math.max(0, limit - b.count), resetAtMs: resetAt };
  }
}
