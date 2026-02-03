import { describe, expect, it } from "vitest";
import { createSensitiveFilter } from "./safety.js";

describe("sensitive filter", () => {
  it("detects builtin sensitive patterns", () => {
    const f = createSensitiveFilter({
      SENSITIVE_RULESET_VERSION: "builtin-v1",
      SENSITIVE_ALLOW_REGEX_JSON: undefined,
      SENSITIVE_DENY_REGEX_JSON: undefined,
    });
    const r = f.detect("api_key=sk_1234567890123456789012345");
    expect(r.sensitive).toBe(true);
    expect(r.reasons.length).toBeGreaterThan(0);
  });

  it("allowlist overrides deny patterns", () => {
    const f = createSensitiveFilter({
      SENSITIVE_RULESET_VERSION: "test",
      SENSITIVE_ALLOW_REGEX_JSON: JSON.stringify(["timezone:\\s*asia/shanghai"]),
      SENSITIVE_DENY_REGEX_JSON: JSON.stringify(["timezone:"]),
    });
    const r = f.detect("timezone: Asia/Shanghai");
    expect(r.sensitive).toBe(false);
  });
});

