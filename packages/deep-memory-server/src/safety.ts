import type { DeepMemoryServerConfig } from "./config.js";

type SensitiveRule = { id: string; re: RegExp };

const BUILTIN_DENY: SensitiveRule[] = [
  { id: "private_key_block", re: /-----BEGIN (?:RSA|EC|OPENSSH|PGP|DSA)? ?PRIVATE KEY-----/i },
  {
    id: "keyword_assignment",
    re: /\b(?:api[_-]?key|secret|password|passwd|token)\b\s*[:=]\s*\S+/i,
  },
  { id: "secret_prefix", re: /\b(?:sk|rk)_[A-Za-z0-9]{20,}\b/ },
  { id: "long_hex", re: /\b[A-Fa-f0-9]{32,}\b/ },
  { id: "long_digits", re: /\b\d{14,}\b/ },
  { id: "jwt_like", re: /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/ },
];

function compileRegexList(raw: string | undefined, prefix: string): SensitiveRule[] {
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    const out: SensitiveRule[] = [];
    for (let i = 0; i < parsed.length; i += 1) {
      const item = parsed[i];
      if (typeof item !== "string" || !item.trim()) {
        continue;
      }
      try {
        out.push({ id: `${prefix}_${i}`, re: new RegExp(item, "i") });
      } catch {
        // ignore invalid regex
      }
    }
    return out;
  } catch {
    return [];
  }
}

export type SensitiveDetection = {
  sensitive: boolean;
  reasons: string[];
  rulesetVersion: string;
};

export type SensitiveFilter = {
  version: string;
  denyRules: SensitiveRule[];
  allowRules: SensitiveRule[];
  detect: (text: string) => SensitiveDetection;
};

export function createSensitiveFilter(
  cfg: Pick<
    DeepMemoryServerConfig,
    "SENSITIVE_RULESET_VERSION" | "SENSITIVE_DENY_REGEX_JSON" | "SENSITIVE_ALLOW_REGEX_JSON"
  >,
): SensitiveFilter {
  const version = cfg.SENSITIVE_RULESET_VERSION?.trim() || "builtin-v1";
  const deny = [...BUILTIN_DENY, ...compileRegexList(cfg.SENSITIVE_DENY_REGEX_JSON, "deny")];
  const allow = compileRegexList(cfg.SENSITIVE_ALLOW_REGEX_JSON, "allow");
  return {
    version,
    denyRules: deny,
    allowRules: allow,
    detect: (text: string): SensitiveDetection => {
      const t = text.trim();
      if (!t) {
        return { sensitive: false, reasons: [], rulesetVersion: version };
      }
      if (allow.some((r) => r.re.test(t))) {
        return { sensitive: false, reasons: [], rulesetVersion: version };
      }
      const reasons = deny.filter((r) => r.re.test(t)).map((r) => r.id);
      return { sensitive: reasons.length > 0, reasons, rulesetVersion: version };
    },
  };
}

export function looksSensitive(text: string): boolean {
  const t = text.trim();
  if (!t) {
    return false;
  }
  return BUILTIN_DENY.some((p) => p.re.test(t));
}
