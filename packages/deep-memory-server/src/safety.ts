const SENSITIVE_PATTERNS: RegExp[] = [
  /-----BEGIN (?:RSA|EC|OPENSSH|PGP|DSA)? ?PRIVATE KEY-----/i,
  /\b(?:api[_-]?key|secret|password|passwd|token)\b\s*[:=]\s*\S+/i,
  /\b(?:sk|rk)_[A-Za-z0-9]{20,}\b/, // common secret key prefixes
  /\b[A-Fa-f0-9]{32,}\b/, // long hex
  /\b\d{14,}\b/, // long digit runs (credit cards / ids)
  /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/, // JWT-ish
];

export function looksSensitive(text: string): boolean {
  const t = text.trim();
  if (!t) return false;
  return SENSITIVE_PATTERNS.some((p) => p.test(t));
}

