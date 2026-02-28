import type {
  CandidateMemoryDraft,
  ExtractedEntity,
  ExtractedEvent,
  ExtractedTopic,
  MemoryKind,
} from "./types.js";
import { safeTrim, stableHash } from "./utils.js";

type TranscriptMessage = {
  role?: string;
  content?: unknown;
};

function extractTextFromContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    const parts: string[] = [];
    for (const part of content) {
      if (!part || typeof part !== "object") {
        continue;
      }
      const p = part as { type?: unknown; text?: unknown };
      if (p.type === "text" && typeof p.text === "string") {
        parts.push(p.text);
      }
    }
    return parts.join("\n");
  }
  return "";
}

function normalizeText(raw: string): string {
  return raw.replace(/\r\n/g, "\n").trim();
}

function detectUserIntentScore(text: string): number {
  const t = text.toLowerCase();
  const patterns = [
    /记住|以后|下次|长期|偏好|习惯|不要|必须|务必|约定|规则/,
    /remember|preference|must|never|always|policy|rule/,
  ];
  return patterns.some((p) => p.test(t)) ? 0.9 : 0.2;
}

function guessEntityType(name: string): ExtractedEntity["type"] {
  const n = name.toLowerCase();
  if (/project|repo|服务|项目|工程/.test(n)) {
    return "project";
  }
  if (/公司|org|organization|团队/.test(n)) {
    return "organization";
  }
  if (/北京|上海|shanghai|beijing|city|地点|地址/.test(n)) {
    return "place";
  }
  return "other";
}

function tokenizeForTopics(text: string): string[] {
  // Simple tokenization: keep ASCII words and CJK runs.
  const tokens = text
    .split(/[^0-9A-Za-z\u4e00-\u9fff]+/g)
    .map((t) => t.trim())
    .filter((t) => t.length >= 2 && t.length <= 32);
  return tokens;
}

const STOPWORDS = new Set(
  [
    // CN
    "我们",
    "你们",
    "他们",
    "这个",
    "那个",
    "然后",
    "所以",
    "但是",
    "如果",
    "因为",
    "可以",
    "需要",
    "觉得",
    "现在",
    "今天",
    "一下",
    // EN
    "the",
    "and",
    "or",
    "to",
    "of",
    "in",
    "on",
    "for",
    "with",
    "is",
    "are",
    "be",
    "this",
    "that",
    "we",
    "you",
    "they",
  ].map((s) => s.toLowerCase()),
);

function filterStopwords(tokens: string[]): string[] {
  return tokens.filter((t) => !STOPWORDS.has(t.toLowerCase()));
}

function topKFrequency(tokens: string[], k: number): Array<{ term: string; count: number }> {
  const map = new Map<string, number>();
  for (const t of tokens) {
    map.set(t, (map.get(t) ?? 0) + 1);
  }
  return Array.from(map.entries())
    .map(([term, count]) => ({ term, count }))
    .toSorted((a, b) => b.count - a.count)
    .slice(0, k);
}

function extractIsoTimestampHint(text: string): string | null {
  // Basic date patterns: YYYY-MM-DD or YYYY/MM/DD
  const m = text.match(/\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b/);
  if (!m) {
    return null;
  }
  const yyyy = m[1];
  const mm = String(Math.max(1, Math.min(12, Number(m[2])))).padStart(2, "0");
  const dd = String(Math.max(1, Math.min(31, Number(m[3])))).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T00:00:00.000Z`;
}

export function extractHintsFromText(input: string): { entities: string[]; topics: string[] } {
  const tokens = filterStopwords(tokenizeForTopics(input));
  const top = topKFrequency(tokens, 12);
  const entities = top.map((t) => t.term).slice(0, 10);
  const topics = top.map((t) => t.term).slice(0, 10);
  return { entities, topics };
}

function detectMemoryKind(text: string): MemoryKind {
  const t = text.toLowerCase();
  if (/临时|本次|仅本次|only this time|temporary/.test(t)) {
    return "ephemeral";
  }
  if (/todo|待办|下一步|接下来|需要完成|计划|task/.test(t)) {
    return "task";
  }
  if (/偏好|喜欢|讨厌|不喜欢|习惯|prefer|preference|like|hate/.test(t)) {
    return "preference";
  }
  if (/规则|约定|必须|务必|不要|永远|policy|rule|must|never|always/.test(t)) {
    return "rule";
  }
  return "fact";
}

function guessSubject(entities: ExtractedEntity[], topics: ExtractedTopic[]): string | undefined {
  const e = entities[0]?.name?.trim();
  if (e) {
    return e;
  }
  const t = topics[0]?.name?.trim();
  return t || undefined;
}

function guessMemoryKey(
  kind: MemoryKind,
  subject: string | undefined,
  _content: string,
): string | undefined {
  if (!subject) {
    return kind === "rule" ? "rule:general" : undefined;
  }
  // Best-effort slotting: kind + subject + a tiny hint.
  const hint =
    kind === "preference"
      ? "preference"
      : kind === "rule"
        ? "rule"
        : kind === "task"
          ? "task"
          : kind;
  const key = `${hint}:${subject}`.toLowerCase();
  // Prevent extremely long keys.
  return key.length > 120 ? `${hint}:${stableHash(key)}` : key;
}

function guessExpiresAt(kind: MemoryKind, text: string, now: Date): string | undefined {
  if (kind !== "ephemeral") {
    return undefined;
  }
  const t = text.toLowerCase();
  const base = now.getTime();
  const dayMs = 24 * 3600_000;
  // Very rough TTL hints.
  if (/今天|today/.test(t)) {
    return new Date(base + dayMs).toISOString();
  }
  if (/本周|this week|一周|7天/.test(t)) {
    return new Date(base + 7 * dayMs).toISOString();
  }
  if (/本月|this month|30天/.test(t)) {
    return new Date(base + 30 * dayMs).toISOString();
  }
  return new Date(base + 7 * dayMs).toISOString();
}

export class SessionAnalyzer {
  analyze(params: {
    sessionId: string;
    messages: unknown[];
    now?: Date;
    maxMemoriesPerSession: number;
    importanceThreshold: number;
  }): {
    entities: ExtractedEntity[];
    topics: ExtractedTopic[];
    events: ExtractedEvent[];
    drafts: CandidateMemoryDraft[];
    filtered: { added: number; filtered: number };
  } {
    const now = params.now ?? new Date();
    const collected: Array<{ role: string; text: string }> = [];
    for (const item of params.messages) {
      const record =
        item && typeof item === "object" && "role" in (item as Record<string, unknown>)
          ? (item as TranscriptMessage)
          : item && typeof item === "object" && "message" in (item as Record<string, unknown>)
            ? ((item as { message?: unknown }).message as TranscriptMessage)
            : null;
      if (!record || typeof record.role !== "string") {
        continue;
      }
      if (record.role !== "user" && record.role !== "assistant") {
        continue;
      }
      const text = normalizeText(extractTextFromContent(record.content));
      if (!text) {
        continue;
      }
      collected.push({ role: record.role, text });
    }

    const joined = collected.map((m) => `${m.role}: ${m.text}`).join("\n");
    const tokens = filterStopwords(tokenizeForTopics(joined));

    // Topics: top frequency tokens (heuristic).
    const topicTerms = topKFrequency(tokens, 12).filter((t) => t.count >= 2);
    const topics: ExtractedTopic[] = topicTerms.map((t) => ({
      name: t.term,
      frequency: t.count,
      importance: Math.min(1, t.count / 10),
    }));

    // Entities: heuristic extraction of "named" terms.
    const entityCandidates = topKFrequency(tokens, 30).filter(
      (t) => /[A-Za-z]/.test(t.term) || /[\u4e00-\u9fff]/.test(t.term),
    );
    const entities: ExtractedEntity[] = entityCandidates.slice(0, 10).map((e) => ({
      name: e.term,
      type: guessEntityType(e.term),
      frequency: e.count,
    }));

    // Events: detect key verbs and map to event types.
    const events: ExtractedEvent[] = [];
    const eventPatterns: Array<{ type: ExtractedEvent["type"]; re: RegExp }> = [
      { type: "requirement_confirmed", re: /确认|定下|需求/ },
      { type: "design_decided", re: /设计|决定|方案/ },
      { type: "implementation_started", re: /开始实现|开工|implement/ },
      { type: "issue_resolved", re: /修复|解决|resolved|fixed/ },
      { type: "milestone_reached", re: /里程碑|完成|发布|shipped/ },
    ];
    for (const msg of collected) {
      for (const ep of eventPatterns) {
        if (ep.re.test(msg.text)) {
          const summary = msg.text.slice(0, 200);
          events.push({
            type: ep.type,
            summary,
            timestamp: extractIsoTimestampHint(msg.text) ?? now.toISOString(),
          });
          break;
        }
      }
    }

    // Candidate memory drafts: extract durable items, defer importance+novelty to updater.
    const candidates: CandidateMemoryDraft[] = [];
    for (const msg of collected) {
      const text = msg.text;
      const intent = detectUserIntentScore(text);
      const frequency = topicTerms[0]?.count ?? 1;
      const content = safeTrim(text.replace(/\s+/g, " "));
      if (!content || content.length < 20) {
        continue;
      }
      const kind = detectMemoryKind(content);
      const subject = guessSubject(entities, topics);
      const memoryKey = guessMemoryKey(kind, subject, content);
      const expiresAt = guessExpiresAt(kind, content, now);
      const confidence = Math.max(0, Math.min(1, 0.4 + 0.6 * intent));
      candidates.push({
        kind,
        subject,
        memoryKey,
        expiresAt,
        confidence,
        content,
        entities: entities.map((e) => e.name).slice(0, 5),
        topics: topics.map((t) => t.name).slice(0, 5),
        createdAt: extractIsoTimestampHint(content) ?? now.toISOString(),
        signals: {
          frequency,
          user_intent: intent,
          length: content.length,
        },
      });
    }
    for (const ev of events) {
      const content = `Event(${ev.type}): ${ev.summary}`;
      const kind: MemoryKind = "fact";
      candidates.push({
        kind,
        content,
        entities: entities.map((e) => e.name).slice(0, 5),
        topics: topics.map((t) => t.name).slice(0, 5),
        createdAt: ev.timestamp,
        signals: {
          frequency: 2,
          user_intent: 0.7,
          length: content.length,
        },
      });
    }

    // Dedup by normalized content hash (in-session).
    const seen = new Set<string>();
    const drafts: CandidateMemoryDraft[] = [];
    for (const m of candidates) {
      const key = stableHash(m.content.toLowerCase());
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      drafts.push(m);
      if (drafts.length >= params.maxMemoriesPerSession) {
        break;
      }
    }

    const filteredCount = Math.max(0, candidates.length - drafts.length);
    return {
      entities,
      topics,
      events,
      drafts,
      filtered: { added: drafts.length, filtered: filteredCount },
    };
  }
}
