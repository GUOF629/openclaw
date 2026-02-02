import type { CandidateMemory, ExtractedEntity, ExtractedEvent, ExtractedTopic } from "./types.js";
import { computeImportance } from "./importance.js";
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
  if (/project|repo|服务|项目|工程/.test(n)) return "project";
  if (/公司|org|organization|团队/.test(n)) return "organization";
  if (/北京|上海|shanghai|beijing|city|地点|地址/.test(n)) return "place";
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

function topKFrequency(tokens: string[], k: number): Array<{ term: string; count: number }> {
  const map = new Map<string, number>();
  for (const t of tokens) {
    map.set(t, (map.get(t) ?? 0) + 1);
  }
  return Array.from(map.entries())
    .map(([term, count]) => ({ term, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, k);
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
    memories: CandidateMemory[];
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
    const tokens = tokenizeForTopics(joined);

    // Topics: top frequency tokens (heuristic).
    const topicTerms = topKFrequency(tokens, 10).filter((t) => t.count >= 2);
    const topics: ExtractedTopic[] = topicTerms.map((t) => ({
      name: t.term,
      frequency: t.count,
      importance: Math.min(1, t.count / 10),
    }));

    // Entities: heuristic extraction of "named" terms.
    const entityCandidates = topKFrequency(tokens, 20).filter((t) => /[A-Za-z]/.test(t.term) || /[\u4e00-\u9fff]/.test(t.term));
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
            timestamp: now.toISOString(),
          });
          break;
        }
      }
    }

    // Candidate memories: pick sentences that look durable (heuristic) + event summaries.
    const candidates: CandidateMemory[] = [];
    for (const msg of collected) {
      const text = msg.text;
      const intent = detectUserIntentScore(text);
      const novelty = 0.7; // real novelty is computed during storage (Qdrant similarity); keep a prior here.
      const frequency = topicTerms.length > 0 ? topicTerms[0]!.count : 1;
      const importance = computeImportance({
        frequency,
        novelty,
        user_intent: intent,
        length: text.length,
      });
      if (importance < params.importanceThreshold) {
        continue;
      }
      const content = safeTrim(text.replace(/\s+/g, " "));
      if (!content || content.length < 20) {
        continue;
      }
      candidates.push({
        content,
        importance,
        entities: entities.map((e) => e.name).slice(0, 5),
        topics: topics.map((t) => t.name).slice(0, 5),
        createdAt: now.toISOString(),
      });
    }
    for (const ev of events) {
      const content = `Event(${ev.type}): ${ev.summary}`;
      const importance = computeImportance({
        frequency: 2,
        novelty: 0.8,
        user_intent: 0.7,
        length: content.length,
      });
      if (importance >= params.importanceThreshold) {
        candidates.push({
          content,
          importance,
          entities: entities.map((e) => e.name).slice(0, 5),
          topics: topics.map((t) => t.name).slice(0, 5),
          createdAt: ev.timestamp,
        });
      }
    }

    // Dedup by normalized content hash (in-session).
    const seen = new Set<string>();
    const memories: CandidateMemory[] = [];
    for (const m of candidates) {
      const key = stableHash(m.content.toLowerCase());
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      memories.push(m);
      if (memories.length >= params.maxMemoriesPerSession) {
        break;
      }
    }

    const filteredCount = Math.max(0, candidates.length - memories.length);
    return {
      entities,
      topics,
      events,
      memories,
      filtered: { added: memories.length, filtered: filteredCount },
    };
  }
}

