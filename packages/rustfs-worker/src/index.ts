import JSZip from "jszip";
import mammoth from "mammoth";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import pdfParse from "pdf-parse";
import pino from "pino";
import { z } from "zod";

type RustFsFileMeta = {
  file_id: string;
  tenant_id: string;
  session_id?: string | null;
  filename: string;
  mime?: string | null;
  size: number;
  sha256: string;
  created_at_ms: number;
  source?: string | null;
  encrypted: boolean;
  extract_status?: string | null;
  extract_updated_at_ms?: number | null;
  extract_attempt?: number | null;
  extract_error?: string | null;
  // annotations is opaque json (optional)
  annotations?: unknown;
};

type ExtractionSegmentV1 = {
  text: string;
  locator?: {
    kind: "chunk" | "page" | "slide" | "section" | "unknown";
    index: number; // 1-based
    total: number;
    startChar?: number;
    endChar?: number;
    label?: string;
  };
};

type ExtractionResultV1 = {
  schema_version: 1;
  docTypeGuess: string;
  languageGuess?: string;
  segments: ExtractionSegmentV1[];
  warnings: string[];
  stats: {
    bytes: number;
    chars: number;
    segments: number;
    truncated: boolean;
  };
};

function buildAnnotationsV1(params: {
  file: RustFsFileMeta;
  existingAnnotations?: unknown;
  deepMemory: {
    baseUrl: string;
    namespace?: string;
    sessionId: string;
    updateResponse?: unknown;
    error?: string;
    overloaded?: boolean;
  };
  extraction: ExtractionResultV1;
}): Record<string, unknown> {
  const base =
    params.existingAnnotations && typeof params.existingAnnotations === "object"
      ? { ...(params.existingAnnotations as Record<string, unknown>) }
      : ({} as Record<string, unknown>);

  const ingestRaw = base.openclaw_ingest;
  const ingest =
    ingestRaw && typeof ingestRaw === "object" && !Array.isArray(ingestRaw)
      ? (ingestRaw as Record<string, unknown>)
      : null;
  const ingestKind = ingest && typeof ingest.kind === "string" ? ingest.kind.trim() : "";
  const ingestHint = ingest && typeof ingest.hint === "string" ? ingest.hint.trim() : "";
  const ingestTags =
    ingest && Array.isArray(ingest.tags)
      ? ingest.tags
          .filter((t): t is string => typeof t === "string")
          .map((t) => t.trim())
          .filter(Boolean)
          .slice(0, 50)
      : undefined;

  return {
    ...base,
    schema_version: 1,
    classification: {
      kind: ingestKind || undefined,
      hint: ingestHint || undefined,
      tags: ingestTags && ingestTags.length > 0 ? ingestTags : undefined,
    },
    // Keep legacy fields for compatibility; downstream can migrate gradually.
    rustfs_worker: {
      version: 1,
      indexed_at_ms: Date.now(),
      deep_memory: {
        base_url: params.deepMemory.baseUrl,
        namespace: params.deepMemory.namespace,
        session_id: params.deepMemory.sessionId,
        update_response: params.deepMemory.updateResponse,
        error: params.deepMemory.error,
        overloaded: params.deepMemory.overloaded,
      },
      extract: {
        mime: params.file.mime ?? undefined,
        bytes: params.extraction.stats.bytes,
        truncated: params.extraction.stats.truncated,
        segments: params.extraction.segments.length,
        doc_type_guess: params.extraction.docTypeGuess,
        language_guess: params.extraction.languageGuess,
        warnings: params.extraction.warnings,
      },
    },
    extraction: {
      status: params.deepMemory.error ? "error" : "indexed",
      extractor: `rustfs-worker:${params.extraction.docTypeGuess}:v1`,
      attempt: params.file.extract_attempt ?? undefined,
      last_error: params.deepMemory.error ?? undefined,
      warnings: params.extraction.warnings,
      doc_type_guess: params.extraction.docTypeGuess,
      language_guess: params.extraction.languageGuess,
      segments: {
        count: params.extraction.segments.length,
      },
      stats: params.extraction.stats,
    },
    deep_memory: {
      session_id: params.deepMemory.sessionId,
      namespace: params.deepMemory.namespace,
      // Optional future fields: memory_ids/source_ref mappings.
    },
  };
}

const EnvSchema = z.object({
  RUSTFS_BASE_URL: z.string().min(1),
  RUSTFS_API_KEY: z.string().optional(),
  RUSTFS_TENANT_ID: z.string().optional(),
  RUSTFS_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(5_000),
  RUSTFS_LEASE_MS: z.coerce.number().int().positive().default(300_000),
  RUSTFS_PENDING_LIMIT: z.coerce.number().int().positive().max(200).default(25),
  RUSTFS_MAX_DOWNLOAD_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(2 * 1024 * 1024),
  RUSTFS_TOMBSTONE_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
  RUSTFS_TOMBSTONE_LIMIT: z.coerce.number().int().positive().max(200).default(100),
  RUSTFS_TOMBSTONE_SINCE_MS: z.coerce.number().int().nonnegative().default(0),
  WORKER_STATE_PATH: z.string().optional(),

  DEEP_MEMORY_BASE_URL: z.string().min(1),
  DEEP_MEMORY_API_KEY: z.string().optional(),
  DEEP_MEMORY_NAMESPACE: z.string().optional(),
  DEEP_MEMORY_ASYNC: z.coerce.boolean().default(true),
  DEEP_MEMORY_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),

  // Chunking: convert extracted text into multiple messages for better semantic indexing.
  DEEP_MEMORY_CHUNK_MAX_CHARS: z.coerce.number().int().positive().default(3500),
  DEEP_MEMORY_CHUNK_OVERLAP_CHARS: z.coerce.number().int().nonnegative().default(200),

  // Backoff when deep-memory is overloaded.
  DEEP_MEMORY_BACKOFF_MS: z.coerce.number().int().positive().default(10_000),
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function isSupportedTextLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m.startsWith("text/")) {
    return true;
  }
  if (
    m === "application/json" ||
    m === "application/xml" ||
    m === "application/yaml" ||
    m === "application/x-yaml" ||
    m === "application/toml" ||
    m === "application/x-ndjson"
  ) {
    return true;
  }
  const lower = filename.toLowerCase();
  return (
    lower.endsWith(".txt") ||
    lower.endsWith(".md") ||
    lower.endsWith(".markdown") ||
    lower.endsWith(".json") ||
    lower.endsWith(".jsonl") ||
    lower.endsWith(".xml") ||
    lower.endsWith(".yaml") ||
    lower.endsWith(".yml") ||
    lower.endsWith(".toml")
  );
}

function isHtmlLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m === "text/html" || m === "application/xhtml+xml") {
    return true;
  }
  const lower = filename.toLowerCase();
  return lower.endsWith(".html") || lower.endsWith(".htm") || lower.endsWith(".xhtml");
}

function isCodeLike(_mime: string | undefined, filename: string): boolean {
  const lang = guessLanguage(filename);
  if (!lang) {
    return false;
  }
  return [
    "typescript",
    "javascript",
    "python",
    "go",
    "rust",
    "java",
    "kotlin",
    "swift",
    "css",
    "shell",
  ].includes(lang);
}

function isPdfLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m === "application/pdf") {
    return true;
  }
  return filename.toLowerCase().endsWith(".pdf");
}

function isDocxLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m === "application/vnd.openxmlformats-officedocument.wordprocessingml.document") {
    return true;
  }
  return filename.toLowerCase().endsWith(".docx");
}

function isPptxLike(mime: string | undefined, filename: string): boolean {
  const m = (mime ?? "").toLowerCase().trim();
  if (m === "application/vnd.openxmlformats-officedocument.presentationml.presentation") {
    return true;
  }
  return filename.toLowerCase().endsWith(".pptx");
}

function decodeHtmlEntities(input: string): string {
  return input
    .replaceAll("&nbsp;", " ")
    .replaceAll("&amp;", "&")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'")
    .replace(/&#(\d+);/g, (_m, n) => {
      const code = Number(n);
      return Number.isFinite(code) ? String.fromCodePoint(code) : "";
    });
}

function extractHtmlLike(params: {
  file: RustFsFileMeta;
  html: string;
  bytes: number;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): ExtractionResultV1 {
  const warnings: string[] = [];
  const raw = params.html;
  const titleMatch = raw.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const title = titleMatch ? decodeHtmlEntities(titleMatch[1]).replace(/\s+/g, " ").trim() : "";

  let body = raw;
  body = body.replace(/<script[\s\S]*?<\/script>/gi, "\n");
  body = body.replace(/<style[\s\S]*?<\/style>/gi, "\n");
  body = body.replace(/<noscript[\s\S]*?<\/noscript>/gi, "\n");

  // Preserve headings as section markers.
  body = body.replace(/<h([1-6])[^>]*>([\s\S]*?)<\/h\1>/gi, (_m, lvl, text) => {
    const t = decodeHtmlEntities(String(text))
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return `\n\n#${"#".repeat(Math.max(0, Number(lvl) - 1))} ${t}\n\n`;
  });

  // Newlines for common block separators.
  body = body.replace(/<br\s*\/?>/gi, "\n");
  body = body.replace(/<\/(p|div|section|article|header|footer|li|ul|ol|table|tr)>/gi, "\n");
  body = body.replace(/<(p|div|section|article|header|footer|li|ul|ol|table|tr)[^>]*>/gi, "\n");

  // Strip tags and decode entities.
  body = decodeHtmlEntities(body.replace(/<[^>]+>/g, " "));
  body = body.replace(/\r\n/g, "\n");
  body = body.replace(/[ \t]+\n/g, "\n");
  body = body.replace(/\n{3,}/g, "\n\n").trim();

  if (!body) {
    warnings.push("empty_html_body");
  }

  const blocks = body
    .split(/\n{2,}/g)
    .map((b) => b.trim())
    .filter(Boolean);
  const segments: ExtractionSegmentV1[] = [];
  let currentLabel: string | undefined = title || undefined;

  for (const block of blocks) {
    if (block.startsWith("#")) {
      currentLabel = block.replace(/^#+\s*/, "").trim() || currentLabel;
      continue;
    }
    const chunks = splitIntoChunks({
      text: block,
      maxChars: params.chunkMaxChars,
      overlapChars: params.chunkOverlapChars,
    });
    for (let i = 0; i < chunks.length; i += 1) {
      const c = chunks[i];
      if (!c) {
        continue;
      }
      segments.push({
        text: c.text,
        locator: {
          kind: "section",
          index: segments.length + 1,
          total: 0, // set below
          label: currentLabel,
          startChar: c.startChar,
          endChar: c.endChar,
        },
      });
    }
  }
  for (let i = 0; i < segments.length; i += 1) {
    const seg = segments[i];
    if (!seg) {
      continue;
    }
    if (seg.locator) {
      seg.locator.index = i + 1;
      seg.locator.total = segments.length;
    }
  }

  return {
    schema_version: 1,
    docTypeGuess: "html",
    languageGuess: "html",
    segments,
    warnings,
    stats: {
      bytes: params.bytes,
      chars: raw.length,
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

function guessDocType(mime: string | undefined, filename: string): string {
  const m = (mime ?? "").toLowerCase().trim();
  const lower = filename.toLowerCase();
  if (
    m === "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ||
    lower.endsWith(".docx")
  ) {
    return "docx";
  }
  if (
    m === "application/vnd.openxmlformats-officedocument.presentationml.presentation" ||
    lower.endsWith(".pptx")
  ) {
    return "pptx";
  }
  if (m === "application/pdf" || lower.endsWith(".pdf")) {
    return "pdf";
  }
  if (
    m === "text/html" ||
    m === "application/xhtml+xml" ||
    lower.endsWith(".html") ||
    lower.endsWith(".htm")
  ) {
    return "html";
  }
  if (m.includes("markdown") || lower.endsWith(".md") || lower.endsWith(".markdown")) {
    return "markdown";
  }
  if (m.includes("json") || lower.endsWith(".json") || lower.endsWith(".jsonl")) {
    return "json";
  }
  if (m.includes("yaml") || lower.endsWith(".yaml") || lower.endsWith(".yml")) {
    return "yaml";
  }
  if (m.includes("xml") || lower.endsWith(".xml")) {
    return "xml";
  }
  if (m.startsWith("text/") || lower.endsWith(".txt")) {
    return "text";
  }
  return m ? `mime:${m}` : "unknown";
}

function guessLanguage(filename: string): string | undefined {
  const lower = filename.toLowerCase();
  const ext = lower.split(".").pop() ?? "";
  const map: Record<string, string> = {
    ts: "typescript",
    tsx: "typescript",
    js: "javascript",
    jsx: "javascript",
    py: "python",
    go: "go",
    rs: "rust",
    java: "java",
    kt: "kotlin",
    swift: "swift",
    md: "markdown",
    json: "json",
    yaml: "yaml",
    yml: "yaml",
    toml: "toml",
    xml: "xml",
    html: "html",
    htm: "html",
    css: "css",
    sh: "shell",
  };
  return map[ext];
}

async function fetchJson<T>(params: {
  url: string;
  method: "GET" | "POST";
  apiKey?: string;
  body?: unknown;
  timeoutMs: number;
}): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), params.timeoutMs);
  try {
    const res = await fetch(params.url, {
      method: params.method,
      headers: {
        ...(params.apiKey ? { "x-api-key": params.apiKey } : {}),
        ...(params.body ? { "content-type": "application/json" } : {}),
      },
      body: params.body ? JSON.stringify(params.body) : undefined,
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
    }
    return (await res.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

function isOverloadLikeError(message: string): boolean {
  const m = message.toLowerCase();
  return (
    m.includes("http 503") ||
    m.includes("queue_overloaded") ||
    m.includes("degraded_read_only") ||
    m.includes("namespace_overloaded") ||
    m.includes("rate limit") ||
    m.includes("http 429")
  );
}

async function fetchBytes(params: {
  url: string;
  apiKey?: string;
  timeoutMs: number;
  maxBytes: number;
}): Promise<Uint8Array> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), params.timeoutMs);
  try {
    const headers: Record<string, string> = {};
    if (params.apiKey) {
      headers["x-api-key"] = params.apiKey;
    }
    const res = await fetch(params.url, {
      method: "GET",
      headers,
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
    }
    const reader = res.body?.getReader();
    if (!reader) {
      const buf = new Uint8Array(await res.arrayBuffer());
      return buf.length > params.maxBytes ? buf.slice(0, params.maxBytes) : buf;
    }
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      if (!value) {
        continue;
      }
      total += value.length;
      if (total > params.maxBytes) {
        chunks.push(value.slice(0, Math.max(0, params.maxBytes - (total - value.length))));
        break;
      }
      chunks.push(value);
    }
    const out = new Uint8Array(chunks.reduce((sum, c) => sum + c.length, 0));
    let offset = 0;
    for (const c of chunks) {
      out.set(c, offset);
      offset += c.length;
    }
    return out;
  } finally {
    clearTimeout(timer);
  }
}

function splitIntoChunks(params: {
  text: string;
  maxChars: number;
  overlapChars: number;
}): Array<{ text: string; truncated: boolean; startChar: number; endChar: number }> {
  const maxChars = Math.max(200, params.maxChars);
  const overlap = Math.max(0, Math.min(params.overlapChars, Math.max(0, maxChars - 50)));
  const t = params.text;
  if (!t.trim()) {
    return [];
  }
  const chunks: Array<{ text: string; truncated: boolean; startChar: number; endChar: number }> =
    [];
  let i = 0;
  while (i < t.length) {
    const end = Math.min(t.length, i + maxChars);
    const slice = t.slice(i, end);
    chunks.push({ text: slice, truncated: end < t.length, startChar: i, endChar: end });
    if (end >= t.length) {
      break;
    }
    i = Math.max(0, end - overlap);
  }
  return chunks;
}

function extractTextLike(params: {
  file: RustFsFileMeta;
  text: string;
  bytes: number;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): ExtractionResultV1 {
  const chunks = splitIntoChunks({
    text: params.text,
    maxChars: params.chunkMaxChars,
    overlapChars: params.chunkOverlapChars,
  });
  const segments: ExtractionSegmentV1[] = chunks.map((c, idx) => ({
    text: c.text,
    locator: {
      kind: "chunk",
      index: idx + 1,
      total: chunks.length,
      startChar: c.startChar,
      endChar: c.endChar,
    },
  }));
  return {
    schema_version: 1,
    docTypeGuess: guessDocType(params.file.mime ?? undefined, params.file.filename),
    languageGuess: guessLanguage(params.file.filename),
    segments,
    warnings: [],
    stats: {
      bytes: params.bytes,
      chars: params.text.length,
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

function extractCodeLike(params: {
  file: RustFsFileMeta;
  code: string;
  bytes: number;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): ExtractionResultV1 {
  const warnings: string[] = [];
  const lang = guessLanguage(params.file.filename);
  const code = params.code.replace(/\r\n/g, "\n");
  const lines = code.split("\n");

  const boundaryPatterns: Array<{ lang?: string; re: RegExp }> = [
    { lang: "python", re: /^\s*(def|class)\s+[A-Za-z0-9_]+\s*\(/ },
    { lang: "go", re: /^\s*func\s+(\([^)]+\)\s*)?[A-Za-z0-9_]+\s*\(/ },
    { lang: "rust", re: /^\s*(pub\s+)?(async\s+)?fn\s+[A-Za-z0-9_]+\s*\(/ },
    {
      lang: "java",
      re: /^\s*(public|private|protected)?\s*(static\s+)?(class|interface)\s+[A-Za-z0-9_]+/,
    },
    {
      lang: "typescript",
      re: /^\s*(export\s+)?(async\s+)?(function|class|interface|type)\s+[A-Za-z0-9_]+/,
    },
    {
      lang: "javascript",
      re: /^\s*(export\s+)?(async\s+)?(function|class)\s+[A-Za-z0-9_]+/,
    },
  ];

  const chosen = boundaryPatterns.filter((p) => !p.lang || p.lang === lang);
  const isBoundary = (line: string): boolean => chosen.some((p) => p.re.test(line));

  const maxChars = Math.max(500, params.chunkMaxChars);
  const overlap = Math.max(0, Math.min(params.chunkOverlapChars, Math.max(0, maxChars - 100)));

  const segments: ExtractionSegmentV1[] = [];
  let buf: string[] = [];
  let bufStartLine = 0;

  const flush = (endLineExclusive: number) => {
    const text = buf.join("\n").trimEnd();
    if (!text.trim()) {
      buf = [];
      bufStartLine = endLineExclusive;
      return;
    }
    // If a block is still too large, fall back to char-based split.
    if (text.length > maxChars) {
      const chunks = splitIntoChunks({ text, maxChars, overlapChars: overlap });
      for (let i = 0; i < chunks.length; i += 1) {
        const c = chunks[i];
        if (!c) {
          continue;
        }
        segments.push({
          text: c.text,
          locator: {
            kind: "section",
            index: 0,
            total: 0,
            label: `lines ${bufStartLine + 1}-${endLineExclusive}`,
          },
        });
      }
    } else {
      segments.push({
        text,
        locator: {
          kind: "section",
          index: 0,
          total: 0,
          label: `lines ${bufStartLine + 1}-${endLineExclusive}`,
        },
      });
    }
    buf = [];
    bufStartLine = endLineExclusive;
  };

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i] ?? "";
    if (buf.length === 0) {
      bufStartLine = i;
    }
    // Start a new segment at structural boundaries (best-effort).
    if (buf.length > 0 && isBoundary(line) && buf.join("\n").length >= Math.floor(maxChars * 0.6)) {
      flush(i);
    }
    buf.push(line);
    if (buf.join("\n").length >= maxChars) {
      flush(i + 1);
    }
  }
  if (buf.length > 0) {
    flush(lines.length);
  }

  if (segments.length === 0 && code.trim()) {
    warnings.push("code_chunking_failed_fallback_to_textlike");
    return extractTextLike({
      file: params.file,
      text: code,
      bytes: params.bytes,
      truncated: params.truncated,
      chunkMaxChars: params.chunkMaxChars,
      chunkOverlapChars: params.chunkOverlapChars,
    });
  }

  for (let i = 0; i < segments.length; i += 1) {
    const seg = segments[i];
    if (!seg?.locator) {
      continue;
    }
    seg.locator.index = i + 1;
    seg.locator.total = segments.length;
  }

  return {
    schema_version: 1,
    docTypeGuess: "code",
    languageGuess: lang,
    segments,
    warnings,
    stats: {
      bytes: params.bytes,
      chars: code.length,
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

async function extractPdfLike(params: {
  file: RustFsFileMeta;
  bytes: Uint8Array;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): Promise<ExtractionResultV1> {
  const warnings: string[] = [];
  const pages: string[] = [];
  const buf = Buffer.from(params.bytes);
  const maxChars = Math.max(500, params.chunkMaxChars);
  const overlap = Math.max(0, Math.min(params.chunkOverlapChars, Math.max(0, maxChars - 100)));
  try {
    await pdfParse(buf, {
      pagerender: async (pageData: unknown) => {
        const page = pageData as {
          getTextContent: () => Promise<{ items: Array<{ str?: string }> }>;
        };
        const tc = await page.getTextContent();
        const text = (tc.items ?? [])
          .map((it) => (typeof it?.str === "string" ? it.str : ""))
          .filter(Boolean)
          .join(" ")
          .replace(/\s+/g, " ")
          .trim();
        pages.push(text);
        return text;
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    warnings.push(`pdf_parse_failed:${message}`);
    return {
      schema_version: 1,
      docTypeGuess: "pdf",
      languageGuess: undefined,
      segments: [],
      warnings,
      stats: {
        bytes: params.bytes.length,
        chars: 0,
        segments: 0,
        truncated: params.truncated,
      },
    };
  }

  if (pages.every((p) => !p.trim())) {
    warnings.push("empty_pdf_text");
  }

  const segments: ExtractionSegmentV1[] = [];
  const totalPages = pages.length || 1;
  for (let pi = 0; pi < pages.length; pi += 1) {
    const pageText = pages[pi] ?? "";
    if (!pageText.trim()) {
      continue;
    }
    const chunks = splitIntoChunks({ text: pageText, maxChars, overlapChars: overlap });
    for (let ci = 0; ci < chunks.length; ci += 1) {
      const c = chunks[ci];
      if (!c) {
        continue;
      }
      segments.push({
        text: c.text,
        locator: {
          kind: "page",
          index: pi + 1,
          total: totalPages,
          label: `page ${pi + 1}`,
          startChar: c.startChar,
          endChar: c.endChar,
        },
      });
    }
  }

  return {
    schema_version: 1,
    docTypeGuess: "pdf",
    languageGuess: undefined,
    segments,
    warnings,
    stats: {
      bytes: params.bytes.length,
      chars: pages.reduce((sum, p) => sum + p.length, 0),
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

async function extractDocxLike(params: {
  file: RustFsFileMeta;
  bytes: Uint8Array;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): Promise<ExtractionResultV1> {
  const warnings: string[] = [];
  const buf = Buffer.from(params.bytes);
  let value = "";
  try {
    const out = await mammoth.extractRawText({ buffer: buf });
    value = out.value ?? "";
    const msgs = out.messages ?? [];
    for (const m of msgs) {
      if (m?.type && m?.message) {
        warnings.push(`docx:${m.type}:${m.message}`);
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    warnings.push(`docx_extract_failed:${message}`);
    return {
      schema_version: 1,
      docTypeGuess: "docx",
      languageGuess: undefined,
      segments: [],
      warnings,
      stats: {
        bytes: params.bytes.length,
        chars: 0,
        segments: 0,
        truncated: params.truncated,
      },
    };
  }

  const text = value.replace(/\r\n/g, "\n").trim();
  const blocks = text
    .split(/\n{2,}/g)
    .map((b) => b.trim())
    .filter(Boolean);
  const segments: ExtractionSegmentV1[] = [];
  const maxChars = Math.max(500, params.chunkMaxChars);
  const overlap = Math.max(0, Math.min(params.chunkOverlapChars, Math.max(0, maxChars - 100)));
  for (let bi = 0; bi < blocks.length; bi += 1) {
    const block = blocks[bi] ?? "";
    if (!block.trim()) {
      continue;
    }
    const chunks = splitIntoChunks({ text: block, maxChars, overlapChars: overlap });
    for (let ci = 0; ci < chunks.length; ci += 1) {
      const c = chunks[ci];
      if (!c) {
        continue;
      }
      segments.push({
        text: c.text,
        locator: {
          kind: "section",
          index: segments.length + 1,
          total: 0,
          label: `paragraph ${bi + 1}`,
          startChar: c.startChar,
          endChar: c.endChar,
        },
      });
    }
  }
  for (let i = 0; i < segments.length; i += 1) {
    const seg = segments[i];
    if (!seg?.locator) {
      continue;
    }
    seg.locator.index = i + 1;
    seg.locator.total = segments.length;
  }

  if (segments.length === 0 && text) {
    warnings.push("empty_docx_text");
  }

  return {
    schema_version: 1,
    docTypeGuess: "docx",
    languageGuess: undefined,
    segments,
    warnings,
    stats: {
      bytes: params.bytes.length,
      chars: text.length,
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

async function extractPptxLike(params: {
  file: RustFsFileMeta;
  bytes: Uint8Array;
  truncated: boolean;
  chunkMaxChars: number;
  chunkOverlapChars: number;
}): Promise<ExtractionResultV1> {
  const warnings: string[] = [];
  const buf = Buffer.from(params.bytes);
  const zip = await JSZip.loadAsync(buf).catch((err) => {
    const message = err instanceof Error ? err.message : String(err);
    warnings.push(`pptx_zip_failed:${message}`);
    return null;
  });
  if (!zip) {
    return {
      schema_version: 1,
      docTypeGuess: "pptx",
      languageGuess: undefined,
      segments: [],
      warnings,
      stats: {
        bytes: params.bytes.length,
        chars: 0,
        segments: 0,
        truncated: params.truncated,
      },
    };
  }

  const slideNames = Object.keys(zip.files)
    .filter((n) => /^ppt\/slides\/slide\d+\.xml$/.test(n))
    .toSorted((a, b) => {
      const ai = Number(a.match(/slide(\d+)\.xml$/)?.[1] ?? 0);
      const bi = Number(b.match(/slide(\d+)\.xml$/)?.[1] ?? 0);
      return ai - bi;
    });
  const totalSlides = slideNames.length || 1;

  const segments: ExtractionSegmentV1[] = [];
  const maxChars = Math.max(500, params.chunkMaxChars);
  const overlap = Math.max(0, Math.min(params.chunkOverlapChars, Math.max(0, maxChars - 100)));
  for (let si = 0; si < slideNames.length; si += 1) {
    const name = slideNames[si];
    if (!name) {
      continue;
    }
    const xml = await zip
      .file(name)
      ?.async("string")
      .catch(() => null);
    if (!xml) {
      warnings.push(`pptx_slide_read_failed:${name}`);
      continue;
    }
    const runs: string[] = [];
    const re = /<a:t[^>]*>([\s\S]*?)<\/a:t>/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(xml))) {
      const raw = decodeHtmlEntities(m[1] ?? "");
      const t = raw.replace(/\s+/g, " ").trim();
      if (t) {
        runs.push(t);
      }
    }
    const slideText = runs.join(" ").trim();
    if (!slideText) {
      continue;
    }
    const chunks = splitIntoChunks({ text: slideText, maxChars, overlapChars: overlap });
    for (let ci = 0; ci < chunks.length; ci += 1) {
      const c = chunks[ci];
      if (!c) {
        continue;
      }
      segments.push({
        text: c.text,
        locator: {
          kind: "slide",
          index: si + 1,
          total: totalSlides,
          label: `slide ${si + 1}`,
          startChar: c.startChar,
          endChar: c.endChar,
        },
      });
    }
  }

  if (segments.length === 0) {
    warnings.push("empty_pptx_text");
  }

  return {
    schema_version: 1,
    docTypeGuess: "pptx",
    languageGuess: undefined,
    segments,
    warnings,
    stats: {
      bytes: params.bytes.length,
      chars: segments.reduce((sum, s) => sum + s.text.length, 0),
      segments: segments.length,
      truncated: params.truncated,
    },
  };
}

function buildDeepMemoryMessages(params: {
  file: RustFsFileMeta;
  extraction: ExtractionResultV1;
}): unknown[] {
  const header = [
    "FILE_CONTEXT",
    `file_id: ${params.file.file_id}`,
    `tenant_id: ${params.file.tenant_id}`,
    `session_id: ${params.file.session_id ?? ""}`,
    `filename: ${params.file.filename}`,
    `mime: ${params.file.mime ?? ""}`,
    `sha256: ${params.file.sha256}`,
    `size: ${params.file.size}`,
    `created_at_ms: ${params.file.created_at_ms}`,
    `doc_type_guess: ${params.extraction.docTypeGuess}`,
    `language_guess: ${params.extraction.languageGuess ?? ""}`,
    `truncated: ${params.extraction.stats.truncated}`,
  ].join("\n");

  const total = params.extraction.segments.length;
  return params.extraction.segments.map((seg, idx) => {
    const loc = seg.locator;
    const locLines = loc
      ? [
          `segment: ${idx + 1}/${total}`,
          `locator_kind: ${loc.kind}`,
          `locator_index: ${loc.index}/${loc.total}`,
          `locator_label: ${loc.label ?? ""}`,
          `start_char: ${loc.startChar ?? ""}`,
          `end_char: ${loc.endChar ?? ""}`,
        ].join("\n")
      : `segment: ${idx + 1}/${total}`;
    const content = `${header}\n${locLines}\n\nCONTENT_EXCERPT\n${seg.text}`;
    return { role: "user", content };
  });
}

const WorkerStateSchema = z.object({
  tombstoneSinceMs: z.coerce.number().int().nonnegative().default(0),
});

type WorkerState = z.infer<typeof WorkerStateSchema>;

async function loadWorkerState(
  path: string | undefined,
  fallback: WorkerState,
): Promise<WorkerState> {
  const p = path?.trim();
  if (!p) {
    return fallback;
  }
  try {
    const raw = await readFile(p, "utf-8");
    return WorkerStateSchema.parse(JSON.parse(raw));
  } catch {
    return fallback;
  }
}

async function saveWorkerState(path: string | undefined, state: WorkerState): Promise<void> {
  const p = path?.trim();
  if (!p) {
    return;
  }
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, JSON.stringify(state, null, 2), { encoding: "utf-8" });
}

async function main(): Promise<void> {
  const env = EnvSchema.parse(process.env);
  const log = pino({ name: "rustfs-worker" });

  const rustfsBaseUrl = normalizeBaseUrl(env.RUSTFS_BASE_URL);
  const deepBaseUrl = normalizeBaseUrl(env.DEEP_MEMORY_BASE_URL);

  const loadedState = await loadWorkerState(env.WORKER_STATE_PATH, {
    tombstoneSinceMs: env.RUSTFS_TOMBSTONE_SINCE_MS,
  });
  let tombstoneSinceMs = Math.max(env.RUSTFS_TOMBSTONE_SINCE_MS, loadedState.tombstoneSinceMs);
  let lastTombstonePollAt = 0;

  log.info(
    {
      rustfsBaseUrl,
      deepBaseUrl,
      tenantHint: env.RUSTFS_TENANT_ID ?? undefined,
      pollIntervalMs: env.RUSTFS_POLL_INTERVAL_MS,
      leaseMs: env.RUSTFS_LEASE_MS,
      pendingLimit: env.RUSTFS_PENDING_LIMIT,
      maxDownloadBytes: env.RUSTFS_MAX_DOWNLOAD_BYTES,
      tombstonePollIntervalMs: env.RUSTFS_TOMBSTONE_POLL_INTERVAL_MS,
      tombstoneLimit: env.RUSTFS_TOMBSTONE_LIMIT,
      tombstoneSinceMs,
      deepNamespace: env.DEEP_MEMORY_NAMESPACE ?? undefined,
      deepAsync: env.DEEP_MEMORY_ASYNC,
    },
    "worker starting",
  );

  async function pollTombstonesIfDue(): Promise<void> {
    const now = Date.now();
    if (now - lastTombstonePollAt < env.RUSTFS_TOMBSTONE_POLL_INTERVAL_MS) {
      return;
    }
    lastTombstonePollAt = now;
    const tombUrl = new URL(`${rustfsBaseUrl}/v1/files/tombstoned`);
    if (env.RUSTFS_TENANT_ID?.trim()) {
      tombUrl.searchParams.set("tenant_id", env.RUSTFS_TENANT_ID.trim());
    }
    tombUrl.searchParams.set("since_ms", String(tombstoneSinceMs));
    tombUrl.searchParams.set("limit", String(env.RUSTFS_TOMBSTONE_LIMIT));

    const tomb = await fetchJson<{
      ok: boolean;
      items: Array<{ file_id: string; deleted_at_ms: number }>;
    }>({
      url: tombUrl.toString(),
      method: "GET",
      apiKey: env.RUSTFS_API_KEY,
      timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
    });

    const tItems = tomb.items ?? [];
    for (const item of tItems) {
      const sessionId = `rustfs:file:${item.file_id}`;
      let res: unknown;
      try {
        res = await fetchJson<unknown>({
          url: `${deepBaseUrl}/forget`,
          method: "POST",
          apiKey: env.DEEP_MEMORY_API_KEY,
          timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
          body: {
            namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
            session_id: sessionId,
            async: true,
          },
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (isOverloadLikeError(message)) {
          log.warn({ err: message }, "deep-memory overloaded during forget; backing off");
          await sleep(env.DEEP_MEMORY_BACKOFF_MS);
          // Do not advance tombstoneSinceMs so we retry later.
          throw err;
        }
        throw err;
      }
      log.info(
        { file_id: item.file_id, sessionId, deleted_at_ms: item.deleted_at_ms, res },
        "forgot tombstoned file",
      );
      tombstoneSinceMs = Math.max(tombstoneSinceMs, item.deleted_at_ms);
      await saveWorkerState(env.WORKER_STATE_PATH, { tombstoneSinceMs }).catch(() => {});
    }
  }

  let overloadHits = 0;
  let lastEmptyLogAt = 0;

  while (true) {
    try {
      const pending = await fetchJson<{
        ok: boolean;
        items: RustFsFileMeta[];
        claimed_at_ms?: number;
      }>({
        url: `${rustfsBaseUrl}/v1/files/claim_extract`,
        method: "POST",
        apiKey: env.RUSTFS_API_KEY,
        timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
        body: {
          tenant_id: env.RUSTFS_TENANT_ID?.trim() || undefined,
          limit: env.RUSTFS_PENDING_LIMIT,
          lease_ms: env.RUSTFS_LEASE_MS,
        },
      });

      const items = pending.items ?? [];
      try {
        await pollTombstonesIfDue();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log.warn({ err: message }, "tombstone forget poll failed");
      }

      if (items.length === 0) {
        const now = Date.now();
        if (now - lastEmptyLogAt > 60_000) {
          lastEmptyLogAt = now;
          log.debug(
            {
              pollIntervalMs: env.RUSTFS_POLL_INTERVAL_MS,
              tombstoneSinceMs,
              overloadHits,
            },
            "no pending files to extract",
          );
        }
        await sleep(env.RUSTFS_POLL_INTERVAL_MS);
        continue;
      }

      for (const file of items) {
        const fileLog = log.child({ file_id: file.file_id, filename: file.filename });
        try {
          const t0 = Date.now();
          const canText = isSupportedTextLike(file.mime ?? undefined, file.filename);
          const canHtml = isHtmlLike(file.mime ?? undefined, file.filename);
          const canCode = isCodeLike(file.mime ?? undefined, file.filename);
          const canPdf = isPdfLike(file.mime ?? undefined, file.filename);
          const canDocx = isDocxLike(file.mime ?? undefined, file.filename);
          const canPptx = isPptxLike(file.mime ?? undefined, file.filename);
          if (!canText && !canHtml && !canCode && !canPdf && !canDocx && !canPptx) {
            await fetchJson({
              url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
              method: "POST",
              apiKey: env.RUSTFS_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: { status: "skipped", error: `unsupported_mime:${file.mime ?? ""}` },
            });
            fileLog.info({ mime: file.mime ?? undefined }, "skipped unsupported mime");
            continue;
          }

          const tDownload0 = Date.now();
          const bytes = await fetchBytes({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}`,
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            maxBytes: env.RUSTFS_MAX_DOWNLOAD_BYTES,
          });
          const downloadMs = Date.now() - tDownload0;
          const truncated = bytes.length >= env.RUSTFS_MAX_DOWNLOAD_BYTES;
          const text =
            canText || canHtml || canCode
              ? new TextDecoder("utf-8", { fatal: false }).decode(bytes)
              : "";

          const sessionId = `rustfs:file:${file.file_id}`;
          const tExtract0 = Date.now();
          const extraction = canPdf
            ? await extractPdfLike({
                file,
                bytes,
                truncated,
                chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
              })
            : canDocx
              ? await extractDocxLike({
                  file,
                  bytes,
                  truncated,
                  chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                  chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
                })
              : canPptx
                ? await extractPptxLike({
                    file,
                    bytes,
                    truncated,
                    chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                    chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
                  })
                : canHtml
                  ? extractHtmlLike({
                      file,
                      html: text,
                      bytes: bytes.length,
                      truncated,
                      chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                      chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
                    })
                  : canCode
                    ? extractCodeLike({
                        file,
                        code: text,
                        bytes: bytes.length,
                        truncated,
                        chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                        chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
                      })
                    : extractTextLike({
                        file,
                        text,
                        bytes: bytes.length,
                        truncated,
                        chunkMaxChars: env.DEEP_MEMORY_CHUNK_MAX_CHARS,
                        chunkOverlapChars: env.DEEP_MEMORY_CHUNK_OVERLAP_CHARS,
                      });
          const extractMs = Date.now() - tExtract0;
          if (extraction.segments.length === 0) {
            await fetchJson({
              url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
              method: "POST",
              apiKey: env.RUSTFS_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: { status: "skipped", error: "empty_text" },
            });
            fileLog.info({ sessionId }, "skipped empty text");
            continue;
          }
          const tMsg0 = Date.now();
          const messages = buildDeepMemoryMessages({ file, extraction });
          const messageBuildMs = Date.now() - tMsg0;
          let updateRes: unknown;
          const tDeep0 = Date.now();
          try {
            updateRes = await fetchJson<unknown>({
              url: `${deepBaseUrl}/update_memory_index`,
              method: "POST",
              apiKey: env.DEEP_MEMORY_API_KEY,
              timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
              body: {
                namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
                session_id: sessionId,
                messages,
                async: env.DEEP_MEMORY_ASYNC,
              },
            });
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            if (isOverloadLikeError(message)) {
              overloadHits += 1;
              const annotations = buildAnnotationsV1({
                file,
                existingAnnotations: file.annotations,
                deepMemory: {
                  baseUrl: deepBaseUrl,
                  namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
                  sessionId,
                  error: message,
                  overloaded: true,
                },
                extraction,
              });
              await fetchJson({
                url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/annotations`,
                method: "POST",
                apiKey: env.RUSTFS_API_KEY,
                timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
                body: { annotations, source: "rustfs-worker" },
              }).catch(() => {});
              fileLog.warn({ err: message }, "deep-memory overloaded; backing off");
              await sleep(env.DEEP_MEMORY_BACKOFF_MS);
            }
            throw err;
          }
          const deepMemoryMs = Date.now() - tDeep0;

          const tAnno0 = Date.now();
          const annotations = buildAnnotationsV1({
            file,
            existingAnnotations: file.annotations,
            deepMemory: {
              baseUrl: deepBaseUrl,
              namespace: env.DEEP_MEMORY_NAMESPACE?.trim() || undefined,
              sessionId,
              updateResponse: updateRes,
            },
            extraction,
          });

          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/annotations`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { annotations, source: "rustfs-worker" },
          });
          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { status: "indexed" },
          });
          const rustfsWriteMs = Date.now() - tAnno0;
          const totalMs = Date.now() - t0;

          fileLog.info(
            {
              sessionId,
              docType: extraction.docTypeGuess,
              segments: extraction.segments.length,
              bytes: extraction.stats.bytes,
              truncated: extraction.stats.truncated,
              timings: {
                downloadMs,
                extractMs,
                messageBuildMs,
                deepMemoryMs,
                rustfsWriteMs,
                totalMs,
              },
            },
            "indexed",
          );
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          await fetchJson({
            url: `${rustfsBaseUrl}/v1/files/${encodeURIComponent(file.file_id)}/extract_status`,
            method: "POST",
            apiKey: env.RUSTFS_API_KEY,
            timeoutMs: env.DEEP_MEMORY_TIMEOUT_MS,
            body: { status: "failed", error: message },
          }).catch(() => {});
          fileLog.warn({ err: message }, "failed");
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      // Best-effort global loop resilience.
      // eslint-disable-next-line no-console
      console.error(`[rustfs-worker] loop error: ${message}`);
      await sleep(env.RUSTFS_POLL_INTERVAL_MS);
    }
  }
}

await main();
