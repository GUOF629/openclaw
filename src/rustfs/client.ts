import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";

export type RustFsFileMeta = {
  file_id: string;
  tenant_id: string;
  session_id?: string;
  filename: string;
  mime?: string;
  size: number;
  sha256: string;
  created_at_ms: number;
  source?: string;
  encrypted: boolean;
};

export type RustFsSearchResponse =
  | { ok: true; items: RustFsFileMeta[] }
  | { ok: false; error: string };

export type RustFsIngestResponse =
  | { ok: true; file_id: string; sha256: string; size: number; encrypted: boolean }
  | { ok: false; error: string };

export type RustFsLinkResponse =
  | { ok: true; token: string; path: string; url?: string; expires_at_ms: number }
  | { ok: false; error: string };

export class RustFsClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly project: string;
  private readonly linkTtlSeconds: number;
  private readonly maxUploadBytes: number;

  constructor(params: {
    baseUrl: string;
    apiKey?: string;
    project: string;
    linkTtlSeconds: number;
    maxUploadBytes: number;
  }) {
    this.baseUrl = params.baseUrl.replace(/\/+$/, "");
    this.apiKey = params.apiKey?.trim() || undefined;
    this.project = params.project.trim();
    this.linkTtlSeconds = params.linkTtlSeconds;
    this.maxUploadBytes = params.maxUploadBytes;
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    return {
      ...(this.apiKey ? { "x-api-key": this.apiKey } : undefined),
      ...extra,
    };
  }

  async search(params: {
    query?: string;
    sessionId?: string;
    mime?: string;
    limit?: number;
  }): Promise<RustFsSearchResponse> {
    const qs = new URLSearchParams();
    qs.set("tenant_id", this.project);
    if (params.query?.trim()) {
      qs.set("q", params.query.trim());
    }
    if (params.sessionId?.trim()) {
      qs.set("session_id", params.sessionId.trim());
    }
    if (params.mime?.trim()) {
      qs.set("mime", params.mime.trim());
    }
    if (typeof params.limit === "number" && Number.isFinite(params.limit)) {
      qs.set("limit", String(Math.max(1, Math.min(200, Math.trunc(params.limit)))));
    }
    const url = `${this.baseUrl}/v1/files?${qs.toString()}`;
    try {
      const res = await fetch(url, { headers: this.headers() });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return {
          ok: false,
          error: `HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`,
        };
      }
      const json = (await res.json()) as { ok?: unknown; items?: unknown };
      const items = Array.isArray(json.items) ? (json.items as RustFsFileMeta[]) : [];
      return { ok: true, items };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }

  async createLink(params: { fileId: string; ttlSeconds?: number }): Promise<RustFsLinkResponse> {
    const fileId = params.fileId.trim();
    if (!fileId) {
      return { ok: false, error: "fileId required" };
    }
    const ttlSeconds =
      typeof params.ttlSeconds === "number" && Number.isFinite(params.ttlSeconds)
        ? Math.max(30, Math.min(3600, Math.trunc(params.ttlSeconds)))
        : this.linkTtlSeconds;
    try {
      const res = await fetch(`${this.baseUrl}/v1/files/${encodeURIComponent(fileId)}/link`, {
        method: "POST",
        headers: this.headers({ "content-type": "application/json" }),
        body: JSON.stringify({ ttl_seconds: ttlSeconds }),
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return {
          ok: false,
          error: `HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`,
        };
      }
      const json = (await res.json()) as Record<string, unknown>;
      if (json.ok !== true) {
        return { ok: false, error: "unexpected response" };
      }
      return json as unknown as RustFsLinkResponse;
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }

  async ingestFile(params: {
    absPath: string;
    sessionId?: string;
    source?: string;
    filename?: string;
    mime?: string;
  }): Promise<RustFsIngestResponse> {
    const absPath = params.absPath.trim();
    if (!absPath) {
      return { ok: false, error: "path required" };
    }
    const stat = await fs.promises.stat(absPath).catch(() => null);
    if (!stat || !stat.isFile()) {
      return { ok: false, error: "file not found" };
    }
    if (stat.size > this.maxUploadBytes) {
      return { ok: false, error: `file too large (${stat.size} bytes)` };
    }
    const filename = (params.filename ?? path.basename(absPath)).trim() || path.basename(absPath);
    const mime = params.mime?.trim() || "application/octet-stream";

    // Stream multipart upload without buffering file in memory.
    const boundary = `----openclaw-rustfs-${crypto.randomUUID()}`;
    const fileStream = fs.createReadStream(absPath);

    const headerField = (name: string, value: string) =>
      Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${value}\r\n`,
        "utf8",
      );
    const fileHeader = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${filename.replaceAll(
        '"',
        "_",
      )}"\r\nContent-Type: ${mime}\r\n\r\n`,
      "utf8",
    );
    const footer = Buffer.from(`\r\n--${boundary}--\r\n`, "utf8");
    const tenantId = this.project;

    const body = Readable.from(
      (async function* () {
        yield headerField("tenant_id", tenantId);
        if (params.sessionId?.trim()) {
          yield headerField("session_id", params.sessionId.trim());
        }
        if (params.source?.trim()) {
          yield headerField("source", params.source.trim());
        }
        yield fileHeader;
        for await (const chunk of fileStream) {
          yield chunk as Buffer;
        }
        yield footer;
      })(),
    );
    const webBody = Readable.toWeb(body) as ReadableStream<Uint8Array>;

    const url = `${this.baseUrl}/v1/files`;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: this.headers({
          "content-type": `multipart/form-data; boundary=${boundary}`,
        }),
        body: webBody,
        duplex: "half",
      } as RequestInit & { duplex: "half" });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return {
          ok: false,
          error: `HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`,
        };
      }
      const json = (await res.json()) as Record<string, unknown>;
      if (json.ok !== true) {
        return { ok: false, error: "unexpected response" };
      }
      return json as unknown as RustFsIngestResponse;
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }
}
