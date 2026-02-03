import { QdrantClient } from "@qdrant/js-client-rest";
import type { SchemaCheckResult } from "./schema.js";

export type QdrantMemoryPayload = {
  id: string;
  namespace: string;
  kind?: string;
  memory_key?: string;
  subject?: string;
  expires_at?: string;
  confidence?: number;
  content: string;
  session_id: string;
  source_transcript_hash?: string;
  source_message_count?: number;
  created_at: string;
  updated_at?: string;
  importance: number;
  frequency?: number;
  entities: string[];
  topics: string[];
};

export class QdrantStore {
  private readonly client: QdrantClient;
  private readonly collection: string;
  private readonly dims: number;

  constructor(params: { url: string; apiKey?: string; collection: string; dims: number }) {
    this.client = new QdrantClient({
      url: params.url,
      apiKey: params.apiKey,
    });
    this.collection = params.collection;
    this.dims = params.dims;
  }

  async schemaStatus(params: {
    mode: SchemaCheckResult["mode"];
    expectedVersion: number;
  }): Promise<SchemaCheckResult> {
    const actions: string[] = [];
    const warnings: string[] = [];
    try {
      const existing = await this.client.getCollections();
      const has = existing.collections.some((c) => c.name === this.collection);
      if (!has) {
        if (params.mode === "apply") {
          await this.client.createCollection(this.collection, {
            vectors: { size: this.dims, distance: "Cosine" },
          });
          actions.push(`created collection ${this.collection}`);
          return {
            ok: true,
            mode: params.mode,
            expectedVersion: params.expectedVersion,
            actions,
            warnings,
          };
        }
        warnings.push(`missing collection ${this.collection}`);
        return {
          ok: false,
          mode: params.mode,
          expectedVersion: params.expectedVersion,
          actions,
          warnings,
        };
      }

      // Validate collection vector size matches configured dims.
      const info = await this.client.getCollection(this.collection);
      const vectors = (info as unknown as { config?: { params?: { vectors?: unknown } } }).config
        ?.params?.vectors;
      const size =
        typeof vectors === "object" && vectors && "size" in vectors
          ? (vectors as Record<string, unknown>).size
          : undefined;
      const actualDims = typeof size === "number" ? size : undefined;
      if (actualDims != null && actualDims !== this.dims) {
        warnings.push(
          `collection dims mismatch: expected=${this.dims} actual=${actualDims} (${this.collection})`,
        );
        warnings.push(
          "migration required: create a new collection with the new dims and reindex memories",
        );
        return {
          ok: false,
          mode: params.mode,
          expectedVersion: params.expectedVersion,
          actions,
          warnings,
        };
      }

      return {
        ok: true,
        mode: params.mode,
        expectedVersion: params.expectedVersion,
        actions,
        warnings,
      };
    } catch (err) {
      return {
        ok: false,
        mode: params.mode,
        expectedVersion: params.expectedVersion,
        actions,
        warnings,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  async ensureCollection(): Promise<void> {
    // Back-compat: keep behavior (create if missing).
    await this.schemaStatus({ mode: "apply", expectedVersion: 0 });
  }

  async healthCheck(): Promise<{ ok: true } | { ok: false; error: string }> {
    try {
      await this.client.getCollections();
      return { ok: true };
    } catch (err) {
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }

  async upsertMemory(params: {
    id: string;
    vector: number[];
    payload: QdrantMemoryPayload;
  }): Promise<void> {
    await this.client.upsert(this.collection, {
      wait: true,
      points: [
        {
          id: params.id,
          vector: params.vector,
          payload: params.payload,
        },
      ],
    });
  }

  async getMemory(id: string): Promise<{ id: string; payload?: QdrantMemoryPayload } | null> {
    const res = await this.client.retrieve(this.collection, {
      ids: [id],
      with_payload: true,
      with_vector: false,
    });
    const hit = res[0];
    if (!hit) {
      return null;
    }
    return {
      id: String(hit.id),
      payload: (hit.payload as QdrantMemoryPayload | undefined) ?? undefined,
    };
  }

  async search(params: {
    vector: number[];
    limit: number;
    minScore: number;
    namespace?: string;
  }): Promise<Array<{ id: string; score: number; payload?: QdrantMemoryPayload }>> {
    const res = await this.client.search(this.collection, {
      vector: params.vector,
      limit: params.limit,
      with_payload: true,
      score_threshold: params.minScore,
      ...(params.namespace
        ? {
            filter: {
              must: [
                {
                  key: "namespace",
                  match: { value: params.namespace },
                },
              ],
            },
          }
        : {}),
    });
    return res.map((r) => ({
      id: String(r.id),
      score: r.score ?? 0,
      payload: (r.payload as QdrantMemoryPayload | undefined) ?? undefined,
    }));
  }

  async deleteByIds(params: { ids: string[] }): Promise<number> {
    const ids = params.ids.filter(Boolean);
    if (ids.length === 0) {
      return 0;
    }
    await this.client.delete(this.collection, {
      wait: true,
      points: ids,
    });
    return ids.length;
  }

  async deleteBySession(params: { namespace: string; sessionId: string }): Promise<void> {
    await this.client.delete(this.collection, {
      wait: true,
      filter: {
        must: [
          { key: "namespace", match: { value: params.namespace } },
          { key: "session_id", match: { value: params.sessionId } },
        ],
      },
    });
  }

  async countMemories(params?: { namespace?: string }): Promise<number> {
    const res = await this.client.count(this.collection, {
      exact: true,
      ...(params?.namespace
        ? {
            filter: {
              must: [{ key: "namespace", match: { value: params.namespace } }],
            },
          }
        : {}),
    });
    const cnt = (res as unknown as { count?: unknown }).count;
    const n = typeof cnt === "number" ? cnt : Number(cnt ?? 0);
    return Number.isFinite(n) ? n : 0;
  }
}
