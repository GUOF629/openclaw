import { QdrantClient } from "@qdrant/js-client-rest";

export type QdrantMemoryPayload = {
  id: string;
  namespace: string;
  content: string;
  session_id: string;
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

  async ensureCollection(): Promise<void> {
    const existing = await this.client.getCollections();
    const has = existing.collections.some((c) => c.name === this.collection);
    if (has) {
      return;
    }
    await this.client.createCollection(this.collection, {
      vectors: { size: this.dims, distance: "Cosine" },
    });
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
}

