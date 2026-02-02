import { QdrantClient } from "@qdrant/js-client-rest";

export type QdrantMemoryPayload = {
  id: string;
  content: string;
  session_id: string;
  created_at: string;
  importance: number;
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

  async search(params: {
    vector: number[];
    limit: number;
    minScore: number;
  }): Promise<Array<{ id: string; score: number; payload?: QdrantMemoryPayload }>> {
    const res = await this.client.search(this.collection, {
      vector: params.vector,
      limit: params.limit,
      with_payload: true,
      score_threshold: params.minScore,
    });
    return res.map((r) => ({
      id: String(r.id),
      score: r.score ?? 0,
      payload: (r.payload as QdrantMemoryPayload | undefined) ?? undefined,
    }));
  }
}

