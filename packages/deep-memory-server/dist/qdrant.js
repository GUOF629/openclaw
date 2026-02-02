import { QdrantClient } from "@qdrant/js-client-rest";
export class QdrantStore {
    client;
    collection;
    dims;
    constructor(params) {
        this.client = new QdrantClient({
            url: params.url,
            apiKey: params.apiKey,
        });
        this.collection = params.collection;
        this.dims = params.dims;
    }
    async ensureCollection() {
        const existing = await this.client.getCollections();
        const has = existing.collections.some((c) => c.name === this.collection);
        if (has) {
            return;
        }
        await this.client.createCollection(this.collection, {
            vectors: { size: this.dims, distance: "Cosine" },
        });
    }
    async upsertMemory(params) {
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
    async getMemory(id) {
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
            payload: hit.payload ?? undefined,
        };
    }
    async search(params) {
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
            payload: r.payload ?? undefined,
        }));
    }
    async deleteByIds(params) {
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
    async deleteBySession(params) {
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
}
//# sourceMappingURL=qdrant.js.map