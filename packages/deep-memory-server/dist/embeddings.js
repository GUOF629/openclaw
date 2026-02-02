import { pipeline } from "@xenova/transformers";
let pipe = null;
export class EmbeddingModel {
    modelId;
    dims;
    constructor(params) {
        this.modelId = params.modelId;
        this.dims = params.dims;
    }
    async ensureLoaded() {
        if (pipe) {
            return;
        }
        pipe = (await pipeline("feature-extraction", this.modelId, {
            quantized: true,
        }));
    }
    /**
     * Returns a normalized embedding vector (cosine-ready).
     * We use mean pooling over token embeddings.
     */
    async embed(text) {
        const cleaned = text.trim();
        if (!cleaned) {
            return Array.from({ length: this.dims }, () => 0);
        }
        await this.ensureLoaded();
        const result = await pipe(cleaned, { pooling: "mean", normalize: true });
        // transformers.js may return a nested typed array; normalize to number[]
        const data = result.data;
        const arr = Array.isArray(data) ? data : data ? Array.from(data) : [];
        if (arr.length !== this.dims) {
            // Best-effort: pad/truncate to expected dims.
            const out = arr.slice(0, this.dims);
            while (out.length < this.dims) {
                out.push(0);
            }
            return out;
        }
        return arr;
    }
}
//# sourceMappingURL=embeddings.js.map