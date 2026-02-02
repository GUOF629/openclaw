import { pipeline } from "@xenova/transformers";

type FeatureExtractor = (
  text: string,
  params: { pooling: "mean"; normalize: boolean },
) => Promise<unknown>;

let pipe: FeatureExtractor | null = null;

export class EmbeddingModel {
  private readonly modelId: string;
  private readonly dims: number;

  constructor(params: { modelId: string; dims: number }) {
    this.modelId = params.modelId;
    this.dims = params.dims;
  }

  async ensureLoaded(): Promise<void> {
    if (pipe) {
      return;
    }
    pipe = await pipeline("feature-extraction", this.modelId, {
      quantized: true,
    });
  }

  /**
   * Returns a normalized embedding vector (cosine-ready).
   * We use mean pooling over token embeddings.
   */
  async embed(text: string): Promise<number[]> {
    const cleaned = text.trim();
    if (!cleaned) {
      return Array.from({ length: this.dims }, () => 0);
    }
    await this.ensureLoaded();
    const result = await pipe!(cleaned, { pooling: "mean", normalize: true });
    // transformers.js may return a nested typed array; normalize to number[]
    const data =
      result && typeof result === "object" && "data" in result
        ? (result as Record<string, unknown>).data
        : undefined;
    const arr = Array.isArray(data) ? data : data instanceof Float32Array ? Array.from(data) : [];
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
