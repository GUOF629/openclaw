import { describe, expect, it } from "vitest";
import type { EmbeddingModel } from "./embeddings.js";
import type { Neo4jStore } from "./neo4j.js";
import type { QdrantStore } from "./qdrant.js";
import { DeepMemoryRetriever } from "./retriever.js";

describe("DeepMemoryRetriever conflict resolution", () => {
  it("dedupes by memory_key and drops expired", async () => {
    const embedder = { embed: async () => [0, 0, 0] };
    const qdrant = {
      search: async () => [
        {
          id: "a",
          score: 0.9,
          payload: {
            id: "a",
            namespace: "default",
            kind: "preference",
            memory_key: "preference:timezone",
            subject: "timezone",
            content: "User prefers timezone UTC+8",
            session_id: "s0",
            created_at: "2020-01-01T00:00:00.000Z",
            updated_at: "2020-01-01T00:00:00.000Z",
            importance: 0.9,
            frequency: 1,
            entities: [],
            topics: [],
          },
        },
        {
          id: "b",
          score: 0.88,
          payload: {
            id: "b",
            namespace: "default",
            kind: "preference",
            memory_key: "preference:timezone",
            subject: "timezone",
            content: "User prefers timezone UTC",
            session_id: "s0",
            created_at: "2021-01-01T00:00:00.000Z",
            updated_at: "2021-01-01T00:00:00.000Z",
            importance: 0.8,
            frequency: 1,
            entities: [],
            topics: [],
          },
        },
        {
          id: "c",
          score: 0.95,
          payload: {
            id: "c",
            namespace: "default",
            kind: "ephemeral",
            memory_key: "ephemeral:test",
            subject: "test",
            expires_at: "2000-01-01T00:00:00.000Z",
            content: "Temporary info (expired)",
            session_id: "s0",
            created_at: "1999-01-01T00:00:00.000Z",
            updated_at: "1999-01-01T00:00:00.000Z",
            importance: 1,
            frequency: 1,
            entities: [],
            topics: [],
          },
        },
      ],
    };
    const neo4j = { queryRelatedMemories: async () => [] };

    const retriever = new DeepMemoryRetriever({
      embedder: embedder as unknown as EmbeddingModel,
      qdrant: qdrant as unknown as QdrantStore,
      neo4j: neo4j as unknown as Neo4jStore,
      minSemanticScore: 0,
      semanticWeight: 1,
      relationWeight: 0,
      decayHalfLifeDays: 90,
      importanceBoost: 0,
      frequencyBoost: 0,
    });

    const out = await retriever.retrieve({
      namespace: "default",
      userInput: "timezone",
      sessionId: "s1",
      maxMemories: 10,
      entities: [],
      topics: [],
    });

    // one of a/b survives due to same memory_key; expired c is dropped.
    expect(out.memories.some((m) => m.id === "c")).toBe(false);
    const tz = out.memories.filter((m) => m.id === "a" || m.id === "b");
    expect(tz.length).toBe(1);
  });
});
