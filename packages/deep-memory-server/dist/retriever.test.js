import { describe, expect, it } from "vitest";
import { DeepMemoryRetriever } from "./retriever.js";
describe("DeepMemoryRetriever", () => {
    it("merges semantic + relation and applies decay/boosts", async () => {
        const embedder = {
            embed: async () => [0, 0, 0],
        };
        const qdrant = {
            search: async () => [
                {
                    id: "m1",
                    score: 0.9,
                    payload: {
                        id: "m1",
                        namespace: "default",
                        kind: "fact",
                        memory_key: "fact:topic",
                        subject: "topic",
                        content: "semantic memory",
                        session_id: "s0",
                        created_at: "2020-01-01T00:00:00.000Z",
                        updated_at: "2020-01-01T00:00:00.000Z",
                        importance: 0.9,
                        frequency: 9,
                        entities: [],
                        topics: [],
                    },
                },
            ],
        };
        const neo4j = {
            queryRelatedMemories: async () => [
                {
                    id: "m2",
                    content: "relation memory",
                    importance: 0.9,
                    frequency: 9,
                    lastSeenAt: new Date().toISOString(),
                    kind: "rule",
                    memoryKey: "rule:topic",
                    subject: "topic",
                    relationScore: 1.0,
                },
            ],
        };
        const retriever = new DeepMemoryRetriever({
            embedder: embedder,
            qdrant: qdrant,
            neo4j: neo4j,
            minSemanticScore: 0,
            semanticWeight: 0.6,
            relationWeight: 0.4,
            decayHalfLifeDays: 90,
            importanceBoost: 0.3,
            frequencyBoost: 0.2,
        });
        const out = await retriever.retrieve({
            namespace: "default",
            userInput: "x",
            sessionId: "s1",
            maxMemories: 2,
            entities: ["x"],
            topics: ["x"],
        });
        expect(out.memories.length).toBe(2);
        // m1 has high semantic but is very old => decayed heavily.
        // m2 has strong relation and is fresh => should rank above m1.
        expect(out.memories[0].id).toBe("m2");
        expect(out.memories[1].id).toBe("m1");
    });
});
//# sourceMappingURL=retriever.test.js.map