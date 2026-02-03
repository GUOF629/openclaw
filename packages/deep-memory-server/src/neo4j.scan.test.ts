import type { Driver } from "neo4j-driver";
import { describe, expect, it } from "vitest";
import { Neo4jStore } from "./neo4j.js";

function createStoreWithRun(result: unknown[]) {
  const store = new Neo4jStore({ uri: "bolt://x", user: "u", password: "p" });
  const fake: Partial<Driver> = {
    session: () =>
      ({
        run: async () => ({
          records: result.map((row) => ({
            get: (k: string) => (row as Record<string, unknown>)[k],
          })),
        }),
        close: async () => {},
      }) as unknown,
  };
  // Inject fake driver (tests only).
  (store as unknown as { driver: Driver }).driver = fake as Driver;
  return store;
}

describe("Neo4jStore.scanMemories", () => {
  it("parses sessionId from namespaced session node id", async () => {
    const store = createStoreWithRun([
      {
        id: "ns1::mem_x",
        namespace: "ns1",
        content: "hello",
        createdAt: "2026-01-01T00:00:00.000Z",
        kind: "fact",
        memoryKey: "",
        subject: "",
        expiresAt: "",
        confidence: 0.5,
        importance: 0.9,
        frequency: 2,
        sessionNodeId: "ns1::session::sess-123",
        topics: ["t1"],
        entities: [{ name: "e1", type: "person" }],
      },
    ]);

    const out = await store.scanMemories({ namespace: "ns1", limit: 10 });
    expect(out[0]?.sessionId).toBe("sess-123");
    expect(out[0]?.topics).toEqual(["t1"]);
    expect(out[0]?.entities[0]?.name).toBe("e1");
  });
});
