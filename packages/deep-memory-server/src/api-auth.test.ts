import { describe, expect, it } from "vitest";
import { createApi } from "./api.js";

describe("API auth", () => {
  it("rejects update without x-api-key when API_KEY configured", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEY: "secret",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as any,
      retriever: { retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }) } as any,
      updater: { update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }) } as any,
      qdrant: {} as any,
      neo4j: {} as any,
      queue: {
        enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
        runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
        cancelBySession: async () => 0,
        stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
      } as any,
    });

    const res = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(res.status).toBe(401);
  });

  it("accepts update with x-api-key", async () => {
    const app = createApi({
      cfg: {
        PORT: 0,
        HOST: "0.0.0.0",
        API_KEY: "secret",
        REQUIRE_API_KEY: false,
        MAX_BODY_BYTES: 1024,
        MAX_UPDATE_BODY_BYTES: 1024,
      } as any,
      retriever: { retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }) } as any,
      updater: { update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }) } as any,
      qdrant: {} as any,
      neo4j: {} as any,
      queue: {
        enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
        runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
        cancelBySession: async () => 0,
        stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
      } as any,
    });

    const res = await app.request("/update_memory_index", {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": "secret" },
      body: JSON.stringify({ namespace: "default", session_id: "s1", messages: [], async: true }),
    });
    expect(res.status).toBe(200);
  });
});

