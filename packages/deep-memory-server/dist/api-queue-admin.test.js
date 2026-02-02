import { describe, expect, it } from "vitest";
import { createApi } from "./api.js";
describe("API queue admin", () => {
    it("requires x-api-key for /queue/failed/export", async () => {
        const app = createApi({
            cfg: {
                PORT: 0,
                HOST: "0.0.0.0",
                API_KEY: "secret",
                REQUIRE_API_KEY: false,
                MAX_BODY_BYTES: 1024,
                MAX_UPDATE_BODY_BYTES: 1024,
            },
            retriever: { retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }) },
            updater: { update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }) },
            qdrant: {},
            neo4j: {},
            queue: {
                stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
                listFailed: async () => [],
                exportFailed: async () => ({ mode: "empty" }),
                retryFailed: async () => ({ status: "not_found" }),
                retryFailedByKey: async () => ({ status: "ok", matched: 0, retried: 0 }),
                enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
                runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
                cancelBySession: async () => 0,
            },
        });
        const res = await app.request("/queue/failed/export?limit=10", { method: "GET" });
        expect(res.status).toBe(401);
    });
    it("allows /queue/failed/retry by key with x-api-key", async () => {
        const app = createApi({
            cfg: {
                PORT: 0,
                HOST: "0.0.0.0",
                API_KEY: "secret",
                REQUIRE_API_KEY: false,
                MAX_BODY_BYTES: 1024,
                MAX_UPDATE_BODY_BYTES: 1024,
            },
            retriever: { retrieve: async () => ({ entities: [], topics: [], memories: [], context: "" }) },
            updater: { update: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }) },
            qdrant: {},
            neo4j: {},
            queue: {
                stats: () => ({ pendingApprox: 0, active: 0, inflightKeys: 0 }),
                listFailed: async () => [],
                exportFailed: async () => ({ mode: "empty" }),
                retryFailed: async () => ({ status: "not_found" }),
                retryFailedByKey: async () => ({ status: "ok", matched: 2, retried: 2 }),
                enqueue: async () => ({ status: "queued", key: "k", transcriptHash: "h" }),
                runNow: async () => ({ status: "processed", memories_added: 0, memories_filtered: 0 }),
                cancelBySession: async () => 0,
            },
        });
        const res = await app.request("/queue/failed/retry", {
            method: "POST",
            headers: { "content-type": "application/json", "x-api-key": "secret" },
            body: JSON.stringify({ key: "default::s1", limit: 50, dry_run: false }),
        });
        expect(res.status).toBe(200);
        const json = await res.json();
        expect(json.mode).toBe("key");
        expect(json.retried).toBe(2);
    });
});
//# sourceMappingURL=api-queue-admin.test.js.map