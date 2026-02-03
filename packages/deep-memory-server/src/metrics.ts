import { Registry, collectDefaultMetrics, Counter, Histogram } from "prom-client";

export type DeepMemoryMetrics = ReturnType<typeof createMetrics>;

export function createMetrics() {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry });

  const httpRequestsTotal = new Counter({
    name: "deep_memory_http_requests_total",
    help: "Total HTTP requests handled by deep-memory-server",
    registers: [registry],
    labelNames: ["route", "method", "status"] as const,
  });

  const httpRequestDurationSeconds = new Histogram({
    name: "deep_memory_http_request_duration_seconds",
    help: "HTTP request duration in seconds (deep-memory-server)",
    registers: [registry],
    labelNames: ["route", "method", "status"] as const,
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
  });

  const retrieveReturnedMemoriesTotal = new Counter({
    name: "deep_memory_retrieve_returned_memories_total",
    help: "Total number of memories returned from /retrieve_context",
    registers: [registry],
    labelNames: ["status"] as const,
  });

  const updateMemoriesAddedTotal = new Counter({
    name: "deep_memory_update_memories_added_total",
    help: "Total number of memories added by /update_memory_index (sync path)",
    registers: [registry],
    labelNames: ["status"] as const,
  });

  const updateMemoriesFilteredTotal = new Counter({
    name: "deep_memory_update_memories_filtered_total",
    help: "Total number of memories filtered by /update_memory_index (sync path)",
    registers: [registry],
    labelNames: ["status"] as const,
  });

  const forgetDeletedTotal = new Counter({
    name: "deep_memory_forget_deleted_total",
    help: "Total deleted memories reported by /forget (best-effort)",
    registers: [registry],
    labelNames: ["status"] as const,
  });

  return {
    registry,
    httpRequestsTotal,
    httpRequestDurationSeconds,
    retrieveReturnedMemoriesTotal,
    updateMemoriesAddedTotal,
    updateMemoriesFilteredTotal,
    forgetDeletedTotal,
  };
}
