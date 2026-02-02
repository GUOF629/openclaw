import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { DeepMemoryServerConfig } from "./config.js";

export type AuditRequester = {
  ip?: string;
  userAgent?: string;
  keyId?: string;
};

type ForgetAuditEntry = {
  id: string;
  ts: string;
  action: "forget";
  namespace: string;
  dryRun: boolean;
  sessionId?: string;
  memoryIdsCount: number;
  deletedReported?: number;
  requester: AuditRequester;
};

type QueueFailedExportAuditEntry = {
  id: string;
  ts: string;
  action: "queue_failed_export";
  file?: string;
  key?: string;
  limit?: number;
  requester: AuditRequester;
};

type QueueFailedRetryAuditEntry = {
  id: string;
  ts: string;
  action: "queue_failed_retry";
  dryRun: boolean;
  file?: string;
  key?: string;
  limit?: number;
  retried?: number;
  requester: AuditRequester;
};

export type AuditEntry =
  | ForgetAuditEntry
  | QueueFailedExportAuditEntry
  | QueueFailedRetryAuditEntry;

export type AuditEntryInput =
  | Omit<ForgetAuditEntry, "id" | "ts">
  | Omit<QueueFailedExportAuditEntry, "id" | "ts">
  | Omit<QueueFailedRetryAuditEntry, "id" | "ts">;

async function ensureParent(filePath: string) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

export async function appendAuditLog(cfg: DeepMemoryServerConfig, entry: AuditEntryInput) {
  const filePath = cfg.AUDIT_LOG_PATH?.trim();
  if (!filePath) {
    return;
  }
  const line: AuditEntry = {
    id: crypto.randomUUID(),
    ts: new Date().toISOString(),
    ...entry,
  };
  await ensureParent(filePath);
  await fs.appendFile(filePath, `${JSON.stringify(line)}\n`, "utf8");
}
