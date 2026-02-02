import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { DeepMemoryServerConfig } from "./config.js";

export type ForgetAuditEntry = {
  id: string;
  ts: string;
  action: "forget";
  namespace: string;
  dryRun: boolean;
  sessionId?: string;
  memoryIdsCount: number;
  deletedReported?: number;
  requester: {
    ip?: string;
    userAgent?: string;
  };
};

async function ensureParent(filePath: string) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

export async function appendAuditLog(cfg: DeepMemoryServerConfig, entry: Omit<ForgetAuditEntry, "id" | "ts">) {
  const filePath = cfg.AUDIT_LOG_PATH?.trim();
  if (!filePath) {
    return;
  }
  const line: ForgetAuditEntry = {
    id: crypto.randomUUID(),
    ts: new Date().toISOString(),
    ...entry,
  };
  await ensureParent(filePath);
  await fs.appendFile(filePath, `${JSON.stringify(line)}\n`, "utf8");
}

