import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
async function ensureParent(filePath) {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
}
export async function appendAuditLog(cfg, entry) {
    const filePath = cfg.AUDIT_LOG_PATH?.trim();
    if (!filePath) {
        return;
    }
    const line = {
        id: crypto.randomUUID(),
        ts: new Date().toISOString(),
        ...entry,
    };
    await ensureParent(filePath);
    await fs.appendFile(filePath, `${JSON.stringify(line)}\n`, "utf8");
}
//# sourceMappingURL=audit-log.js.map