import fs from "node:fs/promises";
import type { OpenClawConfig } from "../config/config.js";
import type { SessionEntry } from "../config/sessions.js";
import { resolveDeepMemoryConfig } from "../agents/deep-memory.js";
import { updateSessionStoreEntry } from "../config/sessions.js";
import { capArrayByJsonBytes, readSessionMessages } from "../gateway/session-utils.fs.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { DeepMemoryClient } from "./client.js";

const log = createSubsystemLogger("deep-memory");

const UPDATE_PAYLOAD_MAX_BYTES = 2 * 1024 * 1024;

type DeltaState = {
  lastSize: number;
  lastLines: number;
  pendingBytes: number;
  pendingLines: number;
};

const STATE_BY_SESSION_KEY = new Map<string, DeltaState>();
const IN_FLIGHT_BY_SESSION_KEY = new Map<string, Promise<void>>();
const DEBOUNCE_TIMER_BY_SESSION_KEY = new Map<string, NodeJS.Timeout>();

async function countNewlines(absPath: string, start: number, end: number): Promise<number> {
  if (end <= start) {
    return 0;
  }
  const handle = await fs.open(absPath, "r");
  try {
    let offset = start;
    let count = 0;
    const buf = Buffer.alloc(64 * 1024);
    while (offset < end) {
      const toRead = Math.min(buf.length, end - offset);
      const { bytesRead } = await handle.read(buf, 0, toRead, offset);
      if (bytesRead <= 0) {
        break;
      }
      for (let i = 0; i < bytesRead; i += 1) {
        if (buf[i] === 10) {
          count += 1;
        }
      }
      offset += bytesRead;
    }
    return count;
  } finally {
    await handle.close();
  }
}

async function readTranscriptStat(absPath: string): Promise<{ size: number }> {
  try {
    const stat = await fs.stat(absPath);
    return { size: stat.size };
  } catch {
    return { size: 0 };
  }
}

function getOrInitDeltaState(sessionKey: string, entry?: SessionEntry): DeltaState {
  const existing = STATE_BY_SESSION_KEY.get(sessionKey);
  if (existing) {
    return existing;
  }
  const state: DeltaState = {
    lastSize: entry?.deepMemoryTranscriptSize ?? 0,
    lastLines: entry?.deepMemoryTranscriptLines ?? 0,
    pendingBytes: 0,
    pendingLines: 0,
  };
  STATE_BY_SESSION_KEY.set(sessionKey, state);
  return state;
}

async function persistDeltaBaseline(params: {
  storePath?: string;
  sessionKey?: string;
  size: number;
  lines: number;
}) {
  if (!params.storePath || !params.sessionKey) {
    return;
  }
  try {
    await updateSessionStoreEntry({
      storePath: params.storePath,
      sessionKey: params.sessionKey,
      update: async () => ({
        deepMemoryTranscriptSize: params.size,
        deepMemoryTranscriptLines: params.lines,
      }),
    });
  } catch (err) {
    log.debug(`failed to persist deep memory delta baseline: ${String(err)}`);
  }
}

async function persistUpdateMarkers(params: {
  storePath?: string;
  sessionKey?: string;
  now: number;
  nearCompactionCount?: number;
}) {
  if (!params.storePath || !params.sessionKey) {
    return;
  }
  try {
    await updateSessionStoreEntry({
      storePath: params.storePath,
      sessionKey: params.sessionKey,
      update: async () => ({
        deepMemoryUpdatedAt: params.now,
        ...(typeof params.nearCompactionCount === "number"
          ? { deepMemoryNearCompactionCount: params.nearCompactionCount }
          : {}),
      }),
    });
  } catch (err) {
    log.debug(`failed to persist deep memory update markers: ${String(err)}`);
  }
}

async function runUpdateNow(params: {
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
  storePath?: string;
  nearCompactionCount?: number;
}) {
  const deepCfg = resolveDeepMemoryConfig(params.cfg, params.agentId);
  if (!deepCfg || !deepCfg.update.enabled) {
    return;
  }
  const client = new DeepMemoryClient({
    baseUrl: deepCfg.baseUrl,
    timeoutMs: deepCfg.timeoutMs,
    cache: deepCfg.retrieve.cache,
  });
  const messages = readSessionMessages(params.sessionId, params.storePath, params.sessionFile);
  const bounded = capArrayByJsonBytes(messages, UPDATE_PAYLOAD_MAX_BYTES).items;
  await client.updateMemoryIndex({
    sessionId: params.sessionId,
    messages: bounded,
    async: true,
  });
  const now = Date.now();
  await persistUpdateMarkers({
    storePath: params.storePath,
    sessionKey: params.sessionKey,
    now,
    nearCompactionCount: params.nearCompactionCount,
  });
}

function enqueueDebounced(params: {
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
  storePath?: string;
  debounceMs: number;
  nearCompactionCount?: number;
}) {
  const existing = DEBOUNCE_TIMER_BY_SESSION_KEY.get(params.sessionKey);
  if (existing) {
    clearTimeout(existing);
    DEBOUNCE_TIMER_BY_SESSION_KEY.delete(params.sessionKey);
  }
  const timer = setTimeout(
    () => {
      DEBOUNCE_TIMER_BY_SESSION_KEY.delete(params.sessionKey);
      void enqueueImmediate(params).catch((err) => {
        log.warn(`deep memory update failed: ${String(err)}`);
      });
    },
    Math.max(0, params.debounceMs),
  );
  DEBOUNCE_TIMER_BY_SESSION_KEY.set(params.sessionKey, timer);
}

async function enqueueImmediate(params: {
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey: string;
  sessionId: string;
  sessionFile: string;
  storePath?: string;
  debounceMs: number;
  nearCompactionCount?: number;
}) {
  const inflight = IN_FLIGHT_BY_SESSION_KEY.get(params.sessionKey);
  if (inflight) {
    return inflight;
  }
  const task = runUpdateNow({
    cfg: params.cfg,
    agentId: params.agentId,
    sessionKey: params.sessionKey,
    sessionId: params.sessionId,
    sessionFile: params.sessionFile,
    storePath: params.storePath,
    nearCompactionCount: params.nearCompactionCount,
  }).finally(() => {
    IN_FLIGHT_BY_SESSION_KEY.delete(params.sessionKey);
  });
  IN_FLIGHT_BY_SESSION_KEY.set(params.sessionKey, task);
  return task;
}

/**
 * Trigger #2: called after a normal turn completes to batch updates by transcript deltas.
 */
export async function considerDeepMemoryUpdateForTranscriptDelta(params: {
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey?: string;
  sessionId: string;
  sessionFile: string;
  sessionEntry?: SessionEntry;
  storePath?: string;
}): Promise<void> {
  const sessionKey = params.sessionKey?.trim();
  if (!sessionKey) {
    return;
  }
  const deepCfg = resolveDeepMemoryConfig(params.cfg, params.agentId);
  if (!deepCfg || !deepCfg.update.enabled) {
    return;
  }
  const thresholds = deepCfg.update.thresholds;
  const bytesThreshold = thresholds.deltaBytes;
  const linesThreshold = thresholds.deltaMessages;
  if (bytesThreshold <= 0 && linesThreshold <= 0) {
    return;
  }
  const state = getOrInitDeltaState(sessionKey, params.sessionEntry);
  const stat = await readTranscriptStat(params.sessionFile);
  const size = stat.size;

  const deltaBytes = Math.max(0, size - state.lastSize);
  state.pendingBytes += deltaBytes;

  let deltaLines = 0;
  if (linesThreshold > 0 && size > state.lastSize) {
    deltaLines = await countNewlines(params.sessionFile, state.lastSize, size);
    state.pendingLines += deltaLines;
  }

  state.lastSize = size;
  state.lastLines += deltaLines;

  // Persist baseline so restarts continue from the right offset.
  await persistDeltaBaseline({
    storePath: params.storePath,
    sessionKey,
    size: state.lastSize,
    lines: state.lastLines,
  });

  const hitBytes =
    bytesThreshold > 0 ? state.pendingBytes >= bytesThreshold : state.pendingBytes > 0;
  const hitLines =
    linesThreshold > 0 ? state.pendingLines >= linesThreshold : state.pendingLines > 0;
  if (!hitBytes && !hitLines) {
    return;
  }

  // Consume thresholds (best-effort) so we don't spam updates if updates are slow.
  if (bytesThreshold > 0) {
    state.pendingBytes = Math.max(0, state.pendingBytes - bytesThreshold);
  } else {
    state.pendingBytes = 0;
  }
  if (linesThreshold > 0) {
    state.pendingLines = Math.max(0, state.pendingLines - linesThreshold);
  } else {
    state.pendingLines = 0;
  }

  enqueueDebounced({
    cfg: params.cfg,
    agentId: params.agentId,
    sessionKey,
    sessionId: params.sessionId,
    sessionFile: params.sessionFile,
    storePath: params.storePath,
    debounceMs: deepCfg.update.debounceMs,
  });
}

/**
 * Trigger #3: called when a session is nearing auto-compaction.
 * Enqueues an update once per compaction cycle (best-effort).
 */
export function considerDeepMemoryUpdateNearCompaction(params: {
  cfg: OpenClawConfig;
  agentId: string;
  sessionKey?: string;
  sessionId: string;
  sessionFile: string;
  storePath?: string;
  compactionCount?: number;
  deepMemoryNearCompactionCount?: number;
}) {
  const sessionKey = params.sessionKey?.trim();
  if (!sessionKey) {
    return;
  }
  const deepCfg = resolveDeepMemoryConfig(params.cfg, params.agentId);
  if (!deepCfg || !deepCfg.update.enabled || !deepCfg.update.nearCompaction.enabled) {
    return;
  }
  const cycle = params.compactionCount ?? 0;
  const last = params.deepMemoryNearCompactionCount;
  if (typeof last === "number" && last === cycle) {
    return;
  }
  // No debounce here: this is a safety/critical trigger (but still async).
  void enqueueImmediate({
    cfg: params.cfg,
    agentId: params.agentId,
    sessionKey,
    sessionId: params.sessionId,
    sessionFile: params.sessionFile,
    storePath: params.storePath,
    debounceMs: 0,
    nearCompactionCount: cycle,
  }).catch((err) => {
    log.warn(`deep memory near-compaction update failed: ${String(err)}`);
  });
}
