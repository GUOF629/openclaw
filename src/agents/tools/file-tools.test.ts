import { describe, expect, test } from "vitest";
import type { OpenClawConfig } from "../../config/config.js";
import { resolveRustFsConfig } from "../rustfs.js";
import { createFileIngestTool } from "./file-tools.js";

describe("resolveRustFsConfig", () => {
  test("returns null when disabled or baseUrl missing", () => {
    const cfg = {
      agents: { defaults: { rustfs: { enabled: false, baseUrl: "http://localhost:8099" } } },
    } as unknown as OpenClawConfig;
    expect(resolveRustFsConfig(cfg, "agent")).toBeNull();

    const cfg2 = {
      agents: { defaults: { rustfs: { enabled: true, baseUrl: "" } } },
    } as unknown as OpenClawConfig;
    expect(resolveRustFsConfig(cfg2, "agent")).toBeNull();
  });

  test("merges defaults + overrides and clamps values", () => {
    const cfg = {
      agents: {
        defaults: {
          rustfs: {
            enabled: true,
            baseUrl: "http://localhost:8099",
            project: "default",
            linkTtlSeconds: 1,
            maxUploadBytes: 999_999_999_999,
          },
        },
        list: [
          {
            id: "a",
            default: true,
            rustfs: { project: "p2", linkTtlSeconds: 99999, maxUploadBytes: 0 },
          },
        ],
      },
    } as unknown as OpenClawConfig;

    const out = resolveRustFsConfig(cfg, "a");
    expect(out).not.toBeNull();
    expect(out?.project).toBe("p2");
    expect(out?.linkTtlSeconds).toBe(3600);
    expect(out?.maxUploadBytes).toBe(1);
  });
});

describe("file_ingest tool schema", () => {
  test("includes semantic hint fields", () => {
    const cfg = {
      agents: {
        defaults: {
          rustfs: {
            enabled: true,
            baseUrl: "http://localhost:8099",
            project: "default",
          },
        },
        list: [{ id: "a", default: true }],
      },
    } as unknown as OpenClawConfig;

    const tool = createFileIngestTool({
      config: cfg,
      agentSessionKey: "session",
      workspaceDir: "/tmp/workspace",
    });
    expect(tool?.name).toBe("file_ingest");

    const props = (tool as unknown as { parameters?: { properties?: Record<string, unknown> } })
      .parameters?.properties;
    expect(props).toBeTruthy();
    expect(props).toHaveProperty("kind");
    expect(props).toHaveProperty("tags");
    expect(props).toHaveProperty("hint");
  });
});

describe("file_search tool schema", () => {
  test("includes semantic retrieval options", async () => {
    const mod = await import("./file-tools.js");
    const cfg = {
      agents: {
        defaults: {
          rustfs: {
            enabled: true,
            baseUrl: "http://localhost:8099",
            project: "default",
          },
        },
        list: [{ id: "a", default: true }],
      },
    } as unknown as OpenClawConfig;

    const tool = mod.createFileSearchTool({ config: cfg, agentSessionKey: "session" });
    expect(tool?.name).toBe("file_search");
    const props = (tool as unknown as { parameters?: { properties?: Record<string, unknown> } })
      .parameters?.properties;
    expect(props).toBeTruthy();
    expect(props).toHaveProperty("includeSemantic");
    expect(props).toHaveProperty("semanticQuery");
    expect(props).toHaveProperty("semanticMaxFiles");
    expect(props).toHaveProperty("semanticMaxMemories");
    expect(props).toHaveProperty("semanticMaxChars");
    expect(props).toHaveProperty("rerank");
    expect(props).toHaveProperty("extractStatus");
  });
});
