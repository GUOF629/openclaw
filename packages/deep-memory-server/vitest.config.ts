import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const pkgRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(pkgRoot, "../..");

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    exclude: ["dist/**", "**/node_modules/**"],
    setupFiles: [path.join(repoRoot, "test", "setup.ts")],
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});

