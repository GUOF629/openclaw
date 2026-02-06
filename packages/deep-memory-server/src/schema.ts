export const DEEPMEM_SCHEMA_VERSION = 1;

export type MigrationMode = "off" | "validate" | "apply";

export type SchemaCheckResult = {
  ok: boolean;
  mode: MigrationMode;
  expectedVersion: number;
  currentVersion?: number;
  actions?: string[];
  warnings?: string[];
  error?: string;
};
