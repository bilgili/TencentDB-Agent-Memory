/**
 * Postgres settings parsing and URL redaction.
 *
 * `parsePostgresSettings` is the one validator for every Postgres setting
 * surface (gateway env `POSTGRES_*`, plugin `memory.postgres.*`, and later the
 * metadata `TDAI_METADATA_POSTGRES_*`): callers pass the setting names so the
 * error names what the operator must fix. `redactUrl` is the only function
 * that may turn a Postgres URL into log or error text.
 */

import { DEFAULT_SCHEMA_PREFIX, validateSchemaPrefix } from "./schema-name.js";

export interface PostgresSettings {
  url: string;
  schemaPrefix: string;
  poolMax: number;
}

export interface RawPostgresSettings {
  url?: string;
  schemaPrefix?: string;
  poolMax?: string | number;
}

export interface PostgresSettingNames {
  url: string;
  schemaPrefix: string;
  poolMax: string;
}

export const GATEWAY_POSTGRES_SETTING_NAMES: PostgresSettingNames = {
  url: "POSTGRES_URL",
  schemaPrefix: "POSTGRES_SCHEMA_PREFIX",
  poolMax: "POSTGRES_POOL_MAX",
};

/** Plugin / TdaiCore path (`memory.postgres.*`). */
export const PLUGIN_POSTGRES_SETTING_NAMES: PostgresSettingNames = {
  url: "memory.postgres.url",
  schemaPrefix: "memory.postgres.schemaPrefix",
  poolMax: "memory.postgres.poolMax",
};

/** Metadata store (`TDAI_METADATA_POSTGRES_*`). */
export const METADATA_POSTGRES_SETTING_NAMES: PostgresSettingNames = {
  url: "TDAI_METADATA_POSTGRES_URL",
  schemaPrefix: "TDAI_METADATA_POSTGRES_SCHEMA_PREFIX",
  poolMax: "TDAI_METADATA_POSTGRES_POOL_MAX",
};

export const DEFAULT_POSTGRES_POOL_MAX = 10;
export const DEFAULT_METADATA_POSTGRES_POOL_MAX = 5;

/** Replace the password (userinfo and `?password=`) with `***`. Never throws. */
export function redactUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = "***";
    if (u.searchParams.has("password")) u.searchParams.set("password", "***");
    return u.toString();
  } catch {
    // Unparseable: still strip anything that looks like `user:pass@` or `password=…`.
    return url
      .replace(/\/\/([^/@:]*):[^@/]*@/, "//$1:***@")
      .replace(/([?&]password=)[^&]*/i, "$1***");
  }
}

export function parsePostgresSettings(
  raw: RawPostgresSettings,
  names: PostgresSettingNames = GATEWAY_POSTGRES_SETTING_NAMES,
  defaultPoolMax: number = DEFAULT_POSTGRES_POOL_MAX,
): PostgresSettings {
  const url = raw.url?.trim() ?? "";
  if (!url) {
    throw new Error(`${names.url} is required for the postgres backend`);
  }
  const scheme = /^([a-z][a-z0-9+.-]*):/i.exec(url)?.[1]?.toLowerCase();
  if (scheme !== "postgres" && scheme !== "postgresql") {
    throw new Error(
      `${names.url} has unsupported scheme "${scheme ?? ""}" (expected postgres:// or postgresql://): ${redactUrl(url)}`,
    );
  }

  const schemaPrefix = validateSchemaPrefix(
    raw.schemaPrefix?.trim() || DEFAULT_SCHEMA_PREFIX,
    names.schemaPrefix,
  );

  let poolMax = defaultPoolMax;
  const rawMax = typeof raw.poolMax === "string" ? raw.poolMax.trim() : raw.poolMax;
  if (rawMax !== undefined && rawMax !== "") {
    poolMax = Number(rawMax);
    if (!Number.isInteger(poolMax) || poolMax < 1) {
      throw new Error(`${names.poolMax}="${raw.poolMax}" is invalid: it must be a positive integer`);
    }
  }

  return { url, schemaPrefix, poolMax };
}
