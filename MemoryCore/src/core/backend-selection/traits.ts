/**
 * Per-kind behavior table. Holds only traits
 * that have a reader; add a column when a new reader appears, never a
 * `kind === "..."` comparison at the call site.
 */

import type { DbKind } from "./types.js";

export interface DbTraits {
  /** Skill store is per-instance and pooled by StorePool (not TdaiCore's SQLite singleton). */
  pooledSkillStore: boolean;
}

export const DB_TRAITS: Record<DbKind, DbTraits> = {
  sqlite: { pooledSkillStore: false },
  tcvdb: { pooledSkillStore: true },
  mongodb: { pooledSkillStore: true },
  postgres: { pooledSkillStore: true },
};

export function isDbKind(value: unknown): value is DbKind {
  return typeof value === "string" && Object.hasOwn(DB_TRAITS, value);
}

/**
 * Parse a configured store kind. Unset or blank gives `defaultKind`; a known
 * kind is returned; anything else throws, naming `setting` and the valid kinds,
 * so a typo stops startup instead of silently selecting another backend.
 */
export function parseStoreMode(value: string | undefined, defaultKind: DbKind, setting = "STORE_MODE"): DbKind {
  const v = value?.trim();
  if (!v) return defaultKind;
  if (isDbKind(v)) return v;
  throw new Error(`${setting} must be one of ${Object.keys(DB_TRAITS).join(", ")}; got "${v}"`);
}
