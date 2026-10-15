/**
 * Per-instance schema naming.
 *
 * `pgSchemaName` is the only owner of the instance-id → schema rule:
 *   - an id already in `[a-z0-9_]` that fits 63 bytes and does not end in
 *     `_` + 10 hex chars is used as is;
 *   - otherwise the id is lowercased, every char outside `[a-z0-9_]` becomes
 *     `_`, the result is truncated, and `_` + 10 hex chars of sha256(original
 *     id) is appended, so two different ids never share a name.
 * Every hashed name ends in `_<10 hex>` and no pass-through name does, so a
 * pass-through id can never equal another id's hashed name.
 */

import { createHash } from "node:crypto";

export type PgSchemaKind = "mem" | "meta";

export const DEFAULT_SCHEMA_PREFIX = "tdai";

/** Postgres NAMEDATALEN - 1. */
const MAX_IDENT_BYTES = 63;
const HASH_HEX_CHARS = 10;
const PREFIX_RE = /^[a-z_][a-z0-9_]{0,15}$/;
/** The suffix of every hashed name; a pass-through id must not end in it. */
const HASHED_SUFFIX_RE = /_[0-9a-f]{10}$/;

/**
 * Throw unless `prefix` is a usable schema prefix. The error names
 * `settingName`, so a startup failure points at the bad variable.
 *
 * Besides the prefix regex, refuse any prefix that makes the schema name start
 * with `pg_` (reserved by Postgres) — that is a `pg_…` prefix and also the
 * bare prefix `pg`, because names are `${prefix}_${kind}_…`.
 */
export function validateSchemaPrefix(prefix: string, settingName = "POSTGRES_SCHEMA_PREFIX"): string {
  if (!PREFIX_RE.test(prefix)) {
    throw new Error(
      `${settingName}="${prefix}" is invalid: it must match ^[a-z_][a-z0-9_]{0,15}$`,
    );
  }
  if (`${prefix}_`.startsWith("pg_")) {
    throw new Error(`${settingName}="${prefix}" is invalid: schema names starting with "pg_" are reserved`);
  }
  return prefix;
}

export function pgSchemaName(kind: PgSchemaKind, instanceId: string, prefix: string): string {
  validateSchemaPrefix(prefix);
  if (!instanceId) throw new Error("pgSchemaName: instanceId must not be empty");
  const base = `${prefix}_${kind}_`;
  const normalized = instanceId.toLowerCase().replace(/[^a-z0-9_]/g, "_");
  // After normalization the name is ASCII, so length === byte length.
  if (
    normalized === instanceId &&
    base.length + normalized.length <= MAX_IDENT_BYTES &&
    !HASHED_SUFFIX_RE.test(instanceId)
  ) {
    return base + normalized;
  }
  const hash = createHash("sha256").update(instanceId, "utf8").digest("hex").slice(0, HASH_HEX_CHARS);
  const room = MAX_IDENT_BYTES - base.length - 1 - HASH_HEX_CHARS;
  return `${base}${normalized.slice(0, room)}_${hash}`;
}

/** Quote an identifier for SQL (`"a""b"`). Schema names from pgSchemaName never need escaping, but be safe. */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
