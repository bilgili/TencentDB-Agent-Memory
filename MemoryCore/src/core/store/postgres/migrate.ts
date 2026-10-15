/**
 * Versioned schema migrations and pgvector setup.
 *
 * Each Postgres store owns an ordered `Migration[]`. `runMigrations` applies the
 * missing versions of one schema in a single `withMigrationTx`, and records each
 * in `<schema>.schema_migrations`. `ensureVectorExtension` runs the
 * database-wide extension step and reports what is really installed.
 */

import type pg from "pg";
import { quoteIdent } from "./schema-name.js";
import { LOCK_CLASS_EXTENSION, MIGRATION_LOCK_TIMEOUT, withMigrationTx, withTx } from "./tx.js";

export interface Migration {
  version: number;
  /** SQL for this version; `schema` is the quoted schema identifier (e.g. `"tdai_mem_a"`). */
  up: (schema: string) => string;
}

/** Throw unless versions are positive integers in strictly ascending order. */
function assertOrdered(migrations: readonly Migration[]): void {
  let prev = 0;
  for (const m of migrations) {
    if (!Number.isInteger(m.version) || m.version <= prev) {
      throw new Error(`migrations must have strictly ascending positive versions (got ${m.version} after ${prev})`);
    }
    prev = m.version;
  }
}

/**
 * Create the schema if needed and apply every missing migration. Returns the
 * versions applied by this call (empty when another caller already did it).
 */
export async function runMigrations(
  pool: pg.Pool,
  schema: string,
  migrations: readonly Migration[],
): Promise<number[]> {
  assertOrdered(migrations);
  const s = quoteIdent(schema);
  return withMigrationTx(pool, schema, async (client) => {
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${s}`);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${s}.schema_migrations (
         version int PRIMARY KEY,
         applied_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    const { rows } = await client.query<{ version: number }>(`SELECT version FROM ${s}.schema_migrations`);
    const done = new Set(rows.map((r) => r.version));
    const applied: number[] = [];
    for (const m of migrations) {
      if (done.has(m.version)) continue;
      await client.query(m.up(s));
      await client.query(`INSERT INTO ${s}.schema_migrations (version) VALUES ($1)`, [m.version]);
      applied.push(m.version);
    }
    return applied;
  });
}

export const MIN_PGVECTOR_VERSION = "0.8.0";

export interface VectorExtensionInfo {
  /** true only when pgvector >= 0.8.0 is installed; otherwise run degraded. */
  available: boolean;
  version: string | null;
  /** Schema that holds the extension; qualify `vector` and `<=>` with it. */
  schema: string | null;
  /** Why `available` is false. */
  reason?: string;
}

/** Numeric compare of dotted versions (`0.7.4` < `0.8.0` < `0.10.0`). */
export function compareVersions(a: string, b: string): number {
  const pa = a.split(".").map((p) => parseInt(p, 10) || 0);
  const pb = b.split(".").map((p) => parseInt(p, 10) || 0);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const d = (pa[i] ?? 0) - (pb[i] ?? 0);
    if (d !== 0) return d;
  }
  return 0;
}

/**
 * Extension step, serialized database-wide:
 *   1. `pg_advisory_xact_lock(LOCK_CLASS_EXTENSION, 0)`;
 *   2. `CREATE EXTENSION IF NOT EXISTS vector` (a failure — e.g. no privilege —
 *      is caught in a savepoint, not thrown);
 *   3. re-read `extversion` / `extnamespace`, because IF NOT EXISTS keeps an
 *      older installed version;
 *   4. below 0.8.0 or missing → `available: false` (degraded, no vector search).
 */
export async function ensureVectorExtension(pool: pg.Pool): Promise<VectorExtensionInfo> {
  return withTx(pool, async (client) => {
    await client.query(`SET LOCAL lock_timeout = '${MIGRATION_LOCK_TIMEOUT}'`);
    await client.query("SELECT pg_advisory_xact_lock($1, 0)", [LOCK_CLASS_EXTENSION]);

    let createError: string | undefined;
    await client.query("SAVEPOINT ensure_vector");
    try {
      await client.query("CREATE EXTENSION IF NOT EXISTS vector");
      await client.query("RELEASE SAVEPOINT ensure_vector");
    } catch (err) {
      await client.query("ROLLBACK TO SAVEPOINT ensure_vector");
      createError = err instanceof Error ? err.message : String(err);
    }

    const { rows } = await client.query<{ extversion: string; nspname: string }>(
      `SELECT e.extversion, n.nspname
         FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'vector'`,
    );
    const row = rows[0];
    if (!row) {
      return {
        available: false,
        version: null,
        schema: null,
        reason: `pgvector extension is not installed${createError ? ` (CREATE EXTENSION failed: ${createError})` : ""}`,
      };
    }
    if (compareVersions(row.extversion, MIN_PGVECTOR_VERSION) < 0) {
      return {
        available: false,
        version: row.extversion,
        schema: row.nspname,
        reason: `pgvector ${row.extversion} is older than ${MIN_PGVECTOR_VERSION}`,
      };
    }
    return { available: true, version: row.extversion, schema: row.nspname };
  });
}
