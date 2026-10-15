/**
 * Central knowledge DB: dialect seam.
 *
 * `createKnowledgeDb(config)` opens SQLite (better-sqlite3) or Postgres (pg pool)
 * and returns a discriminated union. Only this file and the dialect stores touch
 * a driver; module.ts picks the stores from `dialect`.
 *
 * Startup order — the caller keeps it:
 *   1. createKnowledgeDb  — open; on Postgres also take the schema lock (step 2)
 *   3. migrateKnowledgeDb — versioned migrations
 *   4/5. recovery + restore (createKnowledgeModule), 6. serve()
 */

import Database from "better-sqlite3";
import { drizzle, type BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzlePg, type NodePgDatabase } from "drizzle-orm/node-postgres";
import pg from "pg";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import { createLogger } from "../logger.js";
import { PG_MIGRATIONS } from "./migrations/pg.js";
import { SQLITE_MIGRATIONS } from "./migrations/sqlite.js";
import { createPgTables, type PgTables } from "./schema.pg.js";
import * as schema from "./schema.sqlite.js";

const log = createLogger("knowledge-db");

export type Db = BetterSQLite3Database<typeof schema>;
export type PgDb = NodePgDatabase;

// Advisory lock classes, first key of the two-key form. The owner is
// MemoryCore/src/core/store/postgres/tx.ts (LOCK_CLASS_*): keep these values
// identical so MemoryCore and MemoryKnowledge locks never share a key.
export const LOCK_CLASS_MIGRATE = 0x7464_0002;
export const LOCK_CLASS_KNOWLEDGE_SINGLETON = 0x7464_0004;
/** DDL lock wait limit. */
const MIGRATION_LOCK_TIMEOUT = "30s";

// ───────────────────────── Config ─────────────────────────

export interface KnowledgeDbConfig {
  /** SQLite file (KNOWLEDGE_DB_PATH); used when `url` is empty. ":memory:" for tests. */
  path: string;
  /** KNOWLEDGE_DB_URL — a postgres:// or postgresql:// URL selects Postgres. */
  url?: string;
  /** KNOWLEDGE_DB_SCHEMA — Postgres schema. Default "knowledge". */
  schema?: string;
  /** KNOWLEDGE_DB_POOL_MAX — Postgres pool size. Default 10. */
  poolMax?: number;
}

const SCHEMA_RE = /^[a-z_][a-z0-9_]{0,62}$/;

/** Throws unless `name` is a safe, non-reserved schema name. */
export function assertKnowledgeSchemaName(name: string): void {
  // "public": Drizzle cannot qualify it (pgSchema rejects it), and every SQL must be schema-qualified.
  if (!SCHEMA_RE.test(name) || name.startsWith("pg_") || name === "public") {
    throw new Error(
      `KNOWLEDGE_DB_SCHEMA "${name}" is invalid: use ^[a-z_][a-z0-9_]{0,62}$, not pg_* and not public`,
    );
  }
}

/** Remove the password from a connection URL before it is logged. */
export function redactUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = "***";
    if (u.searchParams.has("password")) u.searchParams.set("password", "***");
    return u.toString();
  } catch {
    return url.replace(/\/\/([^/@:]*):[^@/]*@/, "//$1:***@").replace(/([?&]password=)[^&]*/i, "$1***");
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

// ───────────────────────── Dialect union ─────────────────────────

export type KnowledgeDb =
  | { dialect: "sqlite"; db: Db; raw: Database.Database; close(): Promise<void> }
  | {
      dialect: "postgres";
      db: PgDb;
      tables: PgTables;
      schema: string;
      pool: pg.Pool;
      /** Dedicated connection outside the pool that holds the schema lock. */
      lockClient: pg.Client;
      close(): Promise<void>;
    };

export interface CreateKnowledgeDbOptions {
  /**
   * Called once when the Postgres lock connection fails or ends while the
   * process runs (the lock is then released). Default: log and exit(1) —
   * fail-stop. Tests inject a spy.
   */
  onLockLost?: (err: Error) => void;
}

/**
 * Open the central DB. On Postgres this also takes the schema lock
 * and rejects when another process holds it. Does not run migrations.
 */
export async function createKnowledgeDb(
  config: KnowledgeDbConfig,
  opts: CreateKnowledgeDbOptions = {},
): Promise<KnowledgeDb> {
  const url = config.url?.trim();
  if (!url) {
    const { db, raw } = createDb({ path: config.path, autoMigrate: false });
    return { dialect: "sqlite", db, raw, close: async () => void raw.close() };
  }

  const scheme = /^([a-z][a-z0-9+.-]*):/i.exec(url)?.[1]?.toLowerCase();
  if (scheme !== "postgres" && scheme !== "postgresql") {
    throw new Error(
      `KNOWLEDGE_DB_URL scheme "${scheme ?? "(none)"}" is not supported: use postgres:// or postgresql://`,
    );
  }
  const schemaName = config.schema ?? "knowledge";
  assertKnowledgeSchemaName(schemaName);
  const poolMax = config.poolMax ?? 10;
  if (!Number.isInteger(poolMax) || poolMax < 1) {
    throw new Error(`KNOWLEDGE_DB_POOL_MAX must be a positive integer (got ${poolMax})`);
  }
  const onLockLost =
    opts.onLockLost ??
    ((err: Error) => {
      log.error(`lost the Postgres schema lock for "${schemaName}"; stopping (fail-stop)`, { error: err.message });
      process.exit(1);
    });

  const pool = new pg.Pool({ connectionString: url, max: poolMax });
  // An idle pooled client can fail (server restart); without a listener that kills the process.
  pool.on("error", (err) => log.warn("idle Postgres pool client error", { error: err.message }));

  // step 2: session lock on a dedicated client, never a pooled one (the pool
  // closes idle clients, which would release the lock).
  const lockClient = new pg.Client({ connectionString: url });
  let armed = false;
  let closing = false;
  const lost = (err?: Error) => {
    if (!armed || closing) return;
    armed = false;
    onLockLost(err ?? new Error("lock connection ended"));
  };
  lockClient.on("error", lost);
  lockClient.on("end", () => lost());

  try {
    await lockClient.connect();
    const { rows } = await lockClient.query<{ locked: boolean }>(
      "SELECT pg_try_advisory_lock($1, hashtext($2)) AS locked",
      [LOCK_CLASS_KNOWLEDGE_SINGLETON, schemaName],
    );
    if (!rows[0]?.locked) {
      throw new Error(
        `another MemoryKnowledge process holds the Postgres schema "${schemaName}" ` +
          `(${redactUrl(url)}); only one process per schema is supported`,
      );
    }
  } catch (err) {
    await lockClient.end().catch(() => undefined);
    await pool.end().catch(() => undefined);
    throw err;
  }
  armed = true;

  return {
    dialect: "postgres",
    db: drizzlePg({ client: pool }),
    tables: createPgTables(schemaName),
    schema: schemaName,
    pool,
    lockClient,
    async close() {
      closing = true;
      await lockClient.end().catch(() => undefined); // ends the session → releases the lock
      await pool.end();
    },
  };
}

/** Startup step 3: apply the missing versioned migrations of the DB's dialect. */
export async function migrateKnowledgeDb(kdb: KnowledgeDb): Promise<number[]> {
  return kdb.dialect === "sqlite" ? migrateSqlite(kdb.raw) : migratePg(kdb.pool, kdb.schema);
}

// ───────────────────────── SQLite ─────────────────────────

export interface CreateDbOptions {
  /** Path to SQLite file. Use ":memory:" for in-memory DB. */
  path: string;
  /** Whether to run the migrations on open. Default true. */
  autoMigrate?: boolean;
}

/**
 * Create a Drizzle-wrapped better-sqlite3 database.
 * Sets WAL mode + busy_timeout for production safety.
 */
export function createDb(opts: CreateDbOptions): { db: Db; raw: Database.Database } {
  if (opts.path !== ":memory:") {
    mkdirSync(dirname(opts.path), { recursive: true });
  }

  const raw = new Database(opts.path);
  raw.pragma("journal_mode = WAL");
  raw.pragma("busy_timeout = 5000");

  const db = drizzle(raw, { schema });

  if (opts.autoMigrate !== false) {
    migrateSqlite(raw);
  }

  return { db, raw };
}

/**
 * Apply the missing SQLite versions in one IMMEDIATE transaction and record
 * them in `schema_migrations`. Returns the versions applied by this call.
 */
export function migrateSqlite(raw: Database.Database): number[] {
  return raw
    .transaction((): number[] => {
      raw.exec(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
      );
      const done = new Set(raw.prepare("SELECT version FROM schema_migrations").pluck().all() as number[]);
      const insert = raw.prepare("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)");
      const applied: number[] = [];
      for (const m of SQLITE_MIGRATIONS) {
        if (done.has(m.version)) continue;
        m.up(raw);
        insert.run(m.version, new Date().toISOString());
        applied.push(m.version);
      }
      return applied;
    })
    .immediate();
}

// ───────────────────────── Postgres ─────────────────────────

/**
 * one transaction — READ COMMITTED, lock_timeout, the per-schema
 * migrate lock as the first statement that reads data, CREATE SCHEMA IF NOT
 * EXISTS, then only the missing versions. Concurrent callers run one after the other.
 */
export async function migratePg(pool: pg.Pool, schemaName: string): Promise<number[]> {
  const s = quoteIdent(schemaName);
  const client = await pool.connect();
  let broken: Error | undefined;
  try {
    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    await client.query(`SET LOCAL lock_timeout = '${MIGRATION_LOCK_TIMEOUT}'`);
    await client.query("SELECT pg_advisory_xact_lock($1, hashtext($2))", [LOCK_CLASS_MIGRATE, schemaName]);
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
    for (const m of PG_MIGRATIONS) {
      if (done.has(m.version)) continue;
      await client.query(m.up(s));
      await client.query(`INSERT INTO ${s}.schema_migrations (version) VALUES ($1)`, [m.version]);
      applied.push(m.version);
    }
    await client.query("COMMIT");
    return applied;
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackErr) {
      broken = rollbackErr instanceof Error ? rollbackErr : new Error(String(rollbackErr));
    }
    throw err;
  } finally {
    client.release(broken);
  }
}
