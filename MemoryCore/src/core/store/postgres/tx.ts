/**
 * The only owners of Postgres transactions in MemoryCore.
 *
 * - Every BEGIN states `ISOLATION LEVEL READ COMMITTED`: a role or
 *   database default of REPEATABLE READ would otherwise make a waiter read a
 *   snapshot from before the lock holder committed.
 * - Advisory locks use the two-key form `(class, hashtext(name))` with one
 *   class per purpose, so MemoryCore and MemoryKnowledge locks never
 *   share a key.
 * - No embedding, LLM or other network call may run inside `fn`.
 */

import type pg from "pg";

// Advisory lock classes (first key of the two-key form). int4 values; keep them
// distinct and stable — MemoryKnowledge uses LOCK_CLASS_KNOWLEDGE_SINGLETON with
// the same value.
export const LOCK_CLASS_EXTENSION = 0x7464_0001;
export const LOCK_CLASS_MIGRATE = 0x7464_0002;
export const LOCK_CLASS_WRITE = 0x7464_0003;
export const LOCK_CLASS_KNOWLEDGE_SINGLETON = 0x7464_0004;

/** DDL lock wait limit. */
export const MIGRATION_LOCK_TIMEOUT = "30s";
/** Write-transaction limits (withWriteTx): lock wait, and any one statement. */
export const WRITE_LOCK_TIMEOUT = "30s";
export const WRITE_STATEMENT_TIMEOUT = "60s";

export type TxClient = pg.PoolClient;

/**
 * BEGIN (READ COMMITTED) → fn → COMMIT; ROLLBACK if fn throws. Always releases
 * the client; a client whose ROLLBACK failed is destroyed, not reused.
 */
export async function withTx<T>(pool: pg.Pool, fn: (client: TxClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let broken: Error | undefined;
  try {
    await client.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
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

/**
 * One writer per schema, like SQLite `BEGIN IMMEDIATE` per file. Use it for
 * every former `BEGIN IMMEDIATE` site and every read-a-set-then-write method.
 */
export function withWriteTx<T>(pool: pg.Pool, schema: string, fn: (client: TxClient) => Promise<T>): Promise<T> {
  return withTx(pool, async (client) => {
    // Bounded waits: a stuck lock holder fails writers instead of hanging them.
    await client.query(`SET LOCAL lock_timeout = '${WRITE_LOCK_TIMEOUT}'`);
    await client.query(`SET LOCAL statement_timeout = '${WRITE_STATEMENT_TIMEOUT}'`);
    await client.query("SELECT pg_advisory_xact_lock($1, hashtext($2))", [LOCK_CLASS_WRITE, schema]);
    return fn(client);
  });
}

/**
 * Schema DDL transaction: `lock_timeout`, then the per-schema migrate lock
 * as the first statement that reads data, then `fn`. Concurrent callers for one
 * schema — in one process or many — run one after the other.
 */
export function withMigrationTx<T>(pool: pg.Pool, schema: string, fn: (client: TxClient) => Promise<T>): Promise<T> {
  return withTx(pool, async (client) => {
    await client.query(`SET LOCAL lock_timeout = '${MIGRATION_LOCK_TIMEOUT}'`);
    await client.query("SELECT pg_advisory_xact_lock($1, hashtext($2))", [LOCK_CLASS_MIGRATE, schema]);
    return fn(client);
  });
}
