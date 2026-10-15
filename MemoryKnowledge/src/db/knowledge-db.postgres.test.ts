/**
 * Postgres dialect seam: schema lock, fail-stop on lock
 * loss, serialized migrations, and the schema drift test. Skips without TEST_POSTGRES_URL.
 */

import { getTableConfig, type PgTable } from "drizzle-orm/pg-core";
import { describe, expect, it, vi } from "vitest";

import { dropAndClose, openPg, randomSchema, TEST_POSTGRES_URL } from "../store/__contract__/pg-test-db.js";
import { migrateKnowledgeDb, migratePg } from "./client.js";

describe.skipIf(!TEST_POSTGRES_URL)("knowledge DB on Postgres (TEST_POSTGRES_URL)", () => {
  it("a second process on the same schema is refused; the first keeps working", async () => {
    const schema = randomSchema();
    const first = await openPg(schema);
    try {
      await migrateKnowledgeDb(first);
      const err = await openPg(schema).then(
        () => null,
        (e: unknown) => e as Error,
      );
      expect(err?.message).toMatch(/another MemoryKnowledge process holds the Postgres schema/);
      const pw = new URL(TEST_POSTGRES_URL).password;
      if (pw) expect(err?.message).not.toContain(`:${pw}@`); // redacted
      // The first process is unaffected.
      expect((await first.pool.query("SELECT 1 AS ok")).rows[0]).toEqual({ ok: 1 });
    } finally {
      await dropAndClose(first);
    }
    // Released on close: a new process can take the schema.
    const again = await openPg(schema);
    await again.close();
  });

  it("a different schema is not blocked", async () => {
    const a = await openPg(randomSchema());
    const b = await openPg(randomSchema());
    await a.close();
    await b.close();
  });

  it("losing the lock connection calls the fail-stop hook once; close() does not", async () => {
    const onLockLost = vi.fn<(err: Error) => void>();
    const kdb = await openPg(randomSchema(), { onLockLost });
    const { rows } = await kdb.lockClient.query<{ pid: number }>("SELECT pg_backend_pid() AS pid");
    await kdb.pool.query("SELECT pg_terminate_backend($1)", [rows[0]!.pid]);
    await vi.waitFor(() => expect(onLockLost).toHaveBeenCalledTimes(1));
    await kdb.close();
    expect(onLockLost).toHaveBeenCalledTimes(1);

    const quiet = vi.fn<(err: Error) => void>();
    const other = await openPg(randomSchema(), { onLockLost: quiet });
    await other.close();
    expect(quiet).not.toHaveBeenCalled();
  });

  it("concurrent migrations of one new schema run one after the other", async () => {
    const kdb = await openPg(randomSchema());
    try {
      const results = await Promise.all([migratePg(kdb.pool, kdb.schema), migratePg(kdb.pool, kdb.schema)]);
      expect(results.map((r) => r.length).sort()).toEqual([0, 1]);
      expect(await migrateKnowledgeDb(kdb)).toEqual([]);
    } finally {
      await dropAndClose(kdb);
    }
  });

  it("schema drift: every Drizzle column exists with a compatible type, and every DB column is in Drizzle", async () => {
    const kdb = await openPg(randomSchema());
    try {
      await migrateKnowledgeDb(kdb);
      const tables = Object.values(kdb.tables) as PgTable[];
      expect(tables.length).toBe(6);
      for (const table of tables) {
        const cfg = getTableConfig(table);
        expect(cfg.schema).toBe(kdb.schema);
        const { rows } = await kdb.pool.query<{ column_name: string; data_type: string; is_nullable: string }>(
          `SELECT column_name, data_type, is_nullable FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2`,
          [kdb.schema, cfg.name],
        );
        const byName = new Map(rows.map((r) => [r.column_name, r]));
        for (const col of cfg.columns) {
          const dbCol = byName.get(col.name);
          expect(dbCol, `${cfg.name}.${col.name} missing`).toBeDefined();
          expect(dbCol!.data_type, `${cfg.name}.${col.name} type`).toBe(col.getSQLType());
          expect(dbCol!.is_nullable === "NO", `${cfg.name}.${col.name} nullability`).toBe(col.notNull);
        }
        expect(rows.map((r) => r.column_name).sort(), `${cfg.name} extra DB columns`).toEqual(
          cfg.columns.map((c) => c.name).sort(),
        );
      }
    } finally {
      await dropAndClose(kdb);
    }
  });
});
