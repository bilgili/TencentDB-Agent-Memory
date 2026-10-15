/**
 * Dialect seam config validation and the SQLite schema drift test.
 * The Postgres twin is knowledge-db.postgres.test.ts.
 */

import { is } from "drizzle-orm";
import { getTableConfig, SQLiteTable } from "drizzle-orm/sqlite-core";
import { describe, expect, it } from "vitest";

import { createKnowledgeDb, migrateKnowledgeDb, redactUrl } from "./client.js";
import * as sqliteSchema from "./schema.sqlite.js";

describe("createKnowledgeDb config validation", () => {
  it("rejects a URL scheme other than postgres:// / postgresql://, naming the scheme", async () => {
    await expect(createKnowledgeDb({ path: ":memory:", url: "mysql://u:secret@h/db" })).rejects.toThrow(
      /scheme "mysql" is not supported/,
    );
    await expect(createKnowledgeDb({ path: ":memory:", url: "/var/data/k.db" })).rejects.toThrow(/scheme "\(none\)"/);
  });

  it.each(["Knowledge", "pg_knowledge", "public", "1abc", "a-b", "x".repeat(64)])(
    "rejects schema name %j",
    async (schema) => {
      await expect(createKnowledgeDb({ path: ":memory:", url: "postgres://h/db", schema })).rejects.toThrow(
        /KNOWLEDGE_DB_SCHEMA/,
      );
    },
  );

  it("rejects a non-positive pool size", async () => {
    await expect(createKnowledgeDb({ path: ":memory:", url: "postgresql://h/db", poolMax: 0 })).rejects.toThrow(
      /KNOWLEDGE_DB_POOL_MAX/,
    );
  });

  it("empty url → SQLite; migrations are versioned and idempotent", async () => {
    const kdb = await createKnowledgeDb({ path: ":memory:", url: "" });
    expect(kdb.dialect).toBe("sqlite");
    expect(await migrateKnowledgeDb(kdb)).toEqual([1, 2, 3, 4, 5]);
    expect(await migrateKnowledgeDb(kdb)).toEqual([]);
    await kdb.close();
  });

  it("redactUrl never logs the password", () => {
    expect(redactUrl("postgres://u:s3cret@h:5432/db")).toBe("postgres://u:***@h:5432/db");
    expect(redactUrl("postgres://h/db?password=s3cret")).not.toContain("s3cret");
  });
});

describe("schema drift (SQLite): Drizzle schema vs migrated DB", () => {
  it("every Drizzle column exists with a compatible type, and every DB column is in Drizzle", async () => {
    const kdb = await createKnowledgeDb({ path: ":memory:" });
    if (kdb.dialect !== "sqlite") throw new Error("unreachable");
    await migrateKnowledgeDb(kdb);
    const tables = (Object.values(sqliteSchema) as unknown[]).filter((v): v is SQLiteTable => is(v, SQLiteTable));
    expect(tables.length).toBe(6);
    for (const table of tables) {
      const cfg = getTableConfig(table);
      const dbCols = kdb.raw.pragma(`table_info(${cfg.name})`) as Array<{
        name: string;
        type: string;
        notnull: number;
        pk: number;
      }>;
      const byName = new Map(dbCols.map((c) => [c.name, c]));
      for (const col of cfg.columns) {
        const dbCol = byName.get(col.name);
        expect(dbCol, `${cfg.name}.${col.name} missing`).toBeDefined();
        expect(dbCol!.type.toLowerCase(), `${cfg.name}.${col.name} type`).toBe(col.getSQLType().toLowerCase());
        // SQLite reports notnull = 0 for a PRIMARY KEY column; Drizzle marks it notNull.
        expect(dbCol!.notnull === 1 || dbCol!.pk > 0, `${cfg.name}.${col.name} nullability`).toBe(col.notNull);
      }
      expect(dbCols.map((c) => c.name).sort(), `${cfg.name} extra DB columns`).toEqual(
        cfg.columns.map((c) => c.name).sort(),
      );
    }
    await kdb.close();
  });
});
