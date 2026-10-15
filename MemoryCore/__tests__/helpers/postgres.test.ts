/**
 * Self-test of the Postgres test helpers, plus the server preconditions the
 * Postgres stores rely on.
 */
import { afterAll, expect, it } from "vitest";
import { describePg, dropSchemasWithPrefix, randomSchemaPrefix, withTestClient } from "./postgres.js";

describePg("postgres test helpers", () => {
  const prefix = randomSchemaPrefix();
  afterAll(async () => {
    await dropSchemasWithPrefix(prefix);
  });

  it("randomSchemaPrefix satisfies the schema-prefix rule", () => {
    expect(prefix).toMatch(/^[a-z_][a-z0-9_]{0,15}$/);
    expect(prefix.startsWith("pg_")).toBe(false);
  });

  it("dropSchemasWithPrefix drops only schemas with the prefix", async () => {
    const other = randomSchemaPrefix();
    await withTestClient(async (c) => {
      await c.query(`CREATE SCHEMA "${prefix}_a"`);
      await c.query(`CREATE TABLE "${prefix}_a".t (id int)`);
      await c.query(`CREATE SCHEMA "${prefix}_b"`);
      await c.query(`CREATE SCHEMA "${other}_keep"`);
    });
    try {
      expect((await dropSchemasWithPrefix(prefix)).sort()).toEqual([`${prefix}_a`, `${prefix}_b`]);
      const left = await withTestClient(async (c) =>
        (await c.query("SELECT nspname FROM pg_namespace WHERE starts_with(nspname, $1) OR starts_with(nspname, $2)",
          [`${prefix}_`, `${other}_`])).rows.map((r) => r.nspname as string),
      );
      expect(left).toEqual([`${other}_keep`]);
    } finally {
      await dropSchemasWithPrefix(other);
    }
  });

  it("dropSchemasWithPrefix refuses an unsafe prefix", async () => {
    await expect(dropSchemasWithPrefix("x; DROP")).rejects.toThrow(/unsafe prefix/);
  });

  it("server provides pgvector >= 0.8.0", async () => {
    const row = await withTestClient(async (c) => {
      await c.query("CREATE EXTENSION IF NOT EXISTS vector");
      return (await c.query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")).rows[0];
    });
    const [major, minor] = String(row?.extversion).split(".").map(Number);
    expect(major > 0 || minor >= 8).toBe(true);
  });

  it("simple text-search config splits pre-segmented CJK text", async () => {
    const n = await withTestClient(async (c) =>
      (await c.query("SELECT length(to_tsvector('simple', '数据库 主库')) AS n")).rows[0].n as number,
    );
    expect(n).toBe(2);
  });
});
