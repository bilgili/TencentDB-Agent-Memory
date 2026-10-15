/**
 * Backend registry glue and Postgres
 * infrastructure (schema names, settings, redaction, tsquery, error mapping,
 * pool registry, transactions, migrations, extension setup).
 *
 * Pure tests always run; `describePg` blocks need TEST_POSTGRES_URL.
 */
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type pg from "pg";
import { DB_TRAITS, isDbKind, LocalBackendResolver, parseStoreMode } from "../../src/core/backend-selection/index.js";
import { parseConfig } from "../../src/config.js";
import { InstanceConfigProvider, LocalConfigSource } from "../../src/core/instance-config-provider.js";
import { isForeignKeyViolation, isUniqueViolation } from "../../src/core/store/errors.js";
import { compareVersions, ensureVectorExtension, runMigrations } from "../../src/core/store/postgres/migrate.js";
import type { Migration } from "../../src/core/store/postgres/migrate.js";
import { PgPoolRegistry } from "../../src/core/store/postgres/pool-registry.js";
import { pgSchemaName, validateSchemaPrefix } from "../../src/core/store/postgres/schema-name.js";
import { parsePostgresSettings, redactUrl } from "../../src/core/store/postgres/settings.js";
import * as tx from "../../src/core/store/postgres/tx.js";
import { withTx, withWriteTx } from "../../src/core/store/postgres/tx.js";
import { buildPgTsQuery } from "../../src/core/store/tokenize.js";
import { describePg, dropSchemasWithPrefix, randomSchemaPrefix, TEST_POSTGRES_URL } from "../helpers/postgres.js";

const silentLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

describe("backend registry", () => {
  it("DB_TRAITS.pooledSkillStore is true for tcvdb, mongodb, postgres and false for sqlite", () => {
    expect(DB_TRAITS).toEqual({
      sqlite: { pooledSkillStore: false },
      tcvdb: { pooledSkillStore: true },
      mongodb: { pooledSkillStore: true },
      postgres: { pooledSkillStore: true },
    });
  });

  it("isDbKind accepts only the declared kinds", () => {
    for (const k of ["sqlite", "tcvdb", "mongodb", "postgres"]) expect(isDbKind(k)).toBe(true);
    for (const k of ["mysql", "toString", "", undefined, 1]) expect(isDbKind(k)).toBe(false);
  });

  it("parseStoreMode: unset or blank gives the default, a known kind is kept, anything else throws", () => {
    expect(parseStoreMode(undefined, "tcvdb")).toBe("tcvdb");
    expect(parseStoreMode("", "sqlite")).toBe("sqlite");
    expect(parseStoreMode("  ", "sqlite")).toBe("sqlite");
    for (const k of ["sqlite", "tcvdb", "mongodb", "postgres"] as const) expect(parseStoreMode(k, "sqlite")).toBe(k);
    expect(() => parseStoreMode("postgress", "sqlite")).toThrow(/STORE_MODE.*sqlite, tcvdb, mongodb, postgres.*"postgress"/);
    expect(() => parseStoreMode("toString", "sqlite")).toThrow(/STORE_MODE/);
  });

  it("parseConfig throws on an unknown memory.storeBackend and keeps the sqlite default", () => {
    expect(() => parseConfig({ storeBackend: "postgress" })).toThrow(/memory\.storeBackend.*postgres/);
    expect(parseConfig({}).storeBackend).toBe("sqlite");
    expect(parseConfig({ storeBackend: "postgres" }).storeBackend).toBe("postgres");
  });

  it("LocalBackendResolver resolves STORE_MODE=postgres to a PostgresConfig with the instance schema", async () => {
    vi.stubEnv("POSTGRES_URL", "postgres://u:pw@db:5432/tdai");
    vi.stubEnv("POSTGRES_SCHEMA_PREFIX", "acme");
    vi.stubEnv("POSTGRES_POOL_MAX", "7");
    const resolver = new LocalBackendResolver({
      deployMode: "standalone",
      storeMode: "postgres",
      source: new InstanceConfigProvider({ source: new LocalConfigSource(silentLogger), logger: silentLogger }),
    });
    const { db } = await resolver.resolve("inst_1");
    expect(db).toEqual({
      kind: "postgres",
      conn: { url: "postgres://u:pw@db:5432/tdai", schema: "acme_mem_inst_1", poolMax: 7 },
    });
  });

  it("STORE_MODE=postgres without POSTGRES_URL fails and names the variable", async () => {
    vi.stubEnv("POSTGRES_URL", "");
    const resolver = new LocalBackendResolver({
      deployMode: "standalone",
      storeMode: "postgres",
      source: new InstanceConfigProvider({ source: new LocalConfigSource(silentLogger), logger: silentLogger }),
    });
    await expect(resolver.resolve("inst_1")).rejects.toThrow(/POSTGRES_URL/);
  });
});

describe("pgSchemaName / validateSchemaPrefix", () => {
  it("keeps an id that is already valid", () => {
    expect(pgSchemaName("mem", "inst_01", "tdai")).toBe("tdai_mem_inst_01");
    expect(pgSchemaName("meta", "inst_01", "tdai")).toBe("tdai_meta_inst_01");
  });

  it("normalizes a changed id and appends a hash of the original id", () => {
    const name = pgSchemaName("mem", "Inst-01", "tdai");
    expect(name).toMatch(/^tdai_mem_inst_01_[0-9a-f]{10}$/);
    // Same normalized form, different originals → different names.
    expect(pgSchemaName("mem", "inst-01", "tdai")).not.toBe(name);
    expect(pgSchemaName("mem", "inst_01", "tdai")).not.toBe(name);
    expect(pgSchemaName("mem", "实例", "tdai")).toMatch(/^tdai_mem____[0-9a-f]{10}$/);
  });

  it("truncates a long id to 63 bytes and keeps ids distinct", () => {
    const a = pgSchemaName("mem", "a".repeat(100), "tdai");
    const b = pgSchemaName("mem", `${"a".repeat(99)}b`, "tdai");
    expect(Buffer.byteLength(a)).toBeLessThanOrEqual(63);
    expect(Buffer.byteLength(b)).toBeLessThanOrEqual(63);
    expect(a).not.toBe(b);
    const longPrefix = pgSchemaName("meta", "x".repeat(80), "p".repeat(16));
    expect(Buffer.byteLength(longPrefix)).toBeLessThanOrEqual(63);
  });

  it("refuses an empty instance id", () => {
    expect(() => pgSchemaName("mem", "", "tdai")).toThrow(/empty/);
  });

  it("a valid id shaped like a hashed name is hashed, so it never collides with that id's source", () => {
    const hashed = pgSchemaName("mem", "Acme", "tdai");
    expect(hashed).toMatch(/^tdai_mem_acme_[0-9a-f]{10}$/);
    const lookalike = hashed.slice("tdai_mem_".length); // "acme_<10hex>": valid chars, fits
    const other = pgSchemaName("mem", lookalike, "tdai");
    expect(other).not.toBe(hashed);
    expect(other).toMatch(new RegExp(`^tdai_mem_${lookalike}_[0-9a-f]{10}$`));
    // A plain `_` + hex-like tail that is not 10 hex chars still passes through.
    expect(pgSchemaName("mem", "acme_2024", "tdai")).toBe("tdai_mem_acme_2024");
  });

  it("accepts valid prefixes and refuses invalid or reserved ones, naming the setting", () => {
    for (const p of ["tdai", "a1b2", "p".repeat(16)]) expect(validateSchemaPrefix(p)).toBe(p);
    // No "_": the first "_" of a schema name must end the prefix.
    for (const p of ["", "Tdai", "1abc", "a-b", "_x", "a1_b2", "a_mem", "tdai_", "p".repeat(17), "pg_x", "pg"]) {
      expect(() => validateSchemaPrefix(p, "POSTGRES_SCHEMA_PREFIX")).toThrow(/POSTGRES_SCHEMA_PREFIX/);
    }
    // "pgx" does not produce a pg_ schema name.
    expect(validateSchemaPrefix("pgx")).toBe("pgx");
  });
});

describe("PgPoolRegistry owners", () => {
  it("same URL, two owners → two distinct pools, each with its own max and a connect timeout", async () => {
    const registry = new PgPoolRegistry();
    const url = "postgres://u:pw@127.0.0.1:1/none"; // pools connect lazily; nothing dials here
    const store = registry.get("store", url, 10);
    const meta = registry.get("meta", url, 3);
    expect(meta).not.toBe(store);
    expect(registry.size).toBe(2);
    expect(store.options.max).toBe(10);
    expect(meta.options.max).toBe(3);
    expect(store.options.connectionTimeoutMillis).toBe(10_000);
    await registry.endAll();
  });

  it("an idle-client error is logged even when the registry was created without a logger", async () => {
    const registry = new PgPoolRegistry();
    const pool = registry.get("meta", "postgres://u:pw@127.0.0.1:1/none", 1);
    const warn = vi.fn();
    registry.useLogger({ ...silentLogger, warn });
    pool.emit("error", new Error("boom"));
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("boom"));
    expect(warn.mock.calls[0]![0]).not.toContain("pw");
    await registry.endAll();
  });
});

describe("redactUrl / parsePostgresSettings", () => {
  it("redacts the password", () => {
    const out = redactUrl("postgres://u:secret@db/tdai");
    expect(out).toBe("postgres://u:***@db/tdai");
    expect(redactUrl("postgresql://u:secret@db:5432/tdai?password=secret2&sslmode=require")).not.toMatch(/secret/);
    expect(redactUrl("postgres://db/tdai")).toBe("postgres://db/tdai");
    expect(redactUrl("postgres://u:secret@[bad/tdai?password=secret")).not.toMatch(/secret/);
  });

  it("applies defaults", () => {
    expect(parsePostgresSettings({ url: "postgres://u:p@db/tdai" })).toEqual({
      url: "postgres://u:p@db/tdai",
      schemaPrefix: "tdai",
      poolMax: 10,
    });
    expect(parsePostgresSettings({ url: "postgresql://db/x", schemaPrefix: "acme", poolMax: "3" })).toEqual({
      url: "postgresql://db/x",
      schemaPrefix: "acme",
      poolMax: 3,
    });
  });

  it("fails fast with the setting name and without the password", () => {
    expect(() => parsePostgresSettings({})).toThrow(/POSTGRES_URL/);
    let msg = "";
    try {
      parsePostgresSettings({ url: "mysql://u:secret@db/tdai" });
    } catch (e) {
      msg = (e as Error).message;
    }
    expect(msg).toMatch(/POSTGRES_URL/);
    expect(msg).toMatch(/"mysql"/);
    expect(msg).not.toMatch(/secret/);
    expect(() => parsePostgresSettings({ url: "postgres://db/x", schemaPrefix: "pg_bad" })).toThrow(
      /POSTGRES_SCHEMA_PREFIX/,
    );
    expect(() => parsePostgresSettings({ url: "postgres://db/x", schemaPrefix: "a_mem" })).toThrow(
      /POSTGRES_SCHEMA_PREFIX="a_mem"/,
    );
    for (const poolMax of ["0", "-1", "1.5", "abc"]) {
      expect(() => parsePostgresSettings({ url: "postgres://db/x", poolMax })).toThrow(/POSTGRES_POOL_MAX/);
    }
  });
});

describe("buildPgTsQuery", () => {
  it("returns null for no tokens", () => {
    expect(buildPgTsQuery([])).toBeNull();
    expect(buildPgTsQuery([""])).toBeNull();
  });

  it("quotes each token as an exact lexeme, doubles quotes and backslashes, joins with |", () => {
    expect(buildPgTsQuery(["数据库", "主库"])).toBe("'数据库' | '主库'");
    expect(buildPgTsQuery(["a'b", "c\\d"])).toBe("'a''b' | 'c\\\\d'");
    expect(buildPgTsQuery(["foo:*"])).toBe("'foo:*'");
  });
});

describe("isUniqueViolation / isForeignKeyViolation", () => {
  it("classifies SQLite constraint errors", () => {
    const db = new DatabaseSync(":memory:");
    db.exec("PRAGMA foreign_keys = ON");
    db.exec("CREATE TABLE p (id TEXT PRIMARY KEY, name TEXT UNIQUE)");
    db.exec("CREATE TABLE c (pid TEXT REFERENCES p(id))");
    db.exec("INSERT INTO p VALUES ('a', 'x')");
    const errOf = (sql: string): unknown => {
      try {
        db.exec(sql);
      } catch (e) {
        return e;
      }
      throw new Error(`expected failure: ${sql}`);
    };
    const pk = errOf("INSERT INTO p VALUES ('a', 'y')");
    const uq = errOf("INSERT INTO p VALUES ('b', 'x')");
    const fk = errOf("INSERT INTO c VALUES ('zzz')");
    expect(isUniqueViolation(pk)).toBe(true);
    expect(isUniqueViolation(uq)).toBe(true);
    expect(isUniqueViolation(fk)).toBe(false);
    expect(isForeignKeyViolation(fk)).toBe(true);
    expect(isForeignKeyViolation(uq)).toBe(false);
    db.close();
  });

  it("classifies MongoDB E11000 and ignores other errors", () => {
    expect(isUniqueViolation({ code: 11000, keyPattern: { id: 1 } })).toBe(true);
    expect(isUniqueViolation(new Error("boom"))).toBe(false);
    expect(isUniqueViolation(null)).toBe(false);
    expect(isUniqueViolation("UNIQUE constraint failed")).toBe(false);
  });

  it("matches the Postgres constraint name when given", () => {
    const err = { code: "23505", constraint: "uniq_x" };
    expect(isUniqueViolation(err)).toBe(true);
    expect(isUniqueViolation(err, "uniq_x")).toBe(true);
    expect(isUniqueViolation(err, "other")).toBe(false);
    expect(isForeignKeyViolation({ code: "23503", constraint: "fk_a" }, "fk_a")).toBe(true);
    expect(isForeignKeyViolation({ code: "23505" })).toBe(false);
  });
});

// MemoryCore and MemoryKnowledge share one Postgres database and take advisory
// locks keyed by (class, hashtext(schema)). The classes must stay equal across
// the two packages and distinct from each other, or one package's lock blocks
// or fails to exclude the other's.
describe("advisory lock classes", () => {
  it("every MemoryKnowledge class exists in tx.ts with the same value; tx.ts values are distinct", () => {
    const knowledgeSrc = readFileSync(new URL("../../../MemoryKnowledge/src/db/client.ts", import.meta.url), "utf8");
    const knowledge = [...knowledgeSrc.matchAll(/export const (LOCK_CLASS_\w+)\s*=\s*([0-9a-fA-Fx_]+)\s*;/g)].map(
      ([, name, literal]) => [name, Number(literal.replace(/_/g, ""))] as const,
    );
    expect(knowledge.length).toBeGreaterThan(0);

    const core = Object.entries(tx).filter(([name]) => name.startsWith("LOCK_CLASS_")) as Array<[string, number]>;
    const coreByName = new Map(core);
    for (const [name, value] of knowledge) {
      expect(Number.isInteger(value), `${name} parses`).toBe(true);
      expect(coreByName.get(name), name).toBe(value);
    }
    expect(new Set(core.map(([, v]) => v)).size).toBe(core.length);
  });
});

describe("compareVersions", () => {
  it("compares numerically", () => {
    expect(compareVersions("0.8.0", "0.8.0")).toBe(0);
    expect(compareVersions("0.7.4", "0.8.0")).toBeLessThan(0);
    expect(compareVersions("0.10.0", "0.8.0")).toBeGreaterThan(0);
    expect(compareVersions("0.8", "0.8.0")).toBe(0);
  });
});

// ── Postgres ────────────────────────────────────────────────────────────────

const TEST_MIGRATIONS: Migration[] = [
  { version: 1, up: (s) => `CREATE TABLE ${s}.items (id int PRIMARY KEY, body text)` },
  // The sleep keeps the migration lock held, so concurrent callers really overlap.
  { version: 2, up: (s) => `CREATE INDEX items_body_idx ON ${s}.items (body); SELECT pg_sleep(0.3)` },
  { version: 3, up: (s) => `CREATE TYPE ${s}.mood AS ENUM ('a', 'b')` },
];

describePg("postgres infrastructure", () => {
  const prefix = randomSchemaPrefix();
  const registries: PgPoolRegistry[] = [];
  /** A separate registry per simulated process, so pools are not shared. */
  const newPool = (url: string = TEST_POSTGRES_URL!): pg.Pool => {
    const registry = new PgPoolRegistry();
    registries.push(registry);
    return registry.get("store", url, 4);
  };

  let shared: pg.Pool;
  beforeAll(() => {
    shared = newPool();
  });
  afterAll(async () => {
    await dropSchemasWithPrefix(prefix);
    for (const r of registries) await r.endAll();
  });

  async function versionCounts(schema: string): Promise<Record<number, number>> {
    const { rows } = await shared.query<{ version: number; n: number }>(
      `SELECT version, count(*)::int AS n FROM "${schema}".schema_migrations GROUP BY version ORDER BY version`,
    );
    return Object.fromEntries(rows.map((r) => [r.version, r.n]));
  }

  it("PgPoolRegistry keeps one pool per owner and URL and endAll ends it", async () => {
    const registry = new PgPoolRegistry();
    const a = registry.get("store", TEST_POSTGRES_URL!, 2);
    expect(registry.get("store", TEST_POSTGRES_URL!, 5)).toBe(a);
    expect(registry.size).toBe(1);
    expect((await a.query<{ one: number }>("SELECT 1 AS one")).rows[0]!.one).toBe(1);
    await registry.endAll();
    expect(registry.size).toBe(0);
    expect(a.ended).toBe(true);
    const b = registry.get("store", TEST_POSTGRES_URL!, 2);
    expect(b).not.toBe(a);
    await registry.endAll();
  });

  it("buildPgTsQuery output round-trips through to_tsquery('simple', $1)", async () => {
    const tokens = ["数据库", "a'b", "c\\d", "x|y", "!", "&", "foo:*", "<->", "'", "\\"];
    const q = buildPgTsQuery(tokens)!;
    const { rows } = await shared.query<{ q: string; hit: boolean; miss: boolean }>(
      `SELECT to_tsquery('simple', $1)::text AS q,
              to_tsvector('simple', '主库 数据库') @@ to_tsquery('simple', $1) AS hit,
              to_tsvector('simple', 'foobar baz') @@ to_tsquery('simple', $1) AS miss`,
      [q],
    );
    expect(rows[0]!.q).not.toContain(":*");
    expect(rows[0]!.hit).toBe(true);
    // No prefix match: "foo:*" must not turn into a prefix query matching "foo…".
    expect(rows[0]!.miss).toBe(false);
  });

  it("isUniqueViolation recognizes a real 23505 and its constraint name", async () => {
    const schema = `${prefix}_errs`;
    await shared.query(`CREATE SCHEMA "${schema}"`);
    await shared.query(`CREATE TABLE "${schema}".t (id int CONSTRAINT t_pk PRIMARY KEY)`);
    await shared.query(`INSERT INTO "${schema}".t VALUES (1)`);
    const err = await shared.query(`INSERT INTO "${schema}".t VALUES (1)`).catch((e: unknown) => e);
    expect(isUniqueViolation(err)).toBe(true);
    expect(isUniqueViolation(err, "t_pk")).toBe(true);
    expect(isUniqueViolation(err, "other")).toBe(false);
  });

  it("withTx rolls back when fn throws; withWriteTx serializes writers on one schema", async () => {
    const schema = `${prefix}_tx`;
    await shared.query(`CREATE SCHEMA "${schema}"`);
    await shared.query(`CREATE TABLE "${schema}".log (n int)`);
    await expect(
      withTx(shared, async (c) => {
        await c.query(`INSERT INTO "${schema}".log VALUES (1)`);
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect((await shared.query(`SELECT count(*)::int AS n FROM "${schema}".log`)).rows[0].n).toBe(0);

    // Read-then-write under withWriteTx: without the lock both would read max=0 and write 1.
    const bump = () =>
      withWriteTx(shared, schema, async (c) => {
        const { rows } = await c.query<{ m: number }>(`SELECT coalesce(max(n), 0)::int AS m FROM "${schema}".log`);
        await c.query("SELECT pg_sleep(0.1)");
        await c.query(`INSERT INTO "${schema}".log VALUES ($1)`, [rows[0]!.m + 1]);
      });
    await Promise.all([bump(), bump(), bump()]);
    const { rows } = await shared.query<{ n: number }>(`SELECT n FROM "${schema}".log ORDER BY n`);
    expect(rows.map((r) => r.n)).toEqual([1, 2, 3]);
  });

  it("withWriteTx bounds lock waits and statements (SET LOCAL); withTx leaves the defaults", async () => {
    const show = (c: pg.PoolClient) =>
      c.query<{ l: string; s: string }>(
        "SELECT current_setting('lock_timeout') AS l, current_setting('statement_timeout') AS s",
      ).then((r) => r.rows[0]!);
    expect(await withWriteTx(shared, `${prefix}_bounds`, show)).toEqual({ l: "30s", s: "1min" });
    expect(await withTx(shared, show)).toEqual({ l: "0", s: "0" });
  });

  it("two concurrent migration runs on one new schema both succeed; each version recorded once", async () => {
    const schema = pgSchemaName("mem", "concurrent-init", prefix);
    const [a, b] = await Promise.all([
      runMigrations(newPool(), schema, TEST_MIGRATIONS),
      runMigrations(newPool(), schema, TEST_MIGRATIONS),
    ]);
    expect([a, b].sort((x, y) => y.length - x.length)).toEqual([[1, 2, 3], []]);
    expect(await versionCounts(schema)).toEqual({ 1: 1, 2: 1, 3: 1 });
    // A later run applies only new versions.
    const more = [...TEST_MIGRATIONS, { version: 4, up: (s: string) => `ALTER TABLE ${s}.items ADD COLUMN extra text` }];
    expect(await runMigrations(shared, schema, more)).toEqual([4]);
    expect(await runMigrations(shared, schema, more)).toEqual([]);
  });

  it("the same holds when sessions default to REPEATABLE READ", async () => {
    const schema = pgSchemaName("mem", "rr-init", prefix);
    // Every session of these pools starts with REPEATABLE READ as its default.
    const rrUrl = new URL(TEST_POSTGRES_URL!);
    rrUrl.searchParams.set("options", "-c default_transaction_isolation=repeatable\\ read");
    const p1 = newPool(rrUrl.toString());
    const p2 = newPool(rrUrl.toString());
    expect((await p1.query("SHOW default_transaction_isolation")).rows[0]).toEqual({
      default_transaction_isolation: "repeatable read",
    });
    const [a, b] = await Promise.all([
      runMigrations(p1, schema, TEST_MIGRATIONS),
      runMigrations(p2, schema, TEST_MIGRATIONS),
    ]);
    expect([a, b].sort((x, y) => y.length - x.length)).toEqual([[1, 2, 3], []]);
    expect(await versionCounts(schema)).toEqual({ 1: 1, 2: 1, 3: 1 });
  });

  it("runMigrations refuses unordered versions", async () => {
    await expect(
      runMigrations(shared, `${prefix}_bad`, [
        { version: 2, up: () => "SELECT 1" },
        { version: 1, up: () => "SELECT 1" },
      ]),
    ).rejects.toThrow(/ascending/);
  });

  it("ensureVectorExtension reports the installed version and namespace", async () => {
    const [x, y] = await Promise.all([ensureVectorExtension(newPool()), ensureVectorExtension(newPool())]);
    expect(y).toEqual(x);
    const { rows } = await shared.query<{ extversion: string; nspname: string }>(
      `SELECT e.extversion, n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'vector'`,
    );
    expect(x).toEqual({ available: true, version: rows[0]!.extversion, schema: rows[0]!.nspname });
    expect(compareVersions(x.version!, "0.8.0")).toBeGreaterThanOrEqual(0);
  });
});
