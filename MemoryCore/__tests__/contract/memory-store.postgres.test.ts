/**
 * IMemoryStore contract on Postgres, plus the
 * Postgres-only cases: concurrent first use, init retry, dimension mismatch,
 * the reindex --dimension path, keyword-search edge cases, the extension
 * outside `public`, and StorePool wiring. Skipped without TEST_POSTGRES_URL.
 */
import { randomBytes } from "node:crypto";
import { afterAll, expect, it, vi } from "vitest";
import pg from "pg";
import { PostgresMemoryStore } from "../../src/core/store/postgres/memory-store.js";
import { getSharedPgPoolRegistry, PgPoolRegistry } from "../../src/core/store/postgres/pool-registry.js";
import { pgSchemaName } from "../../src/core/store/postgres/schema-name.js";
import { runMemoryStoreContract } from "../../src/core/store/__contract__/memory-store.contract.js";
import type { IMemoryStore } from "../../src/core/store/types.js";
import type { MemoryRecord } from "../../src/core/record/l1-writer.js";
import { buildFtsQuery } from "../../src/core/store/tokenize.js";
import { StorePool } from "../../src/core/store/store-pool.js";
import { createStoreBundle } from "../../src/core/store/factory.js";
import { parseConfig } from "../../src/config.js";
import { describePg, dropSchemasWithPrefix, randomSchemaPrefix, TEST_POSTGRES_URL, withTestClient } from "../helpers/postgres.js";

const DIMS = 8;
const silent = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

describePg("PostgresMemoryStore", () => {
  const prefix = randomSchemaPrefix();
  const registry = new PgPoolRegistry();
  const pool = () => registry.get("store", TEST_POSTGRES_URL!, 10);
  let n = 0;
  const newSchema = () => pgSchemaName("mem", `c${n++}`, prefix);
  const schemaOf = new WeakMap<IMemoryStore, string>();

  async function open(schema: string, dimensions: number, p: pg.Pool = pool()): Promise<PostgresMemoryStore> {
    const store = new PostgresMemoryStore({ pool: p, schema, dimensions, logger: silent });
    await store.init();
    schemaOf.set(store, schema);
    return store;
  }

  afterAll(async () => {
    await registry.endAll();
    await dropSchemasWithPrefix(prefix);
  });

  runMemoryStoreContract({
    backend: "postgres",
    vectorDimensions: DIMS,
    // The isolation filter runs inside the HNSW iterative scan.
    filteredVectorSearchExact: true,
    createStore: (opts) => open(newSchema(), opts?.dimensions ?? DIMS),
    async reopenStore(store, opts) {
      store.close();
      return open(schemaOf.get(store)!, opts.dimensions);
    },
    async disposeStore(store) {
      store.close();
    },
  });

  // ── Postgres-only cases ──

  const l1 = (id: string, content: string): MemoryRecord => {
    const ts = new Date().toISOString();
    return {
      id, content, type: "persona", priority: 50, scene_name: "", source_message_ids: [], metadata: {},
      timestamps: [ts], createdAt: ts, updatedAt: ts, sessionKey: "sk", sessionId: "sid",
    };
  };
  const axis = (dims: number, i: number) => {
    const v = new Float32Array(dims);
    v[i % dims] = 1;
    return v;
  };

  it("two store instances use one new schema at the same time: both succeed, migrations apply once", async () => {
    const schema = newSchema();
    const a = new PostgresMemoryStore({ pool: pool(), schema, dimensions: DIMS, logger: silent });
    const b = new PostgresMemoryStore({ pool: pool(), schema, dimensions: DIMS, logger: silent });
    // Data calls before init() must wait for the schema, not fail.
    const [ra, rb, ok] = await Promise.all([a.countL1(), b.countL0(), b.upsertL1(l1("x", "concurrent"), axis(DIMS, 0))]);
    expect([ra, rb, ok]).toEqual([0, 0, true]);
    expect(a.isDegraded() || b.isDegraded()).toBe(false);
    expect(a.getCapabilities().vectorSearch && b.getCapabilities().vectorSearch).toBe(true);
    const versions = await withTestClient((c) =>
      c.query<{ version: number }>(`SELECT version FROM "${schema}".schema_migrations ORDER BY version`),
    );
    expect(versions.rows.map((r) => r.version)).toEqual([1]);
  });

  it("a failed init is retried by the next data call", async () => {
    const p = pool();
    const spy = vi.spyOn(p, "connect").mockImplementationOnce(() => Promise.reject(new Error("server down")));
    const store = new PostgresMemoryStore({ pool: p, schema: newSchema(), dimensions: DIMS, logger: silent });
    await expect(store.init()).rejects.toThrow(/server down/);
    expect(store.isDegraded()).toBe(true);
    spy.mockRestore();
    expect(await store.upsertL1(l1("r", "retry works"))).toBe(true);
    expect(store.isDegraded()).toBe(false);
    expect(await store.countL1()).toBe(1);
  });

  it("a closed store returns empty results and never ends the shared pool", async () => {
    const store = await open(newSchema(), DIMS);
    await store.upsertL1(l1("c", "closed"));
    store.close();
    expect(await store.countL1()).toBe(0);
    expect(await store.upsertL1(l1("c2", "after close"))).toBe(false);
    expect((await pool().query("SELECT 1 AS one")).rows[0].one).toBe(1);
  });

  it("dimension mismatch keeps the stored vectors and disables vector search; reindex --dimension restores it", async () => {
    const schema = newSchema();
    const first = await open(schema, DIMS);
    await first.upsertL1(l1("d-0", "kept"), axis(DIMS, 0));
    first.close();

    const store = await open(schema, 4);
    expect(store.isDegraded()).toBe(false);
    expect(store.getCapabilities().vectorSearch).toBe(false);
    expect(await store.searchL1Vector(axis(4, 0), 5)).toEqual([]);
    const kept = await withTestClient((c) =>
      c.query<{ n: number }>(`SELECT count(embedding)::int AS n FROM "${schema}".l1_records`),
    );
    expect(kept.rows[0]!.n).toBe(1);

    // Operator path: change the stored dimension, then re-embed.
    await store.changeEmbeddingDimension(4);
    expect(store.getCapabilities().vectorSearch).toBe(true);
    expect(await store.reindexAll(async () => axis(4, 0))).toEqual({ l1Count: 1, l0Count: 0 });
    const hits = await store.searchL1Vector(axis(4, 0), 5);
    expect(hits.map((h) => h.record_id)).toEqual(["d-0"]);
    expect(hits[0]!.score).toBeCloseTo(1, 5);
  });

  it("keyword search: CJK tokens, exact lexemes (no prefix match), operator characters as text", async () => {
    // searchL*Fts never throws (SQLite parity), so a SQL error would only show as a warning.
    const warn = vi.fn();
    const store = new PostgresMemoryStore({ pool: pool(), schema: newSchema(), dimensions: DIMS, logger: { ...silent, warn } });
    await store.init();
    expect(store.isFtsAvailable()).toBe(true);
    await store.upsertL1(l1("zh", "数据库 主库 卡死"));
    await store.upsertL1(l1("en", "the database is slow"));

    const zh = await store.searchL1Fts(buildFtsQuery("主库")!, 10);
    expect(zh.map((h) => h.record_id)).toContain("zh");
    for (const h of zh) {
      expect(h.score).toBeGreaterThan(0);
      expect(h.score).toBeLessThan(1);
    }
    expect((await store.searchL1Fts(buildFtsQuery("data")!, 10)).map((h) => h.record_id)).not.toContain("en");
    expect((await store.searchL1Fts(buildFtsQuery("database")!, 10)).map((h) => h.record_id)).toContain("en");

    // tsquery operators inside tokens are text, never syntax.
    for (const q of [`"a&b" OR "c|d" OR "!x" OR "e:f" OR "it's" OR "(g" OR "h\\\\"`, "a & b | !c (d) 'e' f:*"]) {
      await expect(store.searchL1Fts(q, 10)).resolves.toBeInstanceOf(Array);
      await expect(store.searchL0Fts(q, 10)).resolves.toBeInstanceOf(Array);
    }
    expect(warn).not.toHaveBeenCalled();
  });

  it("vector search works with pgvector installed outside the public schema", async () => {
    const db = `tdai_ext_${randomBytes(4).toString("hex")}`;
    await withTestClient((c) => c.query(`CREATE DATABASE ${db}`));
    const url = new URL(TEST_POSTGRES_URL!);
    url.pathname = `/${db}`;
    const own = new pg.Pool({ connectionString: url.toString(), max: 4 });
    try {
      await own.query("CREATE SCHEMA extensions; CREATE EXTENSION vector SCHEMA extensions");
      const store = await open("tdai_mem_ext", DIMS, own);
      expect(store.getCapabilities().vectorSearch).toBe(true);
      await store.upsertL1(l1("e-0", "ext zero"), axis(DIMS, 0));
      await store.upsertL1(l1("e-1", "ext one"), axis(DIMS, 1));
      expect((await store.searchL1Vector(axis(DIMS, 1), 1))[0]?.record_id).toBe("e-1");
      store.close();
    } finally {
      await own.end();
      await withTestClient((c) => c.query(`DROP DATABASE ${db} WITH (FORCE)`));
    }
  });

  it("createStoreBundle(postgres) without memory.postgres falls back to POSTGRES_* env; config wins over env", async () => {
    vi.stubEnv("POSTGRES_URL", TEST_POSTGRES_URL!);
    vi.stubEnv("POSTGRES_SCHEMA_PREFIX", prefix);
    vi.stubEnv("POSTGRES_POOL_MAX", "3");
    try {
      // Deploy-script shape: storeBackend only, no memory.postgres block.
      const bundle = createStoreBundle(
        parseConfig({ storeBackend: "postgres", embedding: { dimensions: DIMS } }),
        { dataDir: ".", logger: silent },
      );
      expect(bundle.store).toBeInstanceOf(PostgresMemoryStore);
      const schema = pgSchemaName("mem", "default", prefix);
      expect(bundle.storeSnapshot).toMatchObject({ type: "postgres", postgresSchema: schema });
      await bundle.store.init();
      expect(await bundle.store.countL0()).toBe(0);
      bundle.store.close();

      const fromConfig = createStoreBundle(
        parseConfig({
          storeBackend: "postgres",
          postgres: { url: "postgres://cfg:pw@cfg-host:5432/cfgdb", schemaPrefix: "cfgp" },
          embedding: { dimensions: DIMS },
        }),
        { dataDir: ".", logger: silent },
      );
      expect(fromConfig.storeSnapshot).toMatchObject({ postgresSchema: "cfgp_mem_default" });
      expect(String((fromConfig.storeSnapshot as { postgresUrl?: string }).postgresUrl)).toContain("cfg-host");
      fromConfig.store.close();
    } finally {
      await getSharedPgPoolRegistry().endAll();
    }
  });

  it("StorePool.getStore wires the postgres kind to a PostgresMemoryStore on the instance schema", async () => {
    const storePool = new StorePool({
      mode: "postgres",
      memoryCfg: parseConfig({ embedding: { dimensions: DIMS } }),
      logger: silent,
    });
    storePool.setGraceCloseDelay(0);
    const schema = pgSchemaName("mem", "pool_inst", prefix);
    try {
      const { store } = await storePool.getStore("pool_inst", {
        kind: "postgres",
        conn: { url: TEST_POSTGRES_URL!, schema, poolMax: 4 },
      });
      expect(store).toBeInstanceOf(PostgresMemoryStore);
      expect(await store.upsertL0({
        id: "p-0", sessionKey: "sk", sessionId: "sid", role: "user",
        messageText: "pooled", recordedAt: new Date().toISOString(), timestamp: Date.now(),
      })).toBe(true);
      expect(await store.countL0()).toBe(1);
    } finally {
      await storePool.closeAll();
      await getSharedPgPoolRegistry().endAll();
    }
  });
});
