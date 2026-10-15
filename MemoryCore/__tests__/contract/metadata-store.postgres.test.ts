/**
 * IMetadataStore contract on Postgres, plus the
 * Postgres-only cases: the overlapping upstream-group race (agents must never
 * share a group), the init gate, and MetadataStorePool wiring + /v3 purge.
 * Skipped without TEST_POSTGRES_URL.
 */
import { afterAll, describe, expect, it } from "vitest";
import { PostgresMetadataStore } from "../../src/metadata/store/postgres-adapter.js";
import {
  MetadataStartupValidationError,
  MetadataStorePool,
  loadStoreConfig,
  validateMetadataStartupConfig,
} from "../../src/metadata/store/factory.js";
import { runMetadataStoreContract } from "../../src/metadata/store/metadata-store.contract.js";
import { InstanceUpstreamWriteConflictError } from "../../src/metadata/types.js";
import { getSharedPgPoolRegistry, PgPoolRegistry } from "../../src/core/store/postgres/pool-registry.js";
import { pgSchemaName } from "../../src/core/store/postgres/schema-name.js";
import { describePg, dropSchemasWithPrefix, randomSchemaPrefix, TEST_POSTGRES_URL, withTestClient } from "../helpers/postgres.js";

const silent = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

async function schemaExists(schema: string): Promise<boolean> {
  return withTestClient(async (c) => (await c.query("SELECT 1 FROM pg_namespace WHERE nspname = $1", [schema])).rowCount === 1);
}

describe("metadata store config: postgres (no database needed)", () => {
  const URL_WITH_SECRET = "postgres://u:s3cret@db:5432/app";

  it("TDAI_METADATA_POSTGRES_URL selects postgres with defaults tdai / 5", () => {
    expect(loadStoreConfig({ TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET })).toMatchObject({
      backend: "postgres",
      postgres: { url: URL_WITH_SECRET, schemaPrefix: "tdai", poolMax: 5 },
    });
    expect(
      loadStoreConfig({
        TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET,
        TDAI_METADATA_POSTGRES_SCHEMA_PREFIX: "acme",
        TDAI_METADATA_POSTGRES_POOL_MAX: "3",
      }).postgres,
    ).toEqual({ url: URL_WITH_SECRET, schemaPrefix: "acme", poolMax: 3 });
  });

  it("both metadata URLs set → startup error naming both variables", () => {
    const env = { TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET, TDAI_METADATA_MONGO_URI: "mongodb://m:27017" };
    expect(() => loadStoreConfig(env)).toThrow(MetadataStartupValidationError);
    expect(() => loadStoreConfig(env)).toThrow(/TDAI_METADATA_MONGO_URI.*TDAI_METADATA_POSTGRES_URL/);
    expect(() =>
      loadStoreConfig({ TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET, TDAI_METADATA_SQLITE_BASE_DIR: "/data" }),
    ).toThrow(/TDAI_METADATA_POSTGRES_URL.*TDAI_METADATA_SQLITE_BASE_DIR/);
  });

  it("invalid settings fail fast, name the variable and never print the password", () => {
    const bad = (env: Record<string, string>) => {
      try {
        loadStoreConfig(env);
      } catch (err) {
        return (err as Error).message;
      }
      throw new Error("expected loadStoreConfig to throw");
    };
    const scheme = bad({ TDAI_METADATA_POSTGRES_URL: "mysql://u:s3cret@db/app" });
    expect(scheme).toMatch(/TDAI_METADATA_POSTGRES_URL/);
    expect(scheme).not.toContain("s3cret");
    expect(bad({ TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET, TDAI_METADATA_POSTGRES_SCHEMA_PREFIX: "pg" })).toMatch(
      /TDAI_METADATA_POSTGRES_SCHEMA_PREFIX/,
    );
    expect(bad({ TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET, TDAI_METADATA_POSTGRES_POOL_MAX: "0" })).toMatch(
      /TDAI_METADATA_POSTGRES_POOL_MAX/,
    );
  });

  it("deployMode=service accepts the postgres URL and still requires one service URL", () => {
    expect(validateMetadataStartupConfig("service", { TDAI_METADATA_POSTGRES_URL: URL_WITH_SECRET }).backend).toBe("postgres");
    expect(() => validateMetadataStartupConfig("service", {})).toThrow(/TDAI_METADATA_POSTGRES_URL/);
  });
});

describePg("PostgresMetadataStore", () => {
  const prefix = randomSchemaPrefix();
  const registry = new PgPoolRegistry();
  const pool = () => registry.get(TEST_POSTGRES_URL!, 10);
  let n = 0;
  const open = () => new PostgresMetadataStore({ pool: pool(), schema: pgSchemaName("meta", `c${n++}`, prefix), logger: silent });

  afterAll(async () => {
    await registry.endAll();
    await dropSchemasWithPrefix(prefix);
  });

  // The contract calls store.init() itself after makeStore().
  runMetadataStoreContract("postgres", async () => open(), async (store) => store.close());

  it("concurrent overlapping createInstanceUpstreamGroup: exactly one succeeds, no agent in two groups", async () => {
    for (let round = 0; round < 5; round++) {
      const store = open();
      await store.init();
      const results = await Promise.allSettled(
        [["a", "shared"], ["shared", "b"]].map((agents, i) =>
          store.createInstanceUpstreamGroup({ group_type: "custom", name: `g${i}`, agents, mode: "official" }),
        ),
      );
      const ok = results.filter((r) => r.status === "fulfilled");
      const failed = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
      expect(ok).toHaveLength(1);
      expect(failed).toHaveLength(1);
      expect(failed[0]!.reason).toBeInstanceOf(InstanceUpstreamWriteConflictError);
      expect((failed[0]!.reason as InstanceUpstreamWriteConflictError).reason).toBe("agents_overlap");

      const all = (await store.listInstanceUpstreamGroups()).flatMap((g) => g.agents);
      expect(new Set(all).size).toBe(all.length);
      store.close();
    }
  });

  it("concurrent singleton creates: exactly one extraction group", async () => {
    const store = open();
    const results = await Promise.allSettled(
      [0, 1, 2].map(() => store.createInstanceUpstreamGroup({ group_type: "extraction", mode: "official" })),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect((await store.listInstanceUpstreamGroups({ group_type: "extraction" })).length).toBe(1);
  });

  it("data calls before init() wait for the schema; two stores on one new schema both work", async () => {
    const schema = pgSchemaName("meta", `c${n++}`, prefix);
    const a = new PostgresMetadataStore({ pool: pool(), schema, logger: silent });
    const b = new PostgresMetadataStore({ pool: pool(), schema, logger: silent });
    const [ca, cb] = await Promise.all([a.countUsers(), b.countTeams()]);
    expect([ca, cb]).toEqual([0, 0]);
    expect(typeof ca).toBe("number");
    a.close();
    await expect(a.countUsers()).rejects.toThrow(/closed/);
    expect(await b.countUsers()).toBe(0);
  });

  it("MetadataStorePool: postgres config → PostgresMetadataStore; purge drops only that instance schema", async () => {
    const config = loadStoreConfig({
      TDAI_METADATA_POSTGRES_URL: TEST_POSTGRES_URL,
      TDAI_METADATA_POSTGRES_SCHEMA_PREFIX: prefix,
    });
    expect(config.backend).toBe("postgres");
    const metaPool = new MetadataStorePool(config);
    try {
      const a = await metaPool.getStore("purge_a");
      const b = await metaPool.getStore("purge_b");
      expect(a).toBeInstanceOf(PostgresMetadataStore);
      await a.createUser({ auth_provider: "local", external_id: "x", username: "x" });
      await b.createUser({ auth_provider: "local", external_id: "y", username: "y" });
      const schemaA = pgSchemaName("meta", "purge_a", prefix);
      const schemaB = pgSchemaName("meta", "purge_b", prefix);
      // A memory schema of the same instance must survive the metadata purge.
      const memA = pgSchemaName("mem", "purge_a", prefix);
      await withTestClient((c) => c.query(`CREATE SCHEMA "${memA}"`));

      expect(await metaPool.purgeInstance("purge_a")).toEqual({ db_name: schemaA, dropped: true });
      expect(await schemaExists(schemaA)).toBe(false);
      expect(await schemaExists(schemaB)).toBe(true);
      expect(await schemaExists(memA)).toBe(true);
      expect(await b.countUsers()).toBe(1);

      // The evicted store is closed; a later request recreates an empty schema (product decision Q1).
      await expect(a.countUsers()).rejects.toThrow(/closed/);
      const again = await metaPool.getStore("purge_a");
      expect(again).not.toBe(a);
      expect(await again.countUsers()).toBe(0);
    } finally {
      await metaPool.closeAll();
      await getSharedPgPoolRegistry().endAll();
    }
  });
});
