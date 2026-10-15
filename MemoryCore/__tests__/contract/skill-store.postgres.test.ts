/**
 * ISkillStore contract on Postgres, plus the head
 * promotion races and StorePool wiring.
 * Skipped without TEST_POSTGRES_URL.
 */
import { afterAll, expect, it, vi } from "vitest";
import pg from "pg";
import { PostgresSkillStore } from "../../src/core/store/postgres/skill-store.js";
import { PostgresMemoryStore } from "../../src/core/store/postgres/memory-store.js";
import { getSharedPgPoolRegistry, PgPoolRegistry } from "../../src/core/store/postgres/pool-registry.js";
import { pgSchemaName } from "../../src/core/store/postgres/schema-name.js";
import { runSkillStoreContract } from "../../src/core/skill/__contract__/skill-store.contract.js";
import { SkillStoreError, type ISkillStore } from "../../src/core/skill/skill-store.interface.js";
import type { AppendVersionInput } from "../../src/core/skill/types.js";
import { isUniqueViolation } from "../../src/core/store/errors.js";
import { StorePool } from "../../src/core/store/store-pool.js";
import { parseConfig } from "../../src/config.js";
import { describePg, dropSchemasWithPrefix, randomSchemaPrefix, TEST_POSTGRES_URL, withTestClient } from "../helpers/postgres.js";

const DIMS = 8;
const silent = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

describePg("PostgresSkillStore", () => {
  const prefix = randomSchemaPrefix();
  const registry = new PgPoolRegistry();
  const pool = () => registry.get(TEST_POSTGRES_URL!, 10);
  let n = 0;
  const newSchema = () => pgSchemaName("mem", `s${n++}`, prefix);
  const schemaOf = new WeakMap<ISkillStore, string>();

  async function open(schema = newSchema(), dimensions = DIMS): Promise<PostgresSkillStore> {
    const store = new PostgresSkillStore({ pool: pool(), schema, dimensions, logger: silent });
    store.init();
    await store.countVersions("warm-up"); // init(): void — wait for the schema
    schemaOf.set(store, schema);
    return store;
  }

  afterAll(async () => {
    await registry.endAll();
    await dropSchemasWithPrefix(prefix);
  });

  runSkillStoreContract({
    backend: "postgres",
    createStore: () => open(),
    async disposeStore(store) {
      store.close();
    },
  });

  // ── Head promotion ──

  const input = (skill_id: string, content: string): AppendVersionInput => ({
    skill_id, team_id: "t", owner_agent_id: "a", user_id: "u", task_id: "k",
    name: `name-${skill_id}`, description: "race", content, content_hash: content, manifest: [], storage_dir: "",
  });

  async function activeHeads(store: PostgresSkillStore, skillId: string): Promise<number[]> {
    const { rows } = await withTestClient((c) =>
      c.query<{ version: number }>(
        `SELECT version FROM "${schemaOf.get(store)}".skills WHERE skill_id = $1 AND is_head AND status = 'active'`,
        [skillId],
      ),
    );
    return rows.map((r) => r.version);
  }

  const isStale = (e: unknown) => e instanceof SkillStoreError && e.code === "SKILL_VERSION_STALE";

  it("two appends from the same head: exactly one wins, one active head", async () => {
    const store = await open();
    const getHead = store.getHead.bind(store);
    for (let i = 0; i < 10; i++) {
      const id = `two-${i}`;
      await store.appendVersion(input(id, "v1"));
      // Barrier: both appends read the head before either one writes.
      let release!: () => void;
      const bothRead = new Promise<void>((r) => (release = r));
      let reads = 0;
      vi.spyOn(store, "getHead").mockImplementation(async (...args) => {
        const head = await getHead(...args);
        if (++reads === 2) release();
        await bothRead;
        return head;
      });
      const res = await Promise.allSettled([store.appendVersion(input(id, "a")), store.appendVersion(input(id, "b"))]);
      vi.restoreAllMocks();
      const ok = res.filter((r) => r.status === "fulfilled");
      const lost = res.filter((r): r is PromiseRejectedResult => r.status === "rejected");
      expect(ok).toHaveLength(1);
      expect(lost).toHaveLength(1);
      expect(isStale(lost[0]!.reason) || isUniqueViolation(lost[0]!.reason)).toBe(true);
      expect(await activeHeads(store, id)).toEqual([2]);
      expect(await store.countVersions(id)).toBe(2);
    }
  });

  it("an append from a stale head loses deterministically (head replaced in the window)", async () => {
    const store = await open();
    await store.appendVersion(input("stale", "v1"));
    const stale = await store.getHead("stale", "t");
    await store.appendVersion(input("stale", "v2"));
    vi.spyOn(store, "getHead").mockResolvedValueOnce(stale);
    await expect(store.appendVersion(input("stale", "v3"))).rejects.toSatisfy(isStale);
    expect(await activeHeads(store, "stale")).toEqual([2]);
  });

  it("append racing an archive that commits first: no active head, no new version", async () => {
    const store = await open();
    await store.appendVersion(input("arch", "v1"));
    const head = await store.getHead("arch", "t");
    expect(await store.archiveHead("arch", "t")).toEqual({ archived: true });
    vi.spyOn(store, "getHead").mockResolvedValueOnce(head); // the append read v1 before the archive
    await expect(store.appendVersion(input("arch", "v2"))).rejects.toSatisfy(isStale);
    expect(await activeHeads(store, "arch")).toEqual([]);
    expect(await store.countVersions("arch")).toBe(1);
  });

  it("append and archive at the same time: the archive always ends with no active head", async () => {
    const store = await open();
    for (let i = 0; i < 20; i++) {
      const id = `ax-${i}`;
      await store.appendVersion(input(id, "v1"));
      const [, archived] = await Promise.allSettled([store.appendVersion(input(id, "v2")), store.archiveHead(id, "t")]);
      expect(archived).toEqual({ status: "fulfilled", value: { archived: true } });
      expect(await activeHeads(store, id)).toEqual([]);
    }
  });

  it("append racing deleteAllVersions: a deleted skill does not come back", async () => {
    const store = await open();
    await store.appendVersion(input("del", "v1"));
    const head = await store.getHead("del", "t");
    expect(await store.deleteAllVersions("del", "t")).toBe(1);
    vi.spyOn(store, "getHead").mockResolvedValueOnce(head);
    await expect(store.appendVersion(input("del", "v2"))).rejects.toSatisfy(isStale);
    expect(await store.countVersions("del")).toBe(0);

    // Concurrent: whichever commits first, no version > 1 of the old skill survives.
    for (let i = 0; i < 20; i++) {
      const id = `dx-${i}`;
      await store.appendVersion(input(id, "v1"));
      await Promise.allSettled([store.appendVersion(input(id, "v2")), store.deleteAllVersions(id, "t")]);
      const left = await store.listVersions(id);
      expect(left.filter((s) => s.version > 1)).toEqual([]);
    }
  });

  it("an archive or delete issued while an append is mid-transaction waits and removes the new head", async () => {
    // Pause the append right after its demote; the removal starts in that window.
    // Without the per-skill lock it would wait on the old head's row lock only and,
    // under READ COMMITTED, miss the new head the append inserts.
    let hook: (() => Promise<void>) | null = null;
    class HookClient extends pg.Client {}
    const query = pg.Client.prototype.query as (...a: unknown[]) => unknown;
    (HookClient.prototype as unknown as { query: unknown }).query = function (this: pg.Client, ...args: unknown[]) {
      const r = query.apply(this, args) as Promise<unknown> | undefined;
      const text = typeof args[0] === "string" ? args[0] : (args[0] as { text?: string } | undefined)?.text;
      if (hook && r && typeof r.then === "function" && /SET is_head = false/.test(text ?? "")) {
        const h = hook;
        hook = null;
        return r.then(async (res) => (await h(), res));
      }
      return r;
    };
    const own = new pg.Pool({ connectionString: TEST_POSTGRES_URL!, max: 4, Client: HookClient });
    try {
      const removals = {
        archive: (s: PostgresSkillStore) => s.archiveHead("mid", "t"),
        deleteAll: (s: PostgresSkillStore) => s.deleteAllVersions("mid", "t"),
      };
      for (const [what, remove] of Object.entries(removals)) {
        const store = new PostgresSkillStore({ pool: own, schema: newSchema(), dimensions: 0, logger: silent });
        schemaOf.set(store, (store as unknown as { schema: string }).schema);
        store.init();
        await store.appendVersion(input("mid", "v1"));
        let removal: Promise<unknown> | undefined;
        hook = async () => {
          removal = remove(store);
          await new Promise((r) => setTimeout(r, 300)); // let it block on the lock
        };
        expect((await store.appendVersion(input("mid", "v2"))).version).toBe(2);
        await removal;
        expect(await activeHeads(store, "mid"), what).toEqual([]);
        if (what === "deleteAll") expect(await store.countVersions("mid")).toBe(0);
      }
    } finally {
      await own.end();
    }
  });

  it("the partial unique index rejects a second active head with the same name (raw 23505, Q3)", async () => {
    const store = await open();
    const schema = schemaOf.get(store)!;
    await store.appendVersion(input("u1", "v1"));
    const err = await withTestClient((c) =>
      c.query(
        `INSERT INTO "${schema}".skills (row_id, skill_id, version, user_id, owner_agent_id, team_id, name,
           content, content_hash, storage_dir, created_at_ms, updated_at_ms)
         VALUES ('r2', 'u2', 1, 'u', 'a', 't', 'name-u1', 'c', 'h', '', 0, 0)`,
      ),
    ).catch((e: unknown) => e);
    expect(isUniqueViolation(err, "uniq_skills_team_agent_name_head")).toBe(true);
  });

  // ── Search, TTL, vectors ──

  it("keyword search: CJK, filters, active heads only, scores in [0, 1)", async () => {
    const store = await open();
    await store.appendVersion({ ...input("zh", "部署 数据库 主库"), description: "数据库 运维" });
    await store.appendVersion(input("en", "deploy the kubernetes cluster"));
    const zh = await store.searchSkills({ query: "主库", team_id: "t" });
    expect(zh.map((h) => h.skill.skill_id)).toEqual(["zh"]);
    expect(zh[0]!.score).toBeGreaterThan(0);
    expect(zh[0]!.score).toBeLessThan(1);
    expect(await store.searchSkills({ query: "kubernetes", team_id: "other" })).toEqual([]);
    expect(await store.searchSkills({ query: "kubernetes", agent_id: "nobody" })).toEqual([]);
    await store.archiveHead("en", "t");
    expect(await store.searchSkills({ query: "kubernetes" })).toEqual([]);
    expect(await store.searchSkills({ query: "a & b | !c (d) 'e' f:*" })).toBeInstanceOf(Array);
  });

  it("findExpiredVersions lists only old non-head active rows; deleteVersion never deletes a head", async () => {
    let t = 1_000;
    const store = new PostgresSkillStore({ pool: pool(), schema: newSchema(), dimensions: 0, logger: silent, now: () => t });
    store.init();
    await store.appendVersion(input("ttl", "v1"));
    t = 2_000;
    await store.appendVersion(input("ttl", "v2"));
    const expired = await store.findExpiredVersions(5_000);
    expect(expired).toEqual([
      { skill_id: "ttl", version: 1, is_head: false, status: "active", storage_dir: "", created_at_ms: 1_000 },
    ]);
    expect(await store.deleteVersion("ttl", 2)).toBe(false);
    expect(await store.deleteVersion("ttl", 1)).toBe(true);
    expect(await store.countVersions("ttl")).toBe(1);
  });

  it("vectors follow the stored dimension; a mismatch disables them without dropping", async () => {
    const schema = newSchema();
    const mem = new PostgresMemoryStore({ pool: pool(), schema, dimensions: DIMS, logger: silent });
    await mem.init(); // the memory store owns the dimension
    const store = await open(schema, DIMS);
    expect(store.getCapabilities().vectorSearch).toBe(true);
    await store.appendVersion(input("vec", "v1"));
    await store.upsertEmbedding("vec", new Float32Array(DIMS).fill(1));
    const count = () =>
      withTestClient((c) => c.query<{ n: number }>(`SELECT count(embedding)::int AS n FROM "${schema}".skills`));
    expect((await count()).rows[0]!.n).toBe(1);

    const other = await open(schema, 4);
    expect(other.getCapabilities().vectorSearch).toBe(false);
    expect(other.isDegraded()).toBe(false);
    expect((await count()).rows[0]!.n).toBe(1);
    mem.close();
  });

  it("a closed store returns empty reads and never ends the shared pool", async () => {
    const store = await open();
    await store.appendVersion(input("closed", "v1"));
    store.close();
    expect(await store.getHead("closed")).toBeNull();
    expect(await store.listSkills({})).toEqual({ items: [], total: 0 });
    await expect(store.appendVersion(input("closed", "v2"))).rejects.toThrow(/closed/);
    expect((await pool().query("SELECT 1 AS one")).rows[0].one).toBe(1);
  });

  it("StorePool.getSkillStore wires the postgres kind to the memory store's schema", async () => {
    const storePool = new StorePool({
      mode: "postgres",
      memoryCfg: parseConfig({ embedding: { dimensions: DIMS } }),
      logger: silent,
    });
    storePool.setGraceCloseDelay(0);
    const schema = pgSchemaName("mem", "skill_pool_inst", prefix);
    const db = { kind: "postgres" as const, conn: { url: TEST_POSTGRES_URL!, schema, poolMax: 4 } };
    try {
      const skills = await storePool.getSkillStore("skill_pool_inst", db);
      expect(skills).toBeInstanceOf(PostgresSkillStore);
      expect((await skills.appendVersion(input("pooled", "v1"))).version).toBe(1);
      const { store } = await storePool.getStore("skill_pool_inst", db);
      expect(await store.countL1()).toBe(0);
      const versions = await withTestClient((c) =>
        c.query<{ version: number }>(`SELECT version FROM "${schema}".schema_migrations ORDER BY version`),
      );
      expect(versions.rows.map((r) => r.version)).toEqual([1, 1000]);
    } finally {
      await storePool.closeAll();
      await getSharedPgPoolRegistry().endAll();
    }
  });
});
