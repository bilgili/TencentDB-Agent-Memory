/**
 * Test-only Postgres helpers. Tests that use them skip when
 * TEST_POSTGRES_URL is not set. Each open() gets a random schema, which close() drops.
 */

import { randomBytes } from "node:crypto";

import { createKnowledgeDb, migrateKnowledgeDb, type CreateKnowledgeDbOptions, type KnowledgeDb } from "../../db/client.js";
import { openKnowledgeStores } from "../open-stores.js";
import type { IKnowledgeStore } from "../types.js";
import type { KnowledgeStoreHarness } from "./knowledge-store.contract.js";

export const TEST_POSTGRES_URL = process.env.TEST_POSTGRES_URL ?? "";

export type PgKnowledgeDb = Extract<KnowledgeDb, { dialect: "postgres" }>;

export function randomSchema(): string {
  return `mk_test_${randomBytes(6).toString("hex")}`;
}

/** Open (and lock) a Postgres knowledge DB on `schema`; does not migrate. */
export async function openPg(schema: string, opts?: CreateKnowledgeDbOptions): Promise<PgKnowledgeDb> {
  const kdb = await createKnowledgeDb({ path: "", url: TEST_POSTGRES_URL, schema, poolMax: 4 }, opts);
  if (kdb.dialect !== "postgres") throw new Error("expected a Postgres knowledge DB");
  return kdb;
}

/** Drop the schema, then release the pool and the schema lock. */
export async function dropAndClose(kdb: PgKnowledgeDb): Promise<void> {
  try {
    await kdb.pool.query(`DROP SCHEMA IF EXISTS "${kdb.schema}" CASCADE`);
  } finally {
    await kdb.close();
  }
}

/** Contract harness: a fresh random, migrated schema per test. */
export function pgHarness(
  name = "postgres",
  wrap: (store: IKnowledgeStore) => IKnowledgeStore = (s) => s,
): KnowledgeStoreHarness {
  return {
    name,
    async open() {
      const kdb = await openPg(randomSchema());
      await migrateKnowledgeDb(kdb);
      const stores = openKnowledgeStores(kdb);
      return { ...stores, store: wrap(stores.store), close: () => dropAndClose(kdb) };
    },
  };
}
