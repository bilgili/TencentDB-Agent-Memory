import { describe, expect, it } from "vitest";

import { createDb, migrate } from "../../db/client.js";
import { isUniqueViolation } from "../../db/errors.js";
import { createCredentialStore } from "../../source-auth/credential-store.js";
import { createLlmBindingStore } from "../llm-binding-store.js";
import { SqliteKnowledgeStore } from "../sqlite-store.js";
import { runKnowledgeStoreContract } from "./knowledge-store.contract.js";
import { createYieldingKnowledgeStore } from "./yielding-knowledge-store.js";

runKnowledgeStoreContract({
  name: "sqlite :memory:",
  async open() {
    const { db, raw } = createDb({ path: ":memory:" });
    return {
      store: new SqliteKnowledgeStore(db),
      credentialStore: createCredentialStore({ db }),
      llmBindingStore: createLlmBindingStore(db),
      async close() {
        raw.close();
      },
    };
  },
});

// The same contract through the yielding decorator: the interleaving must not change any result.
runKnowledgeStoreContract({
  name: "sqlite :memory: via YieldingKnowledgeStore",
  async open() {
    const { db, raw } = createDb({ path: ":memory:" });
    return {
      store: createYieldingKnowledgeStore(new SqliteKnowledgeStore(db)),
      credentialStore: createCredentialStore({ db }),
      llmBindingStore: createLlmBindingStore(db),
      async close() {
        raw.close();
      },
    };
  },
});

describe("isUniqueViolation", () => {
  it("recognizes the SQLite unique error and the Postgres 23505 code", () => {
    const { raw } = createDb({ path: ":memory:" });
    raw.exec("CREATE TABLE t (id TEXT PRIMARY KEY, n TEXT); CREATE UNIQUE INDEX t_n ON t(n);");
    raw.exec("INSERT INTO t VALUES ('a', 'x')");
    let sqliteErr: unknown;
    try {
      raw.exec("INSERT INTO t VALUES ('b', 'x')");
    } catch (err) {
      sqliteErr = err;
    }
    raw.close();
    expect(isUniqueViolation(sqliteErr)).toBe(true);
    expect(isUniqueViolation(sqliteErr, "t.n")).toBe(true);
    expect(isUniqueViolation(sqliteErr, "t.id")).toBe(false);
    expect(isUniqueViolation({ code: "23505", constraint: "idx_kwiki_team_name" })).toBe(true);
    expect(isUniqueViolation({ code: "23505", constraint: "idx_kwiki_team_name" }, "idx_kwiki_team_name")).toBe(true);
    expect(isUniqueViolation({ code: "23505", constraint: "other" }, "idx_kwiki_team_name")).toBe(false);
    expect(isUniqueViolation({ code: "23503" })).toBe(false);
    expect(isUniqueViolation(new Error("boom"))).toBe(false);
    expect(isUniqueViolation(null)).toBe(false);
  });
});

describe("SQLite migration of build_run_id", () => {
  it("adds build_run_id to a file from an earlier release; existing rows stay readable", async () => {
    const { db, raw } = createDb({ path: ":memory:", autoMigrate: false });
    // knowledge_wiki as an earlier release created it (no service_url / summary / build_run_id).
    raw.exec(`
      CREATE TABLE knowledge_wiki (
        wiki_id TEXT PRIMARY KEY, service_id TEXT NOT NULL, team_id TEXT NOT NULL, name TEXT NOT NULL,
        source_type TEXT, source_url TEXT, owner_user_id TEXT, user_id TEXT, agent_id TEXT, task_id TEXT,
        visibility TEXT NOT NULL DEFAULT 'team', status TEXT NOT NULL DEFAULT 'draft', internal_status TEXT,
        sync_error TEXT, page_count INTEGER, version INTEGER NOT NULL DEFAULT 0, last_sync_at TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT
      );
      INSERT INTO knowledge_wiki (wiki_id, service_id, team_id, name, status, version, created_at, updated_at)
        VALUES ('wiki-old', 's', 't', 'old', 'ready', 4, '2026-01-01', '2026-01-01');
    `);
    migrate(db, raw);
    const cols = (t: string) => (raw.pragma(`table_info(${t})`) as Array<{ name: string }>).map((c) => c.name);
    expect(cols("knowledge_wiki")).toContain("build_run_id");
    expect(cols("knowledge_code_graph")).toContain("build_run_id");
    const store = new SqliteKnowledgeStore(db);
    expect(await store.getWikiById("s", "wiki-old")).toMatchObject({ name: "old", status: "ready", version: 4 });
    expect((await store.claimWikiBuild("s", "t", "wiki-old", { runId: "r" })).kind).toBe("claimed");
    raw.close();
  });
});
