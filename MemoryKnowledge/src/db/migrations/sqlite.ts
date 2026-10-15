/**
 * Versioned SQLite migrations for the central knowledge DB.
 *
 * The runner (db/client.ts migrateSqlite) applies the versions missing from
 * `schema_migrations`. A file from a release before this list has no
 * `schema_migrations`, so every version runs on it: each step must therefore be
 * idempotent on an existing file (IF NOT EXISTS / column check / structure check).
 *
 * Append new versions; never edit or reorder an applied one.
 */

import type Database from "better-sqlite3";

export interface SqliteMigration {
  version: number;
  up(raw: Database.Database): void;
}

/** True iff the given table exists in the DB. */
function tableExists(raw: Database.Database, table: string): boolean {
  return !!raw.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(table);
}

/** True iff the given column exists on the table (SQLite PRAGMA-based). */
function hasColumn(raw: Database.Database, table: string, column: string): boolean {
  const cols = raw.pragma(`table_info(${table})`) as Array<{ name: string }>;
  return cols.some((c) => c.name === column);
}

/** SQLite ALTER TABLE ADD COLUMN is not idempotent, so check first. */
function addColumnIfMissing(raw: Database.Database, table: string, column: string, type: string): void {
  if (hasColumn(raw, table, column)) return;
  raw.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${type};`);
}

export const SQLITE_MIGRATIONS: readonly SqliteMigration[] = [
  {
    // 1 — the base tables as the first release created them.
    version: 1,
    up(raw) {
      raw.exec(`
        CREATE TABLE IF NOT EXISTS knowledge_code_graph (
          code_graph_id   TEXT PRIMARY KEY,
          service_id      TEXT NOT NULL,
          team_id         TEXT NOT NULL,
          repo_name       TEXT NOT NULL DEFAULT '',
          repo_url        TEXT NOT NULL,
          branch          TEXT NOT NULL,
          commit_hash     TEXT,
          owner_user_id   TEXT,
          user_id         TEXT,
          agent_id        TEXT,
          task_id         TEXT,
          visibility      TEXT NOT NULL DEFAULT 'team',
          status          TEXT NOT NULL DEFAULT 'pending',
          internal_status TEXT,
          sync_error      TEXT,
          stats_json      TEXT,
          version         INTEGER NOT NULL DEFAULT 0,
          last_sync_at    TEXT,
          created_at      TEXT NOT NULL,
          updated_at      TEXT NOT NULL,
          deleted_at      TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_kcg_team_repo_branch
          ON knowledge_code_graph(service_id, team_id, repo_url, branch)
          WHERE deleted_at IS NULL;

        CREATE INDEX IF NOT EXISTS idx_kcg_team_status
          ON knowledge_code_graph(service_id, team_id, status);

        CREATE TABLE IF NOT EXISTS knowledge_wiki (
          wiki_id         TEXT PRIMARY KEY,
          service_id      TEXT NOT NULL,
          team_id         TEXT NOT NULL,
          name            TEXT NOT NULL,
          source_type     TEXT,
          source_url      TEXT,
          owner_user_id   TEXT,
          user_id         TEXT,
          agent_id        TEXT,
          task_id         TEXT,
          visibility      TEXT NOT NULL DEFAULT 'team',
          status          TEXT NOT NULL DEFAULT 'draft',
          internal_status TEXT,
          sync_error      TEXT,
          page_count      INTEGER,
          version         INTEGER NOT NULL DEFAULT 0,
          last_sync_at    TEXT,
          created_at      TEXT NOT NULL,
          updated_at      TEXT NOT NULL,
          deleted_at      TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_kwiki_team_name
          ON knowledge_wiki(service_id, team_id, name)
          WHERE deleted_at IS NULL;

        CREATE INDEX IF NOT EXISTS idx_kwiki_team_status
          ON knowledge_wiki(service_id, team_id, status);

        CREATE TABLE IF NOT EXISTS knowledge_wiki_audit (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          wiki_id    TEXT NOT NULL,
          version    INTEGER NOT NULL DEFAULT 0,
          action     TEXT NOT NULL,
          user_id    TEXT,
          agent_id   TEXT,
          detail     TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_kwa_wiki_version
          ON knowledge_wiki_audit(wiki_id, version DESC);

        CREATE TABLE IF NOT EXISTS knowledge_code_graph_audit (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          code_graph_id TEXT NOT NULL,
          version       INTEGER NOT NULL DEFAULT 0,
          action        TEXT NOT NULL,
          user_id       TEXT,
          agent_id      TEXT,
          detail        TEXT,
          created_at    TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_kcga_cg_version
          ON knowledge_code_graph_audit(code_graph_id, version DESC);

        CREATE TABLE IF NOT EXISTS llm_binding (
          service_id     TEXT PRIMARY KEY,
          mode           TEXT NOT NULL DEFAULT 'proxy',
          proxy_base_url TEXT,
          api_key        TEXT,
          model          TEXT,
          base_url       TEXT,
          enabled        INTEGER NOT NULL DEFAULT 1,
          updated_at     TEXT NOT NULL
        );
      `);
    },
  },
  {
    // 2 — resource-scoped source credentials. An older user-scoped table (no
    // resource_id) cannot be upgraded in place: drop it, then create. That
    // capability never shipped, so no real credentials are lost.
    version: 2,
    up(raw) {
      if (
        tableExists(raw, "knowledge_source_credential") &&
        !hasColumn(raw, "knowledge_source_credential", "resource_id")
      ) {
        raw.exec("DROP TABLE knowledge_source_credential");
      }
      raw.exec(`
        CREATE TABLE IF NOT EXISTS knowledge_source_credential (
          service_id       TEXT NOT NULL,
          resource_type    TEXT NOT NULL,        -- 'code-graph' | 'wiki'
          resource_id      TEXT NOT NULL,        -- = code_graph_id 或 wiki_id
          provider_id      TEXT NOT NULL,        -- 'gongfeng' / 'iwiki' / ...（非键）
          cred_kind        TEXT NOT NULL,        -- 'bearer' | 'basic'
          cred_secret      TEXT NOT NULL,        -- base64(UTF-8 令牌)
          cred_username    TEXT,                 -- 仅 basic
          cred_extra_json  TEXT,
          last_verified_at TEXT,
          created_by       TEXT,
          updated_by       TEXT,
          created_at       TEXT NOT NULL,
          updated_at       TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_ksc_pk
          ON knowledge_source_credential(service_id, resource_type, resource_id);
      `);
    },
  },
  {
    // 3 — service_url + summary on both asset tables.
    version: 3,
    up(raw) {
      addColumnIfMissing(raw, "knowledge_code_graph", "service_url", "TEXT");
      addColumnIfMissing(raw, "knowledge_code_graph", "summary", "TEXT");
      addColumnIfMissing(raw, "knowledge_wiki", "service_url", "TEXT");
      addColumnIfMissing(raw, "knowledge_wiki", "summary", "TEXT");
    },
  },
  {
    // 4 — tenant id on the audit tables (nullable → safe on existing rows).
    version: 4,
    up(raw) {
      addColumnIfMissing(raw, "knowledge_wiki_audit", "service_id", "TEXT");
      addColumnIfMissing(raw, "knowledge_code_graph_audit", "service_id", "TEXT");
    },
  },
  {
    // 5 — build-run fence. Nullable → safe on existing rows.
    version: 5,
    up(raw) {
      addColumnIfMissing(raw, "knowledge_code_graph", "build_run_id", "TEXT");
      addColumnIfMissing(raw, "knowledge_wiki", "build_run_id", "TEXT");
    },
  },
];
