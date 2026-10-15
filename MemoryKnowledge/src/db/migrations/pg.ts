/**
 * Versioned Postgres migrations for the central knowledge DB.
 *
 * The runner (db/client.ts migratePg) applies the versions missing from
 * `<schema>.schema_migrations` in one transaction under the per-schema migrate
 * lock. `up(s)` gets the quoted schema identifier; every name is schema-qualified.
 * Timestamps and JSON stay `text` (as on SQLite), so the row mappers are shared.
 *
 * Append new versions; never edit or reorder an applied one.
 */

export interface PgMigration {
  version: number;
  /** SQL for this version; `s` is the quoted schema identifier (e.g. `"knowledge"`). */
  up(s: string): string;
}

export const PG_MIGRATIONS: readonly PgMigration[] = [
  {
    // 1 — the full schema of SQLite version 5 (no earlier Postgres release exists).
    version: 1,
    up: (s) => `
      CREATE TABLE ${s}.knowledge_code_graph (
        code_graph_id   text PRIMARY KEY,
        service_id      text NOT NULL,
        team_id         text NOT NULL,
        repo_name       text NOT NULL DEFAULT '',
        repo_url        text NOT NULL,
        branch          text NOT NULL,
        commit_hash     text,
        owner_user_id   text,
        user_id         text,
        agent_id        text,
        task_id         text,
        visibility      text NOT NULL DEFAULT 'team',
        status          text NOT NULL DEFAULT 'pending',
        internal_status text,
        sync_error      text,
        stats_json      text,
        service_url     text,
        summary         text,
        version         integer NOT NULL DEFAULT 0,
        build_run_id    text,
        last_sync_at    text,
        created_at      text NOT NULL,
        updated_at      text NOT NULL,
        deleted_at      text
      );
      CREATE UNIQUE INDEX idx_kcg_team_repo_branch
        ON ${s}.knowledge_code_graph (service_id, team_id, repo_url, branch)
        WHERE deleted_at IS NULL;
      CREATE INDEX idx_kcg_team_status
        ON ${s}.knowledge_code_graph (service_id, team_id, status);

      CREATE TABLE ${s}.knowledge_wiki (
        wiki_id         text PRIMARY KEY,
        service_id      text NOT NULL,
        team_id         text NOT NULL,
        name            text NOT NULL,
        source_type     text,
        source_url      text,
        owner_user_id   text,
        user_id         text,
        agent_id        text,
        task_id         text,
        visibility      text NOT NULL DEFAULT 'team',
        status          text NOT NULL DEFAULT 'draft',
        internal_status text,
        sync_error      text,
        page_count      integer,
        service_url     text,
        summary         text,
        version         integer NOT NULL DEFAULT 0,
        build_run_id    text,
        last_sync_at    text,
        created_at      text NOT NULL,
        updated_at      text NOT NULL,
        deleted_at      text
      );
      CREATE UNIQUE INDEX idx_kwiki_team_name
        ON ${s}.knowledge_wiki (service_id, team_id, name)
        WHERE deleted_at IS NULL;
      CREATE INDEX idx_kwiki_team_status
        ON ${s}.knowledge_wiki (service_id, team_id, status);

      CREATE TABLE ${s}.knowledge_wiki_audit (
        id         integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        wiki_id    text NOT NULL,
        service_id text,
        version    integer NOT NULL DEFAULT 0,
        action     text NOT NULL,
        user_id    text,
        agent_id   text,
        detail     text,
        created_at text NOT NULL
      );
      CREATE INDEX idx_kwa_wiki_version
        ON ${s}.knowledge_wiki_audit (wiki_id, version DESC);

      CREATE TABLE ${s}.knowledge_code_graph_audit (
        id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        code_graph_id text NOT NULL,
        service_id    text,
        version       integer NOT NULL DEFAULT 0,
        action        text NOT NULL,
        user_id       text,
        agent_id      text,
        detail        text,
        created_at    text NOT NULL
      );
      CREATE INDEX idx_kcga_cg_version
        ON ${s}.knowledge_code_graph_audit (code_graph_id, version DESC);

      CREATE TABLE ${s}.knowledge_source_credential (
        service_id       text NOT NULL,
        resource_type    text NOT NULL,
        resource_id      text NOT NULL,
        provider_id      text NOT NULL,
        cred_kind        text NOT NULL,
        cred_secret      text NOT NULL,
        cred_username    text,
        cred_extra_json  text,
        last_verified_at text,
        created_by       text,
        updated_by       text,
        created_at       text NOT NULL,
        updated_at       text NOT NULL
      );
      CREATE UNIQUE INDEX idx_ksc_pk
        ON ${s}.knowledge_source_credential (service_id, resource_type, resource_id);

      CREATE TABLE ${s}.llm_binding (
        service_id     text PRIMARY KEY,
        mode           text NOT NULL DEFAULT 'proxy',
        proxy_base_url text,
        api_key        text,
        model          text,
        base_url       text,
        enabled        integer NOT NULL DEFAULT 1,
        updated_at     text NOT NULL
      );
    `,
  },
];
