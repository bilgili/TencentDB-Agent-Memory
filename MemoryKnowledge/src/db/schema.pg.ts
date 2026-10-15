/**
 * Drizzle ORM schema (Postgres dialect) — the twin of schema.sqlite.ts
 *
 * Same table names, column names and camelCase keys as the SQLite schema, so the
 * row mappers in store/rows.ts serve both dialects. Timestamps and JSON stay
 * `text`; audit ids are integer identity columns. The DDL lives in
 * db/migrations/pg.ts; src/db/schema-drift.test.ts keeps them aligned.
 *
 * The schema name is configuration (KNOWLEDGE_DB_SCHEMA), so the tables are built
 * per schema: every query Drizzle emits is schema-qualified and nothing sets
 * `search_path`.
 */

import { sql } from "drizzle-orm";
import { index, integer, pgSchema, text, uniqueIndex } from "drizzle-orm/pg-core";

export function createPgTables(schemaName: string) {
  const s = pgSchema(schemaName);

  const knowledgeCodeGraph = s.table(
    "knowledge_code_graph",
    {
      codeGraphId: text("code_graph_id").primaryKey(),
      serviceId: text("service_id").notNull(),
      teamId: text("team_id").notNull(),
      repoName: text("repo_name").notNull().default(""),
      repoUrl: text("repo_url").notNull(),
      branch: text("branch").notNull(),
      commitHash: text("commit_hash"),
      ownerUserId: text("owner_user_id"),
      userId: text("user_id"),
      agentId: text("agent_id"),
      taskId: text("task_id"),
      visibility: text("visibility").notNull().default("team"),
      status: text("status").notNull().default("pending"),
      internalStatus: text("internal_status"),
      syncError: text("sync_error"),
      statsJson: text("stats_json"),
      serviceUrl: text("service_url"),
      summary: text("summary"),
      version: integer("version").notNull().default(0),
      buildRunId: text("build_run_id"),
      lastSyncAt: text("last_sync_at"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
      deletedAt: text("deleted_at"),
    },
    (t) => [
      uniqueIndex("idx_kcg_team_repo_branch")
        .on(t.serviceId, t.teamId, t.repoUrl, t.branch)
        .where(sql`deleted_at IS NULL`),
      index("idx_kcg_team_status").on(t.serviceId, t.teamId, t.status),
    ],
  );

  const knowledgeWiki = s.table(
    "knowledge_wiki",
    {
      wikiId: text("wiki_id").primaryKey(),
      serviceId: text("service_id").notNull(),
      teamId: text("team_id").notNull(),
      name: text("name").notNull(),
      sourceType: text("source_type"),
      sourceUrl: text("source_url"),
      ownerUserId: text("owner_user_id"),
      userId: text("user_id"),
      agentId: text("agent_id"),
      taskId: text("task_id"),
      visibility: text("visibility").notNull().default("team"),
      status: text("status").notNull().default("draft"),
      internalStatus: text("internal_status"),
      syncError: text("sync_error"),
      pageCount: integer("page_count"),
      serviceUrl: text("service_url"),
      summary: text("summary"),
      version: integer("version").notNull().default(0),
      buildRunId: text("build_run_id"),
      lastSyncAt: text("last_sync_at"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
      deletedAt: text("deleted_at"),
    },
    (t) => [
      uniqueIndex("idx_kwiki_team_name")
        .on(t.serviceId, t.teamId, t.name)
        .where(sql`deleted_at IS NULL`),
      index("idx_kwiki_team_status").on(t.serviceId, t.teamId, t.status),
    ],
  );

  const knowledgeWikiAudit = s.table(
    "knowledge_wiki_audit",
    {
      id: integer("id").primaryKey().generatedAlwaysAsIdentity(),
      wikiId: text("wiki_id").notNull(),
      serviceId: text("service_id"),
      version: integer("version").notNull().default(0),
      action: text("action").notNull(),
      userId: text("user_id"),
      agentId: text("agent_id"),
      detail: text("detail"),
      createdAt: text("created_at").notNull(),
    },
    (t) => [index("idx_kwa_wiki_version").on(t.wikiId, t.version)],
  );

  const knowledgeCodeGraphAudit = s.table(
    "knowledge_code_graph_audit",
    {
      id: integer("id").primaryKey().generatedAlwaysAsIdentity(),
      codeGraphId: text("code_graph_id").notNull(),
      serviceId: text("service_id"),
      version: integer("version").notNull().default(0),
      action: text("action").notNull(),
      userId: text("user_id"),
      agentId: text("agent_id"),
      detail: text("detail"),
      createdAt: text("created_at").notNull(),
    },
    (t) => [index("idx_kcga_cg_version").on(t.codeGraphId, t.version)],
  );

  const knowledgeSourceCredential = s.table(
    "knowledge_source_credential",
    {
      serviceId: text("service_id").notNull(),
      resourceType: text("resource_type").notNull(),
      resourceId: text("resource_id").notNull(),
      providerId: text("provider_id").notNull(),
      credKind: text("cred_kind").notNull(),
      credSecret: text("cred_secret").notNull(),
      credUsername: text("cred_username"),
      credExtraJson: text("cred_extra_json"),
      lastVerifiedAt: text("last_verified_at"),
      createdBy: text("created_by"),
      updatedBy: text("updated_by"),
      createdAt: text("created_at").notNull(),
      updatedAt: text("updated_at").notNull(),
    },
    (t) => [uniqueIndex("idx_ksc_pk").on(t.serviceId, t.resourceType, t.resourceId)],
  );

  const llmBinding = s.table("llm_binding", {
    serviceId: text("service_id").primaryKey(),
    mode: text("mode").notNull().default("proxy"),
    proxyBaseUrl: text("proxy_base_url"),
    apiKey: text("api_key"),
    model: text("model"),
    baseUrl: text("base_url"),
    enabled: integer("enabled").notNull().default(1),
    updatedAt: text("updated_at").notNull(),
  });

  return {
    knowledgeCodeGraph,
    knowledgeWiki,
    knowledgeWikiAudit,
    knowledgeCodeGraphAudit,
    knowledgeSourceCredential,
    llmBinding,
  };
}

export type PgTables = ReturnType<typeof createPgTables>;
