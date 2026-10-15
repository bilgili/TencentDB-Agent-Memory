/**
 * Dialect-neutral row mappers and patch → column-set builders.
 *
 * Shared by SqliteKnowledgeStore and PgKnowledgeStore. Both Drizzle
 * schemas use the same camelCase keys, text timestamps and text JSON columns, so
 * these functions take the structural row shape and never touch a driver.
 */

import type {
  knowledgeCodeGraph,
  knowledgeWiki,
  knowledgeWikiAudit,
  knowledgeCodeGraphAudit,
  llmBinding,
} from "../db/schema.sqlite.js";
import type {
  AuditLogRow,
  CodeGraphMetaPatch,
  CodeGraphRow,
  CodeGraphStatusPatch,
  SyncStatus,
  WikiMetaPatch,
  WikiRow,
  WikiStatus,
  WikiStatusPatch,
} from "./types.js";
import type { LlmBindingMode, LlmBindingRow } from "./llm-binding-store.js";

export type CodeGraphSelect = typeof knowledgeCodeGraph.$inferSelect;
export type WikiSelect = typeof knowledgeWiki.$inferSelect;
type WikiAuditSelect = typeof knowledgeWikiAudit.$inferSelect;
type CodeGraphAuditSelect = typeof knowledgeCodeGraphAudit.$inferSelect;
type LlmBindingSelect = typeof llmBinding.$inferSelect;

export function nowIso(): string {
  return new Date().toISOString();
}

// ───────────────────────── Row mappers ─────────────────────────

export function mapCodeGraphRow(r: CodeGraphSelect): CodeGraphRow {
  return {
    code_graph_id: r.codeGraphId,
    service_id: r.serviceId,
    team_id: r.teamId,
    repo_name: r.repoName,
    repo_url: r.repoUrl,
    branch: r.branch,
    commit_hash: r.commitHash,
    owner_user_id: r.ownerUserId,
    user_id: r.userId,
    agent_id: r.agentId,
    task_id: r.taskId,
    visibility: r.visibility,
    status: r.status as SyncStatus,
    internal_status: r.internalStatus,
    sync_error: r.syncError,
    stats_json: r.statsJson,
    service_url: r.serviceUrl ?? null,
    summary: r.summary ?? null,
    version: Number(r.version),
    last_sync_at: r.lastSyncAt,
    created_at: r.createdAt,
    updated_at: r.updatedAt,
    deleted_at: r.deletedAt ?? null,
  };
}

export function mapWikiRow(r: WikiSelect): WikiRow {
  return {
    wiki_id: r.wikiId,
    service_id: r.serviceId,
    team_id: r.teamId,
    name: r.name,
    source_type: r.sourceType,
    source_url: r.sourceUrl,
    owner_user_id: r.ownerUserId,
    user_id: r.userId,
    agent_id: r.agentId,
    task_id: r.taskId,
    visibility: r.visibility,
    status: r.status as WikiStatus,
    internal_status: r.internalStatus,
    sync_error: r.syncError,
    page_count: r.pageCount,
    service_url: r.serviceUrl ?? null,
    summary: r.summary ?? null,
    version: Number(r.version),
    last_sync_at: r.lastSyncAt,
    created_at: r.createdAt,
    updated_at: r.updatedAt,
    deleted_at: r.deletedAt ?? null,
  };
}

export function mapWikiAuditRow(r: WikiAuditSelect): AuditLogRow {
  return {
    id: Number(r.id),
    service_id: r.serviceId ?? null,
    asset_id: r.wikiId,
    version: Number(r.version),
    action: r.action as AuditLogRow["action"],
    user_id: r.userId,
    agent_id: r.agentId,
    detail: r.detail,
    created_at: r.createdAt,
  };
}

export function mapCodeGraphAuditRow(r: CodeGraphAuditSelect): AuditLogRow {
  return {
    id: Number(r.id),
    service_id: r.serviceId ?? null,
    asset_id: r.codeGraphId,
    version: Number(r.version),
    action: r.action as AuditLogRow["action"],
    user_id: r.userId,
    agent_id: r.agentId,
    detail: r.detail,
    created_at: r.createdAt,
  };
}

export function mapLlmBindingRow(r: LlmBindingSelect): LlmBindingRow {
  return {
    service_id: r.serviceId,
    mode: (r.mode === "byo" ? "byo" : "proxy") as LlmBindingMode,
    proxy_base_url: r.proxyBaseUrl ?? null,
    api_key: r.apiKey ?? null,
    base_url: r.baseUrl ?? null,
    enabled: r.enabled !== 0,
    updated_at: r.updatedAt,
  };
}

// ───────────────────────── Patch → column set ─────────────────────────
// Only keys present in the patch are written; updated_at is always refreshed.

export function codeGraphStatusSet(patch: CodeGraphStatusPatch): Partial<CodeGraphSelect> {
  const set: Partial<CodeGraphSelect> = { updatedAt: nowIso() };
  if (patch.status !== undefined) set.status = patch.status;
  if (patch.internal_status !== undefined) set.internalStatus = patch.internal_status;
  if (patch.sync_error !== undefined) set.syncError = patch.sync_error;
  if (patch.commit_hash !== undefined) set.commitHash = patch.commit_hash;
  if (patch.stats_json !== undefined) set.statsJson = patch.stats_json;
  if (patch.last_sync_at !== undefined) set.lastSyncAt = patch.last_sync_at;
  if (patch.service_url !== undefined) set.serviceUrl = patch.service_url;
  if (patch.summary !== undefined) set.summary = patch.summary;
  if (patch.version !== undefined) set.version = patch.version;
  if (patch.build_run_id !== undefined) set.buildRunId = patch.build_run_id;
  return set;
}

export function wikiStatusSet(patch: WikiStatusPatch): Partial<WikiSelect> {
  const set: Partial<WikiSelect> = { updatedAt: nowIso() };
  if (patch.status !== undefined) set.status = patch.status;
  if (patch.internal_status !== undefined) set.internalStatus = patch.internal_status;
  if (patch.sync_error !== undefined) set.syncError = patch.sync_error;
  if (patch.page_count !== undefined) set.pageCount = patch.page_count;
  if (patch.last_sync_at !== undefined) set.lastSyncAt = patch.last_sync_at;
  if (patch.service_url !== undefined) set.serviceUrl = patch.service_url;
  if (patch.summary !== undefined) set.summary = patch.summary;
  if (patch.version !== undefined) set.version = patch.version;
  if (patch.build_run_id !== undefined) set.buildRunId = patch.build_run_id;
  return set;
}

export function codeGraphMetaSet(patch: CodeGraphMetaPatch): Partial<CodeGraphSelect> {
  const set: Partial<CodeGraphSelect> = { updatedAt: nowIso() };
  if (patch.repo_name !== undefined) set.repoName = patch.repo_name;
  if (patch.summary !== undefined) set.summary = patch.summary;
  return set;
}

export function wikiMetaSet(patch: WikiMetaPatch): Partial<WikiSelect> {
  const set: Partial<WikiSelect> = { updatedAt: nowIso() };
  if (patch.name !== undefined) set.name = patch.name;
  if (patch.summary !== undefined) set.summary = patch.summary;
  return set;
}

/** Claim-time column set (status pending, clear step/error, new run id). version + 1 is added in SQL by the store. */
export function claimSet(runId: string): { status: "pending"; internalStatus: null; syncError: null; buildRunId: string; updatedAt: string } {
  return { status: "pending", internalStatus: null, syncError: null, buildRunId: runId, updatedAt: nowIso() };
}

/** pending/processing → the busy status, else null. */
export function inFlightStatus(status: string): "pending" | "processing" | null {
  return status === "pending" || status === "processing" ? status : null;
}
