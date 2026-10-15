/**
 * Knowledge store contract — storage-agnostic interface + row/input/aux types.
 *
 * ZERO third-party dependencies (no drizzle, no better-sqlite3). This is the
 * abstraction seam (001 Q6): `IKnowledgeStore` lets us swap the SQLite backend
 * for e.g. a MySQL implementation without touching service/route code.
 *
 * Multi-tenancy (001, phase 5): every resource is scoped by `service_id`.
 *   - Main tables (code_graph / wiki) carry `service_id` NOT NULL.
 *   - Read/write methods take `serviceId` as their FIRST parameter (compile-time
 *     guard against forgetting the tenant filter), INCLUDING id-only lookups
 *     (getCodeGraphById / getWikiById / update*Status / update*Meta). A resource
 *     whose service_id does not match returns null/false → 404 at the route layer.
 *     This closes the cross-Memory leak risk (001 §2.4 / R1).
 *   - Create/audit inputs carry `service_id` on the input object itself.
 */

export type SyncStatus = "pending" | "processing" | "ready" | "failed";

/**
 * Wiki 专属状态：在 SyncStatus 基础上多一个 `draft`——create 建壳时的初始态，
 * 表示"从未加工过、无内容、可被 ingest"。一旦 ingest 成功变 ready、失败变 failed，
 * 之后重跑 ingest 只会进 pending/processing，永远不再回 draft。
 * code-graph 不使用 draft（其 create 即入队建图，初始 pending 是真 in-flight）。
 */
export type WikiStatus = SyncStatus | "draft";

// ───────────────────────── Code-Graph ─────────────────────────

export interface CodeGraphRow {
  code_graph_id: string;
  service_id: string;
  team_id: string;
  repo_name: string;
  repo_url: string;
  branch: string;
  commit_hash: string | null;
  owner_user_id: string | null;
  user_id: string | null;
  agent_id: string | null;
  task_id: string | null;
  visibility: string;
  status: SyncStatus;
  internal_status: string | null;
  sync_error: string | null;
  stats_json: string | null;
  service_url: string | null;
  summary: string | null;
  version: number;
  last_sync_at: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface CreateCodeGraphInput {
  service_id: string;
  team_id: string;
  repo_url: string;
  branch: string;
  repo_name?: string;
  owner_user_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
  visibility?: string;
  service_url?: string;
  /** First build run id: a new code-graph is born pending, so the insert is its claim. */
  build_run_id?: string;
}

export interface CodeGraphStatusPatch {
  status?: SyncStatus;
  internal_status?: string | null;
  sync_error?: string | null;
  commit_hash?: string | null;
  last_sync_at?: string | null;
  stats_json?: string | null;
  service_url?: string | null;
  summary?: string | null;
  version?: number;
  /** Only clearing is allowed here; a run id is written by claimCodeGraphBuild. */
  build_run_id?: null;
}

export interface CodeGraphMetaPatch {
  repo_name?: string;
  summary?: string | null;
}

// ───────────────────────── Wiki ─────────────────────────

export interface WikiRow {
  wiki_id: string;
  service_id: string;
  team_id: string;
  name: string;
  source_type: string | null;
  source_url: string | null;
  owner_user_id: string | null;
  user_id: string | null;
  agent_id: string | null;
  task_id: string | null;
  visibility: string;
  status: WikiStatus;
  internal_status: string | null;
  sync_error: string | null;
  page_count: number | null;
  service_url: string | null;
  summary: string | null;
  version: number;
  last_sync_at: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

export interface CreateWikiInput {
  service_id: string;
  team_id: string;
  name: string;
  source_type?: string;
  source_url?: string;
  owner_user_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
  visibility?: string;
  service_url?: string;
}

export interface WikiStatusPatch {
  status?: WikiStatus;
  internal_status?: string | null;
  sync_error?: string | null;
  page_count?: number | null;
  last_sync_at?: string | null;
  service_url?: string | null;
  summary?: string | null;
  version?: number;
  /** Only clearing is allowed here; a run id is written by claimWikiBuild. */
  build_run_id?: null;
}

export interface WikiMetaPatch {
  name?: string;
  summary?: string | null;
}

// ───────────────────────── Audit ─────────────────────────

export type AuditAction = "ingest" | "ready" | "failed" | "delete" | "create";

export interface AuditLogInput {
  service_id?: string | null;
  asset_id: string;
  version: number;
  action: AuditAction;
  user_id?: string | null;
  agent_id?: string | null;
  detail?: string | null;
}

export interface AuditLogRow {
  id: number;
  service_id: string | null;
  asset_id: string;
  version: number;
  action: AuditAction;
  user_id: string | null;
  agent_id: string | null;
  detail: string | null;
  created_at: string;
}

// ───────────────────────── Shared ─────────────────────────

export interface CreateResult<T> {
  row: T;
  existed: boolean;
}

export interface ListOpts {
  // Wiki 用 WikiStatus（含 draft）；code-graph 只用 SyncStatus，传 draft 会得到空集（无副作用）。
  syncStatus?: WikiStatus;
  limit?: number;
  offset?: number;
}

export interface CountOpts {
  syncStatus?: WikiStatus;
}

/** Restart-recovery projection — carries service_id so dirs can be rebuilt per-tenant. */
export interface SyncedCodeGraphRef {
  code_graph_id: string;
  service_id: string;
  team_id: string;
}

export interface SyncedWikiRef {
  wiki_id: string;
  service_id: string;
  team_id: string;
}

// ───────────────────────── Build claim / fence ─────────────────────────

/**
 * Build-run fence for status writes: the UPDATE only changes the row while
 * `build_run_id` equals `runId` (`null` = no build in flight). A write from a
 * replaced, cancelled, deleted or recovered run therefore changes 0 rows.
 */
export interface BuildFence {
  runId: string | null;
}

export interface ClaimOptions {
  /** true = claim even while pending/processing (wiki ingest onBusy:'replace'). */
  replace?: boolean;
  /** New build run id; written to build_run_id by the claim. */
  runId: string;
}

/**
 * Result of the atomic "check busy, then mark pending" transition.
 * `claimed.row` is the row after the claim (status pending, version + 1).
 */
export type ClaimResult<T> =
  | { kind: "claimed"; row: T; prevStatus: WikiStatus; prevVersion: number }
  | { kind: "busy"; status: "pending" | "processing"; step: string | null }
  | { kind: "not_found" };

// ───────────────────────── Store interface ─────────────────────────

/**
 * Storage-agnostic knowledge metadata store (001 Q6).
 *
 * Tenant isolation contract: every read/write method takes `serviceId` first and
 * filters on it. id-only accessors that do not match the given memory return
 * null/false (never a foreign tenant's row).
 */
export interface IKnowledgeStore {
  // ── Code-Graph ──
  createCodeGraph(input: CreateCodeGraphInput): Promise<CreateResult<CodeGraphRow>>;
  getCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<CodeGraphRow | null>;
  getCodeGraphById(serviceId: string, codeGraphId: string): Promise<CodeGraphRow | null>;
  listCodeGraphs(serviceId: string, teamId: string, opts?: ListOpts): Promise<CodeGraphRow[]>;
  countCodeGraphs(serviceId: string, teamId: string, opts?: CountOpts): Promise<number>;
  /** UPDATE by id (never inserts). With `fence`, only while build_run_id matches. Returns true iff a row changed. */
  updateCodeGraphStatus(
    serviceId: string,
    codeGraphId: string,
    patch: CodeGraphStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean>;
  deleteCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<boolean>;
  updateCodeGraphMeta(serviceId: string, codeGraphId: string, patch: CodeGraphMetaPatch): Promise<CodeGraphRow | null>;
  /** Atomic busy check + mark pending (version + 1, build_run_id = runId). */
  claimCodeGraphBuild(
    serviceId: string,
    teamId: string,
    codeGraphId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<CodeGraphRow>>;

  // ── Wiki ──
  createWiki(input: CreateWikiInput): Promise<CreateResult<WikiRow>>;
  getWiki(serviceId: string, teamId: string, wikiId: string): Promise<WikiRow | null>;
  getWikiById(serviceId: string, wikiId: string): Promise<WikiRow | null>;
  listWikis(serviceId: string, teamId: string, opts?: ListOpts): Promise<WikiRow[]>;
  countWikis(serviceId: string, teamId: string, opts?: CountOpts): Promise<number>;
  /** UPDATE by id (never inserts). With `fence`, only while build_run_id matches. Returns true iff a row changed. */
  updateWikiStatus(
    serviceId: string,
    wikiId: string,
    patch: WikiStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean>;
  deleteWiki(serviceId: string, teamId: string, wikiId: string): Promise<boolean>;
  updateWikiMeta(serviceId: string, wikiId: string, patch: WikiMetaPatch): Promise<WikiRow | null>;
  /** Atomic busy check + mark pending (version + 1, build_run_id = runId). */
  claimWikiBuild(
    serviceId: string,
    teamId: string,
    wikiId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<WikiRow>>;
  /** `SET version = version + 1` in SQL. Returns true iff a row changed. */
  bumpWikiVersion(serviceId: string, wikiId: string, patch?: { last_sync_at?: string | null }): Promise<boolean>;

  // ── Audit ──
  appendWikiAudit(input: AuditLogInput): Promise<void>;
  appendCodeGraphAudit(input: AuditLogInput): Promise<void>;
  listWikiAudit(serviceId: string, wikiId: string, limit?: number, offset?: number): Promise<AuditLogRow[]>;
  listCodeGraphAudit(serviceId: string, codeGraphId: string, limit?: number, offset?: number): Promise<AuditLogRow[]>;

  // ── Restart recovery ──
  /** Sweep all non-terminal (pending/processing) assets to failed and clear build_run_id, across all tenants. */
  markInterruptedAsFailed(reason?: string): Promise<number>;
  /** All ready code-graphs (with service_id) so module.ts can rebuild per-tenant dirs. */
  listSyncedCodeGraphs(): Promise<SyncedCodeGraphRef[]>;
  listSyncedWikis(): Promise<SyncedWikiRef[]>;
}
