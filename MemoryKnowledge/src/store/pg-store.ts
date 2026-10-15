/**
 * PgKnowledgeStore — Postgres/Drizzle (node-postgres) implementation of IKnowledgeStore
 *
 * Statement-for-statement twin of SqliteKnowledgeStore; read the two side by side.
 * Postgres-specific rules:
 *   - Each method is one statement or one transaction (READ COMMITTED, stated).
 *     Claims read the row with SELECT ... FOR UPDATE inside the transaction.
 *   - createWiki / createCodeGraph run `INSERT ... ON CONFLICT DO NOTHING RETURNING`
 *     outside any transaction: no unique violation can abort a transaction,
 *     and the follow-up SELECT sees the winner's committed row.
 *   - Counts use Drizzle `count()`, which maps int8 to a JS number.
 *   - Every table is schema-qualified by `tables` (createPgTables); no search_path.
 */

import { and, count, desc, eq, inArray, isNull, sql, type SQL } from "drizzle-orm";

import type { PgDb } from "../db/client.js";
import type { PgTables } from "../db/schema.pg.js";
import { CODE_DATA_VERSION, WIKI_DATA_VERSION } from "../db/schema.sqlite.js";
import { genCodeGraphId, genWikiId } from "./ids.js";
import {
  claimSet,
  codeGraphMetaSet,
  codeGraphStatusSet,
  inFlightStatus,
  mapCodeGraphAuditRow,
  mapCodeGraphRow,
  mapWikiAuditRow,
  mapWikiRow,
  nowIso,
  wikiMetaSet,
  wikiStatusSet,
} from "./rows.js";
import type {
  AuditLogInput,
  AuditLogRow,
  BuildFence,
  ClaimOptions,
  ClaimResult,
  CodeGraphMetaPatch,
  CodeGraphRow,
  CodeGraphStatusPatch,
  CountOpts,
  CreateCodeGraphInput,
  CreateResult,
  CreateWikiInput,
  IKnowledgeStore,
  ListOpts,
  SyncedCodeGraphRef,
  SyncedWikiRef,
  WikiMetaPatch,
  WikiRow,
  WikiStatus,
  WikiStatusPatch,
} from "./types.js";

const ID_RETRY = 5;
const TX = { isolationLevel: "read committed" } as const;
const IN_FLIGHT = ["pending", "processing"];

type FencedTable = PgTables["knowledgeCodeGraph"] | PgTables["knowledgeWiki"];

/** `AND build_run_id = $run` (or `IS NULL`) when a fence is given. */
function fenceCond(table: FencedTable, fence?: BuildFence): SQL | undefined {
  if (!fence) return undefined;
  return fence.runId === null ? isNull(table.buildRunId) : eq(table.buildRunId, fence.runId);
}

export class PgKnowledgeStore implements IKnowledgeStore {
  constructor(
    private readonly db: PgDb,
    private readonly t: PgTables,
  ) {}

  // ═══════════════════════ Code-Graph ═══════════════════════

  async createCodeGraph(input: CreateCodeGraphInput): Promise<CreateResult<CodeGraphRow>> {
    const t = this.t.knowledgeCodeGraph;
    const ts = nowIso();
    for (let attempt = 0; attempt < ID_RETRY; attempt++) {
      const [inserted] = await this.db
        .insert(t)
        .values({
          codeGraphId: genCodeGraphId(),
          serviceId: input.service_id,
          teamId: input.team_id,
          repoName: input.repo_name ?? "",
          repoUrl: input.repo_url,
          branch: input.branch,
          ownerUserId: input.owner_user_id ?? null,
          userId: input.user_id ?? null,
          agentId: input.agent_id ?? null,
          taskId: input.task_id ?? null,
          visibility: input.visibility ?? "team",
          status: "pending",
          serviceUrl: input.service_url ?? null,
          buildRunId: input.build_run_id ?? null,
          version: CODE_DATA_VERSION,
          createdAt: ts,
          updatedAt: ts,
        })
        .onConflictDoNothing()
        .returning();
      if (inserted) return { row: mapCodeGraphRow(inserted), existed: false };

      const [live] = await this.db
        .select()
        .from(t)
        .where(
          and(
            eq(t.serviceId, input.service_id),
            eq(t.teamId, input.team_id),
            eq(t.repoUrl, input.repo_url),
            eq(t.branch, input.branch),
            isNull(t.deletedAt),
          ),
        );
      if (live) return { row: mapCodeGraphRow(live), existed: true };
    }
    throw new Error("createCodeGraph: failed to allocate unique id");
  }

  async getCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    const t = this.t.knowledgeCodeGraph;
    const [row] = await this.db
      .select()
      .from(t)
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)));
    return row ? mapCodeGraphRow(row) : null;
  }

  async getCodeGraphById(serviceId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    const t = this.t.knowledgeCodeGraph;
    const [row] = await this.db
      .select()
      .from(t)
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId)));
    return row ? mapCodeGraphRow(row) : null;
  }

  async listCodeGraphs(serviceId: string, teamId: string, opts?: ListOpts): Promise<CodeGraphRow[]> {
    const t = this.t.knowledgeCodeGraph;
    const rows = await this.db
      .select()
      .from(t)
      .where(
        and(
          eq(t.serviceId, serviceId),
          eq(t.teamId, teamId),
          opts?.syncStatus ? eq(t.status, opts.syncStatus) : undefined,
        ),
      )
      .orderBy(desc(t.updatedAt))
      .limit(opts?.limit ?? 20)
      .offset(opts?.offset ?? 0);
    return rows.map(mapCodeGraphRow);
  }

  async countCodeGraphs(serviceId: string, teamId: string, opts?: CountOpts): Promise<number> {
    const t = this.t.knowledgeCodeGraph;
    const [res] = await this.db
      .select({ total: count() })
      .from(t)
      .where(
        and(
          eq(t.serviceId, serviceId),
          eq(t.teamId, teamId),
          opts?.syncStatus ? eq(t.status, opts.syncStatus) : undefined,
        ),
      );
    return Number(res?.total ?? 0);
  }

  async updateCodeGraphStatus(
    serviceId: string,
    codeGraphId: string,
    patch: CodeGraphStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean> {
    const t = this.t.knowledgeCodeGraph;
    const res = await this.db
      .update(t)
      .set(codeGraphStatusSet(patch))
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId), fenceCond(t, fence)));
    return (res.rowCount ?? 0) > 0;
  }

  async deleteCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<boolean> {
    const t = this.t.knowledgeCodeGraph;
    const res = await this.db
      .delete(t)
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)));
    return (res.rowCount ?? 0) > 0;
  }

  async updateCodeGraphMeta(
    serviceId: string,
    codeGraphId: string,
    patch: CodeGraphMetaPatch,
  ): Promise<CodeGraphRow | null> {
    const t = this.t.knowledgeCodeGraph;
    const [row] = await this.db
      .update(t)
      .set(codeGraphMetaSet(patch))
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId)))
      .returning();
    return row ? mapCodeGraphRow(row) : null;
  }

  async claimCodeGraphBuild(
    serviceId: string,
    teamId: string,
    codeGraphId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<CodeGraphRow>> {
    const t = this.t.knowledgeCodeGraph;
    return this.db.transaction(async (tx): Promise<ClaimResult<CodeGraphRow>> => {
      const [cur] = await tx
        .select()
        .from(t)
        .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)))
        .for("update");
      if (!cur) return { kind: "not_found" };
      const busy = inFlightStatus(cur.status);
      if (busy && !opts.replace) return { kind: "busy", status: busy, step: cur.internalStatus };
      const [row] = await tx
        .update(t)
        .set({ ...claimSet(opts.runId), version: sql`${t.version} + 1` })
        .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId)))
        .returning();
      if (!row) return { kind: "not_found" };
      return {
        kind: "claimed",
        row: mapCodeGraphRow(row),
        prevStatus: cur.status as WikiStatus,
        prevVersion: cur.version,
      };
    }, TX);
  }

  // ═══════════════════════ Wiki ═══════════════════════

  async createWiki(input: CreateWikiInput): Promise<CreateResult<WikiRow>> {
    const t = this.t.knowledgeWiki;
    const ts = nowIso();
    for (let attempt = 0; attempt < ID_RETRY; attempt++) {
      const [inserted] = await this.db
        .insert(t)
        .values({
          wikiId: genWikiId(),
          serviceId: input.service_id,
          teamId: input.team_id,
          name: input.name,
          sourceType: input.source_type ?? null,
          sourceUrl: input.source_url ?? null,
          ownerUserId: input.owner_user_id ?? null,
          userId: input.user_id ?? null,
          agentId: input.agent_id ?? null,
          taskId: input.task_id ?? null,
          visibility: input.visibility ?? "team",
          status: "draft",
          serviceUrl: input.service_url ?? null,
          version: WIKI_DATA_VERSION,
          createdAt: ts,
          updatedAt: ts,
        })
        .onConflictDoNothing()
        .returning();
      if (inserted) return { row: mapWikiRow(inserted), existed: false };

      const [live] = await this.db
        .select()
        .from(t)
        .where(
          and(
            eq(t.serviceId, input.service_id),
            eq(t.teamId, input.team_id),
            eq(t.name, input.name),
            isNull(t.deletedAt),
          ),
        );
      if (live) return { row: mapWikiRow(live), existed: true };
    }
    throw new Error("createWiki: failed to allocate unique id");
  }

  async getWiki(serviceId: string, teamId: string, wikiId: string): Promise<WikiRow | null> {
    const t = this.t.knowledgeWiki;
    const [row] = await this.db
      .select()
      .from(t)
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)));
    return row ? mapWikiRow(row) : null;
  }

  async getWikiById(serviceId: string, wikiId: string): Promise<WikiRow | null> {
    const t = this.t.knowledgeWiki;
    const [row] = await this.db
      .select()
      .from(t)
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)));
    return row ? mapWikiRow(row) : null;
  }

  async listWikis(serviceId: string, teamId: string, opts?: ListOpts): Promise<WikiRow[]> {
    const t = this.t.knowledgeWiki;
    const rows = await this.db
      .select()
      .from(t)
      .where(
        and(
          eq(t.serviceId, serviceId),
          eq(t.teamId, teamId),
          opts?.syncStatus ? eq(t.status, opts.syncStatus) : undefined,
        ),
      )
      .orderBy(desc(t.updatedAt))
      .limit(opts?.limit ?? 20)
      .offset(opts?.offset ?? 0);
    return rows.map(mapWikiRow);
  }

  async countWikis(serviceId: string, teamId: string, opts?: CountOpts): Promise<number> {
    const t = this.t.knowledgeWiki;
    const [res] = await this.db
      .select({ total: count() })
      .from(t)
      .where(
        and(
          eq(t.serviceId, serviceId),
          eq(t.teamId, teamId),
          opts?.syncStatus ? eq(t.status, opts.syncStatus) : undefined,
        ),
      );
    return Number(res?.total ?? 0);
  }

  async updateWikiStatus(
    serviceId: string,
    wikiId: string,
    patch: WikiStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean> {
    const t = this.t.knowledgeWiki;
    const res = await this.db
      .update(t)
      .set(wikiStatusSet(patch))
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId), fenceCond(t, fence)));
    return (res.rowCount ?? 0) > 0;
  }

  async deleteWiki(serviceId: string, teamId: string, wikiId: string): Promise<boolean> {
    const t = this.t.knowledgeWiki;
    const res = await this.db
      .delete(t)
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)));
    return (res.rowCount ?? 0) > 0;
  }

  async updateWikiMeta(serviceId: string, wikiId: string, patch: WikiMetaPatch): Promise<WikiRow | null> {
    const t = this.t.knowledgeWiki;
    const [row] = await this.db
      .update(t)
      .set(wikiMetaSet(patch))
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)))
      .returning();
    return row ? mapWikiRow(row) : null;
  }

  async claimWikiBuild(
    serviceId: string,
    teamId: string,
    wikiId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<WikiRow>> {
    const t = this.t.knowledgeWiki;
    return this.db.transaction(async (tx): Promise<ClaimResult<WikiRow>> => {
      const [cur] = await tx
        .select()
        .from(t)
        .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)))
        .for("update");
      if (!cur) return { kind: "not_found" };
      const busy = inFlightStatus(cur.status);
      if (busy && !opts.replace) return { kind: "busy", status: busy, step: cur.internalStatus };
      const [row] = await tx
        .update(t)
        .set({ ...claimSet(opts.runId), version: sql`${t.version} + 1` })
        .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)))
        .returning();
      if (!row) return { kind: "not_found" };
      return {
        kind: "claimed",
        row: mapWikiRow(row),
        prevStatus: cur.status as WikiStatus,
        prevVersion: cur.version,
      };
    }, TX);
  }

  async bumpWikiVersion(
    serviceId: string,
    wikiId: string,
    patch?: { last_sync_at?: string | null },
  ): Promise<boolean> {
    const t = this.t.knowledgeWiki;
    const res = await this.db
      .update(t)
      .set({
        version: sql`${t.version} + 1`,
        updatedAt: nowIso(),
        ...(patch?.last_sync_at !== undefined ? { lastSyncAt: patch.last_sync_at } : {}),
      })
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)));
    return (res.rowCount ?? 0) > 0;
  }

  // ═══════════════════════ Audit ═══════════════════════

  async appendWikiAudit(input: AuditLogInput): Promise<void> {
    await this.db.insert(this.t.knowledgeWikiAudit).values({
      wikiId: input.asset_id,
      serviceId: input.service_id ?? null,
      version: input.version,
      action: input.action,
      userId: input.user_id ?? null,
      agentId: input.agent_id ?? null,
      detail: input.detail ?? null,
      createdAt: nowIso(),
    });
  }

  async appendCodeGraphAudit(input: AuditLogInput): Promise<void> {
    await this.db.insert(this.t.knowledgeCodeGraphAudit).values({
      codeGraphId: input.asset_id,
      serviceId: input.service_id ?? null,
      version: input.version,
      action: input.action,
      userId: input.user_id ?? null,
      agentId: input.agent_id ?? null,
      detail: input.detail ?? null,
      createdAt: nowIso(),
    });
  }

  async listWikiAudit(serviceId: string, wikiId: string, limit = 20, offset = 0): Promise<AuditLogRow[]> {
    const t = this.t.knowledgeWikiAudit;
    const rows = await this.db
      .select()
      .from(t)
      .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)))
      .orderBy(desc(t.version), desc(t.id))
      .limit(limit)
      .offset(offset);
    return rows.map(mapWikiAuditRow);
  }

  async listCodeGraphAudit(
    serviceId: string,
    codeGraphId: string,
    limit = 20,
    offset = 0,
  ): Promise<AuditLogRow[]> {
    const t = this.t.knowledgeCodeGraphAudit;
    const rows = await this.db
      .select()
      .from(t)
      .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId)))
      .orderBy(desc(t.version), desc(t.id))
      .limit(limit)
      .offset(offset);
    return rows.map(mapCodeGraphAuditRow);
  }

  // ═══════════════════════ Restart Recovery ═══════════════════════

  /** All tenants: pending/processing → failed and build_run_id cleared, in one transaction. */
  async markInterruptedAsFailed(reason = "interrupted by restart"): Promise<number> {
    const { knowledgeCodeGraph: cg, knowledgeWiki: w } = this.t;
    const ts = nowIso();
    const set = { status: "failed", syncError: reason, buildRunId: null, updatedAt: ts };
    return this.db.transaction(async (tx) => {
      const a = await tx.update(cg).set(set).where(inArray(cg.status, IN_FLIGHT));
      const b = await tx.update(w).set(set).where(inArray(w.status, IN_FLIGHT));
      return (a.rowCount ?? 0) + (b.rowCount ?? 0);
    }, TX);
  }

  async listSyncedCodeGraphs(): Promise<SyncedCodeGraphRef[]> {
    const t = this.t.knowledgeCodeGraph;
    return this.db
      .select({ code_graph_id: t.codeGraphId, service_id: t.serviceId, team_id: t.teamId })
      .from(t)
      .where(and(eq(t.status, "ready"), isNull(t.deletedAt)));
  }

  async listSyncedWikis(): Promise<SyncedWikiRef[]> {
    const t = this.t.knowledgeWiki;
    return this.db
      .select({ wiki_id: t.wikiId, service_id: t.serviceId, team_id: t.teamId })
      .from(t)
      .where(and(eq(t.status, "ready"), isNull(t.deletedAt)));
  }
}
