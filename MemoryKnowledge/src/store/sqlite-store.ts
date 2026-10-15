/**
 * SqliteKnowledgeStore — SQLite/Drizzle implementation of IKnowledgeStore.
 *
 * Responsibilities:
 *   - code-graph / wiki asset CRUD (hard delete; soft-delete markers via deleted_at)
 *   - Multi-tenant isolation (001, phase 5): EVERY read/write is scoped by
 *     `service_id` (first parameter), then `team_id` where applicable. id-only
 *     accessors also filter service_id so a foreign tenant can never read/mutate
 *     another tenant's row (returns null/false → 404).
 *   - Global ID generation (wiki-/cg-) + idempotency: same
 *     (service_id, team_id, repo_url, branch) or (service_id, team_id, name)
 *     duplicate create returns existing.
 *   - Status state machine (atomic build claim + build_run_id fence) + restart recovery.
 *
 * Async contract: every method returns a
 * Promise, but the body is synchronous better-sqlite3 work, so each method is
 * one statement or one synchronous transaction — atomic by construction.
 */

import { eq, and, isNull, desc, sql, count, type SQL } from "drizzle-orm";
import type { Db } from "../db/client.js";
import {
  knowledgeCodeGraph,
  knowledgeWiki,
  knowledgeWikiAudit,
  knowledgeCodeGraphAudit,
  CODE_DATA_VERSION,
  WIKI_DATA_VERSION,
} from "../db/schema.js";
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
  IKnowledgeStore,
  CodeGraphRow,
  CreateCodeGraphInput,
  CodeGraphStatusPatch,
  CodeGraphMetaPatch,
  WikiRow,
  CreateWikiInput,
  WikiStatusPatch,
  WikiMetaPatch,
  AuditLogInput,
  AuditLogRow,
  BuildFence,
  ClaimOptions,
  ClaimResult,
  CreateResult,
  ListOpts,
  CountOpts,
  SyncedCodeGraphRef,
  SyncedWikiRef,
  WikiStatus,
} from "./types.js";

const ID_RETRY = 5;

type FencedTable = typeof knowledgeCodeGraph | typeof knowledgeWiki;

/** `AND build_run_id = $run` (or `IS NULL`) when a fence is given. */
function fenceCond(table: FencedTable, fence?: BuildFence): SQL | undefined {
  if (!fence) return undefined;
  return fence.runId === null ? isNull(table.buildRunId) : eq(table.buildRunId, fence.runId);
}

// ───────────────────────── Store ─────────────────────────

export class SqliteKnowledgeStore implements IKnowledgeStore {
  constructor(private readonly db: Db) {}

  // ═══════════════════════ Code-Graph ═══════════════════════

  /**
   * Idempotent create: `INSERT ... ON CONFLICT DO NOTHING RETURNING`, then
   * SELECT the live row → existed. Never inside a transaction. A row-less result
   * with no live row means a PK (random id) collision → retry with a new id.
   */
  async createCodeGraph(input: CreateCodeGraphInput): Promise<CreateResult<CodeGraphRow>> {
    const ts = nowIso();
    for (let attempt = 0; attempt < ID_RETRY; attempt++) {
      const inserted = this.db
        .insert(knowledgeCodeGraph)
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
        .returning()
        .get();
      if (inserted) return { row: mapCodeGraphRow(inserted), existed: false };

      const live = this.db
        .select()
        .from(knowledgeCodeGraph)
        .where(
          and(
            eq(knowledgeCodeGraph.serviceId, input.service_id),
            eq(knowledgeCodeGraph.teamId, input.team_id),
            eq(knowledgeCodeGraph.repoUrl, input.repo_url),
            eq(knowledgeCodeGraph.branch, input.branch),
            isNull(knowledgeCodeGraph.deletedAt),
          ),
        )
        .get();
      if (live) return { row: mapCodeGraphRow(live), existed: true };
    }
    throw new Error("createCodeGraph: failed to allocate unique id");
  }

  async getCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    const row = this.db
      .select()
      .from(knowledgeCodeGraph)
      .where(
        and(
          eq(knowledgeCodeGraph.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraph.serviceId, serviceId),
          eq(knowledgeCodeGraph.teamId, teamId),
        ),
      )
      .get();
    return row ? mapCodeGraphRow(row) : null;
  }

  /** id-only accessor — STILL scoped by service_id (cross-Memory leak guard, 001 §2.4). */
  async getCodeGraphById(serviceId: string, codeGraphId: string): Promise<CodeGraphRow | null> {
    const row = this.db
      .select()
      .from(knowledgeCodeGraph)
      .where(
        and(
          eq(knowledgeCodeGraph.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraph.serviceId, serviceId),
        ),
      )
      .get();
    return row ? mapCodeGraphRow(row) : null;
  }

  async listCodeGraphs(serviceId: string, teamId: string, opts?: ListOpts): Promise<CodeGraphRow[]> {
    const conditions: SQL[] = [
      eq(knowledgeCodeGraph.serviceId, serviceId),
      eq(knowledgeCodeGraph.teamId, teamId),
    ];
    if (opts?.syncStatus) {
      conditions.push(eq(knowledgeCodeGraph.status, opts.syncStatus));
    }
    const rows = this.db
      .select()
      .from(knowledgeCodeGraph)
      .where(and(...conditions))
      .orderBy(desc(knowledgeCodeGraph.updatedAt))
      .limit(opts?.limit ?? 20)
      .offset(opts?.offset ?? 0)
      .all();
    return rows.map(mapCodeGraphRow);
  }

  async countCodeGraphs(serviceId: string, teamId: string, opts?: CountOpts): Promise<number> {
    const conditions: SQL[] = [
      eq(knowledgeCodeGraph.serviceId, serviceId),
      eq(knowledgeCodeGraph.teamId, teamId),
    ];
    if (opts?.syncStatus) {
      conditions.push(eq(knowledgeCodeGraph.status, opts.syncStatus));
    }
    const result = this.db
      .select({ total: count() })
      .from(knowledgeCodeGraph)
      .where(and(...conditions))
      .get();
    return Number(result?.total ?? 0);
  }

  /** id-only mutation — scoped by service_id; UPDATE only (never inserts); optional build-run fence. */
  async updateCodeGraphStatus(
    serviceId: string,
    codeGraphId: string,
    patch: CodeGraphStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean> {
    const res = this.db
      .update(knowledgeCodeGraph)
      .set(codeGraphStatusSet(patch))
      .where(
        and(
          eq(knowledgeCodeGraph.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraph.serviceId, serviceId),
          fenceCond(knowledgeCodeGraph, fence),
        ),
      )
      .run();
    return res.changes > 0;
  }

  /** Hard delete; memory/team mismatch returns false. */
  async deleteCodeGraph(serviceId: string, teamId: string, codeGraphId: string): Promise<boolean> {
    const result = this.db
      .delete(knowledgeCodeGraph)
      .where(
        and(
          eq(knowledgeCodeGraph.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraph.serviceId, serviceId),
          eq(knowledgeCodeGraph.teamId, teamId),
        ),
      )
      .run();
    return result.changes > 0;
  }

  /** Update code-graph metadata (repo_name, summary) — one `UPDATE ... RETURNING`. memory mismatch → null. */
  async updateCodeGraphMeta(
    serviceId: string,
    codeGraphId: string,
    patch: CodeGraphMetaPatch,
  ): Promise<CodeGraphRow | null> {
    const row = this.db
      .update(knowledgeCodeGraph)
      .set(codeGraphMetaSet(patch))
      .where(
        and(
          eq(knowledgeCodeGraph.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraph.serviceId, serviceId),
        ),
      )
      .returning()
      .get();
    return row ? mapCodeGraphRow(row) : null;
  }

  async claimCodeGraphBuild(
    serviceId: string,
    teamId: string,
    codeGraphId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<CodeGraphRow>> {
    const t = knowledgeCodeGraph;
    return this.db.transaction(
      (tx): ClaimResult<CodeGraphRow> => {
        const cur = tx
          .select()
          .from(t)
          .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)))
          .get();
        if (!cur) return { kind: "not_found" };
        const busy = inFlightStatus(cur.status);
        if (busy && !opts.replace) return { kind: "busy", status: busy, step: cur.internalStatus };
        const row = tx
          .update(t)
          .set({ ...claimSet(opts.runId), version: sql`${t.version} + 1` })
          .where(and(eq(t.codeGraphId, codeGraphId), eq(t.serviceId, serviceId)))
          .returning()
          .get();
        if (!row) return { kind: "not_found" };
        return {
          kind: "claimed",
          row: mapCodeGraphRow(row),
          prevStatus: cur.status as WikiStatus,
          prevVersion: cur.version,
        };
      },
      { behavior: "immediate" },
    );
  }

  // ═══════════════════════ Wiki ═══════════════════════

  /** Idempotent create — see createCodeGraph. */
  async createWiki(input: CreateWikiInput): Promise<CreateResult<WikiRow>> {
    const ts = nowIso();
    for (let attempt = 0; attempt < ID_RETRY; attempt++) {
      const inserted = this.db
        .insert(knowledgeWiki)
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
        .returning()
        .get();
      if (inserted) return { row: mapWikiRow(inserted), existed: false };

      const live = this.db
        .select()
        .from(knowledgeWiki)
        .where(
          and(
            eq(knowledgeWiki.serviceId, input.service_id),
            eq(knowledgeWiki.teamId, input.team_id),
            eq(knowledgeWiki.name, input.name),
            isNull(knowledgeWiki.deletedAt),
          ),
        )
        .get();
      if (live) return { row: mapWikiRow(live), existed: true };
    }
    throw new Error("createWiki: failed to allocate unique id");
  }

  async getWiki(serviceId: string, teamId: string, wikiId: string): Promise<WikiRow | null> {
    const row = this.db
      .select()
      .from(knowledgeWiki)
      .where(
        and(
          eq(knowledgeWiki.wikiId, wikiId),
          eq(knowledgeWiki.serviceId, serviceId),
          eq(knowledgeWiki.teamId, teamId),
        ),
      )
      .get();
    return row ? mapWikiRow(row) : null;
  }

  /** id-only accessor — STILL scoped by service_id (cross-Memory leak guard, 001 §2.4). */
  async getWikiById(serviceId: string, wikiId: string): Promise<WikiRow | null> {
    const row = this.db
      .select()
      .from(knowledgeWiki)
      .where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId)))
      .get();
    return row ? mapWikiRow(row) : null;
  }

  async listWikis(serviceId: string, teamId: string, opts?: ListOpts): Promise<WikiRow[]> {
    const conditions: SQL[] = [
      eq(knowledgeWiki.serviceId, serviceId),
      eq(knowledgeWiki.teamId, teamId),
    ];
    if (opts?.syncStatus) {
      conditions.push(eq(knowledgeWiki.status, opts.syncStatus));
    }
    const rows = this.db
      .select()
      .from(knowledgeWiki)
      .where(and(...conditions))
      .orderBy(desc(knowledgeWiki.updatedAt))
      .limit(opts?.limit ?? 20)
      .offset(opts?.offset ?? 0)
      .all();
    return rows.map(mapWikiRow);
  }

  async countWikis(serviceId: string, teamId: string, opts?: CountOpts): Promise<number> {
    const conditions: SQL[] = [
      eq(knowledgeWiki.serviceId, serviceId),
      eq(knowledgeWiki.teamId, teamId),
    ];
    if (opts?.syncStatus) {
      conditions.push(eq(knowledgeWiki.status, opts.syncStatus));
    }
    const result = this.db
      .select({ total: count() })
      .from(knowledgeWiki)
      .where(and(...conditions))
      .get();
    return Number(result?.total ?? 0);
  }

  /** id-only mutation — scoped by service_id; UPDATE only (never inserts); optional build-run fence. */
  async updateWikiStatus(
    serviceId: string,
    wikiId: string,
    patch: WikiStatusPatch,
    fence?: BuildFence,
  ): Promise<boolean> {
    const res = this.db
      .update(knowledgeWiki)
      .set(wikiStatusSet(patch))
      .where(
        and(
          eq(knowledgeWiki.wikiId, wikiId),
          eq(knowledgeWiki.serviceId, serviceId),
          fenceCond(knowledgeWiki, fence),
        ),
      )
      .run();
    return res.changes > 0;
  }

  async deleteWiki(serviceId: string, teamId: string, wikiId: string): Promise<boolean> {
    const result = this.db
      .delete(knowledgeWiki)
      .where(
        and(
          eq(knowledgeWiki.wikiId, wikiId),
          eq(knowledgeWiki.serviceId, serviceId),
          eq(knowledgeWiki.teamId, teamId),
        ),
      )
      .run();
    return result.changes > 0;
  }

  /** Update wiki metadata (name, summary) — one `UPDATE ... RETURNING`. memory mismatch → null. */
  async updateWikiMeta(serviceId: string, wikiId: string, patch: WikiMetaPatch): Promise<WikiRow | null> {
    const row = this.db
      .update(knowledgeWiki)
      .set(wikiMetaSet(patch))
      .where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId)))
      .returning()
      .get();
    return row ? mapWikiRow(row) : null;
  }

  async claimWikiBuild(
    serviceId: string,
    teamId: string,
    wikiId: string,
    opts: ClaimOptions,
  ): Promise<ClaimResult<WikiRow>> {
    const t = knowledgeWiki;
    return this.db.transaction(
      (tx): ClaimResult<WikiRow> => {
        const cur = tx
          .select()
          .from(t)
          .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId), eq(t.teamId, teamId)))
          .get();
        if (!cur) return { kind: "not_found" };
        const busy = inFlightStatus(cur.status);
        if (busy && !opts.replace) return { kind: "busy", status: busy, step: cur.internalStatus };
        const row = tx
          .update(t)
          .set({ ...claimSet(opts.runId), version: sql`${t.version} + 1` })
          .where(and(eq(t.wikiId, wikiId), eq(t.serviceId, serviceId)))
          .returning()
          .get();
        if (!row) return { kind: "not_found" };
        return {
          kind: "claimed",
          row: mapWikiRow(row),
          prevStatus: cur.status as WikiStatus,
          prevVersion: cur.version,
        };
      },
      { behavior: "immediate" },
    );
  }

  async bumpWikiVersion(
    serviceId: string,
    wikiId: string,
    patch?: { last_sync_at?: string | null },
  ): Promise<boolean> {
    const res = this.db
      .update(knowledgeWiki)
      .set({
        version: sql`${knowledgeWiki.version} + 1`,
        updatedAt: nowIso(),
        ...(patch?.last_sync_at !== undefined ? { lastSyncAt: patch.last_sync_at } : {}),
      })
      .where(and(eq(knowledgeWiki.wikiId, wikiId), eq(knowledgeWiki.serviceId, serviceId)))
      .run();
    return res.changes > 0;
  }

  // ═══════════════════════ Audit ═══════════════════════

  async appendWikiAudit(input: AuditLogInput): Promise<void> {
    this.db
      .insert(knowledgeWikiAudit)
      .values({
        wikiId: input.asset_id,
        serviceId: input.service_id ?? null,
        version: input.version,
        action: input.action,
        userId: input.user_id ?? null,
        agentId: input.agent_id ?? null,
        detail: input.detail ?? null,
        createdAt: nowIso(),
      })
      .run();
  }

  async appendCodeGraphAudit(input: AuditLogInput): Promise<void> {
    this.db
      .insert(knowledgeCodeGraphAudit)
      .values({
        codeGraphId: input.asset_id,
        serviceId: input.service_id ?? null,
        version: input.version,
        action: input.action,
        userId: input.user_id ?? null,
        agentId: input.agent_id ?? null,
        detail: input.detail ?? null,
        createdAt: nowIso(),
      })
      .run();
  }

  async listWikiAudit(serviceId: string, wikiId: string, limit = 20, offset = 0): Promise<AuditLogRow[]> {
    const rows = this.db
      .select()
      .from(knowledgeWikiAudit)
      .where(and(eq(knowledgeWikiAudit.wikiId, wikiId), eq(knowledgeWikiAudit.serviceId, serviceId)))
      .orderBy(desc(knowledgeWikiAudit.version), desc(knowledgeWikiAudit.id))
      .limit(limit)
      .offset(offset)
      .all();
    return rows.map(mapWikiAuditRow);
  }

  async listCodeGraphAudit(
    serviceId: string,
    codeGraphId: string,
    limit = 20,
    offset = 0,
  ): Promise<AuditLogRow[]> {
    const rows = this.db
      .select()
      .from(knowledgeCodeGraphAudit)
      .where(
        and(
          eq(knowledgeCodeGraphAudit.codeGraphId, codeGraphId),
          eq(knowledgeCodeGraphAudit.serviceId, serviceId),
        ),
      )
      .orderBy(desc(knowledgeCodeGraphAudit.version), desc(knowledgeCodeGraphAudit.id))
      .limit(limit)
      .offset(offset)
      .all();
    return rows.map(mapCodeGraphAuditRow);
  }

  // ═══════════════════════ Restart Recovery ═══════════════════════

  /**
   * Sweep all non-terminal (pending/processing) assets to failed and clear
   * build_run_id, across all tenants, in one transaction. After restart, the
   * in-memory queue is lost; this makes them visible to the control plane, and
   * the cleared run id fences off any write of the old process (K-zombie).
   * @returns total affected rows (code + wiki combined).
   */
  async markInterruptedAsFailed(reason = "interrupted by restart"): Promise<number> {
    const ts = nowIso();
    return this.db.transaction(
      (tx) => {
        const a = tx
          .update(knowledgeCodeGraph)
          .set({ status: "failed", syncError: reason, buildRunId: null, updatedAt: ts })
          .where(sql`status IN ('pending','processing')`)
          .run();
        const b = tx
          .update(knowledgeWiki)
          .set({ status: "failed", syncError: reason, buildRunId: null, updatedAt: ts })
          .where(sql`status IN ('pending','processing')`)
          .run();
        return a.changes + b.changes;
      },
      { behavior: "immediate" },
    );
  }

  /** All ready code-graphs (with service_id) so module.ts can rebuild per-tenant dirs. */
  async listSyncedCodeGraphs(): Promise<SyncedCodeGraphRef[]> {
    return this.db
      .select({
        code_graph_id: knowledgeCodeGraph.codeGraphId,
        service_id: knowledgeCodeGraph.serviceId,
        team_id: knowledgeCodeGraph.teamId,
      })
      .from(knowledgeCodeGraph)
      .where(and(eq(knowledgeCodeGraph.status, "ready"), isNull(knowledgeCodeGraph.deletedAt)))
      .all();
  }

  async listSyncedWikis(): Promise<SyncedWikiRef[]> {
    return this.db
      .select({
        wiki_id: knowledgeWiki.wikiId,
        service_id: knowledgeWiki.serviceId,
        team_id: knowledgeWiki.teamId,
      })
      .from(knowledgeWiki)
      .where(and(eq(knowledgeWiki.status, "ready"), isNull(knowledgeWiki.deletedAt)))
      .all();
  }
}
