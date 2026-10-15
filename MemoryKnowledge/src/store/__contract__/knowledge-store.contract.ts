/**
 * IKnowledgeStore contract suite.
 *
 * Every dialect runs the same cases: SQLite `:memory:` now, Postgres later.
 * Also covers the two small sibling stores the knowledge service reads in one
 * request: the one-row credential read and the LLM binding upsert.
 */

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { IKnowledgeStore } from "../types.js";
import type { ICredentialStore } from "../../source-auth/types.js";
import type { ILlmBindingStore } from "../llm-binding-store.js";

export interface KnowledgeStoreInstance {
  store: IKnowledgeStore;
  credentialStore: ICredentialStore;
  llmBindingStore: ILlmBindingStore;
  close(): Promise<void>;
}

export interface KnowledgeStoreHarness {
  name: string;
  /** A fresh, migrated, empty database per test. */
  open(): Promise<KnowledgeStoreInstance>;
}

const S1 = "svc-1";
const S2 = "svc-2";
const T1 = "team-1";
const T2 = "team-2";

export function runKnowledgeStoreContract(harness: KnowledgeStoreHarness): void {
  describe(`IKnowledgeStore contract (${harness.name})`, () => {
    let inst: KnowledgeStoreInstance;
    let store: IKnowledgeStore;

    beforeEach(async () => {
      inst = await harness.open();
      store = inst.store;
    });
    afterEach(async () => {
      await inst.close();
    });

    const newWiki = async (name = "w", serviceId = S1, teamId = T1) =>
      (await store.createWiki({ service_id: serviceId, team_id: teamId, name })).row;
    const newCg = async (repo = "https://example.com/r.git", serviceId = S1, teamId = T1, runId?: string) =>
      (
        await store.createCodeGraph({
          service_id: serviceId,
          team_id: teamId,
          repo_url: repo,
          branch: "main",
          build_run_id: runId,
        })
      ).row;

    // ─────────────── create / idempotency ───────────────

    describe("create", () => {
      it("createWiki inserts a draft row at version 0", async () => {
        const res = await store.createWiki({ service_id: S1, team_id: T1, name: "a", user_id: "u1" });
        expect(res.existed).toBe(false);
        expect(res.row.wiki_id).toMatch(/^wiki-/);
        expect(res.row.status).toBe("draft");
        expect(res.row.version).toBe(0);
        expect(res.row.service_id).toBe(S1);
        expect(res.row.user_id).toBe("u1");
      });

      it("createWiki with an existing name returns the live row as existed (not an error)", async () => {
        const a = await store.createWiki({ service_id: S1, team_id: T1, name: "dup" });
        const b = await store.createWiki({ service_id: S1, team_id: T1, name: "dup" });
        expect(b.existed).toBe(true);
        expect(b.row.wiki_id).toBe(a.row.wiki_id);
      });

      it("concurrent createWiki with one name gives exactly one row", async () => {
        const results = await Promise.all([
          store.createWiki({ service_id: S1, team_id: T1, name: "race" }),
          store.createWiki({ service_id: S1, team_id: T1, name: "race" }),
          store.createWiki({ service_id: S1, team_id: T1, name: "race" }),
        ]);
        expect(new Set(results.map((r) => r.row.wiki_id)).size).toBe(1);
        expect(results.filter((r) => !r.existed)).toHaveLength(1);
        expect(await store.countWikis(S1, T1)).toBe(1);
      });

      it("same name in another team or service is a different wiki", async () => {
        const a = await newWiki("n", S1, T1);
        const b = await newWiki("n", S1, T2);
        const c = await newWiki("n", S2, T1);
        expect(new Set([a.wiki_id, b.wiki_id, c.wiki_id]).size).toBe(3);
      });

      it("createCodeGraph inserts pending; same repo+branch → existed", async () => {
        const a = await store.createCodeGraph({ service_id: S1, team_id: T1, repo_url: "u", branch: "main" });
        expect(a.existed).toBe(false);
        expect(a.row.code_graph_id).toMatch(/^cg-/);
        expect(a.row.status).toBe("pending");
        expect(a.row.repo_name).toBe("");
        const b = await store.createCodeGraph({ service_id: S1, team_id: T1, repo_url: "u", branch: "main" });
        expect(b.existed).toBe(true);
        expect(b.row.code_graph_id).toBe(a.row.code_graph_id);
        const c = await store.createCodeGraph({ service_id: S1, team_id: T1, repo_url: "u", branch: "dev" });
        expect(c.existed).toBe(false);
      });

      it("concurrent createCodeGraph with one repo+branch gives exactly one row", async () => {
        const input = { service_id: S1, team_id: T1, repo_url: "u", branch: "main" };
        const results = await Promise.all([store.createCodeGraph(input), store.createCodeGraph(input)]);
        expect(new Set(results.map((r) => r.row.code_graph_id)).size).toBe(1);
        expect(await store.countCodeGraphs(S1, T1)).toBe(1);
      });
    });

    // ─────────────── reads / tenant isolation ───────────────

    describe("reads and tenant isolation", () => {
      it("id-only accessors return null for a foreign serviceId", async () => {
        const w = await newWiki();
        const cg = await newCg();
        expect(await store.getWikiById(S2, w.wiki_id)).toBeNull();
        expect(await store.getCodeGraphById(S2, cg.code_graph_id)).toBeNull();
        expect((await store.getWikiById(S1, w.wiki_id))?.wiki_id).toBe(w.wiki_id);
        expect((await store.getCodeGraphById(S1, cg.code_graph_id))?.code_graph_id).toBe(cg.code_graph_id);
      });

      it("team-scoped accessors return null for a wrong team or service", async () => {
        const w = await newWiki();
        const cg = await newCg();
        expect(await store.getWiki(S1, T2, w.wiki_id)).toBeNull();
        expect(await store.getWiki(S2, T1, w.wiki_id)).toBeNull();
        expect(await store.getCodeGraph(S1, T2, cg.code_graph_id)).toBeNull();
        expect(await store.getCodeGraph(S2, T1, cg.code_graph_id)).toBeNull();
      });

      it("id-only writes and claims change nothing for a foreign serviceId", async () => {
        const w = await newWiki();
        expect(await store.updateWikiStatus(S2, w.wiki_id, { status: "failed" })).toBe(false);
        expect(await store.updateWikiMeta(S2, w.wiki_id, { name: "x" })).toBeNull();
        expect(await store.bumpWikiVersion(S2, w.wiki_id)).toBe(false);
        expect(await store.claimWikiBuild(S2, T1, w.wiki_id, { runId: "r" })).toEqual({ kind: "not_found" });
        expect(await store.deleteWiki(S2, T1, w.wiki_id)).toBe(false);
        const cg = await newCg();
        expect(await store.updateCodeGraphStatus(S2, cg.code_graph_id, { status: "failed" })).toBe(false);
        expect(await store.updateCodeGraphMeta(S2, cg.code_graph_id, { repo_name: "x" })).toBeNull();
        expect(await store.deleteCodeGraph(S2, T1, cg.code_graph_id)).toBe(false);
        const row = await store.getWikiById(S1, w.wiki_id);
        expect(row?.status).toBe("draft");
        expect(row?.name).toBe("w");
        expect(row?.version).toBe(0);
      });

      it("list filters by service, team and status; count is a JS number", async () => {
        const a = await newWiki("a");
        await newWiki("b");
        await newWiki("c", S1, T2);
        await newWiki("d", S2, T1);
        await store.updateWikiStatus(S1, a.wiki_id, { status: "ready" });

        const all = await store.listWikis(S1, T1);
        expect(all.map((r) => r.name).sort()).toEqual(["a", "b"]);
        const total = await store.countWikis(S1, T1);
        expect(typeof total).toBe("number");
        expect(total).toBe(2);
        expect(await store.countWikis(S1, T1, { syncStatus: "ready" })).toBe(1);
        expect((await store.listWikis(S1, T1, { syncStatus: "ready" })).map((r) => r.name)).toEqual(["a"]);
        expect(await store.listWikis(S1, T1, { limit: 1 })).toHaveLength(1);
        expect(await store.listWikis(S1, T1, { limit: 10, offset: 1 })).toHaveLength(1);
        expect(await store.countWikis("nobody", T1)).toBe(0);

        await newCg("r1");
        await newCg("r2");
        await newCg("r3", S2);
        const cgTotal = await store.countCodeGraphs(S1, T1);
        expect(typeof cgTotal).toBe("number");
        expect(cgTotal).toBe(2);
        expect(await store.listCodeGraphs(S1, T1)).toHaveLength(2);
        expect(await store.countCodeGraphs(S1, T1, { syncStatus: "ready" })).toBe(0);
        expect(await store.listCodeGraphs(S1, T1, { limit: 1, offset: 1 })).toHaveLength(1);
      });
    });

    // ─────────────── status / meta / delete ───────────────

    describe("status, meta and delete", () => {
      it("updateWikiStatus writes only the given fields and returns true", async () => {
        const w = await newWiki();
        expect(
          await store.updateWikiStatus(S1, w.wiki_id, { status: "ready", page_count: 3, summary: "s" }),
        ).toBe(true);
        const row = await store.getWikiById(S1, w.wiki_id);
        expect(row).toMatchObject({ status: "ready", page_count: 3, summary: "s", name: "w", version: 0 });
        expect(row!.updated_at >= w.updated_at).toBe(true);
      });

      it("updateCodeGraphStatus writes only the given fields", async () => {
        const cg = await newCg();
        expect(
          await store.updateCodeGraphStatus(S1, cg.code_graph_id, {
            status: "ready",
            commit_hash: "abc",
            stats_json: '{"files":1}',
          }),
        ).toBe(true);
        expect(await store.getCodeGraphById(S1, cg.code_graph_id)).toMatchObject({
          status: "ready",
          commit_hash: "abc",
          stats_json: '{"files":1}',
          repo_url: cg.repo_url,
        });
      });

      it("a status write after a hard delete changes nothing and never inserts", async () => {
        const w = await newWiki();
        expect(await store.deleteWiki(S1, T1, w.wiki_id)).toBe(true);
        expect(await store.updateWikiStatus(S1, w.wiki_id, { status: "ready" })).toBe(false);
        expect(await store.getWikiById(S1, w.wiki_id)).toBeNull();
        expect(await store.countWikis(S1, T1)).toBe(0);

        const cg = await newCg();
        expect(await store.deleteCodeGraph(S1, T1, cg.code_graph_id)).toBe(true);
        expect(await store.updateCodeGraphStatus(S1, cg.code_graph_id, { status: "ready" })).toBe(false);
        expect(await store.getCodeGraphById(S1, cg.code_graph_id)).toBeNull();
      });

      it("delete is team-scoped and returns false the second time", async () => {
        const w = await newWiki();
        expect(await store.deleteWiki(S1, T2, w.wiki_id)).toBe(false);
        expect(await store.deleteWiki(S1, T1, w.wiki_id)).toBe(true);
        expect(await store.deleteWiki(S1, T1, w.wiki_id)).toBe(false);
        const cg = await newCg();
        expect(await store.deleteCodeGraph(S1, T2, cg.code_graph_id)).toBe(false);
        expect(await store.deleteCodeGraph(S1, T1, cg.code_graph_id)).toBe(true);
        expect(await store.deleteCodeGraph(S1, T1, cg.code_graph_id)).toBe(false);
      });

      it("updateWikiMeta / updateCodeGraphMeta return the updated row, null when missing", async () => {
        const w = await newWiki();
        const updated = await store.updateWikiMeta(S1, w.wiki_id, { name: "renamed", summary: null });
        expect(updated).toMatchObject({ wiki_id: w.wiki_id, name: "renamed", summary: null });
        expect(await store.updateWikiMeta(S1, "wiki-missing", { name: "x" })).toBeNull();

        const cg = await newCg();
        const cgUpdated = await store.updateCodeGraphMeta(S1, cg.code_graph_id, { repo_name: "repo", summary: "s" });
        expect(cgUpdated).toMatchObject({ code_graph_id: cg.code_graph_id, repo_name: "repo", summary: "s" });
        await store.deleteCodeGraph(S1, T1, cg.code_graph_id);
        expect(await store.updateCodeGraphMeta(S1, cg.code_graph_id, { repo_name: "x" })).toBeNull();
      });

      it("concurrent updateWikiMeta and deleteWiki never leave a visible wiki", async () => {
        const w = await newWiki();
        const [meta, deleted] = await Promise.all([
          store.updateWikiMeta(S1, w.wiki_id, { name: "x" }),
          store.deleteWiki(S1, T1, w.wiki_id),
        ]);
        expect(deleted).toBe(true);
        if (meta) expect(meta.name).toBe("x");
        expect(await store.getWikiById(S1, w.wiki_id)).toBeNull();
      });
    });

    // ─────────────── build claim + run fence ───────────────

    describe("build claim and run fence", () => {
      it("claimWikiBuild: not_found for a missing id or wrong team", async () => {
        const w = await newWiki();
        expect(await store.claimWikiBuild(S1, T1, "wiki-missing", { runId: "r" })).toEqual({ kind: "not_found" });
        expect(await store.claimWikiBuild(S1, T2, w.wiki_id, { runId: "r" })).toEqual({ kind: "not_found" });
      });

      it("claimWikiBuild: draft → pending, version + 1, then busy", async () => {
        const w = await newWiki();
        await store.updateWikiStatus(S1, w.wiki_id, { sync_error: "old", internal_status: "x" });
        const c1 = await store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "run-1" });
        expect(c1.kind).toBe("claimed");
        if (c1.kind !== "claimed") return;
        expect(c1.prevStatus).toBe("draft");
        expect(c1.prevVersion).toBe(0);
        expect(c1.row).toMatchObject({ status: "pending", version: 1, sync_error: null, internal_status: null });

        await store.updateWikiStatus(S1, w.wiki_id, { status: "processing", internal_status: "scanning" }, { runId: "run-1" });
        expect(await store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "run-2" })).toEqual({
          kind: "busy",
          status: "processing",
          step: "scanning",
        });
        expect((await store.getWikiById(S1, w.wiki_id))?.version).toBe(1);
      });

      it("claimWikiBuild with replace takes over an in-flight build and fences the old run", async () => {
        const w = await newWiki();
        await store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "run-1" });
        const c2 = await store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "run-2", replace: true });
        expect(c2.kind).toBe("claimed");
        if (c2.kind !== "claimed") return;
        expect(c2.prevStatus).toBe("pending");
        expect(c2.prevVersion).toBe(1);
        expect(c2.row.version).toBe(2);

        // Old run: every fenced write changes 0 rows.
        expect(
          await store.updateWikiStatus(S1, w.wiki_id, { status: "processing", internal_status: "late" }, { runId: "run-1" }),
        ).toBe(false);
        expect(
          await store.updateWikiStatus(S1, w.wiki_id, { status: "ready", build_run_id: null }, { runId: "run-1" }),
        ).toBe(false);
        expect(await store.getWikiById(S1, w.wiki_id)).toMatchObject({ status: "pending", internal_status: null });

        // New run owns the row; "idle" fence (runId null) fails while it is in flight.
        expect(await store.updateWikiStatus(S1, w.wiki_id, { summary: "s" }, { runId: null })).toBe(false);
        expect(
          await store.updateWikiStatus(S1, w.wiki_id, { status: "ready", build_run_id: null }, { runId: "run-2" }),
        ).toBe(true);
        // Terminal write cleared the run id: the run's own later writes are fenced off, idle writes pass.
        expect(await store.updateWikiStatus(S1, w.wiki_id, { status: "processing" }, { runId: "run-2" })).toBe(false);
        expect(await store.updateWikiStatus(S1, w.wiki_id, { summary: "s" }, { runId: null })).toBe(true);
        expect(await store.getWikiById(S1, w.wiki_id)).toMatchObject({ status: "ready", summary: "s", version: 2 });
      });

      it("concurrent claims: exactly one claimed, version + 1", async () => {
        const w = await newWiki();
        await store.updateWikiStatus(S1, w.wiki_id, { status: "ready" });
        const results = await Promise.all([
          store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "a" }),
          store.claimWikiBuild(S1, T1, w.wiki_id, { runId: "b" }),
        ]);
        expect(results.filter((r) => r.kind === "claimed")).toHaveLength(1);
        expect(results.filter((r) => r.kind === "busy")).toHaveLength(1);
        expect((await store.getWikiById(S1, w.wiki_id))?.version).toBe(1);
      });

      it("claimCodeGraphBuild: create is the first claim; busy until terminal; then version + 1", async () => {
        const cg = await newCg("u", S1, T1, "run-0");
        expect(await store.claimCodeGraphBuild(S1, T1, cg.code_graph_id, { runId: "r" })).toEqual({
          kind: "busy",
          status: "pending",
          step: null,
        });
        expect(await store.claimCodeGraphBuild(S1, T2, cg.code_graph_id, { runId: "r" })).toEqual({ kind: "not_found" });
        expect(
          await store.updateCodeGraphStatus(S1, cg.code_graph_id, { status: "ready", build_run_id: null }, { runId: "run-0" }),
        ).toBe(true);
        const c = await store.claimCodeGraphBuild(S1, T1, cg.code_graph_id, { runId: "run-1" });
        expect(c.kind).toBe("claimed");
        if (c.kind !== "claimed") return;
        expect(c.prevStatus).toBe("ready");
        expect(c.prevVersion).toBe(0);
        expect(c.row).toMatchObject({ status: "pending", version: 1 });
        expect(
          await store.updateCodeGraphStatus(S1, cg.code_graph_id, { internal_status: "x" }, { runId: "run-0" }),
        ).toBe(false);
        expect(
          await store.updateCodeGraphStatus(S1, cg.code_graph_id, { internal_status: "x" }, { runId: "run-1" }),
        ).toBe(true);
      });

      it("bumpWikiVersion increments in SQL (no lost update)", async () => {
        const w = await newWiki();
        await Promise.all([
          store.bumpWikiVersion(S1, w.wiki_id, { last_sync_at: "2026-01-01T00:00:00.000Z" }),
          store.bumpWikiVersion(S1, w.wiki_id),
        ]);
        const row = await store.getWikiById(S1, w.wiki_id);
        expect(row?.version).toBe(2);
        expect(row?.last_sync_at).toBe("2026-01-01T00:00:00.000Z");
        expect(await store.bumpWikiVersion(S1, "wiki-missing")).toBe(false);
      });
    });

    // ─────────────── audit ───────────────

    describe("audit", () => {
      it("appends and lists wiki audit newest version first, tenant-scoped", async () => {
        await store.appendWikiAudit({ service_id: S1, asset_id: "wiki-a", version: 1, action: "create" });
        await store.appendWikiAudit({ service_id: S1, asset_id: "wiki-a", version: 2, action: "ingest", user_id: "u", detail: "d" });
        await store.appendWikiAudit({ service_id: S2, asset_id: "wiki-a", version: 3, action: "ready" });
        const rows = await store.listWikiAudit(S1, "wiki-a");
        expect(rows.map((r) => r.version)).toEqual([2, 1]);
        expect(rows[0]).toMatchObject({ action: "ingest", user_id: "u", detail: "d", asset_id: "wiki-a", service_id: S1 });
        expect(typeof rows[0].id).toBe("number");
        expect(await store.listWikiAudit(S1, "wiki-a", 1, 1)).toHaveLength(1);
      });

      it("appends and lists code-graph audit", async () => {
        await store.appendCodeGraphAudit({ service_id: S1, asset_id: "cg-a", version: 0, action: "create" });
        await store.appendCodeGraphAudit({ service_id: S1, asset_id: "cg-a", version: 1, action: "failed", detail: "x" });
        const rows = await store.listCodeGraphAudit(S1, "cg-a");
        expect(rows.map((r) => r.action)).toEqual(["failed", "create"]);
        expect(await store.listCodeGraphAudit(S2, "cg-a")).toEqual([]);
      });
    });

    // ─────────────── recovery ───────────────

    describe("restart recovery", () => {
      it("markInterruptedAsFailed fails pending/processing assets of all tenants and clears the run", async () => {
        const w1 = await newWiki("a", S1);
        const w2 = await newWiki("b", S2);
        const w3 = await newWiki("c", S1);
        await store.claimWikiBuild(S1, T1, w1.wiki_id, { runId: "r1" });
        await store.claimWikiBuild(S2, T1, w2.wiki_id, { runId: "r2" });
        await store.updateWikiStatus(S2, w2.wiki_id, { status: "processing" }, { runId: "r2" });
        const cg = await newCg("u", S1, T1, "r3");
        const ready = await newCg("v", S2);
        await store.updateCodeGraphStatus(S2, ready.code_graph_id, { status: "ready" });

        expect(await store.markInterruptedAsFailed("boom")).toBe(3);
        expect(await store.getWikiById(S1, w1.wiki_id)).toMatchObject({ status: "failed", sync_error: "boom" });
        expect(await store.getWikiById(S2, w2.wiki_id)).toMatchObject({ status: "failed" });
        expect((await store.getWikiById(S1, w3.wiki_id))?.status).toBe("draft");
        expect((await store.getCodeGraphById(S1, cg.code_graph_id))?.status).toBe("failed");
        expect((await store.getCodeGraphById(S2, ready.code_graph_id))?.status).toBe("ready");

        // K-zombie: a write of the dead run commits after recovery → 0 rows.
        expect(await store.updateWikiStatus(S1, w1.wiki_id, { status: "processing" }, { runId: "r1" })).toBe(false);
        expect(await store.updateCodeGraphStatus(S1, cg.code_graph_id, { status: "processing" }, { runId: "r3" })).toBe(false);
        // ...and a new claim succeeds.
        expect((await store.claimWikiBuild(S1, T1, w1.wiki_id, { runId: "r4" })).kind).toBe("claimed");
        expect(await store.markInterruptedAsFailed()).toBe(1);
      });

      it("listSynced* returns only ready assets with their tenant", async () => {
        const w = await newWiki("a", S1);
        await newWiki("b", S2);
        await store.updateWikiStatus(S1, w.wiki_id, { status: "ready" });
        const cg = await newCg("u", S2, T2);
        await store.updateCodeGraphStatus(S2, cg.code_graph_id, { status: "ready" });
        await newCg("v");
        expect(await store.listSyncedWikis()).toEqual([{ wiki_id: w.wiki_id, service_id: S1, team_id: T1 }]);
        expect(await store.listSyncedCodeGraphs()).toEqual([
          { code_graph_id: cg.code_graph_id, service_id: S2, team_id: T2 },
        ]);
      });
    });

    // ─────────────── credential + llm binding ───────────────

    describe("credential store", () => {
      it("getWithProvider reads secret and provider id from one row; delete is idempotent", async () => {
        const cs = inst.credentialStore;
        const ref = { type: "wiki" as const, serviceId: S1, resourceId: "wiki-x" };
        expect(await cs.getWithProvider(ref)).toBeNull();
        await cs.put(ref, { kind: "basic", secret: "s3cret", username: "bob", extra: { a: 1 } }, "iwiki", "u1");
        expect(await cs.getWithProvider(ref)).toEqual({
          credential: { kind: "basic", secret: "s3cret", username: "bob", extra: { a: 1 } },
          provider_id: "iwiki",
        });
        expect(await cs.get({ ...ref, serviceId: S2 })).toBeNull();
        expect((await cs.status(ref))?.provider_id).toBe("iwiki");
        expect(await cs.listAllByType("wiki")).toEqual([{ serviceId: S1, resourceId: "wiki-x" }]);
        await cs.recordVerify(ref);
        expect((await cs.status(ref))?.last_verified_at).not.toBeNull();
        expect(await cs.delete(ref)).toBe(true);
        expect(await cs.delete(ref)).toBe(false);
        expect(await cs.status(ref)).toBeNull();
      });

      it("put rejects an empty secret", async () => {
        const ref = { type: "code-graph" as const, serviceId: S1, resourceId: "cg-x" };
        await expect(inst.credentialStore.put(ref, { kind: "bearer", secret: "" }, "p")).rejects.toThrow();
      });
    });

    describe("llm binding store", () => {
      it("upsert keeps api_key when undefined, clears it when null", async () => {
        const ls = inst.llmBindingStore;
        expect(await ls.get(S1)).toBeNull();
        expect(await ls.status(S1)).toEqual({ bound: false, mode: null, enabled: false });
        await ls.upsert(S1, { mode: "proxy", proxy_base_url: "http://p", api_key: "k1" });
        const kept = await ls.upsert(S1, { mode: "proxy", proxy_base_url: "http://p2" });
        expect(kept).toMatchObject({ api_key: "k1", proxy_base_url: "http://p2", enabled: true });
        const cleared = await ls.upsert(S1, { mode: "byo", base_url: "http://b", api_key: null, enabled: false });
        expect(cleared).toMatchObject({ api_key: null, mode: "byo", enabled: false });
        expect(await ls.status(S1)).toEqual({ bound: true, mode: "byo", enabled: false });
        expect(await ls.listAll()).toHaveLength(1);
      });
    });
  });
}
