/**
 * Service-level race tests through YieldingKnowledgeStore on SQLite.
 * Each test drives WikiService / CodeGraphService with an interleaving that the
 * synchronous store could never produce; they run on Postgres too.
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createDb } from "../../db/client.js";
import { createCredentialStore } from "../../source-auth/credential-store.js";
import type { ICredentialStore } from "../../source-auth/types.js";
import { BuildQueue } from "../build-queue.js";
import { CodeGraphService, type CodeGraphWorker } from "../code-graph-service.js";
import { SqliteKnowledgeStore } from "../sqlite-store.js";
import type { IKnowledgeStore, WikiStatusPatch } from "../types.js";
import { WikiService, type WikiWorker } from "../wiki-service.js";
import { createYieldingKnowledgeStore } from "./yielding-knowledge-store.js";

const S = "svc";
const T = "team";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe("knowledge build races (SQLite via YieldingKnowledgeStore)", () => {
  let dataRoot: string;
  let closeDb: () => void;
  let base: SqliteKnowledgeStore;
  let credentialStore: ICredentialStore;
  const warn = vi.fn<(msg: string) => void>();

  beforeEach(() => {
    dataRoot = mkdtempSync(join(tmpdir(), "mk-race-"));
    const { db, raw } = createDb({ path: ":memory:" });
    closeDb = () => raw.close();
    base = new SqliteKnowledgeStore(db);
    credentialStore = createCredentialStore({ db });
    warn.mockReset();
  });
  afterEach(() => {
    closeDb();
    rmSync(dataRoot, { recursive: true, force: true });
  });

  const wikiService = (store: IKnowledgeStore, worker: WikiWorker) =>
    new WikiService({ store, dataRoot, worker, queue: new BuildQueue(), logger: { warn }, credentialStore });

  async function readyWiki(svc: WikiService): Promise<string> {
    const { row } = await svc.create({ service_id: S, team_id: T, name: "w" });
    await base.updateWikiStatus(S, row.wiki_id, { status: "ready" });
    return row.wiki_id;
  }

  it("claim race: user ingest vs auto-sync → exactly one claimed, version + 1", async () => {
    const svc = wikiService(createYieldingKnowledgeStore(base), async () => ({ pageCount: 1 }));
    const id = await readyWiki(svc);
    const before = (await base.getWikiById(S, id))!.version;

    const [user, auto] = await Promise.all([svc.ingest(S, T, id, "u1"), svc.sync(S, T, id)]);
    const kinds = [user.kind, auto.kind].sort();
    expect(kinds).toEqual(["busy", "ok"]);
    expect((await base.getWikiById(S, id))!.version).toBe(before + 1);

    await svc.onIdle();
    expect(await base.getWikiById(S, id)).toMatchObject({ status: "ready", version: before + 1 });
    const audit = await base.listWikiAudit(S, id, 50);
    expect(audit.filter((a) => a.action === "ingest")).toHaveLength(1);
  });

  it("claim race on code-graph: two syncs → one ok, one busy, version + 1", async () => {
    const worker: CodeGraphWorker = async () => ({ commitHash: "c" });
    const svc = new CodeGraphService({
      store: createYieldingKnowledgeStore(base),
      dataRoot,
      worker,
      queue: new BuildQueue(),
      credentialStore,
    });
    const { row } = await svc.create({ service_id: S, team_id: T, repo_url: "https://x/r.git", branch: "main" });
    await svc.onIdle();
    expect((await base.getCodeGraphById(S, row.code_graph_id))?.status).toBe("ready");

    const results = await Promise.all([
      svc.sync(S, T, row.code_graph_id),
      svc.sync(S, T, row.code_graph_id),
    ]);
    expect(results.map((r) => r.kind).sort()).toEqual(["busy", "ok"]);
    await svc.onIdle();
    expect(await base.getCodeGraphById(S, row.code_graph_id)).toMatchObject({ status: "ready", version: 1 });
  });

  it("late progress write vs ready: progress completes first, the asset ends ready", async () => {
    // Progress writes take longer than the terminal write (different pooled connections on Postgres).
    const log: string[] = [];
    const slowProgress = createYieldingKnowledgeStore(base, {
      async updateWikiStatus(serviceId: string, wikiId: string, patch: WikiStatusPatch, fence) {
        const tag = patch.internal_status ?? patch.status ?? "other";
        log.push(`start:${tag}`);
        if (patch.internal_status === "rebuilding-index") await sleep(30);
        const changed = await base.updateWikiStatus(serviceId, wikiId, patch, fence);
        log.push(`end:${tag}`);
        return changed;
      },
    });
    const svc = wikiService(slowProgress, async (ctx) => {
      ctx.setInternalStatus("ingesting");
      ctx.setInternalStatus("rebuilding-index"); // last call right before the worker returns (module.ts)
      return { pageCount: 2 };
    });
    const id = await readyWiki(svc);
    expect((await svc.ingest(S, T, id)).kind).toBe("ok");
    await svc.onIdle();
    await sleep(50);
    expect(await base.getWikiById(S, id)).toMatchObject({ status: "ready", internal_status: null, page_count: 2 });
    // The per-build promise chain: every progress write ends before the terminal write starts.
    expect(log.indexOf("end:rebuilding-index")).toBeGreaterThan(-1);
    expect(log.indexOf("end:rebuilding-index")).toBeLessThan(log.indexOf("start:ready"));
  });

  it("delete during build: the wiki stays deleted, credential row removed", async () => {
    const gate = deferred();
    const started = deferred();
    const svc = wikiService(createYieldingKnowledgeStore(base), async (ctx) => {
      started.resolve();
      await gate.promise;
      ctx.setInternalStatus("rebuilding-index");
      return { pageCount: 1 };
    });
    const id = await readyWiki(svc);
    await credentialStore.put({ type: "wiki", serviceId: S, resourceId: id }, { kind: "bearer", secret: "t" }, "iwiki");

    expect((await svc.ingest(S, T, id)).kind).toBe("ok");
    await started.promise;
    expect(await svc.delete(S, T, id)).toBe(true);
    gate.resolve();
    await svc.onIdle();
    await sleep(10);

    expect(await base.getWikiById(S, id)).toBeNull();
    expect(await base.countWikis(S, T)).toBe(0);
    expect(await credentialStore.getWithProvider({ type: "wiki", serviceId: S, resourceId: id })).toBeNull();
  });

  it("replace during build: no write of the old run changes the wiki", async () => {
    const writes: Array<{ patch: WikiStatusPatch; runId: string | null | undefined; changed: boolean }> = [];
    const recording = createYieldingKnowledgeStore(base, {
      async updateWikiStatus(serviceId: string, wikiId: string, patch: WikiStatusPatch, fence) {
        const changed = await base.updateWikiStatus(serviceId, wikiId, patch, fence);
        writes.push({ patch, runId: fence?.runId, changed });
        return changed;
      },
    });
    const oldGate = deferred();
    const oldStarted = deferred();
    let call = 0;
    const svc = wikiService(recording, async (ctx) => {
      call++;
      if (call === 1) {
        oldStarted.resolve();
        await oldGate.promise;
        ctx.setInternalStatus("old-late-step");
        return { pageCount: 99 };
      }
      ctx.setInternalStatus("new-step");
      return { pageCount: 5 };
    });
    const id = await readyWiki(svc);

    const first = await svc.ingest(S, T, id);
    expect(first.kind).toBe("ok");
    await oldStarted.promise;
    const second = await svc.ingest(S, T, id, "u2", { onBusy: "replace" });
    expect(second.kind).toBe("queued");
    const oldRun = writes.find((w) => w.patch.internal_status === "scanning")!.runId;

    oldGate.resolve();
    await svc.onIdle();

    const oldRunChanges = writes.filter((w) => w.runId === oldRun && w.changed);
    // Only the old run's own "processing/scanning" start write (before the replace) changed the row.
    expect(oldRunChanges.map((w) => w.patch.internal_status)).toEqual(["scanning"]);
    expect(writes.some((w) => w.patch.internal_status === "old-late-step" && w.changed)).toBe(false);
    expect(writes.some((w) => w.patch.status === "draft" && w.changed)).toBe(false);
    expect(await base.getWikiById(S, id)).toMatchObject({ status: "ready", page_count: 5, version: 2 });
  });

  it("a failing progress write is logged and does not cause an unhandled rejection", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      const failing = createYieldingKnowledgeStore(base, {
        async updateWikiStatus(serviceId: string, wikiId: string, patch: WikiStatusPatch, fence) {
          if (patch.internal_status === "boom") throw new Error("db down");
          return base.updateWikiStatus(serviceId, wikiId, patch, fence);
        },
      });
      const svc = wikiService(failing, async (ctx) => {
        ctx.setInternalStatus("boom");
        ctx.setInternalStatus("after-boom");
        return { pageCount: 1 };
      });
      const id = await readyWiki(svc);
      await svc.ingest(S, T, id);
      await svc.onIdle();
      await sleep(20);

      expect(unhandled).toEqual([]);
      expect(warn.mock.calls.some(([m]) => m.includes("progress write 'boom' failed"))).toBe(true);
      expect(await base.getWikiById(S, id)).toMatchObject({ status: "ready", internal_status: null });
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("restart recovery fences the dead run: its late write cannot revive the asset", async () => {
    const svc = wikiService(createYieldingKnowledgeStore(base), async () => ({}));
    const id = await readyWiki(svc);
    const claim = await base.claimWikiBuild(S, T, id, { runId: "dead-run" });
    expect(claim.kind).toBe("claimed");
    expect(await base.markInterruptedAsFailed()).toBe(1);
    // In-flight UPDATE of the crashed process commits after recovery.
    expect(
      await base.updateWikiStatus(S, id, { status: "processing", internal_status: "x" }, { runId: "dead-run" }),
    ).toBe(false);
    expect((await base.getWikiById(S, id))?.status).toBe("failed");
    expect((await svc.ingest(S, T, id)).kind).toBe("ok");
    await svc.onIdle();
    expect((await base.getWikiById(S, id))?.status).toBe("ready");
  });
});
