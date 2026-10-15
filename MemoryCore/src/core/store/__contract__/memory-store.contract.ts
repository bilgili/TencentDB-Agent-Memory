/**
 * Shared IMemoryStore contract suite.
 *
 * Backend-agnostic behavioral tests that EVERY IMemoryStore implementation must
 * satisfy (sqlite, tcvdb, mongodb). A concrete spec file supplies a harness that
 * knows how to create/dispose an isolated store for one backend, then calls
 * `runMemoryStoreContract(harness)`.
 *
 * Vector cases (nearest-first, 1% filter, dimension change, no-vector mode)
 * run only when the harness sets `vectorDimensions` — see runVectorCases.
 *
 * Scope (keyword surface):
 *   - lifecycle + capability shape
 *   - L0 write/read/count + session query
 *   - L1 write/read/count + delete
 *   - FTS (BM25) recall for L0 + L1 (jieba-tokenized query via buildFtsQuery)
 *   - isolation pushdown (team/agent) on counts + FTS
 *   - L2/L3 profile sync/read/count/delete
 *   - clearMemoryContent (strict team+agent validation + idempotency)
 *
 * FTS on mongot is eventually consistent (change-stream → Lucene). Harnesses set
 * `ftsEventuallyConsistent: true` so search assertions poll until visible.
 */
import { describe, it, expect } from "vitest";
import type { IMemoryStore, ProfileFilter, ProfileSyncRecord } from "../types.js";
import type { MemoryRecord } from "../../record/l1-writer.js";
import { buildFtsQuery } from "../tokenize.js";
import { isProfileRowStore } from "../profile-row-store.js";
import type { EmbeddingService } from "../embedding.js";
import { executeConversationSearch } from "../../tools/conversation-search.js";

export interface MemoryStoreContractHarness {
  /** Human-readable backend label used in test titles. */
  backend: string;
  /**
   * Create a fresh, isolated (unique namespace/db/dir) store and init it.
   * `dimensions` overrides the harness default embedding dimension
   * (0 = no vector support, used by the degraded-mode case).
   */
  createStore(opts?: { dimensions?: number }): Promise<IMemoryStore>;
  /**
   * Close `store` and open the SAME namespace again with another embedding
   * dimension (dimension-mismatch case). Omit to skip that case.
   */
  reopenStore?(store: IMemoryStore, opts: { dimensions: number }): Promise<IMemoryStore>;
  /** Dispose the store + any underlying resources (files, connections). */
  disposeStore(store: IMemoryStore): Promise<void>;
  /** When true, FTS results are eventually consistent → search assertions poll. */
  ftsEventuallyConsistent?: boolean;
  /** Default embedding dimension of `createStore()`. Vector cases skip when 0/absent. */
  vectorDimensions?: number;
  /**
   * True when a filtered vector search returns the min(topK, M) NEAREST matching
   * rows however selective the filter is (M = matching rows with a usable
   * vector). Every shipped backend with vector search sets true; the filtered
   * cases then assert exact ids, otherwise only filter membership and order.
   */
  filteredVectorSearchExact?: boolean;
}

const now = () => new Date().toISOString();

/** buildFtsQuery narrowed to non-null (throws on empty — a test-authoring bug). */
function fts(text: string): string {
  const q = buildFtsQuery(text);
  if (!q) throw new Error(`empty FTS query for: ${text}`);
  return q;
}

function makeL1(id: string, content: string, over?: Partial<MemoryRecord>): MemoryRecord {
  const ts = now();
  return {
    id,
    content,
    type: "persona",
    priority: 50,
    scene_name: "",
    source_message_ids: [],
    metadata: {},
    timestamps: [ts],
    createdAt: ts,
    updatedAt: ts,
    sessionKey: "sk-contract",
    sessionId: "sid-contract",
    ...over,
  };
}

function makeL0(id: string, text: string, over?: Record<string, unknown>) {
  return {
    id,
    sessionKey: "sk-contract",
    sessionId: "sid-contract",
    role: "user",
    messageText: text,
    recordedAt: now(),
    timestamp: Date.now(),
    ...over,
  } as Parameters<IMemoryStore["upsertL0"]>[0];
}

/** Poll `fn` until it returns a truthy value or the deadline elapses. */
async function pollUntil<T>(
  fn: () => Promise<T>,
  predicate: (v: T) => boolean,
  { timeoutMs = 15_000, intervalMs = 400 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last = await fn();
  while (!predicate(last) && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, intervalMs));
    last = await fn();
  }
  return last;
}

export function runMemoryStoreContract(harness: MemoryStoreContractHarness): void {
  const { backend, ftsEventuallyConsistent } = harness;

  // Search assertion helper: single-shot for strongly-consistent backends,
  // polling for eventually-consistent ones (mongot).
  const searchExpect = async <T>(run: () => Promise<T[]>, want: (rows: T[]) => boolean): Promise<T[]> => {
    if (ftsEventuallyConsistent) {
      return pollUntil(run, want);
    }
    return run();
  };

  // Conversation search polls until it returns `limit` rows on eventually-consistent backends.
  const searchExpectOne = async (
    run: () => Promise<{ results: unknown[] }>,
  ): Promise<Awaited<ReturnType<typeof executeConversationSearch>>> =>
    (ftsEventuallyConsistent ? pollUntil(run, (r) => r.results.length === 3) : run()) as never;

  describe(`IMemoryStore contract [${backend}]`, () => {
    it("init → non-degraded + capability shape", async () => {
      const store = await harness.createStore();
      try {
        expect(store.isDegraded()).toBe(false);
        const caps = store.getCapabilities();
        expect(typeof caps.vectorSearch).toBe("boolean");
        expect(typeof caps.ftsSearch).toBe("boolean");
        expect(typeof caps.nativeHybridSearch).toBe("boolean");
        expect(typeof caps.sparseVectors).toBe("boolean");
        expect(typeof caps.profileRows).toBe("boolean");
        expect(typeof store.isFtsAvailable()).toBe("boolean");
        // profileRows must describe the store's actual shape, not an aspiration:
        // it is what selects a row-backed filesystem (D8 ④).
        expect(caps.profileRows).toBe(isProfileRowStore(store));
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("L0 write → count → session query roundtrip", async () => {
      const store = await harness.createStore();
      try {
        expect(await store.countL0()).toBe(0);
        expect(await store.upsertL0(makeL0("l0-a", "Hello from the contract suite"))).toBe(true);
        expect(await store.upsertL0(makeL0("l0-b", "A second conversation message"))).toBe(true);

        expect(await store.countL0()).toBe(2);

        const rows = await store.queryL0ForL1("sk-contract");
        expect(rows.length).toBe(2);
        expect(rows.map((r) => r.record_id).sort()).toEqual(["l0-a", "l0-b"]);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("L1 write → count → query → delete roundtrip", async () => {
      const store = await harness.createStore();
      try {
        expect(await store.countL1()).toBe(0);
        expect(await store.upsertL1(makeL1("l1-a", "User prefers dark mode in the editor"))).toBe(true);
        expect(await store.upsertL1(makeL1("l1-b", "User writes primarily TypeScript"))).toBe(true);
        expect(await store.countL1()).toBe(2);

        const rows = await store.queryL1Records();
        expect(rows.map((r) => r.record_id).sort()).toEqual(["l1-a", "l1-b"]);

        expect(await store.deleteL1Batch(["l1-a"])).toBe(true);
        expect(await store.countL1()).toBe(1);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("queryL1Records({ recordIds }) returns only the named rows", async () => {
      const store = await harness.createStore();
      try {
        for (const [i, team] of ["team-a", "team-a", "team-b", "team-a", "team-b"].entries()) {
          await store.upsertL1(makeL1(`l1-id-${i}`, `record number ${i}`, { teamId: team } as Partial<MemoryRecord>));
        }
        const ids = async (filter: Parameters<IMemoryStore["queryL1Records"]>[0]) =>
          (await store.queryL1Records(filter)).map((r) => r.record_id).sort();

        expect(await ids({ recordIds: ["l1-id-1", "l1-id-3"] })).toEqual(["l1-id-1", "l1-id-3"]);
        expect(await ids({ recordIds: ["l1-id-missing"] })).toEqual([]);
        // Ids combine with the other filters.
        expect(await ids({ recordIds: ["l1-id-1", "l1-id-2"], teamId: "team-a" })).toEqual(["l1-id-1"]);
        expect(await ids({ recordIds: ["l1-id-1"], sessionKey: "sk-other" })).toEqual([]);
        // An empty id list keeps the unfiltered behaviour.
        expect((await ids({ recordIds: [] })).length).toBe(5);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("L1 FTS (BM25) recalls by keyword", async () => {
      const store = await harness.createStore();
      try {
        await store.upsertL1(makeL1("l1-ts", "User writes primarily TypeScript and React"));
        await store.upsertL1(makeL1("l1-tea", "User prefers tea over coffee in mornings"));

        const q = fts("TypeScript");
        const hits = await searchExpect(
          () => Promise.resolve(store.searchL1Fts(q, 10)),
          (rows) => rows.some((r) => r.record_id === "l1-ts"),
        );
        expect(hits.some((r) => r.record_id === "l1-ts")).toBe(true);
        expect(hits.every((r) => typeof r.score === "number")).toBe(true);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("L0 FTS (BM25) recalls by keyword", async () => {
      const store = await harness.createStore();
      try {
        await store.upsertL0(makeL0("l0-ts", "Can you help me debug this TypeScript compiler error"));
        await store.upsertL0(makeL0("l0-cook", "What is a good recipe for dinner tonight"));

        const q = fts("TypeScript compiler");
        const hits = await searchExpect(
          () => Promise.resolve(store.searchL0Fts(q, 10)),
          (rows) => rows.some((r) => r.record_id === "l0-ts"),
        );
        expect(hits.some((r) => r.record_id === "l0-ts")).toBe(true);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("isolation pushdown on counts + FTS (no cross-tenant leakage)", async () => {
      const store = await harness.createStore();
      try {
        await store.upsertL0(makeL0("l0-ta", "shared keyword alpha", { teamId: "team-a", agentId: "agent-a" }));
        await store.upsertL0(makeL0("l0-tb", "shared keyword alpha", { teamId: "team-b", agentId: "agent-a" }));
        await store.upsertL1(makeL1("l1-ta", "shared keyword alpha", { teamId: "team-a", agentId: "agent-a" } as Partial<MemoryRecord>));
        await store.upsertL1(makeL1("l1-tb", "shared keyword alpha", { teamId: "team-b", agentId: "agent-a" } as Partial<MemoryRecord>));

        expect(await store.countL0({ teamId: "team-a", agentId: "agent-a" })).toBe(1);
        expect(await store.countL1({ teamId: "team-a", agentId: "agent-a" })).toBe(1);

        const q = fts("alpha");
        const filter = { teamId: "team-a", agentId: "agent-a" };
        const l1Hits = await searchExpect(
          () => Promise.resolve(store.searchL1Fts(q, 10, filter)),
          (rows) => rows.some((r) => r.record_id === "l1-ta"),
        );
        expect(l1Hits.some((r) => r.record_id === "l1-ta")).toBe(true);
        expect(l1Hits.some((r) => r.record_id === "l1-tb")).toBe(false);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("filtered FTS applies the filter before the limit (L0 + L1)", async () => {
      const store = await harness.createStore();
      try {
        // 200 better matches outside the filter, 3 weaker matches inside it.
        for (let i = 0; i < 200; i++) {
          await store.upsertL0(makeL0(`l0-far-${i}`, STRONG_TEXT, TEAM_B));
          await store.upsertL1(makeL1(`l1-far-${i}`, STRONG_TEXT, TEAM_B));
        }
        for (let i = 0; i < 3; i++) {
          await store.upsertL0(makeL0(`l0-hit-${i}`, WEAK_TEXT, TEAM_A));
          await store.upsertL1(makeL1(`l1-hit-${i}`, WEAK_TEXT, TEAM_A));
        }
        const q = fts("zebra");
        const l0 = await searchExpect(
          () => Promise.resolve(store.searchL0Fts(q, 3, TEAM_A_FILTER)),
          (rows) => rows.length === 3,
        );
        expect(l0.map((r) => r.record_id).sort()).toEqual(["l0-hit-0", "l0-hit-1", "l0-hit-2"]);
        const l1 = await searchExpect(
          () => Promise.resolve(store.searchL1Fts(q, 3, TEAM_A_FILTER)),
          (rows) => rows.length === 3,
        );
        expect(l1.map((r) => r.record_id).sort()).toEqual(["l1-hit-0", "l1-hit-1", "l1-hit-2"]);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("conversation search by session returns that session's messages (keyword only)", async () => {
      const store = await harness.createStore();
      try {
        await seedSessionSearch(store);
        const res = await searchExpectOne(() =>
          executeConversationSearch({ query: "zebra", limit: 3, sessionKey: "sk-target", vectorStore: store }),
        );
        expect(res.results.map((r) => r.id).sort()).toEqual(["target-0", "target-1", "target-2"]);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("L2/L3 profile sync → read → count → delete", async () => {
      const store = await harness.createStore();
      if (!store.syncProfiles || !store.pullProfiles || !store.countProfiles || !store.deleteProfiles) {
        await harness.disposeStore(store);
        return; // backend without profile surface — skip
      }
      try {
        const ts = Date.now();
        await store.syncProfiles([
          {
            id: "profile:contract:l2:one",
            type: "l2",
            filename: "scene/one.md",
            content: "# Scene One\nThe user works on memory systems.",
            contentMd5: "md5-one",
            teamId: "team-a",
            agentId: "agent-a",
            version: 1,
            createdAtMs: ts,
            updatedAtMs: ts,
          },
          {
            id: "profile:contract:l3:persona",
            type: "l3",
            filename: "persona.md",
            content: "# Persona\nPragmatic senior engineer.",
            contentMd5: "md5-persona",
            teamId: "team-a",
            agentId: "agent-a",
            version: 1,
            createdAtMs: ts,
            updatedAtMs: ts,
          },
        ]);

        const all = await store.pullProfiles();
        expect(all.map((p) => p.id).sort()).toEqual(["profile:contract:l2:one", "profile:contract:l3:persona"]);
        expect(await store.countProfiles()).toBe(2);
        expect(await store.countProfiles({ type: "l2" })).toBe(1);

        await store.deleteProfiles(["profile:contract:l2:one"]);
        expect(await store.countProfiles()).toBe(1);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("queryProfiles selects the same population countProfiles counts (D10)", async () => {
      const store = await harness.createStore();
      if (!isProfileRowStore(store)) {
        await harness.disposeStore(store);
        return; // backend without a profile-row surface (sqlite)
      }
      try {
        const ts = Date.now();
        const row = (
          id: string,
          type: "l2" | "l3",
          filename: string,
          over: Partial<ProfileSyncRecord> = {},
        ): ProfileSyncRecord => ({
          id, type, filename,
          content: `# ${id}`,
          contentMd5: `md5-${id}`,
          teamId: "team-a",
          agentId: "agent-a",
          version: 1,
          createdAtMs: ts,
          updatedAtMs: ts,
          ...over,
        });

        await store.syncProfiles([
          row("p-work-q1", "l2", "scene_blocks/work/q1.md"),
          row("p-work-q2", "l2", "scene_blocks/work/q2.md"),
          row("p-life", "l2", "scene_blocks/life.md"),
          row("p-persona", "l3", "persona.md"),
          row("p-team-b", "l2", "scene_blocks/work/q3.md", { teamId: "team-b" }),
        ]);

        const ids = async (f?: ProfileFilter) =>
          (await store.queryProfiles(f)).map((p) => p.id).sort();

        expect(await ids()).toEqual(
          ["p-life", "p-persona", "p-team-b", "p-work-q1", "p-work-q2"],
        );
        expect(await ids({ type: "l3" })).toEqual(["p-persona"]);
        expect(await ids({ pathPrefix: "scene_blocks/work/" })).toEqual(
          ["p-team-b", "p-work-q1", "p-work-q2"],
        );
        // Scope and prefix compose — this is what backs a tenant-scoped listing.
        expect(await ids({ teamId: "team-a", pathPrefix: "scene_blocks/work/" })).toEqual(
          ["p-work-q1", "p-work-q2"],
        );
        expect(await ids({ pathPrefix: "scene_blocks/no-such/" })).toEqual([]);

        // Rows carry the content, so a file read needs no second round-trip.
        const [q1] = await store.queryProfiles({ pathPrefix: "scene_blocks/work/q1" });
        expect(q1.filename).toBe("scene_blocks/work/q1.md");
        expect(q1.content).toBe("# p-work-q1");

        const filters: (ProfileFilter | undefined)[] = [
          undefined,
          { type: "l2" },
          { pathPrefix: "scene_blocks/work/" },
          { teamId: "team-a" },
        ];
        for (const f of filters) {
          expect((await store.queryProfiles(f)).length).toBe(await store.countProfiles(f));
        }
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("clearMemoryContent clears L0/L1/profiles for a scope + is idempotent", async () => {
      const store = await harness.createStore();
      if (!store.clearMemoryContent) {
        await harness.disposeStore(store);
        return;
      }
      try {
        await store.upsertL0(makeL0("l0-c", "content to be cleared", { teamId: "team-x", agentId: "agent-x" }));
        await store.upsertL1(makeL1("l1-c", "content to be cleared", { teamId: "team-x", agentId: "agent-x" } as Partial<MemoryRecord>));
        if (store.syncProfiles) {
          const ts = Date.now();
          await store.syncProfiles([{
            id: "profile:contract:clear",
            type: "l2",
            filename: "clear.md",
            content: "to clear",
            contentMd5: "md5-clear",
            teamId: "team-x",
            agentId: "agent-x",
            version: 1,
            createdAtMs: ts,
            updatedAtMs: ts,
          }]);
        }

        const res = await store.clearMemoryContent({ teamId: "team-x", agentId: "agent-x" });
        expect(res.l0Deleted).toBeGreaterThanOrEqual(1);
        expect(res.l1Deleted).toBeGreaterThanOrEqual(1);

        expect(await store.countL0({ teamId: "team-x", agentId: "agent-x" })).toBe(0);
        expect(await store.countL1({ teamId: "team-x", agentId: "agent-x" })).toBe(0);

        // Idempotent: second call returns all-zero.
        const res2 = await store.clearMemoryContent({ teamId: "team-x", agentId: "agent-x" });
        expect(res2.l0Deleted).toBe(0);
        expect(res2.l1Deleted).toBe(0);
        expect(res2.profilesDeleted).toBe(0);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("clearMemoryContent rejects missing team/agent (strict validation)", async () => {
      const store = await harness.createStore();
      if (!store.clearMemoryContent) {
        await harness.disposeStore(store);
        return;
      }
      const clear = store.clearMemoryContent.bind(store);
      try {
        // Wrap in an async thunk so a *synchronous* throw (sqlite) and a rejected
        // promise (mongo) both surface as a rejection for `.rejects`.
        await expect((async () => clear({ teamId: "", agentId: "agent-x" }))()).rejects.toThrow();
        await expect((async () => clear({ teamId: "team-x", agentId: "" }))()).rejects.toThrow();
      } finally {
        await harness.disposeStore(store);
      }
    });
  });

  runVectorCases(harness);
}

/** Unit-ish vector: `main` on axis `axis`, `jitter` on the next axis (distinct, stable order). */
function vec(dims: number, axis: number, jitter = 0): Float32Array {
  const v = new Float32Array(dims);
  v[axis % dims] = 1;
  v[(axis + 1) % dims] = jitter;
  return v;
}

/**
 * A matching row far from a query on axis 0: mostly on axis 2, with an axis-0
 * component that shrinks with `rank`, so rank 0 is the nearest of them.
 */
function farMatch(dims: number, rank: number): Float32Array {
  const v = new Float32Array(dims);
  v[0] = 0.5 - rank * 0.04;
  v[2] = 1;
  return v;
}

const TEAM_A = { teamId: "team-a", agentId: "agent-a" } as Partial<MemoryRecord> & Record<string, unknown>;
const TEAM_B = { teamId: "team-b", agentId: "agent-a" } as Partial<MemoryRecord> & Record<string, unknown>;
const TEAM_A_FILTER = { teamId: "team-a", agentId: "agent-a" };

/** A short text that ranks above WEAK_TEXT for the keyword "zebra". */
const STRONG_TEXT = "zebra zebra";
const WEAK_TEXT = "zebra seen once among many other unrelated words about weather, trains, coffee and gardens";

/**
 * 200 messages of other sessions that match "zebra" better (keyword and, when
 * `dims` is set, vector) than the 3 messages of session "sk-target".
 */
async function seedSessionSearch(store: IMemoryStore, dims = 0): Promise<void> {
  for (let i = 0; i < 200; i++) {
    await store.upsertL0(
      makeL0(`other-${i}`, STRONG_TEXT, { sessionKey: `sk-other-${i % 10}` }),
      dims ? vec(dims, 0, i / 1000) : undefined,
    );
  }
  for (let i = 0; i < 3; i++) {
    await store.upsertL0(
      makeL0(`target-${i}`, WEAK_TEXT, { sessionKey: "sk-target" }),
      dims ? farMatch(dims, i) : undefined,
    );
  }
}

/** Embedding stub: every text embeds on axis 0, next to the other sessions' messages. */
function axisZeroEmbedding(dims: number): EmbeddingService {
  return {
    embed: async () => vec(dims, 0),
    embedBatch: async (texts: string[]) => texts.map(() => vec(dims, 0)),
    getDimensions: () => dims,
    getProviderInfo: () => ({ provider: "contract-stub", model: "axis-0" }),
    isReady: () => true,
    startWarmup: () => {},
  } as unknown as EmbeddingService;
}

function expectScoresDescending(rows: Array<{ score: number }>): void {
  for (let i = 1; i < rows.length; i++) {
    expect(rows[i - 1].score).toBeGreaterThanOrEqual(rows[i].score);
  }
}

/**
 * Vector cases. Skipped when the harness has no
 * vector dimension or the store reports no vector capability.
 */
function runVectorCases(harness: MemoryStoreContractHarness): void {
  const dims = harness.vectorDimensions ?? 0;

  describe.skipIf(dims < 4)(`IMemoryStore vector contract [${harness.backend}]`, () => {
    it("L1 vector search returns the nearest record first, scores descending", async () => {
      const store = await harness.createStore();
      try {
        expect(store.getCapabilities().vectorSearch).toBe(true);
        await store.upsertL1(makeL1("v-0", "axis zero"), vec(dims, 0));
        await store.upsertL1(makeL1("v-1", "axis one"), vec(dims, 1));
        await store.upsertL1(makeL1("v-2", "axis two"), vec(dims, 2));

        const hits = await store.searchL1Vector(vec(dims, 1, 0.1), 3);
        expect(hits[0]?.record_id).toBe("v-1");
        expect(hits.map((h) => h.record_id).sort()).toEqual(["v-0", "v-1", "v-2"]);
        expectScoresDescending(hits);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("filtered L1 vector search returns the nearest matching rows (1% filter)", async () => {
      const store = await harness.createStore();
      try {
        // 990 non-matching rows sit right next to the query. The 10 matching
        // (team-a) rows sit far from it, hit-0 nearest, hit-9 farthest.
        for (let i = 0; i < 990; i++) {
          await store.upsertL1(makeL1(`far-${i}`, "noise", TEAM_B), vec(dims, 0, i / 1000));
        }
        for (let i = 0; i < 10; i++) {
          await store.upsertL1(makeL1(`hit-${i}`, "signal", TEAM_A), farMatch(dims, i));
        }
        // A matching record without a vector must never be returned.
        await store.upsertL1(makeL1("hit-novec", "signal without vector", TEAM_A));

        const hits = await store.searchL1Vector(vec(dims, 0), 5, undefined, TEAM_A_FILTER);

        expect(hits.length).toBeLessThanOrEqual(5);
        expect(hits.every((h) => h.record_id.startsWith("hit-") && h.record_id !== "hit-novec")).toBe(true);
        expectScoresDescending(hits);
        if (harness.filteredVectorSearchExact) {
          expect(hits.map((h) => h.record_id)).toEqual(["hit-0", "hit-1", "hit-2", "hit-3", "hit-4"]);
        }
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("filtered L0 vector search returns the nearest matching rows", async () => {
      const store = await harness.createStore();
      try {
        for (let i = 0; i < 200; i++) {
          await store.upsertL0(makeL0(`far-${i}`, "noise", TEAM_B), vec(dims, 0, i / 1000));
        }
        for (let i = 0; i < 6; i++) {
          await store.upsertL0(makeL0(`hit-${i}`, "signal", TEAM_A), farMatch(dims, i));
        }

        const hits = await store.searchL0Vector(vec(dims, 0), 5, undefined, TEAM_A_FILTER);

        expect(hits.every((h) => h.record_id.startsWith("hit-"))).toBe(true);
        expectScoresDescending(hits);
        if (harness.filteredVectorSearchExact) {
          expect(hits.map((h) => h.record_id)).toEqual(["hit-0", "hit-1", "hit-2", "hit-3", "hit-4"]);
        }
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("filtered vector search with fewer matching rows than topK returns all of them", async () => {
      const store = await harness.createStore();
      try {
        // More non-matching rows near the query than any KNN window holds.
        for (let i = 0; i < 60; i++) {
          await store.upsertL1(makeL1(`far-${i}`, "noise", TEAM_B), vec(dims, 0, i / 1000));
        }
        await store.upsertL1(makeL1("hit-0", "signal", TEAM_A), farMatch(dims, 0));
        await store.upsertL1(makeL1("hit-1", "signal", TEAM_A), farMatch(dims, 1));

        const hits = await store.searchL1Vector(vec(dims, 0), 5, undefined, TEAM_A_FILTER);

        expect(hits.every((h) => h.record_id.startsWith("hit-"))).toBe(true);
        if (harness.filteredVectorSearchExact) {
          expect(hits.map((h) => h.record_id)).toEqual(["hit-0", "hit-1"]);
        }
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("a filter with no defined fields behaves as no filter", async () => {
      const store = await harness.createStore();
      try {
        for (let i = 0; i < 20; i++) {
          await store.upsertL1(makeL1(`r-${i}`, "row", i % 2 ? TEAM_A : TEAM_B), vec(dims, i % 3, i / 100));
        }
        const unfiltered = await store.searchL1Vector(vec(dims, 1), 5);
        const empty = await store.searchL1Vector(vec(dims, 1), 5, undefined, { teamId: undefined });
        expect(empty.map((h) => h.record_id)).toEqual(unfiltered.map((h) => h.record_id));
      } finally {
        await harness.disposeStore(store);
      }
    });

    it.skipIf(!harness.reopenStore)(
      "dimension change never serves vectors of the old dimension; rows survive",
      async () => {
        let store = await harness.createStore();
        try {
          await store.upsertL1(makeL1("d-0", "dimension change keeps this row"), vec(dims, 0));
          await store.upsertL1(makeL1("d-1", "and this one"), vec(dims, 1));

          const newDims = dims / 2;
          store = await harness.reopenStore!(store, { dimensions: newDims });

          expect(store.isDegraded()).toBe(false);
          expect(await store.countL1()).toBe(2);
          // SQLite rebuilds the vector tables empty; Postgres keeps them and
          // reports vectorSearch=false. Either way: no stale hits, no throw.
          expect(await store.searchL1Vector(vec(newDims, 0), 5)).toEqual([]);
        } finally {
          await harness.disposeStore(store);
        }
      },
    );

    it("conversation search by session returns that session's messages (hybrid)", async () => {
      const store = await harness.createStore();
      try {
        await seedSessionSearch(store, dims);
        const run = () => executeConversationSearch({
          query: "zebra", limit: 3, sessionKey: "sk-target", vectorStore: store,
          embeddingService: axisZeroEmbedding(dims),
        });
        const res = harness.ftsEventuallyConsistent ? await pollUntil(run, (r) => r.results.length === 3) : await run();
        expect(res.results.map((r) => r.id).sort()).toEqual(["target-0", "target-1", "target-2"]);
      } finally {
        await harness.disposeStore(store);
      }
    });

    it("store without vector support: vector search empty, keyword search works", async () => {
      const store = await harness.createStore({ dimensions: 0 });
      try {
        expect(store.isDegraded()).toBe(false);
        expect(store.getCapabilities().vectorSearch).toBe(false);
        expect(await store.upsertL1(makeL1("nv-ts", "User writes primarily TypeScript"))).toBe(true);

        expect(await store.searchL1Vector(vec(dims, 0), 5)).toEqual([]);
        const hits = await store.searchL1Fts(fts("TypeScript"), 10);
        expect(hits.some((r) => r.record_id === "nv-ts")).toBe(true);
      } finally {
        await harness.disposeStore(store);
      }
    });
  });
}
