import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { VectorStore } from "../../src/core/store/sqlite/memory-store.js";
import { handleAtomicUpdate, type V2RouterDeps } from "../../src/gateway/v2-router.js";
import type { MemoryRecord } from "../../src/core/record/l1-writer.js";

// /atomic/update looks the record up by id. These cases pin that the lookup
// returns the named row only, so the ownership check and the copied fields
// never come from another row.
describe("/atomic/update on SQLite", () => {
  let dir: string;
  let store: VectorStore;

  const record = (id: string, over: Partial<MemoryRecord>, ts: string): MemoryRecord => ({
    id, content: `content of ${id}`, type: "persona", priority: 50, scene_name: `scene of ${id}`,
    source_message_ids: [], metadata: {}, timestamps: [ts], createdAt: ts, updatedAt: ts,
    sessionKey: "sk", sessionId: "sid", ...over,
  });

  const caller = { teamId: "team-a", userId: "user-a", agentId: "agent-a", sessionId: "sid" };
  const deps = (): V2RouterDeps => ({
    getStore: () => store,
    getEmbedding: () => undefined,
    getStorage: () => undefined,
    logger: { info() {}, warn() {}, error() {}, debug() {} } as never,
    deployMode: "standalone",
    requestIsolation: caller,
  });

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), "tdai-atomic-update-"));
    store = new VectorStore(path.join(dir, "vectors.db"), 0);
    store.init();
    // The caller's own row is the oldest, so an unfiltered lookup would return it first.
    store.upsertL1(record("mine", { ...caller }, "2024-01-01T00:00:00.000Z"), undefined);
    store.upsertL1(record("theirs", { teamId: "team-b", userId: "user-b", agentId: "agent-b" }, "2024-02-01T00:00:00.000Z"), undefined);
  });

  afterEach(() => {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  });

  const row = (id: string) => store.queryL1Records({ recordIds: [id] })[0];

  it("returns 404 for an unknown id and writes nothing", async () => {
    const res = await handleAtomicUpdate({ id: "missing", content: "x" }, {} as never, "req-1", deps());
    expect(res.code).toBe(404);
    expect(row("missing")).toBeUndefined();
    expect(row("mine")?.content).toBe("content of mine");
  });

  it("returns 403 for a record of another team and copies no fields", async () => {
    const res = await handleAtomicUpdate({ id: "theirs", content: "hijack" }, {} as never, "req-2", deps());
    expect(res.code).toBe(403);
    const theirs = row("theirs");
    expect(theirs?.content).toBe("content of theirs");
    expect(theirs?.team_id).toBe("team-b");
    expect(theirs?.user_id).toBe("user-b");
    expect(row("mine")?.content).toBe("content of mine");
  });

  it("updates the caller's own record", async () => {
    const res = await handleAtomicUpdate({ id: "mine", content: "new content" }, {} as never, "req-3", deps());
    expect(res.code).toBe(0);
    expect(row("mine")?.content).toBe("new content");
    expect(row("theirs")?.content).toBe("content of theirs");
  });
});
