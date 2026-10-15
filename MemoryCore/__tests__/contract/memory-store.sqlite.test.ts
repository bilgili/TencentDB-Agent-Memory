import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { VectorStore } from "../../src/core/store/sqlite/memory-store.js";
import type { IMemoryStore } from "../../src/core/store/types.js";
import { runMemoryStoreContract } from "../../src/core/store/__contract__/memory-store.contract.js";

const dirs = new WeakMap<IMemoryStore, string>();

function open(dir: string, dimensions: number): VectorStore {
  const store = new VectorStore(path.join(dir, "vectors.db"), dimensions);
  store.init();
  dirs.set(store, dir);
  return store;
}

const DIMS = 8;

runMemoryStoreContract({
  backend: "sqlite",
  vectorDimensions: DIMS,
  // KNN fast path + exact filtered fallback.
  filteredVectorSearchExact: true,
  async createStore(opts) {
    return open(mkdtempSync(path.join(tmpdir(), "tdai-mem-contract-")), opts?.dimensions ?? DIMS);
  },
  async reopenStore(store, opts) {
    const dir = dirs.get(store)!;
    store.close();
    return open(dir, opts.dimensions);
  },
  async disposeStore(store) {
    store.close();
    const dir = dirs.get(store);
    if (dir) rmSync(dir, { recursive: true, force: true });
  },
});

// The public API never writes a zero vector (upsert skips them), but legacy
// rows can hold one. Insert it raw and check that the exact fallback skips it.
describe("sqlite filtered vector search: legacy zero vector", () => {
  it("is never returned, even through the exact fallback", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "tdai-mem-zero-"));
    const store = open(dir, DIMS);
    try {
      const unit = (axis: number, x = 0) => {
        const v = new Float32Array(DIMS);
        v[axis] = 1;
        v[axis + 1] = x;
        return v;
      };
      const base = { type: "persona", priority: 50, scene_name: "", source_message_ids: [], metadata: {},
        timestamps: [], createdAt: "", updatedAt: "", sessionKey: "sk", sessionId: "sid", agentId: "agent-a" };
      for (let i = 0; i < 40; i++) {
        store.upsertL1({ ...base, id: `far-${i}`, content: "noise", teamId: "team-b" } as never, unit(0, i / 100));
      }
      store.upsertL1({ ...base, id: "hit", content: "signal", teamId: "team-a" } as never, unit(2));
      (store as IMemoryStore).upsertL1({ ...base, id: "zero", content: "legacy", teamId: "team-a" } as never);
      store.getRawDb()
        .prepare("INSERT INTO l1_vec (record_id, embedding, updated_time) VALUES (?, ?, '')")
        .run("zero", Buffer.from(new Float32Array(DIMS).buffer));

      const hits = store.searchL1Vector(unit(0), 5, undefined, { teamId: "team-a" });
      expect(hits.map((h) => h.record_id)).toEqual(["hit"]);
      expect(store.searchL1Vector(new Float32Array(DIMS), 5, undefined, { teamId: "team-a" })).toEqual([]);
    } finally {
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
