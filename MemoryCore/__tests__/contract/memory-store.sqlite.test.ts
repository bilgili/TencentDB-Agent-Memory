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
  // Over-fetch + post-filter: a selective filter can return < topK (see harness doc).
  filteredVectorSearchExact: false,
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
