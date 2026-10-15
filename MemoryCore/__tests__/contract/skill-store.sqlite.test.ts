import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { VectorStore } from "../../src/core/store/sqlite/memory-store.js";
import { SqliteSkillStore } from "../../src/core/store/sqlite/skill-store.js";
import type { ISkillStore } from "../../src/core/skill/skill-store.interface.js";
import { runSkillStoreContract } from "../../src/core/skill/__contract__/skill-store.contract.js";

// The SQLite skill store lives on the memory store's connection (tdai-core.ts).
const owners = new WeakMap<ISkillStore, { vectors: VectorStore; dir: string }>();
const DIMS = 8;

runSkillStoreContract({
  backend: "sqlite",
  async createStore() {
    const dir = mkdtempSync(path.join(tmpdir(), "tdai-skill-contract-"));
    const vectors = new VectorStore(path.join(dir, "vectors.db"), DIMS);
    vectors.init();
    const store = new SqliteSkillStore({ db: vectors.getRawDb(), dimensions: DIMS });
    store.init();
    owners.set(store, { vectors, dir });
    return store;
  },
  async disposeStore(store) {
    store.close();
    const owner = owners.get(store);
    if (!owner) return;
    owner.vectors.close();
    rmSync(owner.dir, { recursive: true, force: true });
  },
});
