import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { SqliteMetadataStore } from "../../src/metadata/store/sqlite-adapter.js";
import type { IMetadataStore } from "../../src/metadata/store/interface.js";
import { runMetadataStoreContract } from "../../src/metadata/store/metadata-store.contract.js";

const dirs = new WeakMap<IMetadataStore, string>();

// The contract calls store.init() itself after makeStore().
runMetadataStoreContract(
  "sqlite",
  async () => {
    const dir = mkdtempSync(path.join(tmpdir(), "tdai-meta-contract-"));
    const store = new SqliteMetadataStore(path.join(dir, "metadata.db"));
    dirs.set(store, dir);
    return store;
  },
  async (store) => {
    await store.close();
    const dir = dirs.get(store);
    if (dir) rmSync(dir, { recursive: true, force: true });
  },
);
