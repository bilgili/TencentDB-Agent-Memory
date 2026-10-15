import { describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { VectorStore } from "../../src/core/store/sqlite/memory-store.js";
import { SqliteSkillStore } from "../../src/core/store/sqlite/skill-store.js";
import { SkillStoreError, type ISkillStore } from "../../src/core/skill/skill-store.interface.js";
import { runSkillStoreContract, type SkillStoreContractHarness } from "../../src/core/skill/__contract__/skill-store.contract.js";

// The SQLite skill store lives on the memory store's connection (tdai-core.ts).
const owners = new WeakMap<ISkillStore, { vectors: VectorStore; dir: string }>();
const DIMS = 8;

const harness: SkillStoreContractHarness = {
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
};

runSkillStoreContract(harness);

// On SQLite getHead() runs before BEGIN IMMEDIATE, so an
// archive or delete can land in between; the demote must not resurrect the skill.
describe("SqliteSkillStore head promotion", () => {
  const input = (content: string) => ({
    skill_id: "sk-race", team_id: "t", owner_agent_id: "a", user_id: "u", task_id: "k",
    name: "race", description: "", content, content_hash: content, manifest: [], storage_dir: "",
  });
  const isStale = (e: unknown) => e instanceof SkillStoreError && e.code === "SKILL_VERSION_STALE";

  for (const [what, remove] of [
    ["archiveHead", (s: ISkillStore) => s.archiveHead("sk-race", "t")],
    ["deleteAllVersions", (s: ISkillStore) => s.deleteAllVersions("sk-race", "t")],
  ] as const) {
    it(`an append that read the head before ${what} throws and creates no active head`, async () => {
      const store = await harness.createStore();
      try {
        await store.appendVersion(input("v1"));
        const head = await store.getHead("sk-race", "t");
        await remove(store);
        vi.spyOn(store, "getHead").mockResolvedValueOnce(head);
        await expect(store.appendVersion(input("v2"))).rejects.toSatisfy(isStale);
        expect(await store.getHead("sk-race", "t")).toBeNull();
        expect((await store.listVersions("sk-race")).some((s) => s.version === 2)).toBe(false);
      } finally {
        await harness.disposeStore(store);
      }
    });
  }
});
