import { describe } from "vitest";

import { runKnowledgeStoreContract } from "./knowledge-store.contract.js";
import { pgHarness, TEST_POSTGRES_URL } from "./pg-test-db.js";
import { createYieldingKnowledgeStore } from "./yielding-knowledge-store.js";

describe.skipIf(!TEST_POSTGRES_URL)("knowledge store contract on Postgres (TEST_POSTGRES_URL)", () => {
  runKnowledgeStoreContract(pgHarness("postgres"));
  runKnowledgeStoreContract(pgHarness("postgres via YieldingKnowledgeStore", (s) => createYieldingKnowledgeStore(s)));
});
