import { describe } from "vitest";

import { runBuildRaces } from "./build-races.cases.js";
import { pgHarness, TEST_POSTGRES_URL } from "./pg-test-db.js";

describe.skipIf(!TEST_POSTGRES_URL)("build races on Postgres (TEST_POSTGRES_URL)", () => {
  runBuildRaces(pgHarness("postgres"));
});
