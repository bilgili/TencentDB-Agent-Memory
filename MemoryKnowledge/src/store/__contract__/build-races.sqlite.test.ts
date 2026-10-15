import { createDb } from "../../db/client.js";
import { createCredentialStore } from "../../source-auth/credential-store.js";
import { createLlmBindingStore } from "../llm-binding-store.js";
import { SqliteKnowledgeStore } from "../sqlite-store.js";
import { runBuildRaces } from "./build-races.cases.js";

runBuildRaces({
  name: "sqlite :memory:",
  async open() {
    const { db, raw } = createDb({ path: ":memory:" });
    return {
      store: new SqliteKnowledgeStore(db),
      credentialStore: createCredentialStore({ db }),
      llmBindingStore: createLlmBindingStore(db),
      async close() {
        raw.close();
      },
    };
  },
});
