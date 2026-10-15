/**
 * Dialect → store implementations.
 * The only place that chooses between the SQLite and the Postgres stores.
 */

import type { KnowledgeDb } from "../db/client.js";
import { createCredentialStore, createPgCredentialStore } from "../source-auth/credential-store.js";
import type { ICredentialStore } from "../source-auth/types.js";
import { createLlmBindingStore, createPgLlmBindingStore, type ILlmBindingStore } from "./llm-binding-store.js";
import { PgKnowledgeStore } from "./pg-store.js";
import { SqliteKnowledgeStore } from "./sqlite-store.js";
import type { IKnowledgeStore } from "./types.js";

export interface KnowledgeStores {
  store: IKnowledgeStore;
  llmBindingStore: ILlmBindingStore;
  credentialStore: ICredentialStore;
}

export function openKnowledgeStores(kdb: KnowledgeDb): KnowledgeStores {
  if (kdb.dialect === "postgres") {
    return {
      store: new PgKnowledgeStore(kdb.db, kdb.tables),
      llmBindingStore: createPgLlmBindingStore(kdb.db, kdb.tables),
      credentialStore: createPgCredentialStore({ db: kdb.db, tables: kdb.tables }),
    };
  }
  return {
    store: new SqliteKnowledgeStore(kdb.db),
    llmBindingStore: createLlmBindingStore(kdb.db),
    credentialStore: createCredentialStore({ db: kdb.db }),
  };
}
