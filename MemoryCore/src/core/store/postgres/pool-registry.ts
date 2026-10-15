/**
 * One `pg.Pool` per Postgres URL, shared by every store in the process
 *
 * - `StorePool` and `MetadataStorePool` take pools from the shared registry;
 *   `store.close()` never ends a pool, because other instances use it.
 * - The gateway ends every pool once with `endAll()`, after HTTP intake is
 *   closed and both store pools are closed.
 * - No pgvector session settings here: `hnsw.iterative_scan` is a
 *   `SET LOCAL` inside the search transaction, so a pooled connection carries
 *   no per-store state.
 */

import pg from "pg";
import type { StoreLogger } from "../types.js";
import { redactUrl } from "./settings.js";

const TAG = "[pg-pool]";

export class PgPoolRegistry {
  private readonly pools = new Map<string, pg.Pool>();
  private readonly logger?: StoreLogger;

  constructor(logger?: StoreLogger) {
    this.logger = logger;
  }

  /** Get (or lazily create) the pool for `url`. The first caller's `max` sizes the pool. */
  get(url: string, max: number): pg.Pool {
    const existing = this.pools.get(url);
    if (existing) return existing;
    const pool = new pg.Pool({ connectionString: url, max });
    // An idle client that loses its connection emits 'error' on the pool; without a
    // listener node would crash the process. The pool discards that client itself.
    pool.on("error", (err) => {
      this.logger?.warn(`${TAG} idle client error (${redactUrl(url)}): ${err.message}`);
    });
    this.pools.set(url, pool);
    this.logger?.info(`${TAG} created pool for ${redactUrl(url)} (max=${max})`);
    return pool;
  }

  get size(): number {
    return this.pools.size;
  }

  /** End every pool once. Later `get()` calls create fresh pools. */
  async endAll(): Promise<void> {
    const entries = [...this.pools.entries()];
    this.pools.clear();
    const results = await Promise.allSettled(entries.map(([, pool]) => pool.end()));
    results.forEach((r, i) => {
      if (r.status === "rejected") {
        this.logger?.warn(`${TAG} error ending pool ${redactUrl(entries[i]![0])}: ${String(r.reason)}`);
      }
    });
    if (entries.length > 0) this.logger?.info(`${TAG} ended ${entries.length} pool(s)`);
  }
}

let shared: PgPoolRegistry | null = null;

/** Process-wide registry (mirrors `getSharedMongoClientPool`). */
export function getSharedPgPoolRegistry(logger?: StoreLogger): PgPoolRegistry {
  if (!shared) shared = new PgPoolRegistry(logger);
  return shared;
}
