/**
 * One `pg.Pool` per (owner, Postgres URL), shared by every store of that owner
 * in the process.
 *
 * - Owners: "store" (memory + skill stores, `POSTGRES_POOL_MAX`) and "meta"
 *   (metadata stores, `TDAI_METADATA_POSTGRES_POOL_MAX`). Each owner gets its
 *   own pool even on the same URL, so each pool honours its own size setting.
 * - `StorePool` and `MetadataStorePool` take pools from the shared registry;
 *   `store.close()` never ends a pool, because other instances use it.
 * - The gateway ends every pool once with `endAll()`, after HTTP intake is
 *   closed, both store pools are closed and the core has drained.
 * - No pgvector session settings here: `hnsw.iterative_scan` is a
 *   `SET LOCAL` inside the search transaction, so a pooled connection carries
 *   no per-store state.
 */

import pg from "pg";
import type { StoreLogger } from "../types.js";
import { redactUrl } from "./settings.js";

const TAG = "[pg-pool]";

/** Upper bound for opening a connection; without it a dead host hangs every caller. */
export const PG_CONNECTION_TIMEOUT_MS = 10_000;

export type PgPoolOwner = "store" | "meta";

interface Entry {
  owner: PgPoolOwner;
  url: string;
  pool: pg.Pool;
}

export class PgPoolRegistry {
  private readonly pools = new Map<string, Entry>();
  private logger?: StoreLogger;

  constructor(logger?: StoreLogger) {
    this.logger = logger;
  }

  /** Adopt `logger` if the registry has none yet (the singleton may be created by a caller without one). */
  useLogger(logger?: StoreLogger): void {
    this.logger ??= logger;
  }

  /** Get (or lazily create) the pool for (`owner`, `url`). The first caller's `max` sizes the pool. */
  get(owner: PgPoolOwner, url: string, max: number): pg.Pool {
    const key = `${owner}\u0000${url}`;
    const existing = this.pools.get(key);
    if (existing) return existing.pool;
    const pool = new pg.Pool({ connectionString: url, max, connectionTimeoutMillis: PG_CONNECTION_TIMEOUT_MS });
    // An idle client that loses its connection emits 'error' on the pool; without a
    // listener node would crash the process. The pool discards that client itself.
    // The logger is read at event time, so one adopted later still sees the error.
    pool.on("error", (err) => {
      const msg = `${TAG} idle client error (${owner}, ${redactUrl(url)}): ${err.message}`;
      if (this.logger) this.logger.warn(msg);
      else console.warn(msg);
    });
    this.pools.set(key, { owner, url, pool });
    this.logger?.info(`${TAG} created ${owner} pool for ${redactUrl(url)} (max=${max})`);
    return pool;
  }

  get size(): number {
    return this.pools.size;
  }

  /** End every pool once. Later `get()` calls create fresh pools. */
  async endAll(): Promise<void> {
    const entries = [...this.pools.values()];
    this.pools.clear();
    const results = await Promise.allSettled(entries.map((e) => e.pool.end()));
    results.forEach((r, i) => {
      if (r.status === "rejected") {
        const e = entries[i]!;
        this.logger?.warn(`${TAG} error ending ${e.owner} pool ${redactUrl(e.url)}: ${String(r.reason)}`);
      }
    });
    if (entries.length > 0) this.logger?.info(`${TAG} ended ${entries.length} pool(s)`);
  }
}

let shared: PgPoolRegistry | null = null;

/** Process-wide registry (mirrors `getSharedMongoClientPool`). The first caller with a logger sets it. */
export function getSharedPgPoolRegistry(logger?: StoreLogger): PgPoolRegistry {
  shared ??= new PgPoolRegistry();
  shared.useLogger(logger);
  return shared;
}
