/**
 * PostgresMemoryStore — Postgres + pgvector backend for `IMemoryStore`
 * A port of the SQLite `VectorStore`
 * (`../sqlite/memory-store.ts`) with the same row shapes and semantics.
 *
 * Ownership and data flow:
 * - One schema per instance (`pgSchemaName("mem", …)`). Every statement
 *   uses `${schema}.table`; `search_path` is never touched.
 * - The pool comes from the shared `PgPoolRegistry`; `close()` never ends it.
 * - `_ensureInit()` gates every data call: migrations, the pgvector
 *   step, the CJK self-test and the vector-column step run once; a failure
 *   resets the promise so the next call retries.
 * - Static tables are versioned migrations. The `embedding` columns are
 *   NOT a migration: their dimension is data (`embedding_meta.vector_dimensions`),
 *   so `ensureVectorColumns` adds them for the first configured dimension under
 *   the same migration lock. A configured-vs-stored mismatch never drops
 *   vectors: it logs and sets `vectorSearch=false`. Only the operator
 *   path `changeEmbeddingDimension(n)` changes the dimension.
 * - Keyword search: `content_tsv` is generated from the jieba-segmented
 *   `fts_content` column; `content` keeps the raw text, as `l1_records`
 *   does on SQLite. The SQLite FTS side tables and their delete paths go away.
 * - Transactions: the SQLite `BEGIN IMMEDIATE` sites use `withWriteTx`;
 *   other multi-statement writes use `withTx`. No embedding or other network
 *   call runs inside a transaction.
 *
 * Not ported (no caller outside SQLite internals, or deprecated): the v2
 * `entity_*` Team/User/Agent/Task methods (deprecated, MongoDB also omits them),
 * `getRawDb`, `rebuildFtsIndex` (generated column) and the migration-tool
 * cursor reads `queryL1RecordsCursor`/`queryL0RecordsCursor`.
 */

import pg from "pg";
import type { MemoryRecord } from "../../record/l1-writer.js";
import type { EmbeddingProviderInfo } from "../embedding.js";
import type {
  IMemoryStore,
  StoreCapabilities,
  StoreInitResult,
  StoreLogger,
  L0Record,
  L1SearchResult,
  L1FtsResult,
  L0SearchResult,
  L0FtsResult,
  L0QueryRow,
  L0SessionGroup,
  L1RecordRow,
  L1QueryFilter,
  L0CountFilter,
  L0PaginatedFilter,
  L0PaginatedResult,
  L1CountFilter,
  L1PaginatedFilter,
  L1PaginatedResult,
  IsolationFilter,
  KnowledgeEntity,
  KnowledgeType,
  KnowledgeListResult,
  BatchDeleteResult,
  MemoryContentClearFilter,
  MemoryContentClearResult,
  AuditEntry,
  AuditQueryFilter,
} from "../types.js";
import { DEFAULT_ISOLATION_ID, buildIsolationWherePg } from "../isolation.js";
import { buildPgTsQuery, extractQueryTokens, tokenizeForFts } from "../tokenize.js";
import type {
  MemoryPromptListFilter,
  MemoryPromptRecord,
  MemoryPromptSettingListFilter,
  MemoryPromptSettingLogFilter,
  MemoryPromptSettingLogRecord,
  MemoryPromptSettingRecord,
  MemoryPromptTargetType,
} from "../../memory-prompt/types.js";
import {
  buildMemoryGenerationRefId,
  type MemoryGenerationLayer,
  type MemoryGenerationRefRecord,
} from "../../memory-generation-log/types.js";
import { ensureVectorExtension, runMigrations, type Migration, type VectorExtensionInfo } from "./migrate.js";
import { quoteIdent } from "./schema-name.js";
import { withMigrationTx, withTx, withWriteTx, type TxClient } from "./tx.js";

const TAG = "[memory-tdai][postgres]";

/** Default result limit for keyword searches (same as SQLite). */
const FTS_DEFAULT_LIMIT = 20;
/** HNSW on `vector` indexes at most 2000 dims; above it the store uses `halfvec`. */
const MAX_VECTOR_HNSW_DIMS = 2000;
/** HNSW on `halfvec` indexes at most 4000 dims; above it no index is created. */
const MAX_HALFVEC_HNSW_DIMS = 4000;
const META_VECTOR_DIMS = "vector_dimensions";
const META_PROVIDER = "embedding_provider_info";

/**
 * int8 → JS number (epoch-ms columns are `bigint`; node-postgres returns int8
 * as a string). Passed per query, not set globally, so the shared `pg` module
 * keeps its defaults for other users. Counts still cast `::int`.
 */
export const PG_TYPES = {
  getTypeParser: ((oid: number, format?: string) =>
    oid === pg.types.builtins.INT8 && format !== "binary"
      ? (v: string) => Number(v)
      : pg.types.getTypeParser(oid, format as "text")) as typeof pg.types.getTypeParser,
};

/**
 * Memory schema migrations. Versions 1–999 belong to this store; the skill
 * store shares the schema and its `schema_migrations` table, so it must use
 * versions ≥ 1000.
 */
export const MEMORY_MIGRATIONS: readonly Migration[] = [
  {
    version: 1,
    up: (s) => `
      CREATE TABLE ${s}.embedding_meta (
        key text PRIMARY KEY,
        value text NOT NULL
      );

      CREATE TABLE ${s}.l1_records (
        record_id text COLLATE "C" PRIMARY KEY,
        content text NOT NULL,
        fts_content text NOT NULL DEFAULT '',
        content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', fts_content)) STORED,
        type text DEFAULT '',
        priority double precision DEFAULT 50,
        scene_name text DEFAULT '',
        session_key text DEFAULT '',
        session_id text DEFAULT 'default',
        team_id text DEFAULT 'default',
        task_id text DEFAULT '',
        user_id text NOT NULL DEFAULT 'default',
        agent_id text NOT NULL DEFAULT 'default',
        version integer NOT NULL DEFAULT 0,
        timestamp_str text DEFAULT '',
        timestamp_start text COLLATE "C" DEFAULT '',
        timestamp_end text COLLATE "C" DEFAULT '',
        created_time text COLLATE "C" DEFAULT '',
        updated_time text COLLATE "C" DEFAULT '',
        metadata_json text DEFAULT '{}'
      );
      CREATE INDEX idx_l1_type ON ${s}.l1_records (type);
      CREATE INDEX idx_l1_session_key ON ${s}.l1_records (session_key);
      CREATE INDEX idx_l1_session_id ON ${s}.l1_records (session_id);
      CREATE INDEX idx_l1_scene ON ${s}.l1_records (scene_name);
      CREATE INDEX idx_l1_ts_start ON ${s}.l1_records (timestamp_start);
      CREATE INDEX idx_l1_ts_end ON ${s}.l1_records (timestamp_end);
      CREATE INDEX idx_l1_updated ON ${s}.l1_records (updated_time);
      CREATE INDEX idx_l1_session_updated ON ${s}.l1_records (session_id, updated_time);
      CREATE INDEX idx_l1_sessionkey_updated ON ${s}.l1_records (session_key, updated_time);
      CREATE INDEX idx_l1_task_updated ON ${s}.l1_records (task_id, updated_time);
      CREATE INDEX idx_l1_team_agent_updated ON ${s}.l1_records (team_id, agent_id, updated_time);
      CREATE INDEX idx_l1_user_agent_session ON ${s}.l1_records (user_id, agent_id, session_id);
      CREATE INDEX idx_l1_user_updated ON ${s}.l1_records (user_id, updated_time);
      CREATE INDEX idx_l1_agent_updated ON ${s}.l1_records (agent_id, updated_time);
      CREATE INDEX idx_l1_content_tsv ON ${s}.l1_records USING gin (content_tsv);

      CREATE TABLE ${s}.l0_conversations (
        record_id text COLLATE "C" PRIMARY KEY,
        session_key text NOT NULL,
        session_id text DEFAULT 'default',
        team_id text DEFAULT 'default',
        task_id text DEFAULT '',
        user_id text NOT NULL DEFAULT 'default',
        agent_id text NOT NULL DEFAULT 'default',
        role text NOT NULL DEFAULT '',
        message_text text NOT NULL,
        fts_content text NOT NULL DEFAULT '',
        content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', fts_content)) STORED,
        recorded_at text COLLATE "C" DEFAULT '',
        timestamp bigint DEFAULT 0
      );
      CREATE INDEX idx_l0_session ON ${s}.l0_conversations (session_key, recorded_at);
      CREATE INDEX idx_l0_session_id ON ${s}.l0_conversations (session_id);
      CREATE INDEX idx_l0_task ON ${s}.l0_conversations (task_id);
      CREATE INDEX idx_l0_team_agent ON ${s}.l0_conversations (team_id, agent_id);
      CREATE INDEX idx_l0_recorded ON ${s}.l0_conversations (recorded_at);
      CREATE INDEX idx_l0_timestamp ON ${s}.l0_conversations (timestamp);
      CREATE INDEX idx_l0_user_agent_session ON ${s}.l0_conversations (user_id, agent_id, session_id);
      CREATE INDEX idx_l0_user_recorded ON ${s}.l0_conversations (user_id, recorded_at);
      CREATE INDEX idx_l0_agent_recorded ON ${s}.l0_conversations (agent_id, recorded_at);
      CREATE INDEX idx_l0_content_tsv ON ${s}.l0_conversations USING gin (content_tsv);

      CREATE TABLE ${s}.entity_knowledge (
        knowledge_id text COLLATE "C" PRIMARY KEY,
        type text NOT NULL,
        service_url text NOT NULL,
        name text NOT NULL,
        summary text,
        team_id text NOT NULL,
        agent_id text NOT NULL DEFAULT '',
        user_id text,
        repo_url text,
        branch text,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL
      );
      CREATE INDEX idx_entity_knowledge_team_type ON ${s}.entity_knowledge (team_id, type);

      CREATE TABLE ${s}.memory_audit (
        audit_id text COLLATE "C" PRIMARY KEY,
        record_id text NOT NULL,
        layer text NOT NULL CHECK (layer IN ('L1','L2','L3')),
        action text NOT NULL CHECK (action IN ('update','delete')),
        team_id text,
        agent_id text,
        user_id text,
        task_id text,
        version integer NOT NULL,
        updated_at_ms bigint NOT NULL,
        request_id text
      );
      CREATE INDEX idx_memory_audit_record ON ${s}.memory_audit (record_id, updated_at_ms);
      CREATE INDEX idx_memory_audit_isolation ON ${s}.memory_audit (team_id, agent_id, user_id, task_id);
      CREATE INDEX idx_memory_audit_time ON ${s}.memory_audit (updated_at_ms);

      CREATE TABLE ${s}.memory_prompts (
        memory_prompt_id text COLLATE "C" PRIMARY KEY,
        name text NOT NULL,
        layer text NOT NULL CHECK (layer IN ('l1','l2','l3')),
        prompt text NOT NULL,
        version integer NOT NULL,
        status text NOT NULL CHECK (status IN ('active','deleting')),
        created_by text,
        updated_by text,
        created_at_ms bigint NOT NULL,
        updated_at_ms bigint NOT NULL
      );
      CREATE INDEX idx_memory_prompts_layer_updated ON ${s}.memory_prompts (layer, updated_at_ms);
      CREATE INDEX idx_memory_prompts_status ON ${s}.memory_prompts (status);

      CREATE TABLE ${s}.memory_prompt_settings (
        setting_id text COLLATE "C" PRIMARY KEY,
        target_type text NOT NULL CHECK (target_type IN ('instance','team','agent')),
        team_id text,
        agent_id text,
        layer text NOT NULL CHECK (layer IN ('l1','l2','l3')),
        memory_prompt_id text NOT NULL,
        updated_by text,
        updated_at_ms bigint NOT NULL
      );
      CREATE INDEX idx_memory_prompt_settings_prompt ON ${s}.memory_prompt_settings (memory_prompt_id);
      CREATE INDEX idx_memory_prompt_settings_target ON ${s}.memory_prompt_settings (team_id, agent_id, layer);

      CREATE TABLE ${s}.memory_prompt_setting_logs (
        setting_log_id text COLLATE "C" PRIMARY KEY,
        target_type text NOT NULL CHECK (target_type IN ('instance','team','agent')),
        team_id text,
        agent_id text,
        layer text NOT NULL CHECK (layer IN ('l1','l2','l3')),
        action text NOT NULL CHECK (action IN ('apply','replace','clear')),
        reason text NOT NULL CHECK (reason IN ('explicit','prompt_deleted')),
        before_memory_prompt_id text,
        after_memory_prompt_id text,
        operator_id text,
        operated_at_ms bigint NOT NULL
      );
      CREATE INDEX idx_memory_prompt_logs_prompt_before ON ${s}.memory_prompt_setting_logs (before_memory_prompt_id, operated_at_ms);
      CREATE INDEX idx_memory_prompt_logs_prompt_after ON ${s}.memory_prompt_setting_logs (after_memory_prompt_id, operated_at_ms);
      CREATE INDEX idx_memory_prompt_logs_target ON ${s}.memory_prompt_setting_logs (team_id, agent_id, operated_at_ms);

      CREATE TABLE ${s}.memory_generation_refs (
        generation_ref_id text PRIMARY KEY,
        layer text NOT NULL CHECK (layer IN ('l1','l2','l3')),
        memory_id text NOT NULL,
        generation_id text NOT NULL,
        generation_log_id text NOT NULL,
        generation_log_key text NOT NULL,
        memory_prompt_id text NOT NULL,
        memory_prompt_version integer NOT NULL,
        memory_prompt_source text NOT NULL,
        created_at_ms bigint NOT NULL
      );
      CREATE INDEX idx_memory_generation_refs_memory ON ${s}.memory_generation_refs (layer, memory_id);
    `,
  },
];

const L1_COLS = `record_id, content, type, priority, scene_name, session_key, session_id,
  team_id, task_id, user_id, agent_id, version,
  timestamp_str, timestamp_start, timestamp_end,
  created_time, updated_time, metadata_json`;

const L0_COLS =
  "record_id, session_key, session_id, team_id, task_id, user_id, agent_id, role, message_text, recorded_at, timestamp";

/** L1 search hit columns (L1SearchResult / L1FtsResult minus score). */
const L1_HIT_COLS = `record_id, content, type, priority, scene_name, timestamp_str, timestamp_start, timestamp_end,
  version, session_key, session_id, team_id, task_id, user_id, agent_id, metadata_json`;

type L1HitRow = Omit<L1SearchResult, "score">;
type L0HitRow = Omit<L0SearchResult, "score">;

/** Append `value` to `params` and return its `$n` placeholder. */
function p(params: unknown[], value: unknown): string {
  params.push(value);
  return `$${params.length}`;
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function isZeroVector(v: Float32Array): boolean {
  return v.every((x) => x === 0);
}

/** pgvector text literal (`[1,2,3]`), bound as a parameter and cast in SQL. */
export function vecLiteral(v: Float32Array): string {
  return `[${Array.from(v).join(",")}]`;
}

/** Qualified vector type for `dims`: `"ext".vector` up to 2000 dims, `"ext".halfvec` above. */
export function pgVectorType(extSchema: string, dims: number): string {
  return `${quoteIdent(extSchema)}.${dims > MAX_VECTOR_HNSW_DIMS ? "halfvec" : "vector"}`;
}

/**
 * Add `embedding <type>(dims)` plus its HNSW cosine index to one table of the
 * mem schema (`s` = quoted schema). Shared by the memory and skill stores.
 */
export async function addVectorColumn(client: TxClient, s: string, table: string, extSchema: string, dims: number): Promise<void> {
  const type = pgVectorType(extSchema, dims);
  await client.query(`ALTER TABLE ${s}.${table} ADD COLUMN embedding ${type}(${dims})`);
  // ponytail: above 4000 dims no HNSW index exists; search is an exact scan.
  if (dims <= MAX_HALFVEC_HNSW_DIMS) {
    await client.query(`CREATE INDEX idx_${table}_embedding ON ${s}.${table} USING hnsw (embedding ${type}_cosine_ops)`);
  }
}

/**
 * CJK self-test: the `simple` parser splits by the database
 * LC_CTYPE. Returns the lexeme count of `'数据库 主库'`; 2 means keyword search works.
 */
export async function cjkLexemeCount(pool: pg.Pool): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    "SELECT coalesce(array_length(tsvector_to_array(to_tsvector('simple', '数据库 主库')), 1), 0)::int AS n",
  );
  return rows[0]?.n ?? 0;
}

/**
 * Keyword tokens from the `searchL*Fts` input. Callers pass `buildFtsQuery()`
 * output (`"a" OR "b"`, quotes already stripped from tokens), so the quoted
 * tokens are recovered losslessly; raw text falls back to `extractQueryTokens`.
 */
export function ftsInputToTokens(ftsQuery: string): string[] {
  const quoted = [...ftsQuery.matchAll(/"([^"]+)"/g)].map((m) => m[1]!);
  return quoted.length > 0 ? quoted : extractQueryTokens(ftsQuery);
}

function l1HitWithScore(r: L1HitRow & { score: number }): L1SearchResult {
  return {
    record_id: r.record_id,
    content: r.content,
    type: r.type,
    priority: r.priority,
    scene_name: r.scene_name,
    score: r.score,
    timestamp_str: r.timestamp_str,
    timestamp_start: r.timestamp_start,
    timestamp_end: r.timestamp_end,
    version: r.version ?? 0,
    session_key: r.session_key,
    session_id: r.session_id,
    team_id: r.team_id ?? "",
    task_id: r.task_id ?? "",
    user_id: r.user_id ?? "",
    agent_id: r.agent_id ?? "",
    metadata_json: r.metadata_json,
  };
}

function l0HitWithScore(r: L0HitRow & { score: number }): L0SearchResult {
  return {
    record_id: r.record_id,
    session_key: r.session_key,
    session_id: r.session_id,
    team_id: r.team_id ?? "",
    task_id: r.task_id ?? "",
    user_id: r.user_id ?? "",
    agent_id: r.agent_id ?? "",
    role: r.role,
    message_text: r.message_text,
    score: r.score,
    recorded_at: r.recorded_at,
    timestamp: r.timestamp ?? 0,
  };
}

export interface PostgresMemoryStoreOptions {
  /** Shared pool from `PgPoolRegistry` — never ended by the store. */
  pool: pg.Pool;
  /** Instance schema, from `pgSchemaName("mem", instanceId, prefix)`. */
  schema: string;
  /** Configured embedding dimension; 0 = no vector support. */
  dimensions: number;
  logger?: StoreLogger;
}

export class PostgresMemoryStore implements IMemoryStore {
  /** @see IMemoryStore.supportsDeferredEmbedding */
  readonly supportsDeferredEmbedding = true;

  private readonly pool: pg.Pool;
  private readonly schema: string;
  /** Quoted schema identifier for SQL. */
  private readonly s: string;
  private readonly dimensions: number;
  private readonly logger?: StoreLogger;

  private providerInfo?: EmbeddingProviderInfo;
  private initPromise: Promise<void> | null = null;
  private ready = false;
  private initResult: StoreInitResult = { needsReindex: false };
  private closed = false;
  private degraded = false;

  // Flags cached at the end of _initAsync.
  private ftsAvailable = false;
  private vectorReady = false;
  /** Qualified vector type (`"ext".vector` / `"ext".halfvec`) — set when vectorReady. */
  private vecType = "";
  /** Qualified cosine-distance operator (`OPERATOR("ext".<=>)`). */
  private distOp = "";

  constructor(opts: PostgresMemoryStoreOptions) {
    this.pool = opts.pool;
    this.schema = opts.schema;
    this.s = quoteIdent(opts.schema);
    this.dimensions = opts.dimensions;
    this.logger = opts.logger;
  }

  // ── Lifecycle ───────────────────────────────────────────

  async init(providerInfo?: EmbeddingProviderInfo): Promise<StoreInitResult> {
    if (providerInfo && !this.initPromise) this.providerInfo = providerInfo;
    await this._ensureInit();
    return this.initResult;
  }

  /** Init gate: one in-flight init; a rejection resets it for a retry. */
  private _ensureInit(): Promise<void> {
    if (this.closed) return Promise.reject(new Error(`${TAG} store is closed (${this.schema})`));
    if (this.ready) return Promise.resolve();
    this.initPromise ??= this._initAsync().then(
      () => {
        this.ready = true;
        this.degraded = false;
      },
      (err: unknown) => {
        this.initPromise = null;
        this.degraded = true;
        this.logger?.error(`${TAG} init failed for schema ${this.schema}: ${errMsg(err)}`);
        throw err;
      },
    );
    return this.initPromise;
  }

  private async _initAsync(): Promise<void> {
    const ext = this.dimensions > 0 ? await ensureVectorExtension(this.pool) : null;

    const applied = await runMigrations(this.pool, this.schema, MEMORY_MIGRATIONS);
    if (applied.length > 0) this.logger?.info(`${TAG} ${this.schema}: applied migrations ${applied.join(",")}`);

    // CJK self-test: `simple` splits by the database LC_CTYPE.
    const lexemes = await cjkLexemeCount(this.pool);
    this.ftsAvailable = lexemes === 2;
    if (!this.ftsAvailable) {
      this.logger?.error(
        `${TAG} ${this.schema}: to_tsvector('simple', '数据库 主库') returned ${lexemes} lexemes (expected 2); ` +
          `keyword search disabled. Use a UTF-8 database locale.`,
      );
    }

    const newColumns = await this.setUpVectors(ext);
    this.initResult = await this.checkProvider(newColumns);
    this.logger?.info?.(
      `${TAG} ${this.schema}: ready (dimensions=${this.dimensions}, vectorSearch=${this.vectorReady}, fts=${this.ftsAvailable})`,
    );
  }

  /**
   * Decide `vectorReady`. Creates the vector columns for the first configured
   * dimension; returns true when this call created them.
   */
  private async setUpVectors(ext: VectorExtensionInfo | null): Promise<boolean> {
    this.vectorReady = false;
    if (this.dimensions <= 0 || !ext) return false;
    if (!ext.available) {
      this.logger?.warn(`${TAG} ${this.schema}: vector search disabled — ${ext.reason}`);
      return false;
    }
    let created = false;
    let stored = await this.readStoredDimensions();
    if (stored === null) {
      created = await withMigrationTx(this.pool, this.schema, async (client) => {
        // Re-read under the lock: another process may have created them.
        if ((await this.readStoredDimensions(client)) !== null) return false;
        await this.createVectorColumns(client, ext.schema!, this.dimensions);
        return true;
      });
      stored = await this.readStoredDimensions();
    }
    if (stored !== this.dimensions) {
      this.logger?.error(
        `${TAG} ${this.schema}: configured embedding dimension ${this.dimensions} differs from the stored dimension ${stored}; ` +
          `vector search disabled, stored vectors kept. Run the reindex --dimension operator command to change it.`,
      );
      return created;
    }
    this.useVectorType(ext.schema!, stored);
    this.vectorReady = true;
    return created;
  }

  private useVectorType(extSchema: string, dims: number): void {
    this.vecType = pgVectorType(extSchema, dims);
    this.distOp = `OPERATOR(${quoteIdent(extSchema)}.<=>)`;
  }

  /** Add `embedding` columns + HNSW indexes for `dims`, and record the dimension. */
  private async createVectorColumns(client: TxClient, extSchema: string, dims: number): Promise<void> {
    for (const t of ["l1_records", "l0_conversations"]) {
      await addVectorColumn(client, this.s, t, extSchema, dims);
    }
    await client.query(
      `INSERT INTO ${this.s}.embedding_meta (key, value) VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
      [META_VECTOR_DIMS, String(dims)],
    );
  }

  private async readStoredDimensions(client?: TxClient): Promise<number | null> {
    const { rows } = await this.q<{ value: string }>(
      `SELECT value FROM ${this.s}.embedding_meta WHERE key = $1`,
      [META_VECTOR_DIMS],
      client,
    );
    return rows[0] ? Number(rows[0].value) : null;
  }

  /**
   * SQLite parity for `needsReindex`: a provider/model change (or rows that
   * predate the vector columns) asks the caller to re-embed. Unlike SQLite,
   * vectors are never dropped here — `reindexAll` overwrites them in place.
   */
  private async checkProvider(newColumns: boolean): Promise<StoreInitResult> {
    const info = this.providerInfo;
    if (!info || !this.vectorReady) return { needsReindex: false };
    const { rows } = await this.q<{ value: string }>(
      `SELECT value FROM ${this.s}.embedding_meta WHERE key = $1`,
      [META_PROVIDER],
    );
    let saved: { provider?: string; model?: string } | null = null;
    try {
      saved = rows[0] ? JSON.parse(rows[0].value) : null;
    } catch { /* unreadable meta → treat as absent */ }

    let reason: string | undefined;
    if (saved) {
      const reasons: string[] = [];
      if (saved.provider !== info.provider) reasons.push(`provider: ${saved.provider} → ${info.provider}`);
      if (saved.model !== info.model) reasons.push(`model: ${saved.model} → ${info.model}`);
      if (reasons.length > 0) reason = reasons.join(", ");
    } else if (newColumns) {
      const { rows: c } = await this.q<{ n: number }>(
        `SELECT ((SELECT count(*) FROM ${this.s}.l1_records) + (SELECT count(*) FROM ${this.s}.l0_conversations))::int AS n`,
      );
      if ((c[0]?.n ?? 0) > 0) reason = "existing rows have no vectors";
    }
    await this.q(
      `INSERT INTO ${this.s}.embedding_meta (key, value) VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
      [META_PROVIDER, JSON.stringify({ provider: info.provider, model: info.model, dimensions: this.dimensions })],
    );
    if (reason) this.logger?.info(`${TAG} ${this.schema}: re-embed needed (${reason})`);
    return { needsReindex: reason !== undefined, reason };
  }

  isDegraded(): boolean {
    return this.degraded;
  }

  getCapabilities(): StoreCapabilities {
    return {
      vectorSearch: this.vectorReady,
      ftsSearch: this.ftsAvailable,
      nativeHybridSearch: false,
      sparseVectors: false,
      profileRows: false,
    };
  }

  isFtsAvailable(): boolean {
    return this.ftsAvailable;
  }

  /** Mark closed. Never ends the shared pool. */
  close(): void {
    this.closed = true;
  }

  /** Embedding dimension this store was opened with (0 = no vectors). */
  getEmbeddingDimensions(): number {
    return this.dimensions;
  }

  /**
   * Operator path for `reindex --dimension N`. Under the
   * schema migration lock (with `lock_timeout`), drop the vector columns and
   * create them again for `dimensions`. All stored vectors are dropped; run
   * `reindexAll(embedFn)` next to rebuild them. Other gateways that still have
   * the old dimension must be restarted (their vector writes now fail).
   */
  async changeEmbeddingDimension(dimensions: number): Promise<void> {
    if (!Number.isInteger(dimensions) || dimensions <= 0) {
      throw new Error(`${TAG} changeEmbeddingDimension: dimension must be a positive integer (got ${dimensions})`);
    }
    await this._ensureInit();
    const ext = await ensureVectorExtension(this.pool);
    if (!ext.available) throw new Error(`${TAG} changeEmbeddingDimension: ${ext.reason}`);
    await withMigrationTx(this.pool, this.schema, async (client) => {
      for (const t of ["l1_records", "l0_conversations"]) {
        await client.query(`DROP INDEX IF EXISTS ${this.s}.idx_${t}_embedding`);
        await client.query(`ALTER TABLE ${this.s}.${t} DROP COLUMN IF EXISTS embedding`);
      }
      // The skill store shares this schema; it adds skills.embedding again for
      // the new stored dimension on its next init (PostgresSkillStore).
      await client.query(`ALTER TABLE IF EXISTS ${this.s}.skills DROP COLUMN IF EXISTS embedding`);
      await this.createVectorColumns(client, ext.schema!, dimensions);
    });
    this.vectorReady = dimensions === this.dimensions;
    if (this.vectorReady) this.useVectorType(ext.schema!, dimensions);
    this.logger?.info(`${TAG} ${this.schema}: embedding dimension changed to ${dimensions}; run reindexAll next`);
  }

  // ── Query helpers ───────────────────────────────────────

  private q<R extends pg.QueryResultRow = pg.QueryResultRow>(
    text: string,
    values: unknown[] = [],
    client?: TxClient,
  ): Promise<pg.QueryResult<R>> {
    return (client ?? this.pool).query<R>({ text, values, types: PG_TYPES });
  }

  /** Fault-tolerant wrapper (SQLite parity): await init, return `fallback` on any error. */
  private async safe<T>(label: string, fallback: T, fn: () => Promise<T>): Promise<T> {
    try {
      await this._ensureInit();
      return await fn();
    } catch (err) {
      this.logger?.warn(`${TAG} ${label} failed (non-fatal): ${errMsg(err)}`);
      return fallback;
    }
  }

  // ── L1 write ────────────────────────────────────────────

  upsertL1(record: MemoryRecord, embedding?: Float32Array): Promise<boolean> {
    return this.safe(`[L1-upsert] id=${record.id}`, false, async () => {
      const { id: recordId, timestamps } = record;
      const tsStr = timestamps[0] ?? "";
      const tsStart = timestamps.length > 0 ? timestamps.reduce((a, b) => (a < b ? a : b)) : tsStr;
      const tsEnd = timestamps.length > 0 ? timestamps.reduce((a, b) => (a > b ? a : b)) : tsStr;
      const iso = record as MemoryRecord & { teamId?: string; userId?: string; agentId?: string };
      const withVec = !!embedding && !isZeroVector(embedding) && this.vectorReady;

      const values: unknown[] = [
        recordId, record.content, tokenizeForFts(record.content), record.type, record.priority,
        record.scene_name, record.sessionKey, record.sessionId || DEFAULT_ISOLATION_ID,
        iso.teamId || DEFAULT_ISOLATION_ID, record.taskId || "", record.version ?? 0,
        tsStr, tsStart, tsEnd, record.createdAt, record.updatedAt, JSON.stringify(record.metadata),
        iso.userId || DEFAULT_ISOLATION_ID, iso.agentId || DEFAULT_ISOLATION_ID,
      ];
      // A missing / zero embedding leaves any stored vector untouched (SQLite parity).
      const vecCol = withVec ? ", embedding" : "";
      const vecVal = withVec ? `, ${p(values, vecLiteral(embedding!))}::${this.vecType}` : "";
      const vecSet = withVec ? ", embedding = excluded.embedding" : "";
      await this.q(
        `INSERT INTO ${this.s}.l1_records (
           record_id, content, fts_content, type, priority, scene_name, session_key, session_id,
           team_id, task_id, version, timestamp_str, timestamp_start, timestamp_end,
           created_time, updated_time, metadata_json, user_id, agent_id${vecCol}
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19${vecVal})
         ON CONFLICT (record_id) DO UPDATE SET
           content = excluded.content, fts_content = excluded.fts_content,
           type = excluded.type, priority = excluded.priority, scene_name = excluded.scene_name,
           team_id = excluded.team_id, task_id = excluded.task_id, version = excluded.version,
           timestamp_str = excluded.timestamp_str, timestamp_start = excluded.timestamp_start,
           timestamp_end = excluded.timestamp_end, updated_time = excluded.updated_time,
           metadata_json = excluded.metadata_json, user_id = excluded.user_id,
           agent_id = excluded.agent_id${vecSet}`,
        values,
      );
      return true;
    });
  }

  deleteL1(recordId: string, filter?: IsolationFilter): Promise<boolean> {
    return this.safe("deleteL1", false, async () => {
      const params: unknown[] = [recordId];
      const iso = buildIsolationWherePg(filter, params);
      const res = await this.q(
        `DELETE FROM ${this.s}.l1_records WHERE record_id = $1${iso ? ` AND ${iso}` : ""}`,
        params,
      );
      return (res.rowCount ?? 0) > 0;
    });
  }

  deleteL1Batch(recordIds: string[], filter?: IsolationFilter): Promise<boolean> {
    if (recordIds.length === 0) return Promise.resolve(true);
    return this.safe("deleteL1Batch", false, async () => {
      const params: unknown[] = [recordIds];
      const iso = buildIsolationWherePg(filter, params);
      await this.q(
        `DELETE FROM ${this.s}.l1_records WHERE record_id = ANY($1::text[])${iso ? ` AND ${iso}` : ""}`,
        params,
      );
      return true;
    });
  }

  deleteL1Expired(cutoffIso: string): Promise<number> {
    return this.deleteExpired("l1_records", "updated_time", cutoffIso, "L1");
  }

  /** TTL cleanup with the SQLite 80% safety ratio. Returns rows deleted. */
  private deleteExpired(table: string, col: string, cutoffIso: string, layer: string): Promise<number> {
    return this.safe(`[${layer}-deleteExpired]`, 0, async () => {
      const where = `${col} <> '' AND ${col} < $1`;
      const { rows } = await this.q<{ expired: number; total: number }>(
        `SELECT count(*) FILTER (WHERE ${where})::int AS expired, count(*)::int AS total FROM ${this.s}.${table}`,
        [cutoffIso],
      );
      const { expired = 0, total = 0 } = rows[0] ?? {};
      if (expired <= 0) return 0;
      const ratio = total > 0 ? expired / total : 0;
      if (ratio > 0.8) {
        this.logger?.warn(
          `${TAG} [${layer}-deleteExpired] BLOCKED: would delete ${expired}/${total} ` +
            `(${(ratio * 100).toFixed(1)}%) — exceeds 80% safety threshold, cutoff=${cutoffIso}`,
        );
        return 0;
      }
      const res = await this.q(`DELETE FROM ${this.s}.${table} WHERE ${where}`, [cutoffIso]);
      const deleted = res.rowCount ?? 0;
      this.logger?.info?.(`${TAG} [${layer}-deleteExpired] Deleted ${deleted}/${total} records (cutoff=${cutoffIso})`);
      return deleted;
    });
  }

  // ── L1 read ─────────────────────────────────────────────

  /** WHERE clause for the L1 count / paginated filters (SQLite truthiness rules). */
  private l1FilterWhere(filter: L1CountFilter | undefined, params: unknown[]): string {
    const c: string[] = [];
    if (filter?.type) c.push(`type = ${p(params, filter.type)}`);
    if (filter?.sessionId) c.push(`session_id = ${p(params, filter.sessionId)}`);
    if (filter?.teamId !== undefined) c.push(`team_id = ${p(params, filter.teamId)}`);
    if (filter?.userId !== undefined) c.push(`user_id = ${p(params, filter.userId)}`);
    if (filter?.agentId !== undefined) c.push(`agent_id = ${p(params, filter.agentId)}`);
    if (filter?.taskId !== undefined) c.push(`task_id = ${p(params, filter.taskId)}`);
    if (filter?.timeStart) c.push(`updated_time >= ${p(params, filter.timeStart)}`);
    if (filter?.timeEnd) c.push(`updated_time <= ${p(params, filter.timeEnd)}`);
    return c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
  }

  countL1(filter?: L1CountFilter): Promise<number> {
    return this.safe("countL1", 0, async () => {
      const params: unknown[] = [];
      const where = this.l1FilterWhere(filter, params);
      const { rows } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.l1_records ${where}`, params);
      return rows[0]?.n ?? 0;
    });
  }

  queryL1Records(filter?: L1QueryFilter): Promise<L1RecordRow[]> {
    return this.safe("[L1-query]", [], async () => {
      const params: unknown[] = [];
      const c: string[] = [];
      const { sessionKey, sessionId, taskId, updatedAfter } = filter ?? {};
      // Priority: sessionId > sessionKey (SQLite parity).
      if (sessionId) c.push(`session_id = ${p(params, sessionId)}`);
      else if (sessionKey) c.push(`session_key = ${p(params, sessionKey)}`);
      if (updatedAfter) c.push(`updated_time > ${p(params, updatedAfter)}`);
      // Narrow by ids only when given (MongoDB/TCVDB parity; the SQLite store ignores recordIds).
      if (filter?.recordIds && filter.recordIds.length > 0) c.push(`record_id = ANY(${p(params, filter.recordIds)}::text[])`);
      if (filter?.teamId !== undefined) c.push(`team_id = ${p(params, filter.teamId)}`);
      if (filter?.userId !== undefined) c.push(`user_id = ${p(params, filter.userId)}`);
      if (filter?.agentId !== undefined) c.push(`agent_id = ${p(params, filter.agentId)}`);
      if (taskId !== undefined) c.push(`task_id = ${p(params, taskId)}`);
      const where = c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
      const { rows } = await this.q<L1RecordRow>(
        `SELECT ${L1_COLS} FROM ${this.s}.l1_records ${where} ORDER BY updated_time ASC`,
        params,
      );
      return rows;
    });
  }

  getAllL1Texts(): Promise<Array<{ record_id: string; content: string; updated_time: string }>> {
    return this.safe("getAllL1Texts", [], async () =>
      (await this.q<{ record_id: string; content: string; updated_time: string }>(
        `SELECT record_id, content, updated_time FROM ${this.s}.l1_records`,
      )).rows,
    );
  }

  queryL1Paginated(filter: L1PaginatedFilter): Promise<L1PaginatedResult> {
    return this.safe("queryL1Paginated", { rows: [], total: 0 }, async () => {
      const params: unknown[] = [];
      const where = this.l1FilterWhere(filter, params);
      const { rows: c } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.l1_records ${where}`, params);
      const { rows } = await this.q<L1RecordRow>(
        `SELECT ${L1_COLS} FROM ${this.s}.l1_records ${where}
         ORDER BY updated_time DESC LIMIT ${p(params, filter.limit)} OFFSET ${p(params, filter.offset)}`,
        params,
      );
      return { rows, total: c[0]?.n ?? 0 };
    });
  }

  // ── Search ──────────────────────────────────────────────

  /**
   * Filtered k-NN: one transaction with `SET LOCAL
   * hnsw.iterative_scan = relaxed_order`; the isolation filter sits in the
   * WHERE of a MATERIALIZED CTE ordered by distance with LIMIT k, and the outer
   * query sorts again because relaxed_order can return rows slightly out of
   * order. Score = 1 − cosine distance (SQLite parity).
   */
  private vectorSearch<R>(
    table: string,
    cols: string,
    queryEmbedding: Float32Array,
    topK: number,
    filter: IsolationFilter | undefined,
  ): Promise<Array<R & { score: number }>> {
    return withTx(this.pool, async (client) => {
      await client.query("SET LOCAL hnsw.iterative_scan = relaxed_order");
      const params: unknown[] = [vecLiteral(queryEmbedding)];
      const iso = buildIsolationWherePg(filter, params);
      const { rows } = await this.q<R & { distance: number }>(
        `WITH hits AS MATERIALIZED (
           SELECT ${cols}, embedding ${this.distOp} $1::${this.vecType} AS distance
             FROM ${this.s}.${table}
            WHERE embedding IS NOT NULL${iso ? ` AND ${iso}` : ""}
            ORDER BY distance
            LIMIT ${p(params, topK)}
         )
         SELECT * FROM hits ORDER BY distance`,
        params,
        client,
      );
      return rows
        .filter((r) => r.distance !== null && !Number.isNaN(r.distance))
        .map(({ distance, ...rest }) => ({ ...(rest as unknown as R), score: 1 - distance }));
    });
  }

  searchL1Vector(queryEmbedding: Float32Array, topK = 5, _queryText?: string, filter?: IsolationFilter): Promise<L1SearchResult[]> {
    return this.safe("[L1-search]", [], async () => {
      if (!this.vectorReady || topK <= 0 || isZeroVector(queryEmbedding)) return [];
      const rows = await this.vectorSearch<L1HitRow>("l1_records", L1_HIT_COLS, queryEmbedding, topK, filter);
      return rows.map(l1HitWithScore);
    });
  }

  /**
   * Keyword search: exact lexemes OR-ed (`buildPgTsQuery`, bound as `$1`),
   * ranked by `ts_rank_cd`; score = r / (1 + r) ∈ [0, 1). The isolation filter
   * is in SQL, so `limit` rows match it (SQLite over-fetches and post-filters).
   */
  private ftsSearch<R>(
    table: string,
    cols: string,
    ftsQuery: string,
    limit: number,
    filter: IsolationFilter | undefined,
  ): Promise<Array<R & { score: number }>> {
    const tsq = buildPgTsQuery(ftsInputToTokens(ftsQuery));
    if (!tsq || limit <= 0) return Promise.resolve([]);
    const params: unknown[] = [tsq];
    const iso = buildIsolationWherePg(filter, params);
    return this.q<R & { rank: number }>(
      `SELECT ${cols}, ts_rank_cd(content_tsv, tsq) AS rank
         FROM ${this.s}.${table}, to_tsquery('simple', $1) AS tsq
        WHERE content_tsv @@ tsq${iso ? ` AND ${iso}` : ""}
        ORDER BY rank DESC
        LIMIT ${p(params, limit)}`,
      params,
    ).then(({ rows }) => rows.map(({ rank, ...rest }) => ({ ...(rest as unknown as R), score: rank / (1 + rank) })));
  }

  searchL1Fts(ftsQuery: string, limit = FTS_DEFAULT_LIMIT, filter?: IsolationFilter): Promise<L1FtsResult[]> {
    return this.safe("[L1-fts-search]", [], async () => {
      if (!this.ftsAvailable) return [];
      const rows = await this.ftsSearch<L1HitRow>("l1_records", L1_HIT_COLS, ftsQuery, limit, filter);
      return rows.map(l1HitWithScore);
    });
  }

  // ── L0 write ────────────────────────────────────────────

  private l0Values(record: L0Record): unknown[] {
    return [
      record.id, record.sessionKey, record.sessionId || DEFAULT_ISOLATION_ID,
      record.teamId || DEFAULT_ISOLATION_ID, record.taskId || "", record.role,
      record.messageText, tokenizeForFts(record.messageText), record.recordedAt, record.timestamp,
      record.userId || DEFAULT_ISOLATION_ID, record.agentId || DEFAULT_ISOLATION_ID,
    ];
  }

  private static readonly L0_INSERT_COLS =
    "record_id, session_key, session_id, team_id, task_id, role, message_text, fts_content, recorded_at, timestamp, user_id, agent_id";

  upsertL0(record: L0Record, embedding?: Float32Array): Promise<boolean> {
    return this.safe(`[L0-upsert] id=${record.id}`, false, async () => {
      const values = this.l0Values(record);
      const withVec = !!embedding && !isZeroVector(embedding) && this.vectorReady;
      const vecCol = withVec ? ", embedding" : "";
      const vecVal = withVec ? `, ${p(values, vecLiteral(embedding!))}::${this.vecType}` : "";
      const vecSet = withVec ? ", embedding = excluded.embedding" : "";
      await this.q(
        `INSERT INTO ${this.s}.l0_conversations (${PostgresMemoryStore.L0_INSERT_COLS}${vecCol})
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12${vecVal})
         ON CONFLICT (record_id) DO UPDATE SET
           message_text = excluded.message_text, fts_content = excluded.fts_content,
           recorded_at = excluded.recorded_at, timestamp = excluded.timestamp,
           team_id = excluded.team_id, task_id = excluded.task_id,
           user_id = excluded.user_id, agent_id = excluded.agent_id${vecSet}`,
        values,
      );
      return true;
    });
  }

  /**
   * One multi-row INSERT for a `/conversation/add` group. A duplicate id is an
   * error (no ON CONFLICT), and the statement is atomic: all rows or none.
   */
  async insertL0Batch(records: L0Record[]): Promise<number> {
    if (records.length === 0) return 0;
    await this._ensureInit();
    const values: unknown[] = [];
    const tuples = records.map((r) => `(${this.l0Values(r).map((v) => p(values, v)).join(",")})`);
    const res = await this.q(
      `INSERT INTO ${this.s}.l0_conversations (${PostgresMemoryStore.L0_INSERT_COLS}) VALUES ${tuples.join(",")}`,
      values,
    );
    return res.rowCount ?? 0;
  }

  updateL0Embedding(recordId: string, embedding: Float32Array): Promise<boolean> {
    return this.safe(`[L0-update-embedding] id=${recordId}`, false, async () => {
      if (!this.vectorReady || !embedding || isZeroVector(embedding)) return false;
      const res = await this.q(
        `UPDATE ${this.s}.l0_conversations SET embedding = $2::${this.vecType} WHERE record_id = $1`,
        [recordId, vecLiteral(embedding)],
      );
      return (res.rowCount ?? 0) > 0;
    });
  }

  deleteL0(recordId: string, filter?: IsolationFilter): Promise<boolean> {
    return this.safe("deleteL0", false, async () => {
      const params: unknown[] = [recordId];
      const iso = buildIsolationWherePg(filter, params);
      const res = await this.q(
        `DELETE FROM ${this.s}.l0_conversations WHERE record_id = $1${iso ? ` AND ${iso}` : ""}`,
        params,
      );
      return (res.rowCount ?? 0) > 0;
    });
  }

  deleteL0Expired(cutoffIso: string): Promise<number> {
    return this.deleteExpired("l0_conversations", "recorded_at", cutoffIso, "L0");
  }

  async deleteL0BySession(sessionId: string, filter?: IsolationFilter): Promise<number> {
    // An empty sessionId would match every legacy row with an empty session — refuse it.
    const sid = (sessionId ?? "").trim();
    if (!sid) throw new Error("[postgres] deleteL0BySession requires a non-empty sessionId");
    return this.safe("deleteL0BySession", 0, async () => {
      const params: unknown[] = [sid];
      const iso = buildIsolationWherePg(filter, params);
      const res = await this.q(
        `DELETE FROM ${this.s}.l0_conversations
          WHERE (session_key = $1 OR session_id = $1)${iso ? ` AND ${iso}` : ""}`,
        params,
      );
      return res.rowCount ?? 0;
    });
  }

  // ── L0 read ─────────────────────────────────────────────

  private l0FilterWhere(filter: L0CountFilter | undefined, params: unknown[]): string {
    const c: string[] = [];
    if (filter?.sessionId) {
      const ph = p(params, filter.sessionId);
      c.push(`(session_key = ${ph} OR session_id = ${ph})`);
    }
    if (filter?.teamId !== undefined) c.push(`team_id = ${p(params, filter.teamId)}`);
    if (filter?.userId !== undefined) c.push(`user_id = ${p(params, filter.userId)}`);
    if (filter?.agentId !== undefined) c.push(`agent_id = ${p(params, filter.agentId)}`);
    if (filter?.taskId !== undefined) c.push(`task_id = ${p(params, filter.taskId)}`);
    if (filter?.timeStartMs !== undefined) c.push(`timestamp >= ${p(params, filter.timeStartMs)}`);
    if (filter?.timeEndMs !== undefined) c.push(`timestamp <= ${p(params, filter.timeEndMs)}`);
    return c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
  }

  countL0(filter?: L0CountFilter): Promise<number> {
    return this.safe("countL0", 0, async () => {
      const params: unknown[] = [];
      const where = this.l0FilterWhere(filter, params);
      const { rows } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.l0_conversations ${where}`, params);
      return rows[0]?.n ?? 0;
    });
  }

  /** Oldest-first by recorded_at after a cursor (see SQLite for why ASC + LIMIT). */
  queryL0ForL1(sessionKey: string, afterRecordedAtMs?: number, limit = 50): Promise<L0QueryRow[]> {
    return this.safe("[L0-query]", [], async () => {
      const params: unknown[] = [sessionKey];
      const after = afterRecordedAtMs && afterRecordedAtMs > 0
        ? ` AND recorded_at > ${p(params, new Date(afterRecordedAtMs).toISOString())}`
        : "";
      const { rows } = await this.q<L0QueryRow>(
        `SELECT ${L0_COLS} FROM ${this.s}.l0_conversations
          WHERE session_key = $1${after}
          ORDER BY recorded_at ASC
          LIMIT ${p(params, limit)}`,
        params,
      );
      return rows.map((r) => ({
        record_id: r.record_id,
        session_key: r.session_key,
        session_id: r.session_id || "",
        team_id: r.team_id || "",
        task_id: r.task_id || "",
        user_id: r.user_id || "",
        agent_id: r.agent_id || "",
        role: r.role,
        message_text: r.message_text,
        recorded_at: r.recorded_at || "",
        timestamp: r.timestamp || 0,
      }));
    });
  }

  /** Same grouping as SQLite: full isolation tuple + session_id, groups by earliest message. */
  async queryL0GroupedBySessionId(sessionKey: string, afterRecordedAtMs?: number, limit = 50): Promise<L0SessionGroup[]> {
    const rows = await this.queryL0ForL1(sessionKey, afterRecordedAtMs, limit);
    const groups = new Map<string, L0SessionGroup>();
    for (const row of rows) {
      const teamId = row.team_id || undefined;
      const taskId = row.task_id || undefined;
      const key = `${teamId ?? ""}\u0000${row.user_id}\u0000${row.agent_id}\u0000${row.session_id}\u0000${taskId ?? ""}`;
      let group = groups.get(key);
      if (!group) {
        group = { sessionId: row.session_id, teamId, taskId, userId: row.user_id, agentId: row.agent_id, messages: [] };
        groups.set(key, group);
      }
      group.messages.push({
        id: row.record_id,
        role: row.role,
        content: row.message_text,
        timestamp: row.timestamp,
        recordedAtMs: row.recorded_at ? Date.parse(row.recorded_at) || 0 : 0,
      });
    }
    return [...groups.values()].sort((a, b) => a.messages[0]!.timestamp - b.messages[0]!.timestamp);
  }

  getAllL0Texts(): Promise<Array<{ record_id: string; message_text: string; recorded_at: string }>> {
    return this.safe("getAllL0Texts", [], async () =>
      (await this.q<{ record_id: string; message_text: string; recorded_at: string }>(
        `SELECT record_id, message_text, recorded_at FROM ${this.s}.l0_conversations`,
      )).rows,
    );
  }

  queryL0Paginated(filter: L0PaginatedFilter): Promise<L0PaginatedResult> {
    return this.safe("queryL0Paginated", { rows: [], total: 0 }, async () => {
      const params: unknown[] = [];
      const where = this.l0FilterWhere(filter, params);
      const { rows: c } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.l0_conversations ${where}`, params);
      const { rows } = await this.q<L0QueryRow>(
        `SELECT ${L0_COLS} FROM ${this.s}.l0_conversations ${where}
         ORDER BY timestamp DESC LIMIT ${p(params, filter.limit)} OFFSET ${p(params, filter.offset)}`,
        params,
      );
      return { rows, total: c[0]?.n ?? 0 };
    });
  }

  searchL0Vector(queryEmbedding: Float32Array, topK = 5, _queryText?: string, filter?: IsolationFilter): Promise<L0SearchResult[]> {
    return this.safe("[L0-search]", [], async () => {
      if (!this.vectorReady || topK <= 0 || isZeroVector(queryEmbedding)) return [];
      const rows = await this.vectorSearch<L0HitRow>("l0_conversations", L0_COLS, queryEmbedding, topK, filter);
      return rows.map(l0HitWithScore);
    });
  }

  searchL0Fts(ftsQuery: string, limit = FTS_DEFAULT_LIMIT, filter?: IsolationFilter): Promise<L0FtsResult[]> {
    return this.safe("[L0-fts-search]", [], async () => {
      if (!this.ftsAvailable) return [];
      const rows = await this.ftsSearch<L0HitRow>("l0_conversations", L0_COLS, ftsQuery, limit, filter);
      return rows.map(l0HitWithScore);
    });
  }

  // ── Re-index ────────────────────────────────────────────

  /**
   * Re-embed every L1 and L0 text. Each embedding is computed outside any
   * transaction and written with a single UPDATE, overwriting in place.
   */
  async reindexAll(
    embedFn: (text: string) => Promise<Float32Array>,
    onProgress?: (done: number, total: number, layer: "L1" | "L0") => void,
  ): Promise<{ l1Count: number; l0Count: number }> {
    return this.safe("reindexAll", { l1Count: 0, l0Count: 0 }, async () => {
      if (!this.vectorReady) return { l1Count: 0, l0Count: 0 };
      const layers = [
        { layer: "L1" as const, table: "l1_records", rows: (await this.getAllL1Texts()).map((r) => [r.record_id, r.content] as const) },
        { layer: "L0" as const, table: "l0_conversations", rows: (await this.getAllL0Texts()).map((r) => [r.record_id, r.message_text] as const) },
      ];
      const done = { L1: 0, L0: 0 };
      for (const { layer, table, rows } of layers) {
        for (const [id, text] of rows) {
          try {
            const embedding = await embedFn(text);
            await this.q(`UPDATE ${this.s}.${table} SET embedding = $2::${this.vecType} WHERE record_id = $1`, [
              id,
              vecLiteral(embedding),
            ]);
          } catch (err) {
            this.logger?.warn?.(`${TAG} reindex ${layer} skip ${id}: ${errMsg(err)}`);
          }
          done[layer]++;
          onProgress?.(done[layer], rows.length, layer);
        }
      }
      this.logger?.info(`${TAG} Reindex complete: L1=${done.L1}/${layers[0]!.rows.length}, L0=${done.L0}/${layers[1]!.rows.length}`);
      return { l1Count: done.L1, l0Count: done.L0 };
    });
  }

  // ── Clear ───────────────────────────────────────────────

  /** Delete all L0 + L1 rows of a (team, agent[, user]) scope. No profile rows on this backend. */
  async clearMemoryContent(filter: MemoryContentClearFilter): Promise<MemoryContentClearResult> {
    const teamId = (filter?.teamId ?? "").trim();
    const agentId = (filter?.agentId ?? "").trim();
    if (!teamId || !agentId) throw new Error("clearMemoryContent requires non-empty teamId and agentId");
    const userId = filter.userId?.trim() || undefined;
    await this._ensureInit();
    const params = userId ? [teamId, agentId, userId] : [teamId, agentId];
    const where = `team_id = $1 AND agent_id = $2${userId ? " AND user_id = $3" : ""}`;
    return withTx(this.pool, async (client) => {
      const l0 = await this.q(`DELETE FROM ${this.s}.l0_conversations WHERE ${where}`, params, client);
      const l1 = await this.q(`DELETE FROM ${this.s}.l1_records WHERE ${where}`, params, client);
      return { l0Deleted: l0.rowCount ?? 0, l1Deleted: l1.rowCount ?? 0, profilesDeleted: 0 };
    });
  }

  // ── Knowledge entity ────────────────────────────────────

  private knowledgeFromRow(row: Record<string, unknown>): KnowledgeEntity {
    return {
      knowledge_id: String(row.knowledge_id ?? ""),
      type: (row.type as KnowledgeType) ?? "wiki",
      service_url: String(row.service_url ?? ""),
      name: String(row.name ?? ""),
      summary: (row.summary as string | null) ?? null,
      team_id: String(row.team_id ?? ""),
      agent_id: String(row.agent_id ?? ""),
      user_id: (row.user_id as string | null) ?? null,
      repo_url: (row.repo_url as string | null) ?? undefined,
      branch: (row.branch as string | null) ?? undefined,
      created_at: String(row.created_at ?? ""),
      updated_at: String(row.updated_at ?? ""),
    };
  }

  /** Upsert by knowledge_id; `created_at` is kept on update (SQLite parity). */
  async createKnowledge(input: Omit<KnowledgeEntity, "created_at" | "updated_at">): Promise<KnowledgeEntity> {
    await this._ensureInit();
    const now = new Date().toISOString();
    const { rows } = await this.q(
      `INSERT INTO ${this.s}.entity_knowledge
         (knowledge_id, type, service_url, name, summary, team_id, agent_id, user_id, repo_url, branch, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
       ON CONFLICT (knowledge_id) DO UPDATE SET
         type = excluded.type, service_url = excluded.service_url, name = excluded.name,
         summary = excluded.summary, team_id = excluded.team_id, agent_id = excluded.agent_id,
         user_id = excluded.user_id, repo_url = excluded.repo_url, branch = excluded.branch,
         updated_at = excluded.updated_at
       RETURNING *`,
      [
        input.knowledge_id, input.type, input.service_url, input.name, input.summary ?? null, input.team_id,
        input.agent_id ?? "", input.user_id ?? null, input.repo_url ?? null, input.branch ?? null, now,
      ],
    );
    return this.knowledgeFromRow(rows[0]!);
  }

  async getKnowledge(knowledgeId: string): Promise<KnowledgeEntity | null> {
    await this._ensureInit();
    const { rows } = await this.q(`SELECT * FROM ${this.s}.entity_knowledge WHERE knowledge_id = $1`, [knowledgeId]);
    return rows[0] ? this.knowledgeFromRow(rows[0]) : null;
  }

  async updateKnowledge(
    knowledgeId: string,
    patch: Partial<Pick<KnowledgeEntity, "name" | "summary" | "service_url" | "repo_url" | "branch">>,
  ): Promise<KnowledgeEntity | null> {
    await this._ensureInit();
    const params: unknown[] = [knowledgeId];
    const sets = [`updated_at = ${p(params, new Date().toISOString())}`];
    for (const key of ["name", "summary", "service_url", "repo_url", "branch"] as const) {
      if (patch[key] !== undefined) sets.push(`${key} = ${p(params, patch[key])}`);
    }
    const { rows } = await this.q(
      `UPDATE ${this.s}.entity_knowledge SET ${sets.join(", ")} WHERE knowledge_id = $1 RETURNING *`,
      params,
    );
    return rows[0] ? this.knowledgeFromRow(rows[0]) : null;
  }

  async deleteKnowledge(knowledgeIds: string[], teamId?: string): Promise<BatchDeleteResult> {
    const result: BatchDeleteResult = { deleted_ids: [], failed: [] };
    for (const id of knowledgeIds) {
      const row = await this.getKnowledge(id);
      if (!row) { result.failed.push({ id, reason: "not_found" }); continue; }
      if (teamId && row.team_id !== teamId) { result.failed.push({ id, reason: "team_mismatch" }); continue; }
      await this.q(`DELETE FROM ${this.s}.entity_knowledge WHERE knowledge_id = $1`, [id]);
      result.deleted_ids.push(id);
    }
    return result;
  }

  async listKnowledge(input: {
    team_id: string;
    type?: KnowledgeType;
    knowledge_ids?: string[];
    limit?: number;
    offset?: number;
  }): Promise<KnowledgeListResult> {
    const limit = Math.min(Math.max(input.limit ?? 20, 1), 1000);
    const offset = Math.max(input.offset ?? 0, 0);
    const ids = input.knowledge_ids;
    if (ids && ids.length === 0) return { items: [], total: 0 };
    await this._ensureInit();
    const params: unknown[] = [input.team_id];
    let where = "WHERE team_id = $1";
    if (input.type) where += ` AND type = ${p(params, input.type)}`;
    if (ids) where += ` AND knowledge_id = ANY(${p(params, ids)}::text[])`;
    const { rows: c } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.entity_knowledge ${where}`, params);
    const { rows } = await this.q(
      `SELECT * FROM ${this.s}.entity_knowledge ${where}
       ORDER BY updated_at DESC LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return { items: rows.map((r) => this.knowledgeFromRow(r)), total: c[0]?.n ?? 0 };
  }

  // ── Memory audit ────────────────────────────────────────

  async appendAudit(entry: AuditEntry): Promise<void> {
    await this._ensureInit();
    // INSERT OR REPLACE (SQLite parity).
    await this.q(
      `INSERT INTO ${this.s}.memory_audit
         (audit_id, record_id, layer, action, team_id, agent_id, user_id, task_id, version, updated_at_ms, request_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT (audit_id) DO UPDATE SET
         record_id = excluded.record_id, layer = excluded.layer, action = excluded.action,
         team_id = excluded.team_id, agent_id = excluded.agent_id, user_id = excluded.user_id,
         task_id = excluded.task_id, version = excluded.version,
         updated_at_ms = excluded.updated_at_ms, request_id = excluded.request_id`,
      [
        entry.audit_id, entry.record_id, entry.layer, entry.action, entry.team_id ?? null, entry.agent_id ?? null,
        entry.user_id ?? null, entry.task_id ?? null, entry.version, entry.updated_at_ms, entry.request_id ?? null,
      ],
    );
  }

  async queryAudit(filter: AuditQueryFilter): Promise<AuditEntry[]> {
    await this._ensureInit();
    const params: unknown[] = [];
    const c: string[] = [];
    for (const key of ["record_id", "layer", "action", "team_id", "agent_id", "user_id", "task_id"] as const) {
      if (filter[key] !== undefined) c.push(`${key} = ${p(params, filter[key])}`);
    }
    if (filter.since_ms !== undefined) c.push(`updated_at_ms >= ${p(params, filter.since_ms)}`);
    if (filter.until_ms !== undefined) c.push(`updated_at_ms <= ${p(params, filter.until_ms)}`);
    const where = c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
    const limit = Math.min(Math.max(filter.limit ?? 100, 1), 1000);
    const offset = Math.max(filter.offset ?? 0, 0);
    const { rows } = await this.q(
      `SELECT audit_id, record_id, layer, action, team_id, agent_id, user_id, task_id, version, updated_at_ms, request_id
         FROM ${this.s}.memory_audit ${where}
        ORDER BY updated_at_ms DESC, audit_id DESC
        LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return rows.map((r) => ({
      audit_id: r.audit_id,
      record_id: r.record_id,
      layer: r.layer,
      action: r.action,
      team_id: r.team_id ?? undefined,
      agent_id: r.agent_id ?? undefined,
      user_id: r.user_id ?? undefined,
      task_id: r.task_id ?? undefined,
      version: r.version,
      updated_at_ms: r.updated_at_ms,
      request_id: r.request_id ?? undefined,
    }));
  }

  // ── Memory generation refs ──────────────────────────────

  async upsertMemoryGenerationRefs(records: MemoryGenerationRefRecord[]): Promise<void> {
    await this._ensureInit();
    // SQLite BEGIN IMMEDIATE site → one writer per schema.
    await withWriteTx(this.pool, this.schema, async (client) => {
      for (const r of records) {
        await this.q(
          `INSERT INTO ${this.s}.memory_generation_refs
             (generation_ref_id, layer, memory_id, generation_id, generation_log_id, generation_log_key,
              memory_prompt_id, memory_prompt_version, memory_prompt_source, created_at_ms)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
           ON CONFLICT (generation_ref_id) DO UPDATE SET
             memory_id = excluded.memory_id, generation_id = excluded.generation_id,
             generation_log_id = excluded.generation_log_id, generation_log_key = excluded.generation_log_key,
             memory_prompt_id = excluded.memory_prompt_id, memory_prompt_version = excluded.memory_prompt_version,
             memory_prompt_source = excluded.memory_prompt_source, created_at_ms = excluded.created_at_ms`,
          [
            r.generation_ref_id, r.layer, r.memory_id, r.generation_id, r.generation_log_id, r.generation_log_key,
            r.memory_prompt_id, r.memory_prompt_version, r.memory_prompt_source, r.created_at_ms,
          ],
          client,
        );
      }
    });
  }

  async getMemoryGenerationRef(layer: MemoryGenerationLayer, memoryId: string): Promise<MemoryGenerationRefRecord | null> {
    await this._ensureInit();
    const { rows } = await this.q<MemoryGenerationRefRecord>(
      `SELECT * FROM ${this.s}.memory_generation_refs WHERE generation_ref_id = $1 AND layer = $2 AND memory_id = $3`,
      [buildMemoryGenerationRefId(layer, memoryId), layer, memoryId],
    );
    return rows[0] ?? null;
  }

  // ── Custom memory prompts ───────────────────────────────

  async countMemoryPrompts(): Promise<number> {
    await this._ensureInit();
    const { rows } = await this.q<{ n: number }>(`SELECT count(*)::int AS n FROM ${this.s}.memory_prompts`);
    return rows[0]?.n ?? 0;
  }

  async createMemoryPrompt(record: MemoryPromptRecord): Promise<MemoryPromptRecord> {
    await this._ensureInit();
    await this.q(
      `INSERT INTO ${this.s}.memory_prompts
         (memory_prompt_id, name, layer, prompt, version, status, created_by, updated_by, created_at_ms, updated_at_ms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        record.memory_prompt_id, record.name, record.layer, record.prompt, record.version, record.status,
        record.created_by ?? null, record.updated_by ?? null, record.created_at_ms, record.updated_at_ms,
      ],
    );
    return record;
  }

  async getMemoryPrompts(ids: string[]): Promise<MemoryPromptRecord[]> {
    if (ids.length === 0) return [];
    await this._ensureInit();
    const { rows } = await this.q<MemoryPromptRecord>(
      `SELECT * FROM ${this.s}.memory_prompts WHERE memory_prompt_id = ANY($1::text[])`,
      [ids],
    );
    return rows;
  }

  async listMemoryPrompts(filter: MemoryPromptListFilter): Promise<MemoryPromptRecord[]> {
    await this._ensureInit();
    const params: unknown[] = [];
    const c = ["status = 'active'"];
    if (filter.layer) c.push(`layer = ${p(params, filter.layer)}`);
    const order = filter.timeOrder === "asc" ? "ASC" : "DESC";
    const limit = Math.min(Math.max(filter.limit ?? 20, 1), 100);
    const offset = Math.max(filter.offset ?? 0, 0);
    const { rows } = await this.q<MemoryPromptRecord>(
      `SELECT * FROM ${this.s}.memory_prompts WHERE ${c.join(" AND ")}
        ORDER BY updated_at_ms ${order}, memory_prompt_id ${order}
        LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return rows;
  }

  async updateMemoryPrompt(
    id: string,
    patch: { name?: string; prompt?: string; updated_by?: string; updated_at_ms: number },
  ): Promise<MemoryPromptRecord | null> {
    const current = (await this.getMemoryPrompts([id]))[0];
    if (!current || current.status !== "active") return null;
    const sameName = patch.name === undefined || patch.name === current.name;
    const samePrompt = patch.prompt === undefined || patch.prompt === current.prompt;
    if (sameName && samePrompt) return current;

    const params: unknown[] = [id];
    const sets = [
      "version = version + 1",
      `updated_at_ms = ${p(params, patch.updated_at_ms)}`,
      `updated_by = ${p(params, patch.updated_by ?? null)}`,
    ];
    if (patch.name !== undefined) sets.push(`name = ${p(params, patch.name)}`);
    if (patch.prompt !== undefined) sets.push(`prompt = ${p(params, patch.prompt)}`);
    const { rows } = await this.q<MemoryPromptRecord>(
      `UPDATE ${this.s}.memory_prompts SET ${sets.join(", ")}
        WHERE memory_prompt_id = $1 AND status = 'active' RETURNING *`,
      params,
    );
    return rows[0] ?? null;
  }

  async getMemoryPromptSettings(ids: string[]): Promise<MemoryPromptSettingRecord[]> {
    if (ids.length === 0) return [];
    await this._ensureInit();
    const { rows } = await this.q<MemoryPromptSettingRecord>(
      `SELECT * FROM ${this.s}.memory_prompt_settings WHERE setting_id = ANY($1::text[])`,
      [ids],
    );
    return rows;
  }

  async listMemoryPromptSettings(filter: MemoryPromptSettingListFilter): Promise<MemoryPromptSettingRecord[]> {
    await this._ensureInit();
    const params: unknown[] = [];
    const c: string[] = [];
    if (filter.memoryPromptId) c.push(`memory_prompt_id = ${p(params, filter.memoryPromptId)}`);
    if (filter.targetType) c.push(`target_type = ${p(params, filter.targetType)}`);
    if (filter.teamId) c.push(`team_id = ${p(params, filter.teamId)}`);
    if (filter.agentId) c.push(`agent_id = ${p(params, filter.agentId)}`);
    if (filter.layer) c.push(`layer = ${p(params, filter.layer)}`);
    const where = c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
    const order = filter.timeOrder === "asc" ? "ASC" : "DESC";
    const limit = Math.min(Math.max(filter.limit ?? 20, 1), 100);
    const offset = Math.max(filter.offset ?? 0, 0);
    const { rows } = await this.q<MemoryPromptSettingRecord>(
      `SELECT * FROM ${this.s}.memory_prompt_settings ${where}
        ORDER BY updated_at_ms ${order}, setting_id ${order}
        LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return rows;
  }

  async upsertMemoryPromptSettings(records: MemoryPromptSettingRecord[], logs: MemoryPromptSettingLogRecord[]): Promise<void> {
    await this._ensureInit();
    await withWriteTx(this.pool, this.schema, async (client) => {
      for (const r of records) {
        await this.q(
          `INSERT INTO ${this.s}.memory_prompt_settings
             (setting_id, target_type, team_id, agent_id, layer, memory_prompt_id, updated_by, updated_at_ms)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (setting_id) DO UPDATE SET
             target_type = excluded.target_type, team_id = excluded.team_id, agent_id = excluded.agent_id,
             layer = excluded.layer, memory_prompt_id = excluded.memory_prompt_id,
             updated_by = excluded.updated_by, updated_at_ms = excluded.updated_at_ms`,
          [
            r.setting_id, r.target_type, r.team_id ?? null, r.agent_id ?? null, r.layer, r.memory_prompt_id,
            r.updated_by ?? null, r.updated_at_ms,
          ],
          client,
        );
      }
      await this.insertMemoryPromptSettingLogs(logs, client);
    });
  }

  async clearMemoryPromptSettings(ids: string[], logs: MemoryPromptSettingLogRecord[]): Promise<void> {
    if (ids.length === 0) return;
    await this._ensureInit();
    await withWriteTx(this.pool, this.schema, async (client) => {
      await this.q(`DELETE FROM ${this.s}.memory_prompt_settings WHERE setting_id = ANY($1::text[])`, [ids], client);
      await this.insertMemoryPromptSettingLogs(logs, client);
    });
  }

  /** INSERT OR IGNORE (SQLite parity). */
  private async insertMemoryPromptSettingLogs(logs: MemoryPromptSettingLogRecord[], client: TxClient): Promise<void> {
    for (const l of logs) {
      await this.q(
        `INSERT INTO ${this.s}.memory_prompt_setting_logs
           (setting_log_id, target_type, team_id, agent_id, layer, action, reason,
            before_memory_prompt_id, after_memory_prompt_id, operator_id, operated_at_ms)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT (setting_log_id) DO NOTHING`,
        [
          l.setting_log_id, l.target_type, l.team_id ?? null, l.agent_id ?? null, l.layer, l.action, l.reason,
          l.before_memory_prompt_id ?? null, l.after_memory_prompt_id ?? null, l.operator_id ?? null, l.operated_at_ms,
        ],
        client,
      );
    }
  }

  /**
   * Delete prompts and clear their settings with "prompt_deleted" logs. The
   * reads run inside the write transaction, so the logs match the settings
   * actually removed (SQLite reads them before BEGIN IMMEDIATE).
   */
  async deleteMemoryPrompts(ids: string[], operatorId?: string): Promise<{
    deleted_prompt_ids: string[];
    cleared_settings: Record<MemoryPromptTargetType, number>;
  }> {
    const none = { deleted_prompt_ids: [] as string[], cleared_settings: { instance: 0, team: 0, agent: 0 } };
    if (ids.length === 0) return none;
    await this._ensureInit();
    return withWriteTx(this.pool, this.schema, async (client) => {
      const { rows: found } = await this.q<{ n: number }>(
        `SELECT count(*)::int AS n FROM ${this.s}.memory_prompts WHERE memory_prompt_id = ANY($1::text[])`,
        [ids],
        client,
      );
      if ((found[0]?.n ?? 0) !== ids.length) return none;
      const { rows: settings } = await this.q<MemoryPromptSettingRecord>(
        `SELECT * FROM ${this.s}.memory_prompt_settings WHERE memory_prompt_id = ANY($1::text[])`,
        [ids],
        client,
      );
      const cleared: Record<MemoryPromptTargetType, number> = { instance: 0, team: 0, agent: 0 };
      const now = Date.now();
      const logs: MemoryPromptSettingLogRecord[] = settings.map((s) => {
        cleared[s.target_type] += 1;
        return {
          setting_log_id: `mpsl:delete:${s.setting_id}:${s.memory_prompt_id}`,
          target_type: s.target_type,
          team_id: s.team_id ?? undefined,
          agent_id: s.agent_id ?? undefined,
          layer: s.layer,
          action: "clear",
          reason: "prompt_deleted",
          before_memory_prompt_id: s.memory_prompt_id,
          operator_id: operatorId,
          operated_at_ms: now,
        };
      });
      await this.q(`DELETE FROM ${this.s}.memory_prompt_settings WHERE memory_prompt_id = ANY($1::text[])`, [ids], client);
      await this.insertMemoryPromptSettingLogs(logs, client);
      await this.q(`DELETE FROM ${this.s}.memory_prompts WHERE memory_prompt_id = ANY($1::text[])`, [ids], client);
      return { deleted_prompt_ids: ids, cleared_settings: cleared };
    });
  }

  async queryMemoryPromptSettingLogs(filter: MemoryPromptSettingLogFilter): Promise<MemoryPromptSettingLogRecord[]> {
    await this._ensureInit();
    const params: unknown[] = [];
    const c: string[] = [];
    if (filter.memoryPromptId) {
      const ph = p(params, filter.memoryPromptId);
      c.push(`(before_memory_prompt_id = ${ph} OR after_memory_prompt_id = ${ph})`);
    }
    if (filter.teamId) c.push(`team_id = ${p(params, filter.teamId)}`);
    if (filter.agentId) c.push(`agent_id = ${p(params, filter.agentId)}`);
    if (filter.action) c.push(`action = ${p(params, filter.action)}`);
    if (filter.startTimeMs !== undefined) c.push(`operated_at_ms >= ${p(params, filter.startTimeMs)}`);
    if (filter.endTimeMs !== undefined) c.push(`operated_at_ms <= ${p(params, filter.endTimeMs)}`);
    const where = c.length > 0 ? `WHERE ${c.join(" AND ")}` : "";
    const order = filter.timeOrder === "asc" ? "ASC" : "DESC";
    const limit = Math.min(Math.max(filter.limit ?? 20, 1), 100);
    const offset = Math.max(filter.offset ?? 0, 0);
    const { rows } = await this.q<MemoryPromptSettingLogRecord>(
      `SELECT * FROM ${this.s}.memory_prompt_setting_logs ${where}
        ORDER BY operated_at_ms ${order}, setting_log_id ${order}
        LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return rows;
  }
}
