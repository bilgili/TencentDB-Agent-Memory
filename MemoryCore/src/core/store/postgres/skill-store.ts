/**
 * PostgresSkillStore — Postgres backend for `ISkillStore`.
 * A port of `../sqlite/skill-store.ts` with the same row shapes
 * and semantics.
 *
 * Ownership and data flow:
 * - The skills table lives in the instance mem schema (`pgSchemaName("mem", …)`,
 *   next to the memory tables, and shares their `schema_migrations` table:
 *   memory owns versions 1–999, skills own 1000+. `init` applies both lists, so
 *   either store can be the first to touch a new schema.
 * - The embedding dimension is owned by the memory store (`embedding_meta`).
 *   This store adds `skills.embedding` for the stored dimension; a mismatch
 *   never drops vectors, it disables vector search.
 * - Keyword search: `content_tsv` is generated from the jieba-segmented
 *   `fts_content` (name + description + content). The SQLite `skill_fts`
 *   side table and its delete paths go away.
 * - Head promotion: `getHead()` runs outside
 *   the transaction (ARead); the transaction demotes that head only if it is
 *   still the active head and requires rowCount = 1 (ATx, FIX_HEAD_CAS); the
 *   partial unique index is the second guard (HEAD_INDEX). Errors pass through
 *   unchanged (decision Q3); a lost demote throws the existing
 *   `SKILL_VERSION_STALE` store error.
 * - `appendVersion`, `archiveHead`, `deleteVersion` and `deleteAllVersions` take one advisory
 *   lock per skill (`withWriteTx` keyed by schema + skill_id). The model treats
 *   archive and delete as atomic steps; under READ COMMITTED a statement that
 *   waits on a row lock does not see rows inserted by the lock holder, so
 *   without this lock a delete or archive racing an append misses the new head.
 *   The lock replaces a `SELECT … FOR UPDATE`: it serializes every
 *   writer of the skill, which a row lock on the old head cannot do.
 */

import type pg from "pg";
import { randomBase62 } from "../../../utils/short-id.js";
import { FTS_CONTENT_MAX } from "../../skill/skill-store-ddl.js";
import type {
  ExpiredVersionMeta,
  ISkillStore,
  SkillSearchResult,
  SkillStoreCapabilities,
} from "../../skill/skill-store.interface.js";
import { SkillStoreError } from "../../skill/skill-store.interface.js";
import type {
  AppendVersionInput,
  ListSkillsOptions,
  SearchSkillsOptions,
  Skill,
  SkillManifestEntry,
  SkillStatus,
} from "../../skill/types.js";
import { buildFtsQuery, buildPgTsQuery, tokenizeForFts } from "../tokenize.js";
import type { StoreLogger } from "../types.js";
import {
  MEMORY_MIGRATIONS,
  PG_TYPES,
  addVectorColumn,
  cjkLexemeCount,
  ftsInputToTokens,
  pgVectorType,
  vecLiteral,
} from "./memory-store.js";
import { ensureVectorExtension, runMigrations, type Migration } from "./migrate.js";
import { quoteIdent } from "./schema-name.js";
import { withMigrationTx, withWriteTx, type TxClient } from "./tx.js";

const TAG = "[skill-store][postgres]";

/** Skill migrations of the shared mem schema (versions ≥ 1000; memory owns 1–999). */
export const SKILL_MIGRATIONS: readonly Migration[] = [
  {
    version: 1000,
    up: (s) => `
      CREATE TABLE ${s}.skills (
        row_id          text COLLATE "C" PRIMARY KEY,
        skill_id        text COLLATE "C" NOT NULL,
        version         integer NOT NULL,
        is_head         boolean NOT NULL DEFAULT true,
        user_id         text NOT NULL,
        owner_agent_id  text NOT NULL,
        team_id         text NOT NULL,
        task_id         text NOT NULL DEFAULT '',
        name            text NOT NULL,
        description     text NOT NULL DEFAULT '',
        content         text NOT NULL,
        content_hash    text NOT NULL,
        manifest_json   text NOT NULL DEFAULT '[]',
        storage_dir     text NOT NULL,
        status          text NOT NULL DEFAULT 'active',
        metadata_json   text NOT NULL DEFAULT '{}',
        created_at_ms   bigint NOT NULL,
        updated_at_ms   bigint NOT NULL,
        fts_content     text NOT NULL DEFAULT '',
        content_tsv     tsvector GENERATED ALWAYS AS (to_tsvector('simple', fts_content)) STORED,
        UNIQUE (skill_id, version)
      );
      CREATE UNIQUE INDEX uniq_skills_team_agent_name_head
        ON ${s}.skills (team_id, owner_agent_id, name) WHERE is_head AND status = 'active';
      CREATE INDEX idx_skills_team_head ON ${s}.skills (team_id, is_head, status);
      CREATE INDEX idx_skills_owner_head ON ${s}.skills (owner_agent_id, is_head, status);
      CREATE INDEX idx_skills_user ON ${s}.skills (user_id, is_head);
      CREATE INDEX idx_skills_task_audit ON ${s}.skills (task_id, created_at_ms DESC);
      CREATE INDEX idx_skills_expired ON ${s}.skills (created_at_ms) WHERE NOT is_head AND status = 'active';
      CREATE INDEX idx_skills_content_tsv ON ${s}.skills USING gin (content_tsv) WHERE is_head AND status = 'active';
    `,
  },
];

const SKILL_COLS = `row_id, skill_id, version, is_head, user_id, owner_agent_id, team_id, task_id,
  name, description, content, content_hash, manifest_json, storage_dir,
  status, metadata_json, created_at_ms, updated_at_ms`;

interface SkillRow extends Omit<Skill, "manifest"> {
  manifest_json: string;
}

function toSkill(r: SkillRow): Skill {
  let manifest: SkillManifestEntry[];
  try {
    manifest = JSON.parse(r.manifest_json);
    if (!Array.isArray(manifest)) manifest = [];
  } catch {
    manifest = [];
  }
  const { manifest_json: _m, ...rest } = r;
  return { ...rest, status: r.status as SkillStatus, manifest };
}

/** Append `value` to `params` and return its `$n` placeholder. */
function p(params: unknown[], value: unknown): string {
  params.push(value);
  return `$${params.length}`;
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export interface PostgresSkillStoreOptions {
  /** Shared pool from `PgPoolRegistry` — never ended by the store. */
  pool: pg.Pool;
  /** Instance mem schema, from `pgSchemaName("mem", instanceId, prefix)`. */
  schema: string;
  /** Configured embedding dimension; 0 = no vector column. */
  dimensions: number;
  logger?: StoreLogger;
  now?: () => number;
  ulid?: () => string;
}

export class PostgresSkillStore implements ISkillStore {
  private readonly pool: pg.Pool;
  private readonly schema: string;
  /** Quoted schema identifier for SQL. */
  private readonly s: string;
  private readonly dimensions: number;
  private readonly logger?: StoreLogger;
  private readonly now: () => number;
  private readonly ulid: () => string;

  private initPromise: Promise<void> | null = null;
  private ready = false;
  private closed = false;
  private degraded = false;
  // Flags cached at the end of _initAsync.
  private ftsAvailable = false;
  private vectorReady = false;
  /** Qualified vector type for casts — set when vectorReady. */
  private vecType = "";

  constructor(opts: PostgresSkillStoreOptions) {
    this.pool = opts.pool;
    this.schema = opts.schema;
    this.s = quoteIdent(opts.schema);
    this.dimensions = Math.max(0, Math.floor(opts.dimensions ?? 0));
    this.logger = opts.logger;
    this.now = opts.now ?? (() => Date.now());
    this.ulid = opts.ulid ?? (() => randomBase62(12));
  }

  // ── Lifecycle ───────────────────────────────────────────

  /** Starts the async init and returns; data calls await it. */
  init(): void {
    this._ensureInit().catch(() => { /* logged in _ensureInit; the next call retries */ });
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
    const applied = await runMigrations(this.pool, this.schema, [...MEMORY_MIGRATIONS, ...SKILL_MIGRATIONS]);
    if (applied.length > 0) this.logger?.info(`${TAG} ${this.schema}: applied migrations ${applied.join(",")}`);

    const lexemes = await cjkLexemeCount(this.pool);
    this.ftsAvailable = lexemes === 2;
    if (!this.ftsAvailable) {
      this.logger?.error(
        `${TAG} ${this.schema}: to_tsvector('simple', '数据库 主库') returned ${lexemes} lexemes (expected 2); ` +
          `skill keyword search disabled. Use a UTF-8 database locale.`,
      );
    }

    this.vectorReady = false;
    if (ext && !ext.available) this.logger?.warn(`${TAG} ${this.schema}: vector search disabled — ${ext.reason}`);
    if (ext?.available) {
      const stored = await this.storedDimensions();
      if (stored === this.dimensions) {
        await withMigrationTx(this.pool, this.schema, async (client) => {
          if (!(await this.hasEmbeddingColumn(client))) {
            await addVectorColumn(client, this.s, "skills", ext.schema!, stored);
          }
        });
        this.vecType = pgVectorType(ext.schema!, stored);
        this.vectorReady = true;
      } else if (stored === null) {
        // ponytail: the memory store sets the dimension; skills get the column on the next init after that.
        this.logger?.info(`${TAG} ${this.schema}: no stored embedding dimension yet; skill vectors disabled until restart`);
      } else {
        this.logger?.error(
          `${TAG} ${this.schema}: configured embedding dimension ${this.dimensions} differs from the stored dimension ${stored}; ` +
            `skill vectors disabled, stored vectors kept.`,
        );
      }
    }
    this.logger?.info(`${TAG} ${this.schema}: ready (vectorSearch=${this.vectorReady}, fts=${this.ftsAvailable})`);
  }

  private async storedDimensions(): Promise<number | null> {
    const { rows } = await this.q<{ value: string }>(
      `SELECT value FROM ${this.s}.embedding_meta WHERE key = 'vector_dimensions'`,
    );
    return rows[0] ? Number(rows[0].value) : null;
  }

  private async hasEmbeddingColumn(client: TxClient): Promise<boolean> {
    const { rowCount } = await client.query(
      `SELECT 1 FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'skills' AND column_name = 'embedding'`,
      [this.schema],
    );
    return (rowCount ?? 0) > 0;
  }

  isDegraded(): boolean {
    return this.degraded || this.closed;
  }

  getCapabilities(): SkillStoreCapabilities {
    return {
      vectorSearch: this.vectorReady,
      ftsSearch: this.ftsAvailable,
      nativeHybridSearch: false,
      sparseVectors: false,
    };
  }

  /** Mark closed. Never ends the shared pool. */
  close(): void {
    this.closed = true;
  }

  // ── Helpers ─────────────────────────────────────────────

  private q<R extends pg.QueryResultRow = pg.QueryResultRow>(
    text: string,
    values: unknown[] = [],
    client?: TxClient,
  ): Promise<pg.QueryResult<R>> {
    return (client ?? this.pool).query<R>({ text, values, types: PG_TYPES });
  }

  /** Await init for a read; false after close. */
  private async open(): Promise<boolean> {
    if (this.closed) return false;
    await this._ensureInit();
    return true;
  }

  /** One writer per skill (see header): append, archive, delete-version and delete-all serialize per skill_id. */
  private withSkillTx<T>(skillId: string, fn: (client: TxClient) => Promise<T>): Promise<T> {
    return withWriteTx(this.pool, `${this.schema}/skills/${skillId}`, fn);
  }

  private async one(where: string, params: unknown[]): Promise<Skill | null> {
    const { rows } = await this.q<SkillRow>(`SELECT ${SKILL_COLS} FROM ${this.s}.skills WHERE ${where} LIMIT 1`, params);
    return rows[0] ? toSkill(rows[0]) : null;
  }

  // ── appendVersion ───────────────────────────────────────

  async appendVersion(input: AppendVersionInput): Promise<Skill> {
    await this._ensureInit();
    const tid = input.team_id ?? "default";
    const head = await this.getHead(input.skill_id, tid);

    if (!head) {
      const oid = input.owner_agent_id ?? "default";
      const dup = await this.one(
        "team_id = $1 AND owner_agent_id = $2 AND name = $3 AND is_head AND status = 'active'",
        [tid, oid, input.name],
      );
      if (dup) throw new SkillStoreError("SKILL_NAME_DUPLICATE", `name '${input.name}' already exists for agent in team`);
    } else if (head.name !== input.name) {
      throw new SkillStoreError("SKILL_NAME_DUPLICATE", "name change is not allowed across versions");
    }

    const ts = this.now();
    const ftsContent = [input.name, input.description, input.content.slice(0, FTS_CONTENT_MAX)]
      .map(tokenizeForFts)
      .join(" ");
    const values = [
      this.ulid(), input.skill_id, head ? head.version + 1 : 1,
      input.user_id ?? "default", head ? head.owner_agent_id : (input.owner_agent_id ?? "default"),
      tid, input.task_id ?? "default",
      input.name, input.description, input.content, input.content_hash,
      JSON.stringify(input.manifest ?? []), input.storage_dir,
      input.metadata_json ?? "{}", ts, ftsContent,
    ];

    return this.withSkillTx(input.skill_id, async (client) => {
      if (head) {
        const demoted = await client.query(
          `UPDATE ${this.s}.skills SET is_head = false
            WHERE skill_id = $1 AND version = $2 AND is_head AND status = 'active'`,
          [head.skill_id, head.version],
        );
        if (demoted.rowCount !== 1) {
          throw new SkillStoreError(
            "SKILL_VERSION_STALE",
            `head v${head.version} of ${head.skill_id} was archived, deleted or replaced before append`,
          );
        }
      }
      const { rows } = await this.q<SkillRow>(
        `INSERT INTO ${this.s}.skills (
           row_id, skill_id, version, is_head, user_id, owner_agent_id, team_id, task_id,
           name, description, content, content_hash, manifest_json, storage_dir,
           status, metadata_json, created_at_ms, updated_at_ms, fts_content
         ) VALUES ($1,$2,$3,true,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'active',$14,$15,$15,$16)
         RETURNING ${SKILL_COLS}`,
        values,
        client,
      );
      return toSkill(rows[0]!);
    });
  }

  // ── archive / delete ────────────────────────────────────

  async archiveHead(skillId: string, teamId?: string): Promise<{ archived: boolean }> {
    if (!(await this.open())) return { archived: false };
    const params: unknown[] = [this.now(), skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    return this.withSkillTx(skillId, async (client) => {
      const r = await client.query(
        `UPDATE ${this.s}.skills SET status = 'archived', updated_at_ms = $1 WHERE skill_id = $2${team} AND is_head`,
        params,
      );
      if ((r.rowCount ?? 0) > 0) return { archived: true };
      // Already archived still counts (idempotent).
      const prev = await client.query(
        `SELECT 1 FROM ${this.s}.skills WHERE skill_id = $1${teamId ? " AND team_id = $2" : ""}
            AND is_head AND status = 'archived' LIMIT 1`,
        params.slice(1),
      );
      return { archived: (prev.rowCount ?? 0) > 0 };
    });
  }

  async deleteVersion(skillId: string, version: number): Promise<boolean> {
    if (!(await this.open())) return false;
    // Under the per-skill lock, so it never races appendVersion's head move.
    const r = await this.withSkillTx(skillId, (client) =>
      this.q(`DELETE FROM ${this.s}.skills WHERE skill_id = $1 AND version = $2 AND NOT is_head`, [skillId, version], client),
    );
    return (r.rowCount ?? 0) > 0;
  }

  async deleteAllVersions(skillId: string, teamId?: string): Promise<number> {
    if (!(await this.open())) return 0;
    const params: unknown[] = [skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    return this.withSkillTx(skillId, async (client) => {
      const r = await client.query(`DELETE FROM ${this.s}.skills WHERE skill_id = $1${team}`, params);
      return r.rowCount ?? 0;
    });
  }

  // ── Reads ───────────────────────────────────────────────

  /** Active head only; archived skills read as absent (see ISkillStore.getHead). */
  async getHead(skillId: string, teamId?: string): Promise<Skill | null> {
    if (!(await this.open())) return null;
    const params: unknown[] = [skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    return this.one(`skill_id = $1${team} AND is_head AND status = 'active'`, params);
  }

  async getHeadIncludingArchived(skillId: string, teamId?: string): Promise<Skill | null> {
    if (!(await this.open())) return null;
    const params: unknown[] = [skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    return this.one(`skill_id = $1${team} AND is_head`, params);
  }

  async getByVersion(skillId: string, version: number, teamId?: string): Promise<Skill | null> {
    if (!(await this.open())) return null;
    const params: unknown[] = [skillId, version];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    return this.one(`skill_id = $1 AND version = $2${team}`, params);
  }

  async listVersions(
    skillId: string,
    teamId?: string,
    opts: { limit?: number; offset?: number } = {},
  ): Promise<Skill[]> {
    if (!(await this.open())) return [];
    const limit = Math.min(Math.max(opts.limit ?? 50, 1), 1000);
    const offset = Math.max(opts.offset ?? 0, 0);
    const params: unknown[] = [skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    const { rows } = await this.q<SkillRow>(
      `SELECT ${SKILL_COLS} FROM ${this.s}.skills WHERE skill_id = $1${team}
        ORDER BY version DESC LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return rows.map(toSkill);
  }

  async countVersions(skillId: string, teamId?: string): Promise<number> {
    if (!(await this.open())) return 0;
    const params: unknown[] = [skillId];
    const team = teamId ? ` AND team_id = ${p(params, teamId)}` : "";
    const { rows } = await this.q<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${this.s}.skills WHERE skill_id = $1${team}`,
      params,
    );
    return rows[0]?.n ?? 0;
  }

  async listSkills(opts: ListSkillsOptions): Promise<{ items: Skill[]; total: number }> {
    if (!(await this.open())) return { items: [], total: 0 };
    const status = opts.status?.length ? opts.status : (["active"] as SkillStatus[]);
    const limit = Math.min(Math.max(opts.limit ?? 50, 1), 1000);
    const offset = Math.max(opts.offset ?? 0, 0);

    const params: unknown[] = [];
    const where: string[] = ["is_head"];
    if (opts.team_id) where.push(`team_id = ${p(params, opts.team_id)}`);
    if (opts.owner_agent_id) where.push(`owner_agent_id = ${p(params, opts.owner_agent_id)}`);
    if (opts.user_id) where.push(`user_id = ${p(params, opts.user_id)}`);
    if (opts.task_id) where.push(`task_id = ${p(params, opts.task_id)}`);
    where.push(`status = ANY(${p(params, status)}::text[])`);
    // SQLite LIKE is ASCII case-insensitive and does not escape %/_ in the prefix; ILIKE matches that.
    if (opts.name_prefix) where.push(`name ILIKE ${p(params, `${opts.name_prefix}%`)}`);
    const whereSql = where.join(" AND ");

    const { rows: c } = await this.q<{ n: number }>(
      `SELECT count(*)::int AS n FROM ${this.s}.skills WHERE ${whereSql}`,
      params,
    );
    const { rows } = await this.q<SkillRow>(
      `SELECT ${SKILL_COLS} FROM ${this.s}.skills WHERE ${whereSql}
        ORDER BY updated_at_ms DESC LIMIT ${p(params, limit)} OFFSET ${p(params, offset)}`,
      params,
    );
    return { items: rows.map(toSkill), total: c[0]?.n ?? 0 };
  }

  /**
   * Keyword search, SQLite mode semantics: only the BM25-equivalent path
   * exists; 'embedding'/'hybrid' without vectors or a query embedding log a
   * downgrade warning. Rank `ts_rank_cd`, score = r / (1 + r) ∈ [0, 1).
   */
  async searchSkills(opts: SearchSkillsOptions): Promise<SkillSearchResult[]> {
    const topK = Math.min(Math.max(opts.topK ?? 10, 1), 50);
    const query = (opts.query ?? "").trim();
    if (!query) return [];
    if (!(await this.open())) return [];

    const requestedMode = opts.mode ?? "bm25";
    if ((requestedMode === "embedding" || requestedMode === "hybrid") && (!this.vectorReady || !opts.queryEmbedding)) {
      this.logger?.warn(
        `${TAG} search mode='${requestedMode}' downgraded to 'bm25' ` +
          `(vec_available=${this.vectorReady}, has_embedding=${!!opts.queryEmbedding})`,
      );
    }
    if (!this.ftsAvailable) return [];
    const ftsQuery = buildFtsQuery(query);
    const tsq = ftsQuery ? buildPgTsQuery(ftsInputToTokens(ftsQuery)) : null;
    if (!tsq) return [];

    const params: unknown[] = [tsq];
    const where = ["content_tsv @@ tsq", "is_head", "status = 'active'"];
    if (opts.team_id) where.push(`team_id = ${p(params, opts.team_id)}`);
    if (opts.agent_id) where.push(`owner_agent_id = ${p(params, opts.agent_id)}`);
    if (opts.task_id) where.push(`task_id = ${p(params, opts.task_id)}`);
    if (opts.user_id) where.push(`user_id = ${p(params, opts.user_id)}`);
    try {
      const { rows } = await this.q<SkillRow & { rank: number; snippet: string }>(
        `SELECT ${SKILL_COLS}, ts_rank_cd(content_tsv, tsq) AS rank,
                ts_headline('simple', left(content, ${FTS_CONTENT_MAX}), tsq,
                            'StartSel=<mark>, StopSel=</mark>, MaxWords=16, MinWords=4') AS snippet
           FROM ${this.s}.skills, to_tsquery('simple', $1) AS tsq
          WHERE ${where.join(" AND ")}
          ORDER BY rank DESC
          LIMIT ${p(params, topK)}`,
        params,
      );
      return rows.map(({ rank, snippet, ...row }) => ({
        skill: toSkill(row),
        score: rank / (1 + rank),
        snippet: snippet ?? "",
      }));
    } catch (err) {
      this.logger?.warn(`${TAG} fts query failed: ${errMsg(err)}`);
      return [];
    }
  }

  // ── TTL cleanup ─────────────────────────────────────────

  async findExpiredVersions(cutoffMs: number): Promise<ExpiredVersionMeta[]> {
    if (!(await this.open())) return [];
    const { rows } = await this.q<ExpiredVersionMeta>(
      `SELECT skill_id, version, is_head, status, storage_dir, created_at_ms
         FROM ${this.s}.skills
        WHERE NOT is_head AND status = 'active' AND created_at_ms < $1
        ORDER BY skill_id ASC, version ASC`,
      [cutoffMs],
    );
    return rows;
  }

  // ── Embeddings (SQLite parity; not part of ISkillStore) ─

  /** Store the head row's vector. Non-fatal: a failure only logs. */
  async upsertEmbedding(skillId: string, embedding: Float32Array): Promise<void> {
    try {
      if (!(await this.open()) || !this.vectorReady) return;
      if (embedding.length !== this.dimensions) {
        this.logger?.warn(`${TAG} embedding dim mismatch: ${embedding.length} vs ${this.dimensions}`);
        return;
      }
      await this.q(
        `UPDATE ${this.s}.skills SET embedding = $2::${this.vecType} WHERE skill_id = $1 AND is_head`,
        [skillId, vecLiteral(embedding)],
      );
    } catch (err) {
      this.logger?.warn(`${TAG} upsertEmbedding failed: ${errMsg(err)}`);
    }
  }

  async deleteEmbedding(skillId: string): Promise<void> {
    try {
      if (!(await this.open()) || !this.vectorReady) return;
      await this.q(`UPDATE ${this.s}.skills SET embedding = NULL WHERE skill_id = $1`, [skillId]);
    } catch (err) {
      this.logger?.warn(`${TAG} deleteEmbedding failed: ${errMsg(err)}`);
    }
  }
}
