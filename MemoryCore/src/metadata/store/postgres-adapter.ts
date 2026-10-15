/**
 * PostgresMetadataStore — Postgres backend for `IMetadataStore`
 * A port of `SqliteMetadataStore`
 * (`./sqlite-adapter.ts`) with the same row shapes and semantics.
 *
 * Ownership and data flow:
 * - One schema per instance (`pgSchemaName("meta", …)`). Every statement
 *   uses `${schema}.table`; `search_path` is never touched.
 * - The pool comes from the shared `PgPoolRegistry`; `close()` never ends it.
 * - `_ensureInit()` gates every data call; a failed init resets the
 *   promise so the next call retries. Statements inside a transaction already
 *   passed the gate when the transaction started. A statement or transaction
 *   that finds the schema dropped (3F000/42P01, another gateway's /v3 destroy)
 *   resets the gate, re-inits and retries once (`run`).
 * - Tables are versioned migrations ported from the SQLite `createSchema()`.
 *   Time columns stay ISO-8601 text (the entities expose strings) with
 *   `COLLATE "C"`, so ORDER BY / range filters match SQLite byte order.
 * - Transactions: every SQLite `BEGIN IMMEDIATE` site and every
 *   read-a-set-then-write (upstream groups, default-key promotion) runs in
 *   `withWriteTx` — one writer per schema. Other multi-statement writes run in
 *   `withTx`. Single-row upserts use `INSERT … ON CONFLICT` instead of
 *   SELECT-then-write.
 * - Errors: constraint names via `isUniqueViolation(err, name)`, never
 *   message regexes. Generated-id collisions retry on the table's `_pkey` only.
 */

import type pg from "pg";
import { mapTeamMemberWithProfile } from "./team-member-view.js";
import { generateId, generateRelationId, ID_PREFIX } from "../utils/id-generator.js";
import { pgRelationIdCollision, runWithGeneratedRelationId } from "./relation-id-insert.js";
import { generateUserKey } from "../utils/crypto.js";
import { isUserKeyExpired } from "../utils/user-key.js";
import { DEFAULT_GROUP_NAME, InstanceUpstreamWriteConflictError } from "../types.js";
import type {
  UserEntity,
  UserKeyEntity,
  TeamEntity,
  TeamMemberEntity,
  TeamMemberView,
  AgentEntity,
  TaskEntity,
  TaskAgentEntity,
  ParticipationLogEntity,
  AppendParticipationLogInput,
  ParticipationLogFilter,
  AssetEntity,
  FixedAssetBindingEntity,
  AclEntity,
  CreateUserInput,
  CreateUserKeyInput,
  CreateTeamInput,
  AddTeamMemberInput,
  CreateAgentInput,
  CreateTaskInput,
  CreateAssetInput,
  FixedAssetBindingInput,
  GrantAclInput,
  AgentFilter,
  TaskFilter,
  AssetFilter,
  BatchDeleteResult,
  ListPage,
  PaginationParams,
  InstanceUserListFilter,
  AgentFixedAssetCountRow,
  AssetType,
  ConfigParamEntity,
  UpsertConfigParamInput,
  ListConfigParamsFilter,
  UserSessionDefaultEntity,
  UpsertUserSessionDefaultInput,
  InstanceUpstreamConfigEntity,
  InstanceUpstreamConfigFilter,
  CreateInstanceUpstreamGroupInput,
  UpdateInstanceUpstreamGroupInput,
  ToggleInstanceUpstreamGroupInput,
  DeleteInstanceUpstreamGroupInput,
  SupportedAgent,
  GroupType,
  UpstreamMode,
} from "../types.js";
import { DEFAULT_PAGINATION } from "../pagination.js";
import { buildChatMemoryAssetId } from "../utils/chat-memory-asset.js";
import { DuplicateUserKeyError, type IMetadataStore } from "./interface.js";
import { isUniqueViolation } from "../../core/store/errors.js";
import { runMigrations, type Migration } from "../../core/store/postgres/migrate.js";
import { quoteIdent } from "../../core/store/postgres/schema-name.js";
import { withTx, withWriteTx, type TxClient } from "../../core/store/postgres/tx.js";
import type { StoreLogger } from "../../core/store/types.js";

const TAG = "[metadata][postgres]";

const PK_RETRY_LIMIT = 3;

/** Named constraints the store maps to domain errors. */
const USER_KEY_VALUE_UNIQUE = "meta_user_keys_key_value_key";
const UPSTREAM_SINGLETON_INDEX = "ux_iuc_singleton_group_type";

/**
 * Metadata schema migrations, ported from `SqliteMetadataStore.createSchema()`.
 * Not ported: the SQLite-only legacy steps (`user_key` column backfill,
 * `user_type` ALTER, the v1 upstream table DROP) — a Postgres schema starts at v2.
 */
export const METADATA_MIGRATIONS: readonly Migration[] = [
  {
    version: 1,
    up: (s) => `
      CREATE TABLE ${s}.meta_users (
        user_id text COLLATE "C" PRIMARY KEY,
        password text,
        auth_provider text NOT NULL,
        external_id text NOT NULL,
        username text NOT NULL,
        display_name text,
        email text,
        raw_profile_json text NOT NULL DEFAULT '{}',
        status text NOT NULL DEFAULT 'active',
        user_type text NOT NULL DEFAULT 'normal',
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE UNIQUE INDEX ux_meta_users_system_admin ON ${s}.meta_users (user_type) WHERE user_type = 'system_admin';
      CREATE INDEX idx_meta_users_auth_username ON ${s}.meta_users (auth_provider, username);
      CREATE INDEX idx_meta_users_auth_external ON ${s}.meta_users (auth_provider, external_id);
      CREATE INDEX idx_meta_users_email ON ${s}.meta_users (email) WHERE email IS NOT NULL;
      CREATE INDEX idx_meta_users_created ON ${s}.meta_users (created_at DESC);

      CREATE TABLE ${s}.meta_user_keys (
        key_id text COLLATE "C" PRIMARY KEY,
        user_id text COLLATE "C" NOT NULL,
        key_value text NOT NULL CONSTRAINT ${USER_KEY_VALUE_UNIQUE} UNIQUE,
        name text,
        status text NOT NULL DEFAULT 'active',
        is_default boolean NOT NULL DEFAULT false,
        last_used_at text COLLATE "C",
        expires_at text COLLATE "C",
        created_at text COLLATE "C" NOT NULL,
        revoked_at text COLLATE "C",
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE INDEX idx_meta_user_keys_user ON ${s}.meta_user_keys (user_id, status);
      CREATE INDEX idx_meta_user_keys_user_created ON ${s}.meta_user_keys (user_id, created_at DESC);

      CREATE TABLE ${s}.meta_teams (
        team_id text COLLATE "C" PRIMARY KEY,
        name text NOT NULL,
        description text,
        owner_user_id text COLLATE "C" NOT NULL,
        status text NOT NULL DEFAULT 'active',
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE INDEX idx_meta_teams_created ON ${s}.meta_teams (created_at DESC);

      CREATE TABLE ${s}.meta_team_members (
        id text COLLATE "C" PRIMARY KEY,
        team_id text COLLATE "C" NOT NULL,
        user_id text COLLATE "C" NOT NULL,
        role text NOT NULL DEFAULT 'member',
        joined_at text COLLATE "C" NOT NULL,
        status text NOT NULL DEFAULT 'active',
        UNIQUE (team_id, user_id)
      );
      CREATE INDEX idx_meta_members_user_status ON ${s}.meta_team_members (user_id, status);
      CREATE INDEX idx_meta_members_team_status_joined ON ${s}.meta_team_members (team_id, status, joined_at DESC);

      CREATE TABLE ${s}.meta_agents (
        agent_id text COLLATE "C" PRIMARY KEY,
        team_id text COLLATE "C" NOT NULL,
        owner_user_id text COLLATE "C" NOT NULL,
        name text NOT NULL,
        description text,
        prompt text,
        visibility text NOT NULL DEFAULT 'team',
        status text NOT NULL DEFAULT 'active',
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE INDEX idx_meta_agents_team_status ON ${s}.meta_agents (team_id, status, created_at DESC);
      CREATE INDEX idx_meta_agents_owner_status_created ON ${s}.meta_agents (owner_user_id, status, created_at DESC);

      CREATE TABLE ${s}.meta_tasks (
        task_id text COLLATE "C" PRIMARY KEY,
        team_id text COLLATE "C" NOT NULL,
        creator_user_id text COLLATE "C" NOT NULL,
        title text NOT NULL,
        description text,
        source_type text NOT NULL DEFAULT 'manual',
        source_url text,
        status text NOT NULL DEFAULT 'running',
        auto_assign_floating_assets boolean NOT NULL DEFAULT false,
        risk_level text,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE INDEX idx_meta_tasks_team_status ON ${s}.meta_tasks (team_id, status, created_at DESC);
      CREATE INDEX idx_meta_tasks_creator_status_created ON ${s}.meta_tasks (creator_user_id, status, created_at DESC);

      CREATE TABLE ${s}.meta_task_agents (
        id text COLLATE "C" PRIMARY KEY,
        task_id text COLLATE "C" NOT NULL,
        agent_id text COLLATE "C" NOT NULL,
        role_in_task text,
        status text NOT NULL DEFAULT 'active',
        created_at text COLLATE "C" NOT NULL,
        UNIQUE (task_id, agent_id)
      );
      CREATE INDEX idx_meta_task_agents_task_status_created ON ${s}.meta_task_agents (task_id, status, created_at DESC);

      CREATE TABLE ${s}.meta_participation_logs (
        id text COLLATE "C" PRIMARY KEY,
        team_id text COLLATE "C" NOT NULL,
        task_id text COLLATE "C" NOT NULL,
        agent_id text COLLATE "C" NOT NULL,
        user_id text COLLATE "C" NOT NULL,
        source text NOT NULL DEFAULT 'unknown',
        metadata_json text NOT NULL DEFAULT '{}',
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL
      );
      CREATE INDEX idx_meta_pl_team_created ON ${s}.meta_participation_logs (team_id, created_at DESC);
      CREATE INDEX idx_meta_pl_team_task_agent_created ON ${s}.meta_participation_logs (team_id, task_id, agent_id, created_at DESC);
      CREATE INDEX idx_meta_pl_team_user_created ON ${s}.meta_participation_logs (team_id, user_id, created_at DESC);
      CREATE INDEX idx_meta_pl_team_dims_created ON ${s}.meta_participation_logs (team_id, task_id, agent_id, user_id, created_at DESC);

      CREATE TABLE ${s}.meta_assets (
        asset_id text COLLATE "C" PRIMARY KEY,
        team_id text COLLATE "C" NOT NULL,
        asset_type text NOT NULL,
        name text NOT NULL,
        description text,
        owner_user_id text COLLATE "C" NOT NULL,
        source_type text NOT NULL,
        source_ref text,
        version integer NOT NULL DEFAULT 1,
        visibility text NOT NULL DEFAULT 'team',
        status text NOT NULL DEFAULT 'draft',
        confidence double precision,
        expires_at text COLLATE "C",
        last_used_at text COLLATE "C",
        usage_count integer NOT NULL DEFAULT 0,
        content_ref text,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        metadata_json text NOT NULL DEFAULT '{}'
      );
      CREATE INDEX idx_meta_assets_team_status ON ${s}.meta_assets (team_id, status, created_at DESC);

      CREATE TABLE ${s}.meta_agent_fixed_assets (
        id text COLLATE "C" PRIMARY KEY,
        agent_id text COLLATE "C" NOT NULL,
        asset_id text COLLATE "C" NOT NULL,
        asset_type text NOT NULL,
        injection_mode text NOT NULL DEFAULT 'summary',
        priority integer NOT NULL DEFAULT 50,
        created_by text NOT NULL,
        created_at text COLLATE "C" NOT NULL,
        UNIQUE (agent_id, asset_id)
      );
      CREATE INDEX idx_meta_fixed_agent_prio_created ON ${s}.meta_agent_fixed_assets (agent_id, priority DESC, created_at DESC);

      CREATE TABLE ${s}.meta_asset_acl (
        id text COLLATE "C" PRIMARY KEY,
        asset_id text COLLATE "C" NOT NULL,
        subject_type text NOT NULL,
        subject_id text COLLATE "C" NOT NULL,
        permission text NOT NULL,
        effect text NOT NULL DEFAULT 'allow',
        granted_by text NOT NULL,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        UNIQUE (asset_id, subject_type, subject_id, permission)
      );
      CREATE INDEX idx_meta_acl_asset_created ON ${s}.meta_asset_acl (asset_id, created_at DESC);
      CREATE INDEX idx_meta_acl_subject_created ON ${s}.meta_asset_acl (subject_type, subject_id, created_at DESC);

      CREATE TABLE ${s}.meta_config_params (
        id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        scope text COLLATE "C" NOT NULL CHECK (scope IN ('global', 'user')),
        user_id text COLLATE "C",
        module text NOT NULL,
        param_name text COLLATE "C" NOT NULL,
        param_value text NOT NULL,
        description text NOT NULL,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL,
        CHECK ((scope = 'global' AND user_id IS NULL) OR (scope = 'user' AND user_id IS NOT NULL))
      );
      CREATE UNIQUE INDEX ux_meta_config_params_global ON ${s}.meta_config_params (module, param_name) WHERE scope = 'global';
      CREATE UNIQUE INDEX ux_meta_config_params_user ON ${s}.meta_config_params (user_id, module, param_name) WHERE scope = 'user';
      CREATE INDEX idx_meta_config_params_module ON ${s}.meta_config_params (module);

      CREATE TABLE ${s}.meta_user_session_defaults (
        user_id text COLLATE "C" PRIMARY KEY REFERENCES ${s}.meta_users (user_id) ON DELETE CASCADE,
        team_id text COLLATE "C" NOT NULL,
        agent_id text COLLATE "C" NOT NULL,
        task_id text COLLATE "C" NOT NULL,
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL
      );
      CREATE INDEX idx_meta_user_session_defaults_team ON ${s}.meta_user_session_defaults (team_id);
      CREATE INDEX idx_meta_user_session_defaults_agent ON ${s}.meta_user_session_defaults (agent_id);
      CREATE INDEX idx_meta_user_session_defaults_task ON ${s}.meta_user_session_defaults (task_id);

      CREATE TABLE ${s}.meta_instance_upstream_config (
        id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        group_id text COLLATE "C" NOT NULL UNIQUE,
        group_type text NOT NULL CHECK (group_type IN ('default', 'custom', 'extraction')),
        name text NOT NULL DEFAULT '',
        agents text NOT NULL DEFAULT '[]',
        enabled boolean NOT NULL DEFAULT true,
        mode text NOT NULL DEFAULT 'official' CHECK (mode IN ('official', 'custom_unified', 'custom_passthrough')),
        base_url text NOT NULL DEFAULT '',
        api_key text NOT NULL DEFAULT '',
        model_id text NOT NULL DEFAULT '',
        description text NOT NULL DEFAULT '',
        version integer NOT NULL DEFAULT 1,
        supported_agents_snapshot text NOT NULL DEFAULT '[]',
        created_at text COLLATE "C" NOT NULL,
        updated_at text COLLATE "C" NOT NULL
      );
      -- default / extraction singletons: second guard behind withWriteTx.
      CREATE UNIQUE INDEX ${UPSTREAM_SINGLETON_INDEX} ON ${s}.meta_instance_upstream_config (group_type)
        WHERE group_type IN ('default', 'extraction');
    `,
  },
];

const TABLES = [
  "meta_users",
  "meta_user_keys",
  "meta_teams",
  "meta_team_members",
  "meta_agents",
  "meta_tasks",
  "meta_task_agents",
  "meta_participation_logs",
  "meta_assets",
  "meta_agent_fixed_assets",
  "meta_asset_acl",
  "meta_config_params",
  "meta_user_session_defaults",
  "meta_instance_upstream_config",
] as const;
type Table = (typeof TABLES)[number];

/** Tables whose `applyUpdate` also refreshes `updated_at` (SQLite parity). */
const TABLES_WITH_UPDATED_AT: ReadonlySet<Table> = new Set([
  "meta_users",
  "meta_teams",
  "meta_agents",
  "meta_tasks",
  "meta_assets",
]);

type Row = Record<string, unknown>;
type Queryable = pg.Pool | TxClient;

function nowIso(): string {
  return new Date().toISOString();
}

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * The schema or a table is gone: `3F000` invalid_schema_name, `42P01`
 * undefined_table. Seen after another gateway's /v3 destroy dropped the schema
 * while this process still has the store cached as initialised.
 */
function isSchemaGone(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code;
  return code === "3F000" || code === "42P01";
}

function isPkCollision(err: unknown, table: Table): boolean {
  return isUniqueViolation(err, `${table}_pkey`);
}

/** `$n` placeholder builder: `$(value)` appends the value and returns its placeholder. */
function sqlParams(initial: unknown[] = []) {
  const values: unknown[] = [...initial];
  return {
    values,
    $: (v: unknown): string => {
      values.push(v);
      return `$${values.length}`;
    },
  };
}

function parseJsonArray(raw: unknown): string[] {
  try {
    const v = JSON.parse(String(raw ?? "[]"));
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}

/** Run `fn`; retry up to PK_RETRY_LIMIT times while `retryable(err)` (generated-id collisions). */
async function retryOnCollision<T>(fn: () => Promise<T>, retryable: (err: unknown) => boolean): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt < PK_RETRY_LIMIT && retryable(err)) continue;
      throw err;
    }
  }
}

export interface PostgresMetadataStoreOptions {
  /** Shared pool from `PgPoolRegistry` — never ended by the store. */
  pool: pg.Pool;
  /** Instance schema, from `pgSchemaName("meta", instanceId, prefix)`. */
  schema: string;
  logger?: StoreLogger;
}

export class PostgresMetadataStore implements IMetadataStore {
  private readonly pool: pg.Pool;
  private readonly schema: string;
  private readonly logger?: StoreLogger;
  /** Schema-qualified, quoted table names. */
  private readonly t: Record<Table, string>;

  private initPromise: Promise<void> | null = null;
  private ready = false;
  private closed = false;

  constructor(opts: PostgresMetadataStoreOptions) {
    this.pool = opts.pool;
    this.schema = opts.schema;
    this.logger = opts.logger;
    const s = quoteIdent(opts.schema);
    this.t = Object.fromEntries(TABLES.map((n) => [n, `${s}.${n}`])) as Record<Table, string>;
  }

  // ── Lifecycle ───────────────────────────────────────────

  init(): Promise<void> {
    return this._ensureInit();
  }

  /** Mark closed. Never ends the shared pool. */
  close(): void {
    this.closed = true;
  }

  /** Init gate: one in-flight init; a rejection resets it for a retry. */
  private _ensureInit(): Promise<void> {
    if (this.closed) return Promise.reject(new Error(`${TAG} store is closed (${this.schema})`));
    if (this.ready) return Promise.resolve();
    this.initPromise ??= this._initAsync().then(
      () => {
        this.ready = true;
      },
      (err: unknown) => {
        this.initPromise = null;
        this.logger?.error(`${TAG} init failed for schema ${this.schema}: ${errMsg(err)}`);
        throw err;
      },
    );
    return this.initPromise;
  }

  private async _initAsync(): Promise<void> {
    const applied = await runMigrations(this.pool, this.schema, METADATA_MIGRATIONS);
    if (applied.length > 0) this.logger?.info(`${TAG} ${this.schema}: applied migrations ${applied.join(",")}`);
  }

  /**
   * Every pool-level operation goes through here: wait for the init gate, run
   * `op`; if the schema was dropped under a cached store (another gateway's
   * /v3 destroy), reset the gate, re-run init (migrations recreate an empty
   * schema, as a fresh store would) and retry `op` once. `op` is one statement
   * or one whole transaction (rolled back on the error), so a retry is safe.
   */
  private async run<T>(op: () => Promise<T>): Promise<T> {
    await this._ensureInit();
    try {
      return await op();
    } catch (err) {
      if (!isSchemaGone(err)) throw err;
      this.logger?.warn(`${TAG} ${this.schema} is gone (${errMsg(err)}); re-initialising and retrying once`);
      // Only the first failing caller resets; others join the init it started.
      if (this.ready) {
        this.ready = false;
        this.initPromise = null;
      }
      await this._ensureInit();
      return op();
    }
  }

  // ── Query helpers ───────────────────────────────────────

  /** Rows of one statement. Without `c` the call waits for the init gate. */
  private async q<R extends Row = Row>(sql: string, params: unknown[] = [], c?: Queryable): Promise<R[]> {
    if (c) return (await c.query<R>(sql, params)).rows;
    return this.run(async () => (await this.pool.query<R>(sql, params)).rows);
  }

  private async one<R extends Row = Row>(sql: string, params: unknown[] = [], c?: Queryable): Promise<R | null> {
    return (await this.q<R>(sql, params, c))[0] ?? null;
  }

  /** Affected-row count of one statement. */
  private async exec(sql: string, params: unknown[] = [], c?: Queryable): Promise<number> {
    if (c) return (await c.query(sql, params)).rowCount ?? 0;
    return this.run(async () => (await this.pool.query(sql, params)).rowCount ?? 0);
  }

  private tx<T>(fn: (c: TxClient) => Promise<T>): Promise<T> {
    return this.run(() => withTx(this.pool, fn));
  }

  /** One writer per schema: former `BEGIN IMMEDIATE` sites and set-checking writes. */
  private writeTx<T>(fn: (c: TxClient) => Promise<T>): Promise<T> {
    return this.run(() => withWriteTx(this.pool, this.schema, fn));
  }

  private async selectList<T>(
    countSql: string,
    dataSql: string,
    params: unknown[],
    pagination: PaginationParams | null | undefined,
    mapper: (row: Row) => T | null,
  ): Promise<ListPage<T>> {
    const total = (await this.one<{ c: number }>(countSql, params))?.c ?? 0;
    const p = pagination ?? DEFAULT_PAGINATION;
    const n = params.length;
    const rows = await this.q(`${dataSql} LIMIT $${n + 1} OFFSET $${n + 2}`, [...params, p.limit, p.offset]);
    const items: T[] = [];
    for (const r of rows) {
      const mapped = mapper(r);
      if (mapped) items.push(mapped);
    }
    return { items, total };
  }

  // ============================================================
  // User
  // ============================================================

  /** One INSERT (no retry: inside a transaction a collision aborts it; callers retry). */
  private async insertUserKeyRow(
    c: Queryable,
    input: {
      user_id: string;
      key_value: string;
      name?: string | null;
      is_default?: boolean;
      expires_at?: string | null;
      created_at?: string;
      metadata_json?: string;
    },
  ): Promise<string> {
    const keyId = generateId(ID_PREFIX.userKey);
    await this.q(
      `INSERT INTO ${this.t.meta_user_keys}
         (key_id, user_id, key_value, name, status, is_default, last_used_at, expires_at, created_at, revoked_at, metadata_json)
       VALUES ($1, $2, $3, $4, 'active', $5, NULL, $6, $7, NULL, $8)`,
      [
        keyId,
        input.user_id,
        input.key_value,
        input.name ?? null,
        input.is_default ?? false,
        input.expires_at ?? null,
        input.created_at ?? nowIso(),
        input.metadata_json ?? "{}",
      ],
      c,
    );
    return keyId;
  }

  async createUser(input: CreateUserInput): Promise<UserEntity> {
    const now = nowIso();
    const defaultKeyValue = input.default_key_value ?? generateUserKey();
    return retryOnCollision(
      async () => {
        const userId = input.user_id ?? generateId(ID_PREFIX.user);
        try {
          await this.tx(async (c) => {
            await this.q(
              `INSERT INTO ${this.t.meta_users}
                 (user_id, password, auth_provider, external_id, username, display_name, email,
                  raw_profile_json, status, user_type, created_at, updated_at, metadata_json)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)`,
              [
                userId,
                input.password ?? null,
                input.auth_provider,
                input.external_id,
                input.username,
                input.display_name ?? null,
                input.email ?? null,
                input.raw_profile_json ?? "{}",
                input.status ?? "active",
                input.user_type ?? "normal",
                now,
                input.metadata_json ?? "{}",
              ],
              c,
            );
            await this.insertUserKeyRow(c, { user_id: userId, key_value: defaultKeyValue, is_default: true, created_at: now });
          });
        } catch (err) {
          // An explicit default_key_value hit the UNIQUE key: business error (HTTP 409).
          if (isUniqueViolation(err, USER_KEY_VALUE_UNIQUE)) throw new DuplicateUserKeyError(defaultKeyValue);
          throw err;
        }
        return (await this.getUserById(userId))!;
      },
      (err) => (!input.user_id && isPkCollision(err, "meta_users")) || isPkCollision(err, "meta_user_keys"),
    );
  }

  async getUserById(userId: string): Promise<UserEntity | null> {
    return this.mapUser(await this.one(`SELECT * FROM ${this.t.meta_users} WHERE user_id = $1`, [userId]));
  }

  async getUserByKey(userKey: string): Promise<UserEntity | null> {
    const keyRow = this.mapUserKey(
      await this.one(`SELECT * FROM ${this.t.meta_user_keys} WHERE key_value = $1 AND status = 'active'`, [userKey]),
    );
    if (!keyRow || isUserKeyExpired(keyRow.expires_at)) return null;
    await this.touchUserKeyUsage(keyRow.key_id);
    return this.getUserById(keyRow.user_id);
  }

  async getDefaultUserKey(userId: string): Promise<UserKeyEntity | null> {
    return this.mapUserKey(
      await this.one(
        `SELECT * FROM ${this.t.meta_user_keys} WHERE user_id = $1 AND is_default AND status = 'active' LIMIT 1`,
        [userId],
      ),
    );
  }

  async getUserByUsername(authProvider: string, username: string): Promise<UserEntity | null> {
    return this.mapUser(
      await this.one(`SELECT * FROM ${this.t.meta_users} WHERE auth_provider = $1 AND username = $2`, [authProvider, username]),
    );
  }

  async getUserByEmail(email: string): Promise<UserEntity | null> {
    return this.mapUser(await this.one(`SELECT * FROM ${this.t.meta_users} WHERE email = $1`, [email]));
  }

  async getUserByExternalId(authProvider: string, externalId: string): Promise<UserEntity | null> {
    return this.mapUser(
      await this.one(`SELECT * FROM ${this.t.meta_users} WHERE auth_provider = $1 AND external_id = $2`, [
        authProvider,
        externalId,
      ]),
    );
  }

  async updateUser(userId: string, patch: Partial<UserEntity>): Promise<UserEntity | null> {
    // external_id / auth_provider must stay in the whitelist (see SqliteMetadataStore.updateUser).
    const allowed = ["password", "display_name", "email", "raw_profile_json", "status", "metadata_json", "username", "external_id", "auth_provider"] as const;
    await this.applyUpdate("meta_users", "user_id", userId, allowed, patch);
    return this.getUserById(userId);
  }

  async deleteUsers(userIds: string[]): Promise<BatchDeleteResult> {
    return this.tx(async (c) => {
      const result = await this.batchDelete(c, "meta_users", "user_id", userIds);
      if (result.deleted_ids.length > 0) {
        const ids = [result.deleted_ids];
        await this.exec(`DELETE FROM ${this.t.meta_user_keys} WHERE user_id = ANY($1)`, ids, c);
        await this.exec(`DELETE FROM ${this.t.meta_team_members} WHERE user_id = ANY($1)`, ids, c);
        await this.exec(`DELETE FROM ${this.t.meta_asset_acl} WHERE subject_type = 'user' AND subject_id = ANY($1)`, ids, c);
      }
      return result;
    });
  }

  async listUsersByTeam(
    teamId: string,
    pagination?: PaginationParams | null,
    filter?: InstanceUserListFilter,
  ): Promise<ListPage<UserEntity>> {
    const p = sqlParams();
    let base = `FROM ${this.t.meta_users} u JOIN ${this.t.meta_team_members} m ON m.user_id = u.user_id
      WHERE m.team_id = ${p.$(teamId)} AND m.status = 'active'`;
    if (filter?.user_ids?.length) base += ` AND u.user_id = ANY(${p.$(filter.user_ids)})`;
    if (filter?.username) base += ` AND u.username = ${p.$(filter.username)}`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT u.* ${base} ORDER BY u.created_at DESC`,
      p.values,
      pagination,
      (r) => this.mapUser(r),
    );
  }

  async listUsers(pagination?: PaginationParams | null, filter?: InstanceUserListFilter): Promise<ListPage<UserEntity>> {
    const p = sqlParams();
    let where = "WHERE true";
    if (filter?.status) where += ` AND status = ${p.$(filter.status)}`;
    if (filter?.user_type) where += ` AND user_type = ${p.$(filter.user_type)}`;
    if (filter?.user_ids?.length) where += ` AND user_id = ANY(${p.$(filter.user_ids)})`;
    if (filter?.username) where += ` AND username = ${p.$(filter.username)}`;
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_users} ${where}`,
      `SELECT * FROM ${this.t.meta_users} ${where} ORDER BY created_at DESC`,
      p.values,
      pagination,
      (r) => this.mapUser(r),
    );
  }

  private async count(sql: string, params: unknown[] = []): Promise<number> {
    return (await this.one<{ c: number }>(sql, params))?.c ?? 0;
  }

  countUsers(): Promise<number> {
    return this.count(`SELECT count(*)::int AS c FROM ${this.t.meta_users}`);
  }

  countSystemAdmins(): Promise<number> {
    return this.count(`SELECT count(*)::int AS c FROM ${this.t.meta_users} WHERE user_type = 'system_admin'`);
  }

  countTeams(): Promise<number> {
    return this.count(`SELECT count(*)::int AS c FROM ${this.t.meta_teams}`);
  }

  // ============================================================
  // UserKey — default-key changes read the user's key set, so they hold the write lock.
  // ============================================================

  async createUserKey(input: CreateUserKeyInput): Promise<UserKeyEntity> {
    const keyValue = input.key_value ?? generateUserKey();
    const keyId = await retryOnCollision(
      () =>
        this.writeTx(async (c) => {
          if (input.is_default) {
            await this.exec(
              `UPDATE ${this.t.meta_user_keys} SET is_default = false WHERE user_id = $1 AND status = 'active'`,
              [input.user_id],
              c,
            );
          }
          return this.insertUserKeyRow(c, {
            user_id: input.user_id,
            key_value: keyValue,
            name: input.name,
            is_default: input.is_default ?? false,
            expires_at: input.expires_at,
            metadata_json: input.metadata_json,
          });
        }),
      (err) => isPkCollision(err, "meta_user_keys"),
    );
    return (await this.getUserKeyById(keyId))!;
  }

  async getUserKeyById(keyId: string): Promise<UserKeyEntity | null> {
    return this.mapUserKey(await this.one(`SELECT * FROM ${this.t.meta_user_keys} WHERE key_id = $1`, [keyId]));
  }

  async listUserKeys(userId: string, pagination?: PaginationParams | null): Promise<ListPage<UserKeyEntity>> {
    const base = `FROM ${this.t.meta_user_keys} WHERE user_id = $1`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY created_at DESC`,
      [userId],
      pagination,
      (r) => this.mapUserKey(r),
    );
  }

  countActiveUserKeys(userId: string): Promise<number> {
    return this.count(`SELECT count(*)::int AS c FROM ${this.t.meta_user_keys} WHERE user_id = $1 AND status = 'active'`, [
      userId,
    ]);
  }

  async revokeUserKey(keyId: string, options?: { promoteNextDefault?: boolean }): Promise<UserKeyEntity | null> {
    const promoteNextDefault = options?.promoteNextDefault ?? true;
    return this.writeTx(async (c) => {
      const existing = this.mapUserKey(await this.one(`SELECT * FROM ${this.t.meta_user_keys} WHERE key_id = $1`, [keyId], c));
      if (!existing) return null;
      if (existing.is_default && promoteNextDefault) {
        await this.exec(
          `UPDATE ${this.t.meta_user_keys} SET is_default = true
            WHERE key_id = (SELECT key_id FROM ${this.t.meta_user_keys}
                             WHERE user_id = $1 AND status = 'active' AND key_id <> $2
                             ORDER BY created_at ASC LIMIT 1)`,
          [existing.user_id, keyId],
          c,
        );
      }
      await this.exec(`DELETE FROM ${this.t.meta_user_keys} WHERE key_id = $1`, [keyId], c);
      return existing;
    });
  }

  async updateUserKey(
    keyId: string,
    patch: Partial<Pick<UserKeyEntity, "name" | "expires_at" | "is_default" | "metadata_json">>,
  ): Promise<UserKeyEntity | null> {
    return this.writeTx(async (c) => {
      const existing = this.mapUserKey(await this.one(`SELECT * FROM ${this.t.meta_user_keys} WHERE key_id = $1`, [keyId], c));
      if (!existing) return null;
      if (patch.is_default === true) {
        await this.exec(
          `UPDATE ${this.t.meta_user_keys} SET is_default = false WHERE user_id = $1 AND status = 'active'`,
          [existing.user_id],
          c,
        );
      }
      const allowed = ["name", "expires_at", "is_default", "metadata_json"] as const;
      await this.applyUpdate("meta_user_keys", "key_id", keyId, allowed, patch, c);
      return this.mapUserKey(await this.one(`SELECT * FROM ${this.t.meta_user_keys} WHERE key_id = $1`, [keyId], c));
    });
  }

  async touchUserKeyUsage(keyId: string): Promise<void> {
    await this.exec(`UPDATE ${this.t.meta_user_keys} SET last_used_at = $1 WHERE key_id = $2`, [nowIso(), keyId]);
  }

  async revokeAllUserKeysForUser(userId: string): Promise<void> {
    await this.exec(`DELETE FROM ${this.t.meta_user_keys} WHERE user_id = $1 AND status = 'active'`, [userId]);
  }

  // ============================================================
  // Team (owner becomes admin member atomically)
  // ============================================================

  async createTeam(input: CreateTeamInput): Promise<TeamEntity> {
    const now = nowIso();
    return retryOnCollision(
      async () => {
        const teamId = input.team_id ?? generateId(ID_PREFIX.team);
        await this.tx(async (c) => {
          await this.q(
            `INSERT INTO ${this.t.meta_teams}
               (team_id, name, description, owner_user_id, status, created_at, updated_at, metadata_json)
             VALUES ($1, $2, $3, $4, $5, $6, $6, $7)`,
            [teamId, input.name, input.description ?? null, input.owner_user_id, input.status ?? "active", now, input.metadata_json ?? "{}"],
            c,
          );
          await this.q(
            `INSERT INTO ${this.t.meta_team_members} (id, team_id, user_id, role, joined_at, status)
             VALUES ($1, $2, $3, 'admin', $4, 'active')`,
            [generateRelationId(), teamId, input.owner_user_id, now],
            c,
          );
        });
        return (await this.getTeamById(teamId))!;
      },
      (err) => (!input.team_id && isPkCollision(err, "meta_teams")) || isPkCollision(err, "meta_team_members"),
    );
  }

  async getTeamById(teamId: string): Promise<TeamEntity | null> {
    return this.mapTeam(await this.one(`SELECT * FROM ${this.t.meta_teams} WHERE team_id = $1`, [teamId]));
  }

  async updateTeam(teamId: string, patch: Partial<TeamEntity>): Promise<TeamEntity | null> {
    const allowed = ["name", "description", "status", "metadata_json"] as const;
    await this.applyUpdate("meta_teams", "team_id", teamId, allowed, patch);
    return this.getTeamById(teamId);
  }

  async deleteTeams(teamIds: string[]): Promise<BatchDeleteResult> {
    return this.tx(async (c) => {
      const result = await this.batchDelete(c, "meta_teams", "team_id", teamIds);
      if (result.deleted_ids.length > 0) {
        const ids = [result.deleted_ids];
        for (const table of ["meta_team_members", "meta_agents", "meta_tasks", "meta_assets"] as const) {
          await this.exec(`DELETE FROM ${this.t[table]} WHERE team_id = ANY($1)`, ids, c);
        }
      }
      return result;
    });
  }

  async listTeamsByUser(
    userId: string,
    pagination?: PaginationParams | null,
    filter?: { name?: string },
  ): Promise<ListPage<TeamEntity>> {
    const p = sqlParams();
    let base = `FROM ${this.t.meta_teams} t JOIN ${this.t.meta_team_members} m ON m.team_id = t.team_id
      WHERE m.user_id = ${p.$(userId)} AND m.status = 'active'`;
    if (filter?.name) base += ` AND t.name = ${p.$(filter.name)}`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT t.* ${base} ORDER BY t.created_at DESC`,
      p.values,
      pagination,
      (r) => this.mapTeam(r),
    );
  }

  // ============================================================
  // TeamMember
  // ============================================================

  async addTeamMember(input: AddTeamMemberInput): Promise<TeamMemberEntity> {
    const now = nowIso();
    await runWithGeneratedRelationId(input.id, pgRelationIdCollision("meta_team_members"), (id) =>
      this.exec(
        `INSERT INTO ${this.t.meta_team_members} (id, team_id, user_id, role, joined_at, status)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (team_id, user_id) DO UPDATE SET role = EXCLUDED.role, status = EXCLUDED.status`,
        [id, input.team_id, input.user_id, input.role ?? "member", now, input.status ?? "active"],
      ),
    );
    return (await this.getTeamMember(input.team_id, input.user_id))!;
  }

  async removeTeamMember(teamId: string, userId: string): Promise<void> {
    await this.exec(`DELETE FROM ${this.t.meta_team_members} WHERE team_id = $1 AND user_id = $2`, [teamId, userId]);
  }

  async listTeamMembers(teamId: string, pagination?: PaginationParams | null): Promise<ListPage<TeamMemberEntity>> {
    const base = `FROM ${this.t.meta_team_members} WHERE team_id = $1 AND status = 'active'`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY joined_at DESC`,
      [teamId],
      pagination,
      (r) => r as unknown as TeamMemberEntity,
    );
  }

  async getTeamMember(teamId: string, userId: string): Promise<TeamMemberEntity | null> {
    return (await this.one(`SELECT * FROM ${this.t.meta_team_members} WHERE team_id = $1 AND user_id = $2`, [
      teamId,
      userId,
    ])) as TeamMemberEntity | null;
  }

  async listTeamMembersWithProfile(teamId: string, pagination?: PaginationParams | null): Promise<ListPage<TeamMemberView>> {
    const base = `FROM ${this.t.meta_team_members} m LEFT JOIN ${this.t.meta_users} u ON u.user_id = m.user_id
      WHERE m.team_id = $1 AND m.status = 'active'`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT m.id, m.team_id, m.user_id, m.role, m.joined_at, m.status, COALESCE(u.username, '') AS username ${base}
       ORDER BY m.joined_at DESC`,
      [teamId],
      pagination,
      (r) => mapTeamMemberWithProfile(r as unknown as TeamMemberEntity & { username?: string }),
    );
  }

  async getTeamMemberWithProfile(teamId: string, userId: string): Promise<TeamMemberView | null> {
    const row = await this.one(
      `SELECT m.id, m.team_id, m.user_id, m.role, m.joined_at, m.status, COALESCE(u.username, '') AS username
         FROM ${this.t.meta_team_members} m
         LEFT JOIN ${this.t.meta_users} u ON u.user_id = m.user_id
        WHERE m.team_id = $1 AND m.user_id = $2`,
      [teamId, userId],
    );
    return row ? mapTeamMemberWithProfile(row as unknown as TeamMemberEntity & { username?: string }) : null;
  }

  // ============================================================
  // Agent
  // ============================================================

  async createAgent(input: CreateAgentInput): Promise<AgentEntity> {
    const now = nowIso();
    return retryOnCollision(
      async () => {
        const agentId = input.agent_id ?? generateId(ID_PREFIX.agent);
        await this.exec(
          `INSERT INTO ${this.t.meta_agents}
             (agent_id, team_id, owner_user_id, name, description, prompt, visibility, status, created_at, updated_at, metadata_json)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)`,
          [
            agentId,
            input.team_id,
            input.owner_user_id,
            input.name,
            input.description ?? null,
            input.prompt ?? null,
            input.visibility ?? "team",
            input.status ?? "active",
            now,
            input.metadata_json ?? "{}",
          ],
        );
        return (await this.getAgentById(agentId))!;
      },
      (err) => !input.agent_id && isPkCollision(err, "meta_agents"),
    );
  }

  async getAgentById(agentId: string): Promise<AgentEntity | null> {
    return this.mapAgent(await this.one(`SELECT * FROM ${this.t.meta_agents} WHERE agent_id = $1`, [agentId]));
  }

  async updateAgent(agentId: string, patch: Partial<AgentEntity>): Promise<AgentEntity | null> {
    const allowed = ["name", "description", "prompt", "visibility", "status", "metadata_json"] as const;
    await this.applyUpdate("meta_agents", "agent_id", agentId, allowed, patch);
    return this.getAgentById(agentId);
  }

  async deleteAgents(agentIds: string[]): Promise<BatchDeleteResult> {
    return this.tx(async (c) => {
      const existing = await this.q<{ agent_id: string; team_id: string }>(
        `SELECT agent_id, team_id FROM ${this.t.meta_agents} WHERE agent_id = ANY($1)`,
        [agentIds],
        c,
      );
      const selfMemoryByAgent = new Map(existing.map((a) => [a.agent_id, buildChatMemoryAssetId(a.team_id, a.agent_id)]));

      const result = await this.batchDelete(c, "meta_agents", "agent_id", agentIds);
      if (result.deleted_ids.length > 0) {
        const ids = [result.deleted_ids];
        await this.exec(`DELETE FROM ${this.t.meta_task_agents} WHERE agent_id = ANY($1)`, ids, c);
        await this.exec(`DELETE FROM ${this.t.meta_agent_fixed_assets} WHERE agent_id = ANY($1)`, ids, c);
        const selfMemoryAssetIds = result.deleted_ids
          .map((agentId) => selfMemoryByAgent.get(agentId))
          .filter((assetId): assetId is string => !!assetId);
        if (selfMemoryAssetIds.length > 0) await this.deleteAssetRows(c, selfMemoryAssetIds);
      }
      return result;
    });
  }

  private agentListWhere(first: { col: string; val: string }, filter: AgentFilter | undefined, withOwner: boolean) {
    const p = sqlParams();
    let where = `WHERE ${first.col} = ${p.$(first.val)}`;
    if (filter?.status) where += ` AND status = ${p.$(filter.status)}`;
    if (withOwner && filter?.owner_user_id) where += ` AND owner_user_id = ${p.$(filter.owner_user_id)}`;
    if (filter?.name) where += ` AND name = ${p.$(filter.name)}`;
    return { where, values: p.values };
  }

  async listAgentsByTeam(teamId: string, pagination?: PaginationParams | null, filter?: AgentFilter): Promise<ListPage<AgentEntity>> {
    const { where, values } = this.agentListWhere({ col: "team_id", val: teamId }, filter, true);
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_agents} ${where}`,
      `SELECT * FROM ${this.t.meta_agents} ${where} ORDER BY created_at DESC`,
      values,
      pagination,
      (r) => this.mapAgent(r),
    );
  }

  async listAgentsByOwner(userId: string, pagination?: PaginationParams | null, filter?: AgentFilter): Promise<ListPage<AgentEntity>> {
    const { where, values } = this.agentListWhere({ col: "owner_user_id", val: userId }, filter, false);
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_agents} ${where}`,
      `SELECT * FROM ${this.t.meta_agents} ${where} ORDER BY created_at DESC`,
      values,
      pagination,
      (r) => this.mapAgent(r),
    );
  }

  // ============================================================
  // Task (linked agents in the same transaction)
  // ============================================================

  async createTask(input: CreateTaskInput): Promise<TaskEntity> {
    const now = nowIso();
    return retryOnCollision(
      async () => {
        const taskId = input.task_id ?? generateId(ID_PREFIX.task);
        await this.tx(async (c) => {
          await this.q(
            `INSERT INTO ${this.t.meta_tasks}
               (task_id, team_id, creator_user_id, title, description, source_type, source_url,
                status, auto_assign_floating_assets, risk_level, created_at, updated_at, metadata_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12)`,
            [
              taskId,
              input.team_id,
              input.creator_user_id,
              input.title,
              input.description ?? null,
              input.source_type ?? "manual",
              input.source_url ?? null,
              input.status ?? "running",
              !!input.auto_assign_floating_assets,
              input.risk_level ?? null,
              now,
              input.metadata_json ?? "{}",
            ],
            c,
          );
          for (const link of input.linked_agents ?? []) {
            await this.q(
              `INSERT INTO ${this.t.meta_task_agents} (id, task_id, agent_id, role_in_task, status, created_at)
               VALUES ($1, $2, $3, $4, 'active', $5)`,
              [generateRelationId(), taskId, link.agent_id, link.role_in_task ?? null, now],
              c,
            );
          }
        });
        return (await this.getTaskById(taskId))!;
      },
      (err) => (!input.task_id && isPkCollision(err, "meta_tasks")) || isPkCollision(err, "meta_task_agents"),
    );
  }

  async getTaskById(taskId: string): Promise<TaskEntity | null> {
    return this.mapTask(await this.one(`SELECT * FROM ${this.t.meta_tasks} WHERE task_id = $1`, [taskId]));
  }

  async updateTask(taskId: string, patch: Partial<TaskEntity>): Promise<TaskEntity | null> {
    const allowed = ["title", "description", "source_type", "source_url", "status", "auto_assign_floating_assets", "risk_level", "metadata_json"] as const;
    const normalized: Partial<TaskEntity> = { ...patch };
    if (patch.auto_assign_floating_assets !== undefined) normalized.auto_assign_floating_assets = !!patch.auto_assign_floating_assets;
    await this.applyUpdate("meta_tasks", "task_id", taskId, allowed, normalized);
    return this.getTaskById(taskId);
  }

  async deleteTasks(taskIds: string[]): Promise<BatchDeleteResult> {
    return this.tx(async (c) => {
      const result = await this.batchDelete(c, "meta_tasks", "task_id", taskIds);
      if (result.deleted_ids.length > 0) {
        await this.exec(`DELETE FROM ${this.t.meta_task_agents} WHERE task_id = ANY($1)`, [result.deleted_ids], c);
      }
      return result;
    });
  }

  private taskListWhere(first: { col: string; val: string } | null, filter: TaskFilter | undefined) {
    const p = sqlParams();
    let where = first ? `WHERE ${first.col} = ${p.$(first.val)}` : "WHERE true";
    if (filter?.status) where += ` AND status = ${p.$(filter.status)}`;
    if (filter?.creator_user_id) where += ` AND creator_user_id = ${p.$(filter.creator_user_id)}`;
    if (filter?.title) where += ` AND title = ${p.$(filter.title)}`;
    return { where, values: p.values };
  }

  async listTasksByTeam(teamId: string, pagination?: PaginationParams | null, filter?: TaskFilter): Promise<ListPage<TaskEntity>> {
    const { where, values } = this.taskListWhere({ col: "team_id", val: teamId }, filter);
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_tasks} ${where}`,
      `SELECT * FROM ${this.t.meta_tasks} ${where} ORDER BY created_at DESC`,
      values,
      pagination,
      (r) => this.mapTask(r),
    );
  }

  async listTasks(filter: TaskFilter, pagination?: PaginationParams | null): Promise<ListPage<TaskEntity>> {
    const { where, values } = this.taskListWhere(null, filter);
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_tasks} ${where}`,
      `SELECT * FROM ${this.t.meta_tasks} ${where} ORDER BY created_at DESC`,
      values,
      pagination,
      (r) => this.mapTask(r),
    );
  }

  // ============================================================
  // TaskAgent
  // ============================================================

  async linkTaskAgent(taskId: string, agentId: string, roleInTask?: string): Promise<TaskAgentEntity> {
    const now = nowIso();
    await runWithGeneratedRelationId(undefined, pgRelationIdCollision("meta_task_agents"), (id) =>
      this.exec(
        `INSERT INTO ${this.t.meta_task_agents} (id, task_id, agent_id, role_in_task, status, created_at)
         VALUES ($1, $2, $3, $4, 'active', $5)
         ON CONFLICT (task_id, agent_id) DO UPDATE SET role_in_task = EXCLUDED.role_in_task, status = 'active'`,
        [id, taskId, agentId, roleInTask ?? null, now],
      ),
    );
    return (await this.one(`SELECT * FROM ${this.t.meta_task_agents} WHERE task_id = $1 AND agent_id = $2`, [
      taskId,
      agentId,
    ])) as unknown as TaskAgentEntity;
  }

  async unlinkTaskAgent(taskId: string, agentId: string): Promise<void> {
    await this.exec(`DELETE FROM ${this.t.meta_task_agents} WHERE task_id = $1 AND agent_id = $2`, [taskId, agentId]);
  }

  async listTaskAgents(taskId: string, pagination?: PaginationParams | null): Promise<ListPage<TaskAgentEntity>> {
    const base = `FROM ${this.t.meta_task_agents} WHERE task_id = $1 AND status = 'active'`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY created_at DESC`,
      [taskId],
      pagination,
      (r) => r as unknown as TaskAgentEntity,
    );
  }

  // ============================================================
  // ParticipationLog
  // ============================================================

  async appendParticipationLog(input: AppendParticipationLogInput): Promise<ParticipationLogEntity> {
    const createdAt = input.created_at ?? nowIso();
    const entity: ParticipationLogEntity = {
      id: generateRelationId(),
      team_id: input.team_id,
      task_id: input.task_id,
      agent_id: input.agent_id,
      user_id: input.user_id,
      source: input.source ?? "unknown",
      metadata_json: input.metadata_json ?? "{}",
      created_at: createdAt,
      updated_at: createdAt,
    };
    await this.exec(
      `INSERT INTO ${this.t.meta_participation_logs}
         (id, team_id, task_id, agent_id, user_id, source, metadata_json, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        entity.id,
        entity.team_id,
        entity.task_id,
        entity.agent_id,
        entity.user_id,
        entity.source,
        entity.metadata_json,
        entity.created_at,
        entity.updated_at,
      ],
    );
    return entity;
  }

  async listParticipationLogs(
    filter: ParticipationLogFilter,
    pagination?: PaginationParams | null,
  ): Promise<ListPage<ParticipationLogEntity>> {
    const p = sqlParams();
    const conditions = [`team_id = ${p.$(filter.team_id)}`];
    if (filter.task_id) conditions.push(`task_id = ${p.$(filter.task_id)}`);
    if (filter.agent_id) conditions.push(`agent_id = ${p.$(filter.agent_id)}`);
    if (filter.user_id) conditions.push(`user_id = ${p.$(filter.user_id)}`);
    if (filter.created_after) conditions.push(`created_at >= ${p.$(filter.created_after)}`);
    if (filter.created_before) conditions.push(`created_at <= ${p.$(filter.created_before)}`);
    const where = conditions.join(" AND ");
    const table = this.t.meta_participation_logs;
    const mapper = (r: Row) => this.mapParticipationLog(r);

    if (filter.dedupe) {
      const dedupeIds = `
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) AS rn
            FROM ${table}
           WHERE ${where}
        ) ranked WHERE rn = 1`;
      return this.selectList(
        `SELECT count(*)::int AS c FROM (${dedupeIds}) d`,
        `SELECT * FROM ${table} WHERE id IN (${dedupeIds}) ORDER BY created_at DESC, id DESC`,
        p.values,
        pagination,
        mapper,
      );
    }
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${table} WHERE ${where}`,
      `SELECT * FROM ${table} WHERE ${where} ORDER BY created_at DESC, id DESC`,
      p.values,
      pagination,
      mapper,
    );
  }

  // ============================================================
  // Asset
  // ============================================================

  async createAsset(input: CreateAssetInput): Promise<AssetEntity> {
    const now = nowIso();
    await this.exec(
      `INSERT INTO ${this.t.meta_assets}
         (asset_id, team_id, asset_type, name, description, owner_user_id, source_type, source_ref,
          version, visibility, status, confidence, expires_at, last_used_at, usage_count, content_ref,
          created_at, updated_at, metadata_json)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $10, $11, $12, NULL, 0, $13, $14, $14, $15)`,
      [
        input.asset_id,
        input.team_id,
        input.asset_type,
        input.name,
        input.description ?? null,
        input.owner_user_id,
        input.source_type,
        input.source_ref ?? null,
        input.visibility ?? "team",
        input.status ?? "draft",
        input.confidence ?? null,
        input.expires_at ?? null,
        input.content_ref ?? null,
        now,
        input.metadata_json ?? "{}",
      ],
    );
    return (await this.getAssetById(input.asset_id))!;
  }

  async getAssetById(assetId: string): Promise<AssetEntity | null> {
    return this.mapAsset(await this.one(`SELECT * FROM ${this.t.meta_assets} WHERE asset_id = $1`, [assetId]));
  }

  async updateAsset(assetId: string, patch: Partial<AssetEntity>): Promise<AssetEntity | null> {
    const allowed = ["name", "description", "visibility", "status", "confidence", "expires_at", "content_ref", "version", "source_ref", "metadata_json"] as const;
    await this.applyUpdate("meta_assets", "asset_id", assetId, allowed, patch);
    return this.getAssetById(assetId);
  }

  /** Delete assets with their bindings and ACL rows. */
  private async deleteAssetRows(c: Queryable, assetIds: string[]): Promise<void> {
    for (const table of ["meta_agent_fixed_assets", "meta_asset_acl", "meta_assets"] as const) {
      await this.exec(`DELETE FROM ${this.t[table]} WHERE asset_id = ANY($1)`, [assetIds], c);
    }
  }

  async deleteAssets(assetIds: string[]): Promise<BatchDeleteResult> {
    // A missing asset counts as deleted (idempotent; Skill hook and handler both call this).
    if (assetIds.length > 0) await this.tx((c) => this.deleteAssetRows(c, assetIds));
    return { deleted_ids: [...assetIds], failed: [] };
  }

  async listAssetsByTeam(teamId: string, pagination?: PaginationParams | null, filter?: AssetFilter): Promise<ListPage<AssetEntity>> {
    const p = sqlParams();
    let where = `WHERE team_id = ${p.$(teamId)}`;
    if (filter?.asset_type) where += ` AND asset_type = ${p.$(filter.asset_type)}`;
    if (filter?.status) where += ` AND status = ${p.$(filter.status)}`;
    if (filter?.owner_user_id) where += ` AND owner_user_id = ${p.$(filter.owner_user_id)}`;
    if (filter?.visibility) where += ` AND visibility = ${p.$(filter.visibility)}`;
    return this.selectList(
      `SELECT count(*)::int AS c FROM ${this.t.meta_assets} ${where}`,
      `SELECT * FROM ${this.t.meta_assets} ${where} ORDER BY created_at DESC`,
      p.values,
      pagination,
      (r) => this.mapAsset(r),
    );
  }

  async touchAssetUsage(assetId: string): Promise<void> {
    await this.exec(`UPDATE ${this.t.meta_assets} SET usage_count = usage_count + 1, last_used_at = $1 WHERE asset_id = $2`, [
      nowIso(),
      assetId,
    ]);
  }

  // ============================================================
  // AgentFixedAsset
  // ============================================================

  async setAgentFixedAssets(agentId: string, bindings: FixedAssetBindingInput[]): Promise<void> {
    const now = nowIso();
    await retryOnCollision(
      () =>
        this.tx(async (c) => {
          await this.exec(`DELETE FROM ${this.t.meta_agent_fixed_assets} WHERE agent_id = $1`, [agentId], c);
          for (const b of bindings) {
            await this.exec(
              `INSERT INTO ${this.t.meta_agent_fixed_assets}
                 (id, agent_id, asset_id, asset_type, injection_mode, priority, created_by, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
              [generateRelationId(), agentId, b.asset_id, b.asset_type, b.injection_mode ?? "summary", b.priority ?? 50, b.created_by, now],
              c,
            );
          }
        }),
      pgRelationIdCollision("meta_agent_fixed_assets"),
    );
  }

  async addAgentFixedAsset(agentId: string, b: FixedAssetBindingInput): Promise<void> {
    // Idempotent on (agent_id, asset_id). Unlike SQLite INSERT OR IGNORE, an `id`
    // collision is retried instead of silently dropping the binding.
    await runWithGeneratedRelationId(undefined, pgRelationIdCollision("meta_agent_fixed_assets"), (id) =>
      this.exec(
        `INSERT INTO ${this.t.meta_agent_fixed_assets}
           (id, agent_id, asset_id, asset_type, injection_mode, priority, created_by, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (agent_id, asset_id) DO NOTHING`,
        [id, agentId, b.asset_id, b.asset_type, b.injection_mode ?? "summary", b.priority ?? 50, b.created_by, nowIso()],
      ),
    );
  }

  async listAgentFixedAssets(
    agentId: string,
    pagination?: PaginationParams | null,
    filter?: { assetTypes?: readonly string[] },
  ): Promise<ListPage<FixedAssetBindingEntity>> {
    const types = filter?.assetTypes ?? [];
    const mapper = (r: Row) => r as unknown as FixedAssetBindingEntity;
    if (types.length > 0) {
      // JOIN meta_assets so the type filter runs before pagination.
      const base = `FROM ${this.t.meta_agent_fixed_assets} b
        JOIN ${this.t.meta_assets} a ON a.asset_id = b.asset_id
        WHERE b.agent_id = $1 AND a.asset_type = ANY($2)`;
      return this.selectList(
        `SELECT count(*)::int AS c ${base}`,
        `SELECT b.* ${base} ORDER BY b.priority DESC, b.created_at DESC`,
        [agentId, [...types]],
        pagination,
        mapper,
      );
    }
    const base = `FROM ${this.t.meta_agent_fixed_assets} WHERE agent_id = $1`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY priority DESC, created_at DESC`,
      [agentId],
      pagination,
      mapper,
    );
  }

  async getAgentFixedAsset(agentId: string, assetId: string): Promise<FixedAssetBindingEntity | null> {
    return (await this.one(`SELECT * FROM ${this.t.meta_agent_fixed_assets} WHERE agent_id = $1 AND asset_id = $2`, [
      agentId,
      assetId,
    ])) as FixedAssetBindingEntity | null;
  }

  async summarizeAgentFixedAssetsByAgents(
    agentIds: string[],
    options?: { assetId?: string },
  ): Promise<AgentFixedAssetCountRow[]> {
    if (agentIds.length === 0) return [];
    const p = sqlParams();
    let sql = `SELECT agent_id, asset_type, count(DISTINCT asset_id)::int AS cnt
                 FROM ${this.t.meta_agent_fixed_assets}
                WHERE agent_id = ANY(${p.$(agentIds)})`;
    if (options?.assetId) sql += ` AND asset_id = ${p.$(options.assetId)}`;
    sql += " GROUP BY agent_id, asset_type";
    const rows = await this.q<{ agent_id: string; asset_type: string; cnt: number }>(sql, p.values);
    return rows.map((r) => ({ agent_id: r.agent_id, asset_type: r.asset_type as AssetType, cnt: r.cnt }));
  }

  // ============================================================
  // ACL
  // ============================================================

  async grantAcl(input: GrantAclInput): Promise<AclEntity> {
    const now = nowIso();
    await runWithGeneratedRelationId(input.id, pgRelationIdCollision("meta_asset_acl"), (id) =>
      this.exec(
        `INSERT INTO ${this.t.meta_asset_acl}
           (id, asset_id, subject_type, subject_id, permission, effect, granted_by, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
         ON CONFLICT (asset_id, subject_type, subject_id, permission)
         DO UPDATE SET effect = EXCLUDED.effect, granted_by = EXCLUDED.granted_by, updated_at = EXCLUDED.updated_at`,
        [id, input.asset_id, input.subject_type, input.subject_id, input.permission, input.effect ?? "allow", input.granted_by, now],
      ),
    );
    return (await this.one(
      `SELECT * FROM ${this.t.meta_asset_acl}
        WHERE asset_id = $1 AND subject_type = $2 AND subject_id = $3 AND permission = $4`,
      [input.asset_id, input.subject_type, input.subject_id, input.permission],
    )) as unknown as AclEntity;
  }

  async getAclById(id: string): Promise<AclEntity | null> {
    return (await this.one(`SELECT * FROM ${this.t.meta_asset_acl} WHERE id = $1`, [id])) as AclEntity | null;
  }

  async revokeAcl(id: string): Promise<void> {
    await this.exec(`DELETE FROM ${this.t.meta_asset_acl} WHERE id = $1`, [id]);
  }

  async listAclByAsset(assetId: string, pagination?: PaginationParams | null): Promise<ListPage<AclEntity>> {
    const base = `FROM ${this.t.meta_asset_acl} WHERE asset_id = $1`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY created_at DESC`,
      [assetId],
      pagination,
      (r) => r as unknown as AclEntity,
    );
  }

  async listAclBySubject(subjectType: string, subjectId: string, pagination?: PaginationParams | null): Promise<ListPage<AclEntity>> {
    const base = `FROM ${this.t.meta_asset_acl} WHERE subject_type = $1 AND subject_id = $2`;
    return this.selectList(
      `SELECT count(*)::int AS c ${base}`,
      `SELECT * ${base} ORDER BY created_at DESC`,
      [subjectType, subjectId],
      pagination,
      (r) => r as unknown as AclEntity,
    );
  }

  // ============================================================
  // Helpers
  // ============================================================

  /** UPDATE the whitelisted, defined fields of `patch` (plus `updated_at` where the table has it). */
  private async applyUpdate<T>(
    table: Table,
    pkCol: string,
    pkVal: string,
    allowed: readonly string[],
    patch: Partial<T>,
    c?: Queryable,
  ): Promise<void> {
    const p = sqlParams();
    const sets: string[] = [];
    for (const k of allowed) {
      const v = (patch as Record<string, unknown>)[k];
      if (k in (patch as object) && v !== undefined) sets.push(`${k} = ${p.$(v)}`);
    }
    if (TABLES_WITH_UPDATED_AT.has(table)) sets.push(`updated_at = ${p.$(nowIso())}`);
    if (sets.length === 0) return;
    await this.exec(`UPDATE ${this.t[table]} SET ${sets.join(", ")} WHERE ${pkCol} = ${p.$(pkVal)}`, p.values, c);
  }

  /** DELETE by primary key; ids that did not exist (or repeat) are reported as `not_found`, in input order. */
  private async batchDelete(c: Queryable, table: Table, pkCol: string, ids: string[]): Promise<BatchDeleteResult> {
    const result: BatchDeleteResult = { deleted_ids: [], failed: [] };
    if (ids.length === 0) return result;
    const rows = await this.q<Record<string, string>>(
      `DELETE FROM ${this.t[table]} WHERE ${pkCol} = ANY($1) RETURNING ${pkCol}`,
      [ids],
      c,
    );
    const deleted = new Set(rows.map((r) => r[pkCol]));
    for (const id of ids) {
      if (deleted.delete(id)) result.deleted_ids.push(id);
      else result.failed.push({ id, reason: "not_found" });
    }
    return result;
  }

  // ── Row mappers ──
  private mapUserKey(row: Row | null | undefined): UserKeyEntity | null {
    if (!row) return null;
    return {
      key_id: String(row.key_id),
      user_id: String(row.user_id),
      key_value: String(row.key_value),
      name: row.name != null ? String(row.name) : null,
      status: String(row.status) as UserKeyEntity["status"],
      is_default: row.is_default === true,
      last_used_at: row.last_used_at != null ? String(row.last_used_at) : null,
      expires_at: row.expires_at != null ? String(row.expires_at) : null,
      created_at: String(row.created_at),
      revoked_at: row.revoked_at != null ? String(row.revoked_at) : null,
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapUser(r: Row | null): UserEntity | null {
    if (!r) return null;
    return {
      user_id: String(r.user_id),
      password: r.password != null ? String(r.password) : null,
      auth_provider: String(r.auth_provider),
      external_id: String(r.external_id),
      username: String(r.username),
      display_name: r.display_name != null ? String(r.display_name) : null,
      email: r.email != null ? String(r.email) : null,
      raw_profile_json: String(r.raw_profile_json ?? "{}"),
      status: String(r.status) as UserEntity["status"],
      user_type: String(r.user_type ?? "normal") as UserEntity["user_type"],
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
      metadata_json: String(r.metadata_json ?? "{}"),
    };
  }

  private mapTeam(r: Row | null): TeamEntity | null {
    if (!r) return null;
    return {
      team_id: String(r.team_id),
      name: String(r.name),
      description: r.description != null ? String(r.description) : null,
      owner_user_id: String(r.owner_user_id),
      status: String(r.status) as TeamEntity["status"],
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
      metadata_json: String(r.metadata_json ?? "{}"),
    };
  }

  private mapAgent(r: Row | null): AgentEntity | null {
    if (!r) return null;
    return {
      agent_id: String(r.agent_id),
      team_id: String(r.team_id),
      owner_user_id: String(r.owner_user_id),
      name: String(r.name),
      description: r.description != null ? String(r.description) : null,
      prompt: r.prompt != null ? String(r.prompt) : null,
      visibility: String(r.visibility ?? "team") as AgentEntity["visibility"],
      status: String(r.status) as AgentEntity["status"],
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
      metadata_json: String(r.metadata_json ?? "{}"),
    };
  }

  private mapTask(r: Row | null): TaskEntity | null {
    if (!r) return null;
    return {
      task_id: String(r.task_id),
      team_id: String(r.team_id),
      creator_user_id: String(r.creator_user_id),
      title: String(r.title),
      description: r.description != null ? String(r.description) : null,
      source_type: String(r.source_type) as TaskEntity["source_type"],
      source_url: r.source_url != null ? String(r.source_url) : null,
      status: String(r.status) as TaskEntity["status"],
      auto_assign_floating_assets: r.auto_assign_floating_assets === true,
      risk_level: r.risk_level != null ? String(r.risk_level) : null,
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
      metadata_json: String(r.metadata_json ?? "{}"),
    };
  }

  private mapAsset(r: Row | null): AssetEntity | null {
    if (!r) return null;
    return {
      asset_id: String(r.asset_id),
      team_id: String(r.team_id),
      asset_type: String(r.asset_type) as AssetEntity["asset_type"],
      name: String(r.name),
      description: r.description != null ? String(r.description) : null,
      owner_user_id: String(r.owner_user_id),
      source_type: String(r.source_type),
      source_ref: r.source_ref != null ? String(r.source_ref) : null,
      version: Number(r.version ?? 1),
      visibility: String(r.visibility) as AssetEntity["visibility"],
      status: String(r.status) as AssetEntity["status"],
      confidence: r.confidence != null ? Number(r.confidence) : null,
      expires_at: r.expires_at != null ? String(r.expires_at) : null,
      last_used_at: r.last_used_at != null ? String(r.last_used_at) : null,
      usage_count: Number(r.usage_count ?? 0),
      content_ref: r.content_ref != null ? String(r.content_ref) : null,
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
      metadata_json: String(r.metadata_json ?? "{}"),
    };
  }

  private mapParticipationLog(r: Row | null): ParticipationLogEntity | null {
    if (!r) return null;
    return {
      id: String(r.id),
      team_id: String(r.team_id),
      task_id: String(r.task_id),
      agent_id: String(r.agent_id),
      user_id: String(r.user_id),
      source: String(r.source),
      metadata_json: String(r.metadata_json),
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
    };
  }

  // ============================================================
  // ConfigParam — upserts use the partial unique indexes (no SELECT-then-write).
  // ============================================================

  async getConfigParam(
    scope: "global" | "user",
    userId: string | null,
    module: string,
    paramName: string,
  ): Promise<ConfigParamEntity | null> {
    const row =
      scope === "global"
        ? await this.one(
            `SELECT * FROM ${this.t.meta_config_params} WHERE scope = 'global' AND module = $1 AND param_name = $2`,
            [module, paramName],
          )
        : await this.one(
            `SELECT * FROM ${this.t.meta_config_params} WHERE scope = 'user' AND user_id = $1 AND module = $2 AND param_name = $3`,
            [userId, module, paramName],
          );
    return this.mapConfigParam(row);
  }

  async upsertConfigParam(input: UpsertConfigParamInput): Promise<ConfigParamEntity> {
    const now = nowIso();
    const isGlobal = input.scope === "global";
    const conflict = isGlobal ? "(module, param_name) WHERE scope = 'global'" : "(user_id, module, param_name) WHERE scope = 'user'";
    const row = await this.one(
      `INSERT INTO ${this.t.meta_config_params}
         (scope, user_id, module, param_name, param_value, description, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
       ON CONFLICT ${conflict}
       DO UPDATE SET param_value = EXCLUDED.param_value, description = EXCLUDED.description, updated_at = EXCLUDED.updated_at
       RETURNING *`,
      [input.scope, isGlobal ? null : input.user_id, input.module, input.param_name, input.param_value, input.description, now],
    );
    return this.mapConfigParam(row)!;
  }

  async listConfigParams(filter: ListConfigParamsFilter): Promise<ConfigParamEntity[]> {
    const p = sqlParams();
    const conditions = [`module = ${p.$(filter.module)}`];
    if (filter.scope) conditions.push(`scope = ${p.$(filter.scope)}`);
    if (filter.userId) conditions.push(`(scope = 'global' OR (scope = 'user' AND user_id = ${p.$(filter.userId)}))`);
    if (filter.paramNames && filter.paramNames.length > 0) conditions.push(`param_name = ANY(${p.$(filter.paramNames)})`);
    const rows = await this.q(
      `SELECT * FROM ${this.t.meta_config_params} WHERE ${conditions.join(" AND ")} ORDER BY scope ASC, param_name ASC`,
      p.values,
    );
    return rows.map((r) => this.mapConfigParam(r)!);
  }

  private mapConfigParam(r: Row | null): ConfigParamEntity | null {
    if (!r) return null;
    return {
      id: Number(r.id),
      scope: String(r.scope) as ConfigParamEntity["scope"],
      user_id: r.user_id != null ? String(r.user_id) : null,
      module: String(r.module),
      param_name: String(r.param_name),
      param_value: String(r.param_value),
      description: String(r.description),
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
    };
  }

  // ============================================================
  // UserSessionDefault (v3.3)
  // ============================================================

  async getUserSessionDefault(userId: string): Promise<UserSessionDefaultEntity | null> {
    return this.mapUserSessionDefault(
      await this.one(`SELECT * FROM ${this.t.meta_user_session_defaults} WHERE user_id = $1`, [userId]),
    );
  }

  async upsertUserSessionDefault(input: UpsertUserSessionDefaultInput): Promise<UserSessionDefaultEntity> {
    // An existing row keeps its created_at; only the triple and updated_at change.
    const row = await this.one(
      `INSERT INTO ${this.t.meta_user_session_defaults} (user_id, team_id, agent_id, task_id, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $5)
       ON CONFLICT (user_id) DO UPDATE
         SET team_id = EXCLUDED.team_id, agent_id = EXCLUDED.agent_id, task_id = EXCLUDED.task_id, updated_at = EXCLUDED.updated_at
       RETURNING *`,
      [input.user_id, input.team_id, input.agent_id, input.task_id, nowIso()],
    );
    return this.mapUserSessionDefault(row)!;
  }

  deleteUserSessionDefault(userId: string): Promise<number> {
    return this.exec(`DELETE FROM ${this.t.meta_user_session_defaults} WHERE user_id = $1`, [userId]);
  }

  private mapUserSessionDefault(r: Row | null): UserSessionDefaultEntity | null {
    if (!r) return null;
    return {
      user_id: String(r.user_id),
      team_id: String(r.team_id),
      agent_id: String(r.agent_id),
      task_id: String(r.task_id),
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
    };
  }

  // ============================================================
  // InstanceUpstreamConfig (v2 model groups)
  //
  // Every write that checks the group set (singleton, agents overlap, name
  // uniqueness, version) runs under withWriteTx, as SQLite runs it under
  // BEGIN IMMEDIATE. Row locks alone do
  // not cover groups that do not exist yet.
  // ============================================================

  async getInstanceUpstreamGroup(groupId: string): Promise<InstanceUpstreamConfigEntity | null> {
    return this.mapInstanceUpstreamGroup(
      await this.one(`SELECT * FROM ${this.t.meta_instance_upstream_config} WHERE group_id = $1`, [groupId]),
    );
  }

  async listInstanceUpstreamGroups(
    filter?: InstanceUpstreamConfigFilter,
    seedIfEmpty?: SupportedAgent[],
  ): Promise<InstanceUpstreamConfigEntity[]> {
    if (seedIfEmpty && seedIfEmpty.length > 0) {
      await this.ensureDefaultSeeded(seedIfEmpty);
      await this.syncDefaultAgentsWithSnapshot(seedIfEmpty);
    }
    const p = sqlParams();
    const where = filter?.group_type ? ` WHERE group_type = ${p.$(filter.group_type)}` : "";
    const rows = await this.q(
      `SELECT * FROM ${this.t.meta_instance_upstream_config}${where}
        ORDER BY CASE group_type WHEN 'default' THEN 0 WHEN 'custom' THEN 1 ELSE 2 END, updated_at DESC`,
      p.values,
    );
    return rows.map((r) => this.mapInstanceUpstreamGroup(r)!);
  }

  private async hasDefaultGroup(c?: Queryable): Promise<boolean> {
    return !!(await this.one(`SELECT id FROM ${this.t.meta_instance_upstream_config} WHERE group_type = 'default' LIMIT 1`, [], c));
  }

  async ensureDefaultSeeded(supported: SupportedAgent[]): Promise<void> {
    if (!supported || supported.length === 0) return;
    if (await this.hasDefaultGroup()) return;
    try {
      await this.writeTx(async (c) => {
        if (await this.hasDefaultGroup(c)) return; // seeded by someone else meanwhile
        const now = nowIso();
        const agentsJson = JSON.stringify(Array.from(new Set(supported.map((a) => a.agent_source))).sort());
        await this.exec(
          `INSERT INTO ${this.t.meta_instance_upstream_config}
             (group_id, group_type, name, agents, enabled, mode, base_url, api_key, model_id, description,
              version, supported_agents_snapshot, created_at, updated_at)
           VALUES ($1, 'default', $2, $3, true, 'official', '', '', '', '', 1, $3, $4, $4)`,
          [this.newGroupId("default"), DEFAULT_GROUP_NAME, agentsJson, now],
          c,
        );
      });
    } catch (err) {
      // Singleton index (second guard): another gateway seeded first — fine.
      if (!isUniqueViolation(err, UPSTREAM_SINGLETON_INDEX)) throw err;
    }
  }

  /**
   * Plan of the default-group diff-append (方案 E, see SqliteMetadataStore
   * `_syncDefaultAgentsWithSnapshot`). Returns null when nothing changes.
   */
  private async planDefaultSync(
    c: Queryable | undefined,
    supported: SupportedAgent[],
  ): Promise<{ id: number; version: number; agents: string[] | null; snapshot: string[] } | null> {
    const supportedNames = Array.from(new Set(supported.map((a) => a.agent_source))).sort();
    const supportedSet = new Set(supportedNames);
    const row = await this.one(
      `SELECT id, agents, supported_agents_snapshot, version FROM ${this.t.meta_instance_upstream_config}
        WHERE group_type = 'default' LIMIT 1`,
      [],
      c,
    );
    if (!row) return null;
    const currentAgents = parseJsonArray(row.agents);
    const currentSnapshot = parseJsonArray(row.supported_agents_snapshot);
    const base = { id: Number(row.id), version: Number(row.version ?? 1) };

    // B: empty snapshot (legacy row) → fill the snapshot only, agents unchanged.
    if (currentSnapshot.length === 0) return { ...base, agents: null, snapshot: supportedNames };

    const newAgents = supportedNames.filter((a) => !currentSnapshot.includes(a));
    const snapshotShrunk = currentSnapshot.some((a) => !supportedSet.has(a));
    if (newAgents.length === 0 && !snapshotShrunk) return null; // F: hot path

    const nextAgents = [...currentAgents];
    if (newAgents.length > 0) {
      // C/D: append new agents unless a custom group owns them (agents stay globally unique).
      const customRows = await this.q(
        `SELECT agents FROM ${this.t.meta_instance_upstream_config} WHERE group_type = 'custom'`,
        [],
        c,
      );
      const customUsed = new Set(customRows.flatMap((r) => parseJsonArray(r.agents)));
      for (const a of newAgents) {
        if (!customUsed.has(a) && !nextAgents.includes(a)) nextAgents.push(a);
      }
    }
    return { ...base, agents: nextAgents, snapshot: supportedNames }; // E: snapshot follows supported
  }

  private async syncDefaultAgentsWithSnapshot(supported: SupportedAgent[]): Promise<void> {
    // Unlocked read first so the common no-change list takes no write lock.
    if (!(await this.planDefaultSync(undefined, supported))) return;
    await this.writeTx(async (c) => {
      const plan = await this.planDefaultSync(c, supported);
      if (!plan) return;
      const now = nowIso();
      if (plan.agents === null) {
        await this.exec(
          `UPDATE ${this.t.meta_instance_upstream_config}
              SET supported_agents_snapshot = $1, updated_at = $2
            WHERE id = $3 AND version = $4`,
          [JSON.stringify(plan.snapshot), now, plan.id, plan.version],
          c,
        );
        return;
      }
      await this.exec(
        `UPDATE ${this.t.meta_instance_upstream_config}
            SET agents = $1, supported_agents_snapshot = $2, version = version + 1, updated_at = $3
          WHERE id = $4 AND version = $5`,
        [JSON.stringify(plan.agents), JSON.stringify(plan.snapshot), now, plan.id, plan.version],
        c,
      );
    });
  }

  async createInstanceUpstreamGroup(input: CreateInstanceUpstreamGroupInput): Promise<InstanceUpstreamConfigEntity> {
    const groupId = this.newGroupId(input.group_type);
    const now = nowIso();
    const agents = input.agents ?? [];
    const singletonConflict = () =>
      new InstanceUpstreamWriteConflictError(
        input.group_type === "default" ? "default_already_exists" : "extraction_already_exists",
        `${input.group_type} group already exists in this instance`,
      );

    try {
      return await this.writeTx(async (c) => {
        // 1. default / extraction are singletons.
        if (input.group_type === "default" || input.group_type === "extraction") {
          const existing = await this.one(
            `SELECT group_id FROM ${this.t.meta_instance_upstream_config} WHERE group_type = $1 LIMIT 1`,
            [input.group_type],
            c,
          );
          if (existing) throw singletonConflict();
        }
        // 2. agents are globally unique across default/custom groups.
        if (input.group_type !== "extraction") await this.assertAgentsNotOverlap(c, agents, null);
        // 2b. custom group names are unique (trimmed) across all groups.
        if (input.group_type === "custom") await this.assertGroupNameUnique(c, input.name ?? "", null);
        // 3. INSERT (custom/extraction snapshot is always []).
        const row = await this.one(
          `INSERT INTO ${this.t.meta_instance_upstream_config}
             (group_id, group_type, name, agents, enabled, mode, base_url, api_key, model_id, description,
              version, supported_agents_snapshot, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, '[]', $11, $11)
           RETURNING *`,
          [
            groupId,
            input.group_type,
            input.name ?? "",
            JSON.stringify(agents),
            input.enabled !== false,
            input.mode,
            input.base_url ?? "",
            input.api_key ?? "",
            input.model_id ?? "",
            input.description ?? "",
            now,
          ],
          c,
        );
        return this.mapInstanceUpstreamGroup(row)!;
      });
    } catch (err) {
      if (isUniqueViolation(err, UPSTREAM_SINGLETON_INDEX)) throw singletonConflict();
      throw err;
    }
  }

  /** Lock-held read of one group, or `group_not_found`. */
  private async lockedGroup(c: Queryable, groupId: string): Promise<InstanceUpstreamConfigEntity> {
    const current = this.mapInstanceUpstreamGroup(
      await this.one(`SELECT * FROM ${this.t.meta_instance_upstream_config} WHERE group_id = $1`, [groupId], c),
    );
    if (!current) throw new InstanceUpstreamWriteConflictError("group_not_found", `group not found: ${groupId}`);
    return current;
  }

  private assertVersion(expected: number | undefined, current: InstanceUpstreamConfigEntity): void {
    if (expected !== undefined && expected !== current.version) {
      throw new InstanceUpstreamWriteConflictError(
        "version_mismatch",
        `version mismatch: expected ${expected}, actual ${current.version}`,
        { expected, actual: current.version },
      );
    }
  }

  async updateInstanceUpstreamGroup(input: UpdateInstanceUpstreamGroupInput): Promise<InstanceUpstreamConfigEntity> {
    return this.writeTx(async (c) => {
      const current = await this.lockedGroup(c, input.group_id);
      if (current.group_type !== input.expected_group_type) {
        throw new InstanceUpstreamWriteConflictError(
          "group_type_mismatch",
          `expected ${input.expected_group_type}, actual ${current.group_type}`,
          { expected: input.expected_group_type, actual: current.group_type },
        );
      }
      this.assertVersion(input.expected_version, current);

      const nextAgents = input.agents ?? current.agents;
      if (input.agents !== undefined && current.group_type !== "extraction") {
        await this.assertAgentsNotOverlap(c, nextAgents, current.group_id);
      }
      // Re-sending the unchanged (trimmed) name never trips the uniqueness check (legacy duplicates).
      if (
        input.name !== undefined &&
        current.group_type === "custom" &&
        (input.name ?? "").trim() !== (current.name ?? "").trim()
      ) {
        await this.assertGroupNameUnique(c, input.name, current.group_id);
      }

      const row = await this.one(
        `UPDATE ${this.t.meta_instance_upstream_config}
            SET name = $1, agents = $2, enabled = $3, mode = $4, base_url = $5, api_key = $6, model_id = $7,
                description = $8, version = version + 1, updated_at = $9
          WHERE group_id = $10
          RETURNING *`,
        [
          input.name ?? current.name,
          JSON.stringify(nextAgents),
          input.enabled ?? current.enabled,
          input.mode ?? current.mode,
          input.base_url ?? current.base_url,
          input.api_key ?? current.api_key,
          input.model_id ?? current.model_id,
          input.description ?? current.description,
          nowIso(),
          input.group_id,
        ],
        c,
      );
      return this.mapInstanceUpstreamGroup(row)!;
    });
  }

  async toggleInstanceUpstreamGroup(input: ToggleInstanceUpstreamGroupInput): Promise<InstanceUpstreamConfigEntity> {
    return this.writeTx(async (c) => {
      const current = await this.lockedGroup(c, input.group_id);
      this.assertVersion(input.expected_version, current);
      const row = await this.one(
        `UPDATE ${this.t.meta_instance_upstream_config}
            SET enabled = $1, version = version + 1, updated_at = $2
          WHERE group_id = $3
          RETURNING *`,
        [input.enabled, nowIso(), input.group_id],
        c,
      );
      return this.mapInstanceUpstreamGroup(row)!;
    });
  }

  async deleteInstanceUpstreamGroup(input: DeleteInstanceUpstreamGroupInput): Promise<boolean> {
    return (await this.exec(`DELETE FROM ${this.t.meta_instance_upstream_config} WHERE group_id = $1`, [input.group_id])) > 0;
  }

  /** agents_overlap unless `candidate` is disjoint from every other default/custom group. */
  private async assertAgentsNotOverlap(c: Queryable, candidate: string[], selfGroupId: string | null): Promise<void> {
    if (candidate.length === 0) return;
    const candidateSet = new Set(candidate);
    const rows = await this.q(
      `SELECT group_id, group_type, name, agents FROM ${this.t.meta_instance_upstream_config}
        WHERE group_type IN ('default', 'custom')`,
      [],
      c,
    );
    const conflicts: Array<{ group_id: string; group_type: string; name: string; overlapping_agents: string[] }> = [];
    for (const r of rows) {
      const gid = String(r.group_id);
      if (gid === selfGroupId) continue;
      const overlap = parseJsonArray(r.agents).filter((a) => candidateSet.has(a));
      if (overlap.length > 0) {
        conflicts.push({ group_id: gid, group_type: String(r.group_type), name: String(r.name), overlapping_agents: overlap });
      }
    }
    if (conflicts.length > 0) {
      throw new InstanceUpstreamWriteConflictError(
        "agents_overlap",
        `agents overlap with ${conflicts.length} existing group(s)`,
        { conflict_groups: conflicts },
      );
    }
  }

  /** name_duplicate unless the trimmed name is unused by every other group (blank names pass). */
  private async assertGroupNameUnique(c: Queryable, candidateName: string, selfGroupId: string | null): Promise<void> {
    const candidate = (candidateName ?? "").trim();
    if (!candidate) return;
    // JS trim (not btrim) so Unicode whitespace matches SQLite exactly.
    const rows = await this.q(`SELECT group_id, group_type, name FROM ${this.t.meta_instance_upstream_config}`, [], c);
    const row = rows.find((r) => String(r.group_id) !== selfGroupId && String(r.name ?? "").trim() === candidate);
    if (row) {
      throw new InstanceUpstreamWriteConflictError(
        "name_duplicate",
        `group name "${candidate}" is already used by another group in this instance`,
        { conflict_group: { group_id: String(row.group_id), group_type: String(row.group_type), name: String(row.name ?? "") } },
      );
    }
  }

  /** group_id: default→dflt-<r> / custom→grp-<r> / extraction→ext-<r>. */
  private newGroupId(gt: GroupType): string {
    return generateId(gt === "default" ? "dflt" : gt === "extraction" ? "ext" : "grp");
  }

  private mapInstanceUpstreamGroup(r: Row | null): InstanceUpstreamConfigEntity | null {
    if (!r) return null;
    return {
      id: Number(r.id),
      group_id: String(r.group_id),
      group_type: String(r.group_type) as GroupType,
      name: String(r.name ?? ""),
      agents: parseJsonArray(r.agents),
      enabled: r.enabled === true,
      mode: String(r.mode) as UpstreamMode,
      base_url: String(r.base_url ?? ""),
      api_key: String(r.api_key ?? ""),
      model_id: String(r.model_id ?? ""),
      description: String(r.description ?? ""),
      version: Number(r.version ?? 1),
      supported_agents_snapshot: parseJsonArray(r.supported_agents_snapshot),
      created_at: String(r.created_at),
      updated_at: String(r.updated_at),
    };
  }
}
