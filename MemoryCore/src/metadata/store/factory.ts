/**
 * 元数据存储工厂 + 配置解析 + MetadataStorePool（v3.0 按实例分库）。
 */

import { rm } from "node:fs/promises";
import type { IMetadataStore, MetadataBackend } from "./interface.js";
import { SqliteMetadataStore } from "./sqlite-adapter.js";
import {
  DEFAULT_METADATA_POSTGRES_POOL_MAX,
  METADATA_POSTGRES_SETTING_NAMES,
  parsePostgresSettings,
  type PostgresSettings,
} from "../../core/store/postgres/settings.js";
import { pgSchemaName, quoteIdent } from "../../core/store/postgres/schema-name.js";
import { withMigrationTx } from "../../core/store/postgres/tx.js";
import {
  DEFAULT_METADATA_DB_PREFIX,
  resolveMetadataDbName,
  resolveSqliteDbDir,
  resolveSqliteDbPath,
} from "./db-name.js";

export interface MetadataStoreConfig {
  backend: MetadataBackend;
  /** SQLite 根目录（backend=sqlite）。每个实例：{baseDir}/tdai_metadata_{id}/metadata.db */
  sqliteBaseDir?: string;
  /** MongoDB 连接串（backend=mongodb）。 */
  mongoUri?: string;
  /** MongoDB 是否启用事务（默认 true，需副本集）。 */
  mongoTransactions?: boolean;
  /** 内存中最多缓存多少个实例 store 连接（LRU 驱逐，仅 close 不删库）。 */
  storeCacheMaxInstances?: number;
  /** 元数据库名前缀，默认 `tdai_metadata`；完整库名 `{prefix}_{instance_id}`。 */
  mongoDbPrefix?: string;
  /** Postgres（backend=postgres）：URL、schema 前缀、连接池上限。每实例 schema `{prefix}_meta_{id}`。 */
  postgres?: PostgresSettings;
}

export interface PurgeMetadataResult {
  db_name: string;
  dropped: boolean;
}

export type MetadataDeployMode = "standalone" | "service";

const DEFAULT_SQLITE_BASE = "./data/metadata";
const DEFAULT_STORE_CACHE_MAX = 128;

function hasExplicitMongoUri(env: NodeJS.ProcessEnv): boolean {
  return !!env.TDAI_METADATA_MONGO_URI?.trim();
}

function hasExplicitSqliteBaseDir(env: NodeJS.ProcessEnv): boolean {
  return !!env.TDAI_METADATA_SQLITE_BASE_DIR?.trim();
}

function hasExplicitPostgresUrl(env: NodeJS.ProcessEnv): boolean {
  return !!env.TDAI_METADATA_POSTGRES_URL?.trim();
}

/**
 * Mongo / Postgres / SQLite 根目录至多显式配置一个（env / yaml 回填后校验）。
 */
export function assertMetadataStoreConfigExclusive(env: NodeJS.ProcessEnv = process.env): void {
  const set = [
    hasExplicitMongoUri(env) && "TDAI_METADATA_MONGO_URI",
    hasExplicitPostgresUrl(env) && "TDAI_METADATA_POSTGRES_URL",
    hasExplicitSqliteBaseDir(env) && "TDAI_METADATA_SQLITE_BASE_DIR",
  ].filter((name): name is string => !!name);
  if (set.length > 1) {
    throw new MetadataStartupValidationError(
      `Metadata startup validation failed: set only one of ${set.join(", ")} (all are set)`,
    );
  }
}

/**
 * 从环境变量解析存储配置。
 *
 * 推断规则（v3.0）：
 *   - TDAI_METADATA_MONGO_URI 非空 → mongodb
 *   - TDAI_METADATA_POSTGRES_URL 非空 → postgres（_SCHEMA_PREFIX 默认 tdai，_POOL_MAX 默认 5）
 *   - 否则 → sqlite（显式 TDAI_METADATA_SQLITE_BASE_DIR 或 fallback）
 *   - 多个同时显式配置 → 启动报错（见 assertMetadataStoreConfigExclusive）
 *
 * deployMode=service 时须 mongodb 或 postgres（见 validateMetadataStartupConfig）。
 *
 * 废弃：TDAI_METADATA_BACKEND、TDAI_METADATA_MONGO_DB、TDAI_METADATA_SQLITE_PATH
 */
export function loadStoreConfig(
  env: NodeJS.ProcessEnv = process.env,
  fallbackSqliteBaseDir?: string,
): MetadataStoreConfig {
  assertMetadataStoreConfigExclusive(env);

  const mongoUri = env.TDAI_METADATA_MONGO_URI?.trim();
  const cacheMax = parseInt(env.TDAI_METADATA_STORE_CACHE_MAX ?? "", 10);
  const storeCacheMaxInstances =
    Number.isFinite(cacheMax) && cacheMax > 0 ? cacheMax : DEFAULT_STORE_CACHE_MAX;

  const mongoDbPrefix =
    env.TDAI_METADATA_MONGO_DB_PREFIX?.trim() || DEFAULT_METADATA_DB_PREFIX;

  if (mongoUri) {
    return {
      backend: "mongodb",
      mongoUri,
      mongoTransactions: env.TDAI_METADATA_MONGO_TRANSACTIONS !== "false",
      storeCacheMaxInstances,
      mongoDbPrefix,
    };
  }

  if (hasExplicitPostgresUrl(env)) {
    // Throws (naming the variable, URL redacted) on a bad scheme, prefix or pool size.
    const postgres = parsePostgresSettings(
      {
        url: env.TDAI_METADATA_POSTGRES_URL,
        schemaPrefix: env.TDAI_METADATA_POSTGRES_SCHEMA_PREFIX,
        poolMax: env.TDAI_METADATA_POSTGRES_POOL_MAX,
      },
      METADATA_POSTGRES_SETTING_NAMES,
      DEFAULT_METADATA_POSTGRES_POOL_MAX,
    );
    return { backend: "postgres", postgres, storeCacheMaxInstances, mongoDbPrefix };
  }

  return {
    backend: "sqlite",
    sqliteBaseDir: env.TDAI_METADATA_SQLITE_BASE_DIR ?? fallbackSqliteBaseDir ?? DEFAULT_SQLITE_BASE,
    storeCacheMaxInstances,
    mongoDbPrefix,
  };
}

/** service 模式元数据配置校验失败时抛出，Gateway 启动应 fail-fast。 */
export class MetadataStartupValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MetadataStartupValidationError";
  }
}

export function validateMetadataStartupConfig(
  deployMode: MetadataDeployMode,
  env: NodeJS.ProcessEnv = process.env,
  fallbackSqliteBaseDir?: string,
): MetadataStoreConfig {
  const config = loadStoreConfig(env, fallbackSqliteBaseDir);
  if (deployMode !== "service") {
    return config;
  }

  const errors: string[] = [];
  if (!config.mongoUri?.trim() && !config.postgres) {
    errors.push("TDAI_METADATA_MONGO_URI or TDAI_METADATA_POSTGRES_URL is required when deployMode=service");
  }
  if (errors.length > 0) {
    throw new MetadataStartupValidationError(
      `Metadata startup validation failed: ${errors.join("; ")}`,
    );
  }
  return config;
}

/**
 * 构造并初始化单个实例库的 IMetadataStore。
 */
export async function createMetadataStore(
  config: MetadataStoreConfig,
  instanceId: string,
): Promise<IMetadataStore> {
  switch (config.backend) {
    case "sqlite": {
      const baseDir = config.sqliteBaseDir ?? DEFAULT_SQLITE_BASE;
      const dbPath = resolveSqliteDbPath(baseDir, instanceId, config.mongoDbPrefix);
      const store = new SqliteMetadataStore(dbPath);
      await store.init();
      return store;
    }
    case "mongodb": {
      if (!config.mongoUri) {
        throw new Error("TDAI_METADATA_MONGO_URI is required when backend=mongodb");
      }
      const { MongoClient } = await import("mongodb");
      const { MongoMetadataStore } = await import("./mongodb-adapter.js");
      const client = new MongoClient(config.mongoUri);
      await client.connect();
      const dbName = resolveMetadataDbName(instanceId, config.mongoDbPrefix);
      const store = new MongoMetadataStore(client, dbName, {
        useTransactions: config.mongoTransactions ?? true,
        ownsClient: true,
      });
      await store.init();
      return store;
    }
    case "postgres": {
      const pg = requirePostgres(config);
      const { PostgresMetadataStore } = await import("./postgres-adapter.js");
      const { getSharedPgPoolRegistry } = await import("../../core/store/postgres/pool-registry.js");
      const store = new PostgresMetadataStore({
        pool: getSharedPgPoolRegistry().get(pg.url, pg.poolMax),
        schema: pgSchemaName("meta", instanceId, pg.schemaPrefix),
      });
      await store.init();
      return store;
    }
    default: {
      const unknown: never = config.backend;
      throw new Error(`Unknown metadata backend: ${unknown as string}`);
    }
  }
}

function requirePostgres(config: MetadataStoreConfig): PostgresSettings {
  if (!config.postgres) throw new Error("TDAI_METADATA_POSTGRES_URL is required when backend=postgres");
  return config.postgres;
}

interface CachedStore {
  instanceId: string;
  store: IMetadataStore;
  /** mongodb 时持有 client 引用以便 close */
  mongoClient?: import("mongodb").MongoClient;
}

/**
 * 按实例懒建库、LRU 缓存、purge dropDatabase。
 */
export class MetadataStorePool {
  private readonly cache = new Map<string, CachedStore>();
  private readonly config: MetadataStoreConfig;
  private sharedMongoClient: import("mongodb").MongoClient | null = null;
  private sharedMongoClientPromise: Promise<import("mongodb").MongoClient> | null = null;

  constructor(config: MetadataStoreConfig) {
    this.config = config;
  }

  get backend(): MetadataBackend {
    return this.config.backend;
  }

  private get dbPrefix(): string {
    return this.config.mongoDbPrefix?.trim() || DEFAULT_METADATA_DB_PREFIX;
  }

  private async getSharedMongoClient(): Promise<import("mongodb").MongoClient> {
    if (this.sharedMongoClient) return this.sharedMongoClient;
    if (!this.sharedMongoClientPromise) {
      this.sharedMongoClientPromise = (async () => {
        if (!this.config.mongoUri) throw new Error("TDAI_METADATA_MONGO_URI is required");
        const { MongoClient } = await import("mongodb");
        const client = new MongoClient(this.config.mongoUri);
        await client.connect();
        this.sharedMongoClient = client;
        return client;
      })();
    }
    return this.sharedMongoClientPromise;
  }

  private touchLru(instanceId: string, entry: CachedStore): void {
    this.cache.delete(instanceId);
    this.cache.set(instanceId, entry);
    this.evictIfNeeded();
  }

  private evictIfNeeded(): void {
    const max = this.config.storeCacheMaxInstances ?? DEFAULT_STORE_CACHE_MAX;
    while (this.cache.size > max) {
      const oldestKey = this.cache.keys().next().value as string | undefined;
      if (!oldestKey) break;
      const oldest = this.cache.get(oldestKey);
      this.cache.delete(oldestKey);
      if (oldest) {
        void Promise.resolve(oldest.store.close()).catch(() => {});
      }
    }
  }

  async getStore(instanceId: string): Promise<IMetadataStore> {
    const existing = this.cache.get(instanceId);
    if (existing) {
      this.touchLru(instanceId, existing);
      return existing.store;
    }

    if (this.config.backend === "mongodb") {
      const client = await this.getSharedMongoClient();
      const { MongoMetadataStore } = await import("./mongodb-adapter.js");
      const dbName = resolveMetadataDbName(instanceId, this.dbPrefix);
      const store = new MongoMetadataStore(client, dbName, {
        useTransactions: this.config.mongoTransactions ?? true,
        ownsClient: false,
      });
      await store.init();
      const entry: CachedStore = { instanceId, store, mongoClient: client };
      this.cache.set(instanceId, entry);
      this.evictIfNeeded();
      return store;
    }

    const store = await createMetadataStore(this.config, instanceId);
    const entry: CachedStore = { instanceId, store };
    this.cache.set(instanceId, entry);
    this.evictIfNeeded();
    return store;
  }

  private async evict(instanceId: string): Promise<void> {
    const cached = this.cache.get(instanceId);
    if (cached) {
      this.cache.delete(instanceId);
      await Promise.resolve(cached.store.close()).catch(() => {});
    }
  }

  async purgeInstance(instanceId: string): Promise<PurgeMetadataResult> {
    if (this.config.backend === "postgres") {
      // evict, then DROP SCHEMA under the schema's migration
      // lock (so it never interleaves with a concurrent init's migrations), then
      // evict again — a store created and initialised between the first evict
      // and the drop would otherwise stay cached on a dropped schema. A later
      // request recreates an empty schema, as MongoDB does (product decision Q1).
      const pg = requirePostgres(this.config);
      const schema = pgSchemaName("meta", instanceId, pg.schemaPrefix);
      await this.evict(instanceId);
      const { getSharedPgPoolRegistry } = await import("../../core/store/postgres/pool-registry.js");
      await withMigrationTx(getSharedPgPoolRegistry().get(pg.url, pg.poolMax), schema, (c) =>
        c.query(`DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE`),
      );
      await this.evict(instanceId);
      return { db_name: schema, dropped: true };
    }

    const dbName = resolveMetadataDbName(instanceId, this.dbPrefix);
    await this.evict(instanceId);

    if (this.config.backend === "mongodb") {
      const client = await this.getSharedMongoClient();
      await client.db(dbName).dropDatabase();
      return { db_name: dbName, dropped: true };
    }

    const baseDir = this.config.sqliteBaseDir ?? DEFAULT_SQLITE_BASE;
    const dir = resolveSqliteDbDir(baseDir, instanceId, this.dbPrefix);
    await rm(dir, { recursive: true, force: true });
    return { db_name: dbName, dropped: true };
  }

  async closeAll(): Promise<void> {
    for (const [key, entry] of this.cache) {
      await Promise.resolve(entry.store.close()).catch(() => {});
      this.cache.delete(key);
    }
    if (this.sharedMongoClient) {
      await this.sharedMongoClient.close().catch(() => {});
      this.sharedMongoClient = null;
      this.sharedMongoClientPromise = null;
    }
  }
}

export async function createMetadataStorePool(
  config: MetadataStoreConfig,
): Promise<MetadataStorePool> {
  return new MetadataStorePool(config);
}
