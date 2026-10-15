/**
 * StorePool — per-instanceId 的 Store 实例池
 *
 * 双模式支持:
 *   - standalone: 使用 SQLite 本地存储 (每个 instanceId 一个 SQLite 文件)
 *   - service: 使用 TCVDB 向量数据库 (每个 instanceId 一个远程 VDB 连接)
 *
 * 与 InstanceConfigProvider 配合:
 *   1. 请求到达时, 从 InstanceConfigProvider 获取该 instanceId 的 VDB 配置
 *   2. 用 VDB 配置创建/复用 Store 实例
 *   3. 池化管理, 避免重复创建连接
 *
 * standalone 模式下:
 *   - VdbConfig 为空或来自环境变量 → 创建 SQLite Store
 *   - 固定一个 "default" instanceId, 行为与原 createStoreBundle 一致
 */

import path from "node:path";
import { existsSync, mkdirSync } from "node:fs";
import type { MemoryTdaiConfig } from "../../config.js";
import type { IMemoryStore, StoreLogger } from "./types.js";
import type { EmbeddingService } from "./embedding.js";
import { createEmbeddingService, NoopEmbeddingService } from "./embedding.js";
import { VectorStore } from "./sqlite/memory-store.js";
import { TcvdbMemoryStore } from "./tcvdb/memory-store.js";
import { TcvdbSkillStore } from "./tcvdb/skill-store.js";
import { MongoMemoryStore } from "./mongodb/memory-store.js";
import { MongoSkillStore } from "./mongodb/skill-store.js";
import { getSharedMongoClientPool } from "./mongodb/client-pool.js";
import type { MongoClientPool } from "./mongodb/client-pool.js";
import { createBM25Encoder } from "./bm25-local.js";
import type { BM25LocalEncoder } from "./bm25-local.js";
import type { VdbConfig, MongoConfig } from "../instance-config-provider.js";
import type { DbChoice, DbKind } from "../backend-selection/types.js";
import type { ISkillStore } from "../skill/skill-store.interface.js";
import { redactUrl } from "./postgres/settings.js";
import { getSharedPgPoolRegistry } from "./postgres/pool-registry.js";
import { PostgresMemoryStore } from "./postgres/memory-store.js";
import { PostgresSkillStore } from "./postgres/skill-store.js";
import type { PostgresConfig } from "../instance-config-provider.js";
import { metricProducer } from "../report/kafka-metric-producer.js";

const TAG = "[store-pool]";

// ════════════════════════════════════════════════════════
// Types
// ════════════════════════════════════════════════════════

export interface PooledStore {
  store: IMemoryStore;
  embedding: EmbeddingService;
  bm25Encoder?: BM25LocalEncoder;
}

interface PoolEntry {
  pooledStore: PooledStore;
  /** VDB 配置指纹, 用于检测配置变更 */
  configFingerprint: string;
  lastAccessedAt: number;
}

interface Logger {
  debug?: (message: string) => void;
  info: (message: string) => void;
  warn: (message: string) => void;
  error: (message: string) => void;
}

/** Process default store kind — an alias of the one kind list, `DbKind`. */
export type StoreMode = DbKind;

function unknownDbKind(db: never): never {
  throw new Error(`${TAG} unknown db kind: ${(db as { kind?: unknown }).kind}`);
}

export interface KafkaMetricOptions {
  /** Kafka Broker 列表 (逗号分隔或数组) */
  brokers?: string[] | string;
  /** Topic 名称 (默认: "memory_monitor") */
  topic?: string;
  /** 是否启用 (默认: 根据 brokers 是否非空自动判断) */
  enabled?: boolean;
}

export interface StorePoolOptions {
  /** 存储模式: "sqlite" (standalone 本地) 或 "tcvdb" (service 远程) */
  mode: StoreMode;
  /** 记忆插件配置 (用于 BM25/embedding 设置) */
  memoryCfg: MemoryTdaiConfig;
  /** 数据目录 (SQLite 模式下使用) */
  dataDir?: string;
  /** 最大池化实例数 (Memory store), 默认 100 */
  maxStores?: number;
  /** 最大 Skill store 缓存数, 默认 100 */
  maxSkillStores?: number;
  /** Kafka 指标上报配置 (可选, 不配置则不上报) */
  kafka?: KafkaMetricOptions;
  logger: Logger;
}

// ════════════════════════════════════════════════════════
// StorePool
// ════════════════════════════════════════════════════════

export class StorePool {
  private pool = new Map<string, PoolEntry>();
  private maxStores: number;
  readonly mode: StoreMode;
  private memoryCfg: MemoryTdaiConfig;
  private dataDir: string;
  private logger: Logger;
  /** 全局共享的 BM25 编码器 (避免重复加载 jieba 词典导致 OOM) */
  private sharedBm25Encoder: BM25LocalEncoder | undefined;
  /** 共享 MongoClient 连接池 (mongodb 后端；按 endpoint 复用一个 client)。 */
  private mongoPool: MongoClientPool | undefined;

  /** Skill store 缓存上限 */
  private maxSkillStores: number;
  /** Skill store 最后访问时间 (LRU 淘汰用) */
  private skillStoreAccessTimes = new Map<string, number>();



  /**
   * Grace-close 跟踪：已从 pool 移除但底层 close 推迟执行的 entries。
   * CR-5 mitigation (2026-05-19): evict / config-change 不立刻 close 底层 store,
   * 而是延迟 graceCloseDelayMs 后再 close, 让 in-flight 请求(同步路径 recall/capture
   * <100ms; 异步 worker 路径 L1/L2/L3 受 maxRetries=3 + 指数退避兜底)有时间完成.
   */
  private pendingCloses = new Set<Promise<void>>();
  /** Grace period 默认 30s, 远大于同步路径墙钟 (recall ~49ms / capture ~54ms),
   *  且 worker 路径即使撞上也有 reEnqueue 重试机制 (5s/15s/45s). */
  private graceCloseDelayMs = 30_000;

  constructor(opts: StorePoolOptions) {
    this.maxStores = opts.maxStores ?? 100;
    this.maxSkillStores = opts.maxSkillStores ?? 100;
    this.mode = opts.mode;
    this.memoryCfg = opts.memoryCfg;
    this.dataDir = opts.dataDir ?? ".";
    this.logger = opts.logger;

    // 初始化时创建一次 BM25 编码器, 所有 Store 共享
    this.sharedBm25Encoder = createBM25Encoder(this.memoryCfg.bm25, this.logger as StoreLogger);

    // 初始化 Kafka Metric Producer（异步，不阻塞构造）
    this.initKafkaMetricProducer(opts.kafka);

    this.logger.info(`${TAG} Initialized: mode=${this.mode}, maxStores=${this.maxStores}, maxSkillStores=${this.maxSkillStores}, bm25=${this.sharedBm25Encoder ? "shared" : "disabled"}`);
  }

  /**
   * 初始化 Kafka Metric Producer。
   * 异步执行，不阻塞 StorePool 构造。初始化失败静默忽略。
   * 配置优先级：StorePoolOptions.kafka > 环境变量（兜底）
   */
  private initKafkaMetricProducer(kafka?: KafkaMetricOptions): void {
    // 配置优先，环境变量仅作兜底
    const rawBrokers = kafka?.brokers ?? "";
    const brokers = Array.isArray(rawBrokers)
      ? rawBrokers
      : rawBrokers.split(",").map(s => s.trim()).filter(Boolean);

    const enabled = kafka?.enabled ?? brokers.length > 0;
    if (!enabled || brokers.length === 0) {
      this.logger.info(`${TAG} Kafka metric producer disabled (no brokers configured)`);
      return;
    }

    // 异步初始化，不阻塞业务
    metricProducer.initialize({
      brokers,
      topic: kafka?.topic ?? "memory_monitor",
      enabled: true,
    }).catch((err) => {
      // 初始化失败静默处理，不影响业务
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.warn(`${TAG} Kafka metric producer init failed: ${msg}. Metrics disabled.`);
    });
  }

  /**
   * 获取指定 instanceId 对应的 Store 实例。
   *
   * `db` is the selection layer's explicit kind + conn:
   * the pool switches on `db.kind` and never infers the kind from which config
   * happens to be present.
   */
  async getStore(instanceId: string, db: DbChoice): Promise<PooledStore> {
    const now = Date.now();
    const { fingerprint, label } = this.describeDb(instanceId, db);
    const cached = this.pool.get(instanceId);

    // 命中且配置未变
    if (cached && cached.configFingerprint === fingerprint) {
      cached.lastAccessedAt = now;
      return cached.pooledStore;
    }

    // 配置变了 → 关闭旧的
    if (cached) {
      this.logger.info(`${TAG} Config changed for ${instanceId}, recreating store`);
      await this.closeEntry(instanceId, cached);
    }

    // LRU 淘汰
    if (this.pool.size >= this.maxStores) {
      await this.evictLru();
    }

    // 创建新 Store
    const pooledStore = this.createStore(instanceId, db);

    this.pool.set(instanceId, {
      pooledStore,
      configFingerprint: fingerprint,
      lastAccessedAt: now,
    });

    this.logger.info(
      `${TAG} Created ${db.kind} store for ${instanceId}: ${label} (pool size: ${this.pool.size})`,
    );

    // 初始化 Store (建表/检查连接)
    try {
      await pooledStore.store.init();
    } catch (e) {
      this.logger.warn(`${TAG} Store init failed for ${instanceId}: ${e}`);
    }

    return pooledStore;
  }

  private createStore(instanceId: string, db: DbChoice): PooledStore {
    switch (db.kind) {
      case "sqlite":
        return this.createSqliteStore(instanceId);
      case "tcvdb":
        return this.createTcvdbStore(db.conn);
      case "mongodb":
        return this.createMongoStore(db.conn);
      case "postgres":
        return this.createPostgresStore(db.conn);
      default:
        return unknownDbKind(db);
    }
  }

  /** Cache fingerprint (detects a config change) and a log label (no secrets) for `db`. */
  private describeDb(instanceId: string, db: DbChoice): { fingerprint: string; label: string } {
    switch (db.kind) {
      case "sqlite":
        return { fingerprint: `sqlite:${instanceId}`, label: `sqlite @ ${this.getSqlitePath(instanceId)}` };
      case "tcvdb":
        return { fingerprint: this.computeFingerprint(db.conn), label: `${db.conn.url} / ${db.conn.database}` };
      case "mongodb":
        return {
          fingerprint: this.computeMongoFingerprint(db.conn),
          label: `mongodb ${db.conn.endpoint} / ${db.conn.database}`,
        };
      case "postgres":
        return {
          fingerprint: `postgres:${db.conn.url}|${db.conn.schema}`,
          label: `postgres ${redactUrl(db.conn.url)} / ${db.conn.schema}`,
        };
      default:
        return unknownDbKind(db);
    }
  }

  /**
   * 移除指定实例的 Store (实例下线时调用)
   */
  async evict(instanceId: string): Promise<void> {
    const entry = this.pool.get(instanceId);
    if (entry) {
      await this.closeEntry(instanceId, entry);
    }
  }

  /**
   * 关闭所有 Store
   *
   * CR-5: 原 closeAll 直接同步关闭所有 pool 内 store, 会导致 in-flight 请求崩溃.
   * 现在分两步:
   *   1. 把所有 entries 触发 closeEntry (延迟关闭, 加入 pendingCloses)
   *   2. 等待所有 pendingCloses 完成 (含本次新加 + 之前 evict 留下的)
   * 进程关停场景下, 调用方可以选择缩短 grace 时间避免阻塞: setGraceCloseDelay(0)
   */
  async closeAll(): Promise<void> {
    // 关闭 Memory store pool
    const entries = [...this.pool.entries()];
    this.pool.clear();
    // 触发延迟关闭 (这些 promise 会自动加入 pendingCloses)
    for (const [id, entry] of entries) {
      await this.closeEntry(id, entry);  // closeEntry 内部不阻塞, 立即返回
    }

    // 关闭 Skill store 缓存
    for (const [key, store] of this.skillStoreCache) {
      try {
        store.close();
        this.logger.debug?.(`${TAG} Closed skill store: ${key}`);
      } catch (e) {
        this.logger.warn(`${TAG} Error closing skill store ${key}: ${e}`);
      }
    }
    this.skillStoreCache.clear();
    this.skillStoreAccessTimes.clear();

    // 等所有 pending close 完成 (含本次 + 之前 evict 留下的)
    await Promise.allSettled([...this.pendingCloses]);

    // 关闭共享 MongoClient 连接池 (所有 store 已释放引用后再断连)
    if (this.mongoPool) {
      try {
        await this.mongoPool.closeAll();
        this.logger.debug?.(`${TAG} Closed shared MongoClient pool`);
      } catch (e) {
        this.logger.warn(`${TAG} Error closing shared MongoClient pool: ${e}`);
      }
      this.mongoPool = undefined;
    }

    this.logger.info(`${TAG} All stores closed (${entries.length} memory + skill caches cleared)`);
  }

  /**
   * 移除指定 instance 的 Skill store (实例销毁时调用).
   * 调用 store.close() 设 degraded=true, 然后从缓存中移除.
   */
  evictSkillStore(instanceId: string): void {
    const key = `skill:${instanceId}`;
    const store = this.skillStoreCache.get(key);
    if (store) {
      try {
        store.close();
      } catch (e) {
        this.logger.warn(`${TAG} Error closing skill store ${key}: ${e}`);
      }
      this.skillStoreCache.delete(key);
      this.skillStoreAccessTimes.delete(key);
      this.logger.info(`${TAG} Evicted skill store for ${instanceId} (cached: ${this.skillStoreCache.size})`);
    }
  }

  /**
   * 设置 grace-close 延迟 (毫秒). 设为 0 时立即关闭, 失去 in-flight 保护.
   * 主要用于测试或进程紧急退出场景.
   */
  setGraceCloseDelay(ms: number): void {
    this.graceCloseDelayMs = Math.max(0, ms);
  }

  get size(): number { return this.pool.size; }
  has(instanceId: string): boolean { return this.pool.has(instanceId); }

  /**
   * 获取指定 instanceId 的 per-instance Skill Store (kinds with
   * `DB_TRAITS[kind].pooledSkillStore`).
   *
   * 与 getStore() 使用相同的后端实例/连接，只是不同的 Collection/表。
   * Skill store 有独立缓存 (skillStoreCache)，不受 Memory store 池化管理影响。
   */
  async getSkillStore(instanceId: string, db: DbChoice): Promise<ISkillStore> {
    const key = `skill:${instanceId}`;
    const cached = this.skillStoreCache.get(key);
    if (cached) {
      this.skillStoreAccessTimes.set(key, Date.now());
      return cached;
    }

    let store: ISkillStore;
    let label: string;
    switch (db.kind) {
      case "mongodb":
        store = new MongoSkillStore({
          pool: this.getMongoPool(),
          mongoConfig: db.conn,
          logger: this.logger as StoreLogger,
        });
        label = `mongodb ${db.conn.endpoint}/${db.conn.database}`;
        break;
      case "tcvdb":
        store = new TcvdbSkillStore({
          url: db.conn.url,
          username: db.conn.user,
          apiKey: db.conn.apiKey,
          database: db.conn.database,
          embeddingModel: this.memoryCfg.tcvdb?.embeddingModel ?? "bge-large-zh",
          timeout: this.memoryCfg.tcvdb?.timeout ?? 10000,
          logger: this.logger as StoreLogger,
          bm25Encoder: this.sharedBm25Encoder,
        });
        label = `tcvdb ${db.conn.url}/${db.conn.database}`;
        break;
      case "postgres":
        // Same shared pool and mem schema as the memory store.
        store = new PostgresSkillStore({
          pool: getSharedPgPoolRegistry(this.logger as StoreLogger).get(db.conn.url, db.conn.poolMax),
          schema: db.conn.schema,
          dimensions: this.memoryCfg.embedding.dimensions ?? 0,
          logger: this.logger as StoreLogger,
        });
        label = `postgres ${redactUrl(db.conn.url)}/${db.conn.schema}`;
        break;
      case "sqlite":
        throw new Error(
          `${TAG} getSkillStore(${instanceId}): sqlite has no pooled skill store ` +
            `(DB_TRAITS.sqlite.pooledSkillStore=false; TdaiCore owns the SQLite skill store)`,
        );
      default:
        return unknownDbKind(db);
    }

    // LRU 淘汰
    if (this.skillStoreCache.size >= this.maxSkillStores) {
      this.evictSkillStoreLru();
    }

    store.init();
    this.skillStoreCache.set(key, store);
    this.skillStoreAccessTimes.set(key, Date.now());
    this.logger.info(`${TAG} Created skill store for ${instanceId}: ${label} (cached: ${this.skillStoreCache.size})`);
    return store;
  }

  private skillStoreCache = new Map<string, ISkillStore>();

  // ════════════════════════════════════════════════════════
  // Internal — TCVDB Store
  // ════════════════════════════════════════════════════════

  private createTcvdbStore(vdbConfig: VdbConfig): PooledStore {
    // [DEBUG] 本地调试用: 公网 HTTPS 连接 VDB 时需要 CA 证书。
    // 内网部署走 HTTP 80 端口无需此逻辑。
    // 通过环境变量 VDB_CA_PEM_PATH 指定 PEM 文件路径。
    const caPemPath = vdbConfig.url.startsWith("https://")
      ? (process.env.VDB_CA_PEM_PATH || undefined)
      : undefined;

    const store = new TcvdbMemoryStore({
      url: vdbConfig.url,
      username: vdbConfig.user,
      apiKey: vdbConfig.apiKey,
      database: vdbConfig.database,
      embeddingEnabled: this.memoryCfg.tcvdb?.embeddingEnabled,
      embeddingModel: this.memoryCfg.tcvdb?.embeddingModel ?? "bge-large-zh",
      timeout: this.memoryCfg.tcvdb?.timeout ?? 10000,
      caPemPath,
      logger: this.logger as StoreLogger,
      bm25Encoder: this.sharedBm25Encoder ?? undefined,
    });

    return {
      store,
      embedding: new NoopEmbeddingService() as unknown as EmbeddingService,
      bm25Encoder: this.sharedBm25Encoder,
    };
  }

  // ════════════════════════════════════════════════════════
  // Internal — MongoDB Store
  // ════════════════════════════════════════════════════════

  /** Lazily create the shared MongoClient pool (one client per endpoint). */
  private getMongoPool(): MongoClientPool {
    if (!this.mongoPool) {
      this.mongoPool = getSharedMongoClientPool(this.logger as StoreLogger);
    }
    return this.mongoPool;
  }

  private createMongoStore(mongoConfig: MongoConfig): PooledStore {
    const store = new MongoMemoryStore({
      pool: this.getMongoPool(),
      mongoConfig,
      logger: this.logger as StoreLogger,
    });
    // Mongo uses native mongot/BM25 — no application-layer embedding or bm25 encoder.
    return {
      store,
      embedding: new NoopEmbeddingService() as unknown as EmbeddingService,
      bm25Encoder: undefined,
    };
  }

  private computeMongoFingerprint(mongoConfig: MongoConfig): string {
    return `mongodb:${mongoConfig.endpoint}|${mongoConfig.database}|${mongoConfig.user}`;
  }

  // ════════════════════════════════════════════════════════
  // Internal — SQLite Store
  // ════════════════════════════════════════════════════════

  /** App-side embedding service for stores that keep their own vectors (sqlite, postgres). */
  private createAppEmbedding(): EmbeddingService {
    // Embedding service (远端 API, 如 OpenAI text-embedding)
    const embCfg = this.memoryCfg.embedding;
    if (embCfg.enabled && embCfg.provider !== "local" && embCfg.provider !== "none" && embCfg.apiKey) {
      return createEmbeddingService({
        provider: embCfg.provider,
        baseUrl: embCfg.baseUrl,
        apiKey: embCfg.apiKey,
        model: embCfg.model,
        dimensions: embCfg.dimensions,
        maxInputChars: embCfg.maxInputChars,
      }, this.logger as StoreLogger);
    }
    return new NoopEmbeddingService() as unknown as EmbeddingService;
  }

  // ════════════════════════════════════════════════════════
  // Internal — Postgres Store
  // ════════════════════════════════════════════════════════

  /** One shared pg.Pool per URL; the store owns its schema, never the pool. */
  private createPostgresStore(conn: PostgresConfig): PooledStore {
    const store = new PostgresMemoryStore({
      pool: getSharedPgPoolRegistry(this.logger as StoreLogger).get(conn.url, conn.poolMax),
      schema: conn.schema,
      dimensions: this.memoryCfg.embedding.dimensions ?? 0,
      logger: this.logger as StoreLogger,
    });
    return { store, embedding: this.createAppEmbedding(), bm25Encoder: this.sharedBm25Encoder };
  }

  private createSqliteStore(instanceId: string): PooledStore {
    const embCfg = this.memoryCfg.embedding;
    const dims = embCfg.dimensions ?? 0;
    const dbPath = this.getSqlitePath(instanceId);
    // 确保数据库目录存在（对于非 default instance）
    const dbDir = path.dirname(dbPath);
    if (!existsSync(dbDir)) {
      mkdirSync(dbDir, { recursive: true });
    }
    const store = new VectorStore(dbPath, dims, this.logger as StoreLogger);

    return { store, embedding: this.createAppEmbedding(), bm25Encoder: this.sharedBm25Encoder };
  }

  /**
   * SQLite 文件路径:
   *   - "default" → dataDir/vectors.db (兼容原逻辑)
   *   - 其他 instanceId → dataDir/instances/{instanceId}/vectors.db
   */
  private getSqlitePath(instanceId: string): string {
    if (instanceId === "default") {
      return path.join(this.dataDir, "vectors.db");
    }
    return path.join(this.dataDir, "instances", instanceId, "vectors.db");
  }

  // ════════════════════════════════════════════════════════
  // Internal — Common
  // ════════════════════════════════════════════════════════

  private computeFingerprint(cfg: VdbConfig): string {
    return `tcvdb:${cfg.url}|${cfg.database}|${cfg.apiKey}`;
  }

  private async closeEntry(instanceId: string, entry: PoolEntry): Promise<void> {
    // CR-5 mitigation: 立刻从 pool 移除 (新请求拿不到这个 entry, 会创建一个新 store),
    // 但底层 store.close() 推迟 graceCloseDelayMs 才执行, 让任何持有此 entry 引用
    // 的 in-flight 请求有时间完成. 不加引用计数避免修改所有调用方;
    // worker 路径有 maxRetries=3 重试兜底, 同步路径墙钟远小于 grace period, 双保险.
    this.pool.delete(instanceId);

    const closePromise = (async () => {
      // 等 grace period
      if (this.graceCloseDelayMs > 0) {
        await new Promise<void>((resolve) => {
          const t = setTimeout(resolve, this.graceCloseDelayMs);
          // unref 避免阻塞进程退出 (closeAll 会主动 await 这些 promise)
          if (typeof (t as { unref?: () => void }).unref === "function") {
            (t as { unref: () => void }).unref();
          }
        });
      }
      try {
        await entry.pooledStore.store.close();
        this.logger.debug?.(`${TAG} Closed store for ${instanceId} (after ${this.graceCloseDelayMs}ms grace)`);
      } catch (e) {
        this.logger.warn(`${TAG} Error closing store for ${instanceId}: ${e}`);
      }
    })();

    this.pendingCloses.add(closePromise);
    closePromise.finally(() => this.pendingCloses.delete(closePromise));
    // 不 await — 调用方立即返回, 真 close 在后台进行
  }

  private async evictLru(): Promise<void> {
    let oldestKey: string | null = null;
    let oldestTime = Infinity;

    for (const [key, entry] of this.pool) {
      if (entry.lastAccessedAt < oldestTime) {
        oldestTime = entry.lastAccessedAt;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      const entry = this.pool.get(oldestKey)!;
      await this.closeEntry(oldestKey, entry);
      this.logger.debug?.(`${TAG} LRU evicted store for ${oldestKey}`);
    }
  }

  /** Skill store LRU 淘汰：踢掉最久未访问的 entry. */
  private evictSkillStoreLru(): void {
    let oldestKey: string | null = null;
    let oldestTime = Infinity;

    for (const [key, time] of this.skillStoreAccessTimes) {
      if (time < oldestTime) {
        oldestTime = time;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      const store = this.skillStoreCache.get(oldestKey);
      if (store) {
        try {
          store.close();
        } catch (e) {
          this.logger.warn(`${TAG} Error closing skill store ${oldestKey} during LRU evict: ${e}`);
        }
      }
      this.skillStoreCache.delete(oldestKey);
      this.skillStoreAccessTimes.delete(oldestKey);
      this.logger.debug?.(`${TAG} LRU evicted skill store: ${oldestKey}`);
    }
  }
}
