/**
 * LlmBindingStore — per-instance (service_id) LLM routing binding.
 *
 * Decoupled from IKnowledgeStore: a single small table keyed by service_id that
 * tells wiki ingest/summary which LLM endpoint to use for that instance.
 *   - mode='proxy' → route through context_proxy with a dedicated knowledge-service
 *     user_key so LLM usage is billed per instance.
 *   - mode='byo'   → user-supplied OpenAI-compatible endpoint.
 *
 * No binding (or disabled/incomplete) → behaviour depends on the global LLM_MODE:
 *   - LLM_MODE=custom → fall back to the global LLM_* (direct BYO) config.
 *   - LLM_MODE=proxy (default) → NO silent direct fallback; an "unconfigured"
 *     config is returned so wiki ingest fails loudly (forces the proxy binding
 *     chain to actually be verified instead of masking bugs).
 *
 * Model is NOT stored per-instance — it always comes from the global `LLM_MODEL`
 * env (single source of truth, see resolveLlmConfig).
 */

import { eq } from "drizzle-orm";
import type { Db, PgDb } from "../db/client.js";
import type { PgTables } from "../db/schema.pg.js";
import { llmBinding } from "../db/schema.sqlite.js";
import type { LlmConfig } from "../config.js";
import { mapLlmBindingRow } from "./rows.js";

export type LlmBindingMode = "proxy" | "byo";

export interface LlmBindingRow {
  service_id: string;
  mode: LlmBindingMode;
  proxy_base_url: string | null;
  api_key: string | null;
  base_url: string | null;
  enabled: boolean;
  updated_at: string;
}

/** Upsert payload (service_id comes from the header, not the body).
 *  `api_key: undefined` 表示保留原值（用于只更新 proxy_base_url 的场景）；
 *  `api_key: null` 表示清空；`api_key: "xxx"` 表示更新。 */
export interface LlmBindingInput {
  mode: LlmBindingMode;
  proxy_base_url?: string | null;
  api_key?: string | null;
  base_url?: string | null;
  enabled?: boolean;
}

/** Read-side status (never exposes api_key). */
export interface LlmBindingStatus {
  bound: boolean;
  mode: LlmBindingMode | null;
  enabled: boolean;
}

export interface ILlmBindingStore {
  get(serviceId: string): Promise<LlmBindingRow | null>;
  listAll(): Promise<LlmBindingRow[]>;
  upsert(serviceId: string, input: LlmBindingInput): Promise<LlmBindingRow>;
  status(serviceId: string): Promise<LlmBindingStatus>;
}

/**
 * Upsert column sets. api_key: undefined → 保留原值（仅 upsert 已存在记录时）；null → 清空；string → 更新。
 * One statement: the "keep" case simply leaves api_key out of the conflict SET.
 */
function upsertSets(serviceId: string, input: LlmBindingInput) {
  const values = {
    serviceId,
    mode: input.mode,
    proxyBaseUrl: input.proxy_base_url ?? null,
    apiKey: input.api_key ?? null,
    baseUrl: input.base_url ?? null,
    enabled: input.enabled === false ? 0 : 1,
    updatedAt: new Date().toISOString(),
  };
  const set = {
    mode: values.mode,
    proxyBaseUrl: values.proxyBaseUrl,
    ...(input.api_key !== undefined ? { apiKey: values.apiKey } : {}),
    baseUrl: values.baseUrl,
    enabled: values.enabled,
    updatedAt: values.updatedAt,
  };
  return { values, set };
}

function toStatus(row: LlmBindingRow | null): LlmBindingStatus {
  if (!row) return { bound: false, mode: null, enabled: false };
  return { bound: true, mode: row.mode, enabled: row.enabled };
}

export function createLlmBindingStore(db: Db): ILlmBindingStore {
  // Synchronous better-sqlite3 work inside async methods: each method body
  // runs without an await, so it stays atomic.
  const getSync = (serviceId: string): LlmBindingRow | null => {
    const row = db.select().from(llmBinding).where(eq(llmBinding.serviceId, serviceId)).get();
    return row ? mapLlmBindingRow(row) : null;
  };

  return {
    async get(serviceId: string): Promise<LlmBindingRow | null> {
      return getSync(serviceId);
    },

    async listAll(): Promise<LlmBindingRow[]> {
      return db.select().from(llmBinding).all().map(mapLlmBindingRow);
    },

    async upsert(serviceId: string, input: LlmBindingInput): Promise<LlmBindingRow> {
      const { values, set } = upsertSets(serviceId, input);
      const row = db
        .insert(llmBinding)
        .values(values)
        .onConflictDoUpdate({ target: llmBinding.serviceId, set })
        .returning()
        .get();
      return mapLlmBindingRow(row);
    },

    async status(serviceId: string): Promise<LlmBindingStatus> {
      return toStatus(getSync(serviceId));
    },
  };
}

/** Postgres twin of createLlmBindingStore; same statements. */
export function createPgLlmBindingStore(db: PgDb, tables: PgTables): ILlmBindingStore {
  const t = tables.llmBinding;
  const get = async (serviceId: string): Promise<LlmBindingRow | null> => {
    const [row] = await db.select().from(t).where(eq(t.serviceId, serviceId));
    return row ? mapLlmBindingRow(row) : null;
  };

  return {
    get,

    async listAll(): Promise<LlmBindingRow[]> {
      return (await db.select().from(t)).map(mapLlmBindingRow);
    },

    async upsert(serviceId: string, input: LlmBindingInput): Promise<LlmBindingRow> {
      const { values, set } = upsertSets(serviceId, input);
      const [row] = await db
        .insert(t)
        .values(values)
        .onConflictDoUpdate({ target: t.serviceId, set })
        .returning();
      return mapLlmBindingRow(row!);
    },

    async status(serviceId: string): Promise<LlmBindingStatus> {
      return toStatus(await get(serviceId));
    },
  };
}

function trimTrailingSlash(url: string): string {
  return url.replace(/\/+$/, "");
}

/**
 * Pure resolver: turn a binding (or its absence) into an effective LlmConfig.
 * maxTokens/timeoutMs/provider/mode always inherit from the fallback; the binding
 * only overrides endpoint/key.
 *
 * Model always comes from `fallback.model` (= global `LLM_MODEL` env) — no
 * per-instance model storage, single source of truth.
 *
 * proxy mode → baseUrl = {proxy_base_url}/proxy/{service_id}/v1 (so context_proxy
 * uses service_id as x-tdai-service-id for auth/verify + per-instance billing).
 *
 * When there is no usable binding, the global default is decided by fallback.mode:
 *   - 'custom' → return the global config as-is (direct BYO).
 *   - 'proxy'  → return the global config with baseUrl/apiKey blanked, so
 *     createLlmClient throws instead of silently calling a direct endpoint.
 */
export function resolveLlmConfig(
  serviceId: string,
  binding: LlmBindingRow | null,
  fallback: LlmConfig,
): LlmConfig {
  // Global default when no per-instance binding is usable.
  const globalDefault = (): LlmConfig =>
    fallback.mode === "custom"
      ? fallback
      : { ...fallback, baseUrl: "", apiKey: "" };

  if (!binding || !binding.enabled) return globalDefault();

  if (binding.mode === "proxy") {
    if (!binding.proxy_base_url || !binding.api_key) return globalDefault();
    return {
      mode: fallback.mode,
      protocol: fallback.protocol,
      provider: fallback.provider,
      apiKey: binding.api_key,
      model: fallback.model,
      baseUrl: `${trimTrailingSlash(binding.proxy_base_url)}/proxy/${serviceId}/v1`,
      maxTokens: fallback.maxTokens,
      timeoutMs: fallback.timeoutMs,
      stream: fallback.stream,
    };
  }

  // byo
  if (!binding.base_url || !binding.api_key) return globalDefault();
  return {
    mode: fallback.mode,
    protocol: fallback.protocol,
    provider: fallback.provider,
    apiKey: binding.api_key,
    model: fallback.model,
    baseUrl: binding.base_url,
    maxTokens: fallback.maxTokens,
    timeoutMs: fallback.timeoutMs,
    stream: fallback.stream,
  };
}
