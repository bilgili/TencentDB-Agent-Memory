/**
 * CredentialStore —— 资源级凭据存取。
 *
 * 主键：(service_id, resource_type, resource_id)。凭据挂在资源上，与用户解耦。
 * 存储：`cred_secret` 用 base64（非加密），使用时在内存解码，明文永不出 KS。
 * 保密性由文件权限 + 接口不回吐 + 明文不进日志承担，见 §4.1.3。
 */

import { and, eq } from "drizzle-orm";

import type { Db, PgDb } from "../db/client.js";
import type { PgTables } from "../db/schema.pg.js";
import { knowledgeSourceCredential } from "../db/schema.sqlite.js";
import { createLogger } from "../logger.js";
import { encodeSecret, decodeSecret } from "./credential-codec.js";
import type {
  CredentialKind,
  CredentialWithProvider,
  CredentialStatus,
  ICredentialStore,
  ResourceRef,
  ResourceType,
  SourceCredential,
} from "./types.js";

const log = createLogger("source-auth-credential-store");

const nowIso = (): string => new Date().toISOString();

type CredentialSelect = typeof knowledgeSourceCredential.$inferSelect;
type StatusFields = Pick<
  CredentialSelect,
  "resourceType" | "resourceId" | "providerId" | "credKind" | "lastVerifiedAt" | "updatedAt"
>;

const rowToStatus = (row: StatusFields): CredentialStatus => ({
  resource_type: row.resourceType as ResourceType,
  resource_id: row.resourceId,
  provider_id: row.providerId,
  cred_kind: row.credKind as CredentialKind,
  last_verified_at: row.lastVerifiedAt,
  updated_at: row.updatedAt,
});

/** 一行 → 明文凭据 + provider_id；decode 失败 → null（当作未配置）。 */
function decodeRow(row: CredentialSelect | undefined, ref: ResourceRef): CredentialWithProvider | null {
  if (!row) return null;

  // decode 失败 → null（当作未配置），不抛异常。避免损坏内容被当有效令牌用去 clone。
  const secret = decodeSecret(row.credSecret);
  if (secret === null) {
    log.warn("credential secret malformed, treating as missing", {
      resourceType: ref.type,
      resourceId: ref.resourceId,
    });
    return null;
  }

  let extra: Record<string, unknown> | undefined;
  if (row.credExtraJson) {
    try {
      extra = JSON.parse(row.credExtraJson) as Record<string, unknown>;
    } catch {
      log.warn("credential extra_json malformed, ignored", {
        resourceType: ref.type,
        resourceId: ref.resourceId,
      });
    }
  }

  return {
    credential: {
      kind: row.credKind as CredentialKind,
      secret,
      username: row.credUsername ?? undefined,
      extra,
    },
    provider_id: row.providerId,
  };
}

/** put() insert values + conflict SET (shared by both dialects). */
function putSets(ref: ResourceRef, cred: SourceCredential, providerId: string, updatedBy?: string) {
  if (!cred.secret) throw new Error("credential secret must be non-empty");
  const extraJson =
    cred.extra && Object.keys(cred.extra).length > 0 ? JSON.stringify(cred.extra) : null;
  const ts = nowIso();
  const values = {
    serviceId: ref.serviceId,
    resourceType: ref.type,
    resourceId: ref.resourceId,
    providerId,
    credKind: cred.kind,
    credSecret: encodeSecret(cred.secret),
    credUsername: cred.username ?? null,
    credExtraJson: extraJson,
    lastVerifiedAt: null as string | null,
    createdBy: updatedBy ?? null,
    updatedBy: updatedBy ?? null,
    createdAt: ts,
    updatedAt: ts,
  };
  const set = {
    providerId: values.providerId,
    credKind: values.credKind,
    credSecret: values.credSecret,
    credUsername: values.credUsername,
    credExtraJson: values.credExtraJson,
    updatedBy: values.updatedBy,
    updatedAt: values.updatedAt,
  };
  return { values, set };
}

// 日志绝不带 secret / cred_secret；仅记归属。
function logSaved(ref: ResourceRef, cred: SourceCredential, providerId: string): void {
  log.info("credential saved", {
    resourceType: ref.type,
    resourceId: ref.resourceId,
    providerId,
    kind: cred.kind,
  });
}

type CredTable = typeof knowledgeSourceCredential | PgTables["knowledgeSourceCredential"];

/** 组一个 where 三元组（复用度高）。 */
const whereRef = (t: CredTable, ref: ResourceRef) =>
  and(eq(t.serviceId, ref.serviceId), eq(t.resourceType, ref.type), eq(t.resourceId, ref.resourceId));

export interface CredentialStoreOptions {
  db: Db;
}

export function createCredentialStore(opts: CredentialStoreOptions): ICredentialStore {
  const { db } = opts;
  const t = knowledgeSourceCredential;

  const readOne = (ref: ResourceRef): CredentialWithProvider | null =>
    decodeRow(db.select().from(t).where(whereRef(t, ref)).get(), ref);

  // Synchronous better-sqlite3 work inside async methods.
  return {
    async get(ref) {
      return readOne(ref)?.credential ?? null;
    },

    async getWithProvider(ref) {
      return readOne(ref);
    },

    async put(ref, cred, providerId, updatedBy) {
      const { values, set } = putSets(ref, cred, providerId, updatedBy);
      db.insert(t)
        .values(values)
        .onConflictDoUpdate({ target: [t.serviceId, t.resourceType, t.resourceId], set })
        .run();
      logSaved(ref, cred, providerId);
    },

    async delete(ref) {
      const res = db.delete(t).where(whereRef(t, ref)).run();
      return (res.changes ?? 0) > 0;
    },

    async status(ref) {
      const row = db.select().from(t).where(whereRef(t, ref)).get();
      return row ? rowToStatus(row) : null;
    },

    async listAllByType(type) {
      return db
        .select({ serviceId: t.serviceId, resourceId: t.resourceId })
        .from(t)
        .where(eq(t.resourceType, type))
        .all();
    },

    async recordVerify(ref) {
      db.update(t)
        .set({ lastVerifiedAt: nowIso(), updatedAt: nowIso() })
        .where(whereRef(t, ref))
        .run();
    },
  };
}

/** Postgres twin of createCredentialStore; same statements. */
export function createPgCredentialStore(opts: { db: PgDb; tables: PgTables }): ICredentialStore {
  const { db } = opts;
  const t = opts.tables.knowledgeSourceCredential;

  const readOne = async (ref: ResourceRef): Promise<CredentialWithProvider | null> => {
    const [row] = await db.select().from(t).where(whereRef(t, ref));
    return decodeRow(row, ref);
  };

  return {
    async get(ref) {
      return (await readOne(ref))?.credential ?? null;
    },

    getWithProvider: readOne,

    async put(ref, cred, providerId, updatedBy) {
      const { values, set } = putSets(ref, cred, providerId, updatedBy);
      await db
        .insert(t)
        .values(values)
        .onConflictDoUpdate({ target: [t.serviceId, t.resourceType, t.resourceId], set });
      logSaved(ref, cred, providerId);
    },

    async delete(ref) {
      const res = await db.delete(t).where(whereRef(t, ref));
      return (res.rowCount ?? 0) > 0;
    },

    async status(ref) {
      const [row] = await db.select().from(t).where(whereRef(t, ref));
      return row ? rowToStatus(row) : null;
    },

    async listAllByType(type) {
      return db
        .select({ serviceId: t.serviceId, resourceId: t.resourceId })
        .from(t)
        .where(eq(t.resourceType, type));
    },

    async recordVerify(ref) {
      await db
        .update(t)
        .set({ lastVerifiedAt: nowIso(), updatedAt: nowIso() })
        .where(whereRef(t, ref));
    },
  };
}
