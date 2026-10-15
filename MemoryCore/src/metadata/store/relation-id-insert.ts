/**
 * 关联表 `id` 列插入：碰撞检测与重试（配合 generateRelationId）。
 */
import { generateRelationId } from "../utils/id-generator.js";
import { isUniqueViolation } from "../../core/store/errors.js";

export const RELATION_ID_RETRY_LIMIT = 3;

/** SQLite：关联表主键 `id` 唯一约束冲突。 */
export function isSqliteRelationIdCollision(err: unknown): boolean {
  return isUniqueViolation(err) && err instanceof Error && /\bmeta_\w+\.id\b/.test(err.message);
}

/** MongoDB：关联表主键 `id` 重复键（E11000）。 */
export function isMongoRelationIdCollision(err: unknown): boolean {
  return isUniqueViolation(err) && Boolean((err as { keyPattern?: Record<string, unknown> }).keyPattern?.id);
}

/**
 * Postgres：关联表主键 `id` 冲突。只认该表的主键约束名（`<table>_pkey`），
 * 其他唯一约束（如 UNIQUE(team_id, user_id)）不重试。
 */
export function pgRelationIdCollision(table: string): (err: unknown) => boolean {
  return (err) => isUniqueViolation(err, `${table}_pkey`);
}

/**
 * 使用自动生成的 relation id 执行插入；`fixedId` 已指定时不重试。
 *
 * async：MongoDB / Postgres 的 insert 是异步的，同步版本永远接不到 rejection
 * 。SQLite 调用点都不在事务内，await 不会进入 SQLite 事务。
 */
export async function runWithGeneratedRelationId<T>(
  fixedId: string | undefined,
  isCollision: (err: unknown) => boolean,
  insert: (id: string) => Promise<T> | T,
): Promise<T> {
  if (fixedId) return insert(fixedId);
  let lastErr: unknown;
  for (let attempt = 0; attempt < RELATION_ID_RETRY_LIMIT; attempt++) {
    try {
      return await insert(generateRelationId());
    } catch (err) {
      if (isCollision(err)) {
        lastErr = err;
        continue;
      }
      throw err;
    }
  }
  if (lastErr instanceof Error) throw lastErr;
  throw new Error("relation id collision after max retries");
}
