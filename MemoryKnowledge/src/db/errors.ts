/**
 * Driver-neutral database error predicates.
 * MemoryKnowledge owns this copy; MemoryCore has its own.
 */

interface DbErrorLike {
  code?: unknown;
  constraint?: unknown;
  message?: unknown;
}

/**
 * True for a unique-constraint violation:
 *   - Postgres: SQLSTATE 23505 (and, when `constraint` is given, `err.constraint === constraint`)
 *   - SQLite (better-sqlite3): code SQLITE_CONSTRAINT_UNIQUE / SQLITE_CONSTRAINT_PRIMARYKEY,
 *     or message "UNIQUE constraint failed: ..." (when `constraint` is given, the message must name it)
 */
export function isUniqueViolation(err: unknown, constraint?: string): boolean {
  if (!err || typeof err !== "object") return false;
  const e = err as DbErrorLike;
  if (e.code === "23505") {
    return constraint === undefined || e.constraint === constraint;
  }
  const msg = typeof e.message === "string" ? e.message : "";
  const sqliteUnique =
    e.code === "SQLITE_CONSTRAINT_UNIQUE" ||
    e.code === "SQLITE_CONSTRAINT_PRIMARYKEY" ||
    /UNIQUE constraint failed/i.test(msg);
  if (!sqliteUnique) return false;
  return constraint === undefined || msg.includes(constraint);
}
