/**
 * Driver-neutral constraint-error classification.
 *
 * The one place in MemoryCore that knows how each driver reports a unique or
 * foreign-key violation. Callers that need a specific column/key add their own
 * narrowing on top (SQLite and MongoDB do not report constraint names).
 */

interface DriverError {
  code?: unknown;
  errcode?: unknown;
  constraint?: unknown;
  message?: unknown;
}

function asDriverError(err: unknown): DriverError | null {
  return typeof err === "object" && err !== null ? (err as DriverError) : null;
}

// SQLite extended result codes (node:sqlite exposes them as `errcode`).
const SQLITE_CONSTRAINT_PRIMARYKEY = 1555;
const SQLITE_CONSTRAINT_UNIQUE = 2067;
const SQLITE_CONSTRAINT_FOREIGNKEY = 787;

/**
 * True for a unique-key violation from any MemoryCore driver:
 * - Postgres `23505` — and, when `constraint` is given, only if
 *   `err.constraint === constraint`;
 * - SQLite `SQLITE_CONSTRAINT_UNIQUE` / `_PRIMARYKEY` or "UNIQUE constraint failed";
 * - MongoDB `E11000` (code 11000).
 *
 * `constraint` filters Postgres errors only; the other drivers do not name the
 * constraint, so callers narrow those by column or key pattern.
 */
export function isUniqueViolation(err: unknown, constraint?: string): boolean {
  const e = asDriverError(err);
  if (!e) return false;
  if (e.code === "23505") return constraint === undefined || e.constraint === constraint;
  if (e.code === 11000) return true;
  if (e.code === "SQLITE_CONSTRAINT_UNIQUE" || e.code === "SQLITE_CONSTRAINT_PRIMARYKEY") return true;
  if (e.errcode === SQLITE_CONSTRAINT_UNIQUE || e.errcode === SQLITE_CONSTRAINT_PRIMARYKEY) return true;
  return typeof e.message === "string" && e.message.includes("UNIQUE constraint failed");
}

/** True for a foreign-key violation: Postgres `23503`, SQLite FOREIGNKEY. */
export function isForeignKeyViolation(err: unknown, constraint?: string): boolean {
  const e = asDriverError(err);
  if (!e) return false;
  if (e.code === "23503") return constraint === undefined || e.constraint === constraint;
  if (e.code === "SQLITE_CONSTRAINT_FOREIGNKEY" || e.errcode === SQLITE_CONSTRAINT_FOREIGNKEY) return true;
  return typeof e.message === "string" && e.message.includes("FOREIGN KEY constraint failed");
}
