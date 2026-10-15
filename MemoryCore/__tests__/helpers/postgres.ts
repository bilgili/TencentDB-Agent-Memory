/**
 * Postgres test helpers.
 *
 * Postgres suites run only when TEST_POSTGRES_URL is set (see
 * deploy/test/postgres.yaml); otherwise they are skipped. Each run uses a
 * random schema prefix and drops every schema with that prefix afterwards, so
 * parallel runs and leftovers never collide.
 */
import { randomBytes } from "node:crypto";
import { describe } from "vitest";
import pg from "pg";

export const TEST_POSTGRES_URL = process.env.TEST_POSTGRES_URL?.trim() || undefined;

/** `describe` that skips when no test database is configured. */
export const describePg = describe.skipIf(!TEST_POSTGRES_URL);

/** Random prefix that satisfies the schema-prefix rule `^[a-z_][a-z0-9_]{0,15}$`. */
export function randomSchemaPrefix(): string {
  return `t${randomBytes(4).toString("hex")}`;
}

function requireUrl(): string {
  if (!TEST_POSTGRES_URL) throw new Error("TEST_POSTGRES_URL is not set");
  return TEST_POSTGRES_URL;
}

/** Run `fn` with a short-lived client (tests only; stores use the pool registry). */
export async function withTestClient<T>(fn: (client: pg.Client) => Promise<T>): Promise<T> {
  const client = new pg.Client({ connectionString: requireUrl() });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

/** Drop every schema whose name starts with `${prefix}_`. Returns the dropped names. */
export async function dropSchemasWithPrefix(prefix: string): Promise<string[]> {
  if (!/^[a-z_][a-z0-9_]{0,15}$/.test(prefix)) {
    throw new Error(`refusing to drop schemas for unsafe prefix: ${prefix}`);
  }
  return withTestClient(async (client) => {
    const { rows } = await client.query<{ nspname: string }>(
      "SELECT nspname FROM pg_namespace WHERE starts_with(nspname, $1)",
      [`${prefix}_`],
    );
    for (const { nspname } of rows) {
      await client.query(`DROP SCHEMA "${nspname.replace(/"/g, '""')}" CASCADE`);
    }
    return rows.map((r) => r.nspname);
  });
}
