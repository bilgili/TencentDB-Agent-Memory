/**
 * Bounded HTTP drain for gateway shutdown.
 */
import type http from "node:http";
import type { Logger } from "../core/types.js";

/** Default drain limit: below the 10 s `docker stop` waits before SIGKILL. */
export const DEFAULT_SHUTDOWN_DRAIN_MS = 5000;

/** Drain limit from `TDAI_SHUTDOWN_DRAIN_MS`; unset or not a non-negative integer gives the default. */
export function shutdownDrainMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.TDAI_SHUTDOWN_DRAIN_MS?.trim();
  const n = Number(raw);
  return raw && Number.isInteger(n) && n >= 0 ? n : DEFAULT_SHUTDOWN_DRAIN_MS;
}

/**
 * Stop accepting connections and wait for in-flight requests, at most `ms`.
 * After the limit, every remaining connection is closed. Resolves with the
 * number of connections closed at the limit (0 when everything drained).
 *
 * Closing a socket does not cancel its handler: a handler still running at the
 * limit keeps running, and its later store calls fail or return a fallback.
 */
export function drainHttpServer(
  server: http.Server,
  ms: number = shutdownDrainMs(),
  logger?: Pick<Logger, "warn">,
): Promise<number> {
  return new Promise((resolve) => {
    let forced = 0;
    const timer = setTimeout(() => {
      server.getConnections((_err, count) => {
        forced = count;
        logger?.warn(`HTTP drain limit of ${ms} ms reached; closing ${count} open connection(s)`);
        server.closeAllConnections();
      });
    }, ms);
    timer.unref();
    server.close(() => {
      clearTimeout(timer);
      resolve(forced);
    });
    // Idle keep-alive sockets would otherwise hold close() until the limit.
    server.closeIdleConnections();
  });
}
