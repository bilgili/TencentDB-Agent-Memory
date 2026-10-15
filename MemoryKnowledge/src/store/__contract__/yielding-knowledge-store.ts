/**
 * Test-only YieldingKnowledgeStore decorator.
 *
 * A plain SQLite store resolves each call at call time, so between two awaits
 * only microtasks run and other requests cannot interleave. This decorator
 * awaits `setImmediate` before and after every store call, so concurrent
 * service calls interleave at every await — the way a pooled Postgres store does.
 *
 * `overrides` replace single methods (fault or delay injection); they are wrapped too.
 */

import { setImmediate as tick } from "node:timers/promises";

import type { IKnowledgeStore } from "../types.js";

export function createYieldingKnowledgeStore(
  inner: IKnowledgeStore,
  overrides: Partial<IKnowledgeStore> = {},
): IKnowledgeStore {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      const own = (overrides as Record<PropertyKey, unknown>)[prop];
      const value: unknown = own ?? Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      const fn = value as (...args: unknown[]) => Promise<unknown>;
      return async (...args: unknown[]): Promise<unknown> => {
        await tick();
        const result = await fn.apply(own ? overrides : target, args);
        await tick();
        return result;
      };
    },
  });
}
