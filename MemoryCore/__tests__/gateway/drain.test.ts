import http from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_SHUTDOWN_DRAIN_MS, drainHttpServer, shutdownDrainMs } from "../../src/gateway/drain.js";

async function listen(handler: http.RequestListener): Promise<{ server: http.Server; port: number }> {
  const server = http.createServer(handler);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  return { server, port: (server.address() as AddressInfo).port };
}

/** Send a request and resolve once the handler has received it. */
function request(port: number, entered: Promise<void>): Promise<void> {
  const req = http.get({ port, host: "127.0.0.1", path: "/" });
  req.on("error", () => {}); // the hung request is cut at the drain limit
  req.on("response", (res) => res.resume());
  return entered;
}

describe("drainHttpServer", () => {
  it("closes a hung request at the limit and resolves with the number of closed connections", async () => {
    let enter!: () => void;
    const entered = new Promise<void>((r) => (enter = r));
    const { server, port } = await listen(() => enter()); // never responds
    await request(port, entered);

    const logger = { warn: vi.fn() };
    const started = Date.now();
    const forced = await drainHttpServer(server, 200, logger);

    expect(forced).toBe(1);
    expect(Date.now() - started).toBeGreaterThanOrEqual(150);
    expect(Date.now() - started).toBeLessThan(5000);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining("closing 1 open connection"));
    expect(server.listening).toBe(false);
  });

  it("waits for an in-flight request that finishes within the limit and closes nothing", async () => {
    let enter!: () => void;
    const entered = new Promise<void>((r) => (enter = r));
    const { server, port } = await listen((_req, res) => {
      enter();
      setTimeout(() => res.end("done"), 100);
    });
    await request(port, entered);

    const logger = { warn: vi.fn() };
    expect(await drainHttpServer(server, 10_000, logger)).toBe(0);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it("reads the limit from TDAI_SHUTDOWN_DRAIN_MS, default 5000", () => {
    expect(DEFAULT_SHUTDOWN_DRAIN_MS).toBe(5000);
    expect(shutdownDrainMs({})).toBe(5000);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "" })).toBe(5000);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "abc" })).toBe(5000);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "-1" })).toBe(5000);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "1.5" })).toBe(5000);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "1500" })).toBe(1500);
    expect(shutdownDrainMs({ TDAI_SHUTDOWN_DRAIN_MS: "0" })).toBe(0);
  });
});
