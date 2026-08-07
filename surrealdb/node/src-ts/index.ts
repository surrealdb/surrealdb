import type { Engines } from "surrealdb";
import type { ConnectionOptions } from "../napi";
import { NodeEngine } from "./engine";

/**
 * Configure the `mem`, `rocksdb`, `surrealkv`, and `surrealkv+versioned` Nodejs engines for the JavaScript SDK.
 *
 * While this package is called `@surrealdb/node`, it is also compatible with Bun and Deno.
 *
 * @param options Optional connection options to configure the Nodejs engines.
 * @example
 * ```ts
 * import { Surreal, createRemoteEngines } from "surrealdb";
 * import { createNodeEngines } from "@surrealdb/node";
 *
 * const db = new Surreal({
 *     engines: {
 *         ...createRemoteEngines(),
 *         ...createNodeEngines(),
 *     },
 * });
 * ```
 */
export const createNodeEngines = (options?: ConnectionOptions): Engines => ({
    mem: (ctx) => new NodeEngine(ctx, options),
    rocksdb: (ctx) => new NodeEngine(ctx, options),
    surrealkv: (ctx) => new NodeEngine(ctx, options),
    "surrealkv+versioned": (ctx) => new NodeEngine(ctx, options),
});

export * from "./engine";
