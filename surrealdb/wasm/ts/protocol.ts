/**
 * The messages between a page and the dedicated worker that hosts the engine.
 *
 * Every request carries an `id` that its single reply repeats. Payloads are
 * the engine's own: CBOR bytes in and out, and `ReadableStream`s that travel
 * by transfer, so a stream's backpressure crosses the worker boundary
 * unchanged.
 */

import type { ConnectionOptions } from "../dist/index.js";

export type Request =
	| { id: number; op: "init"; module?: string }
	| { id: number; op: "connect"; endpoint: string; options?: ConnectionOptions }
	| { id: number; op: "execute"; data: Uint8Array }
	| { id: number; op: "queryStream"; data: Uint8Array }
	| { id: number; op: "notifications" }
	| { id: number; op: "export"; config?: Uint8Array }
	| { id: number; op: "import"; input: string }
	| { id: number; op: "version" }
	| { id: number; op: "persisted" }
	| { id: number; op: "close" };

export type Reply =
	| { id: number; ok: true; value?: unknown }
	| { id: number; ok: false; error: string };
