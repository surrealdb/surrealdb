/**
 * The dedicated worker that hosts one `SurrealWasmEngine`.
 *
 * A `surrealkv://` database lives in the origin private file system, whose
 * synchronous file handles only a dedicated worker has, so the engine runs
 * here and the page talks to it by message (see `client.ts`).
 *
 * The engine is loaded from `./index.js` beside this file, the loader that
 * wasm-bindgen emits, so the worker can be started from the URL it is
 * published at without a bundler's help.
 */

import type { SurrealWasmEngine } from "../dist/index.js";
import type { Reply, Request } from "./protocol.ts";

type Wasm = typeof import("../dist/index.js");

let wasm: Wasm | undefined;
let engine: SurrealWasmEngine | undefined;

/**
 * The requests that use the engine and have not settled. Each holds the
 * engine's shared borrow, so the engine is freed only once they have.
 */
const inFlight = new Set<Promise<unknown>>();

/** Runs `work` as a request that uses the engine. */
function tracked<T>(work: Promise<T>): Promise<T> {
	inFlight.add(work);
	const done = () => inFlight.delete(work);
	work.then(done, done);
	return work;
}

function loaded(): Wasm {
	if (!wasm) throw new Error("The engine module has not been initialised");
	return wasm;
}

function connected(): SurrealWasmEngine {
	if (!engine) throw new Error("The engine is not connected");
	return engine;
}

/** A thrown value as the text the page reports, since an error does not clone. */
function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/** Runs one request. A stream in the result is returned for transfer. */
async function handle(request: Request): Promise<{ value?: unknown; transfer?: Transferable[] }> {
	switch (request.op) {
		case "init": {
			wasm = (await import(new URL("./index.js", import.meta.url).href)) as Wasm;
			await wasm.default(request.module ? { module_or_path: request.module } : undefined);
			return {};
		}
		case "connect": {
			if (engine) throw new Error("The worker already hosts an engine");
			engine = await loaded().SurrealWasmEngine.connect(request.endpoint, request.options);
			return {};
		}
		case "execute": {
			const value = await tracked(connected().execute(request.data));
			return { value, transfer: [value.buffer] };
		}
		case "queryStream": {
			const value = await tracked(connected().query_stream(request.data));
			return { value, transfer: [value] };
		}
		case "notifications": {
			const value = connected().notifications();
			return { value, transfer: [value] };
		}
		case "export":
			return { value: await tracked(connected().export(request.config)) };
		case "import":
			await tracked(connected().import(request.input));
			return {};
		case "version":
			return { value: loaded().SurrealWasmEngine.version() };
		case "persisted":
			return { value: await loaded().SurrealWasmEngine.persisted() };
		case "close": {
			// The datastore is closed before the handle is freed so that the
			// storage lock is released by the time the page hears back.
			const closing = engine;
			engine = undefined;
			if (closing) {
				await closing.close();
				// A request that was running when the close began finishes
				// against the closed datastore, and the engine is freed after it.
				await Promise.allSettled([...inFlight]);
				closing.free();
			}
			return {};
		}
	}
}

/**
 * Whether this script is running as a dedicated worker. Only then are the
 * messages it receives limited to the page that started it; as a page script,
 * `onmessage` would also hear any window that can post to it.
 */
const dedicated = (() => {
	const scope = (globalThis as { DedicatedWorkerGlobalScope?: new () => unknown })
		.DedicatedWorkerGlobalScope;
	return scope !== undefined && self instanceof scope;
})();

self.onmessage = async (event: MessageEvent<Request>) => {
	// A dedicated worker has no other sender than its owner, and the messages
	// of its owner carry an empty origin: refuse to serve anywhere else, and
	// any message that names a sender's origin.
	if (!dedicated || event.origin !== "") return;
	const request = event.data;
	try {
		const { value, transfer } = await handle(request);
		const reply: Reply = { id: request.id, ok: true, value };
		(self as unknown as Worker).postMessage(reply, transfer ?? []);
	} catch (error) {
		const reply: Reply = { id: request.id, ok: false, error: describe(error) };
		(self as unknown as Worker).postMessage(reply);
	}
};
