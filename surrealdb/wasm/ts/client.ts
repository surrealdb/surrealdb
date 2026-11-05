/**
 * The page's side of the worker bridge: starts the dedicated worker that
 * hosts the engine and presents it with the engine's own surface.
 *
 * ```ts
 * import { connect } from "@surrealdb/wasm-native/worker";
 *
 * const engine = await connect("surrealkv://my-database");
 * const reply = await engine.execute(request);
 * await engine.close(); // before the page ends: releases the storage lock
 * ```
 *
 * A `ReadableStream` result is transferred rather than copied, so a reader
 * that stops reading stops the engine's producer. The page, not the worker,
 * asks for persistent storage: `navigator.storage.persist()` is unavailable
 * in a worker.
 */

import type { ConnectionOptions } from "../dist/index.js";
import type { Reply, Request } from "./protocol.ts";

export type { ConnectionOptions };

export type ConnectOptions = {
	/** Where the engine's options go; see the module's `ConnectionOptions`. */
	options?: ConnectionOptions;
	/** The worker script. Defaults to `worker.js` beside this module. */
	workerUrl?: string | URL;
	/** Where the worker reads the `.wasm` from. Defaults to the loader's own location. */
	module?: string | URL;
};

/** A `Request` without its id, which the client assigns. */
type Call = Request extends infer R ? (R extends Request ? Omit<R, "id"> : never) : never;

/** One embedded engine, hosted in a dedicated worker. */
export class WorkerEngine {
	#worker: Worker;
	#next = 0;
	#pending = new Map<number, { resolve: (value: unknown) => void; reject: (e: Error) => void }>();
	#notifications: ReadableStream<Uint8Array> | undefined;
	#closed = false;
	#closing: Promise<void> | undefined;

	constructor(worker: Worker) {
		this.#worker = worker;
		worker.onmessage = (event: MessageEvent<Reply>) => {
			const reply = event.data;
			const pending = this.#pending.get(reply.id);
			if (!pending) return;
			this.#pending.delete(reply.id);
			if (reply.ok) pending.resolve(reply.value);
			else pending.reject(new Error(reply.error));
		};
		worker.onerror = (event) => this.#fail(new Error(event.message || "The worker failed"));
		worker.onmessageerror = () => this.#fail(new Error("The worker sent an unreadable message"));
	}

	/**
	 * Ends the engine and rejects every request still waiting. The worker is
	 * terminated and later calls fail at once, since a lost worker answers
	 * nothing and a request posted to it would wait forever.
	 */
	#fail(error: Error): void {
		this.#closed = true;
		this.#worker.terminate();
		for (const pending of this.#pending.values()) pending.reject(error);
		this.#pending.clear();
	}

	/** Sends one request and waits for its reply. */
	call<T = unknown>(request: Call, transfer: Transferable[] = []): Promise<T> {
		if (this.#closed) return Promise.reject(new Error("The engine is closed"));
		return new Promise<T>((resolve, reject) => {
			const id = ++this.#next;
			this.#pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
			this.#worker.postMessage({ ...request, id }, transfer);
		});
	}

	/** Runs one RPC request. */
	execute(data: Uint8Array): Promise<Uint8Array> {
		return this.call<Uint8Array>({ op: "execute", data });
	}

	/** Runs a query, answering with its frames as they are produced. */
	queryStream(data: Uint8Array): Promise<ReadableStream<Uint8Array>> {
		return this.call<ReadableStream<Uint8Array>>({ op: "queryStream", data });
	}

	/**
	 * The live-query notifications of this connection. There is one stream per
	 * engine: a second call returns the same stream, which is locked while a
	 * reader holds it.
	 */
	notifications(): ReadableStream<Uint8Array> {
		if (!this.#notifications) throw new Error("The engine is not connected");
		return this.#notifications;
	}

	export(config?: Uint8Array): Promise<string> {
		return this.call<string>({ op: "export", config });
	}

	import(input: string): Promise<void> {
		return this.call<void>({ op: "import", input });
	}

	version(): Promise<string> {
		return this.call<string>({ op: "version" });
	}

	/**
	 * Whether the browser keeps this origin's storage until the user clears
	 * it. `false` until the page has asked with `navigator.storage.persist()`.
	 */
	persisted(): Promise<boolean> {
		return this.call<boolean>({ op: "persisted" });
	}

	/**
	 * Closes the datastore, frees the engine and ends the worker. When this
	 * resolves the storage lock is released, so another tab can open the
	 * database. Call it as early as the application knows it is ending: a page
	 * cannot await it from an unload handler.
	 */
	close(): Promise<void> {
		// Every caller waits on the one close in flight: a second close that
		// returned early would let its caller end the worker mid-shutdown.
		this.#closing ??= this.#close();
		return this.#closing;
	}

	async #close(): Promise<void> {
		if (this.#closed) return;
		try {
			await this.call<void>({ op: "close" });
		} finally {
			this.#fail(new Error("The engine is closed"));
		}
	}

	[Symbol.asyncDispose](): Promise<void> {
		return this.close();
	}

	/** @internal Takes the notification stream handed over at connect. */
	_setNotifications(stream: ReadableStream<Uint8Array>): void {
		this.#notifications = stream;
	}
}

/**
 * Starts a dedicated worker, loads the engine in it and connects to
 * `endpoint`. A `surrealkv://` endpoint that is already open in another tab
 * rejects with a message saying so.
 */
export async function connect(
	endpoint: string,
	{ options, workerUrl, module }: ConnectOptions = {},
): Promise<WorkerEngine> {
	const worker = new Worker(workerUrl ?? new URL("./worker.js", import.meta.url), {
		type: "module",
	});
	const engine = new WorkerEngine(worker);
	try {
		await engine.call({ op: "init", module: module === undefined ? undefined : String(module) });
		await engine.call({ op: "connect", endpoint, options });
		engine._setNotifications(await engine.call<ReadableStream<Uint8Array>>({ op: "notifications" }));
		return engine;
	} catch (error) {
		await engine.close().catch(() => {});
		throw error;
	}
}
