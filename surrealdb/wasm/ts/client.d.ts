import type { ConnectionOptions } from "./index.js";

export type { ConnectionOptions };

export type ConnectOptions = {
	/** Where the engine's options go; see the module's `ConnectionOptions`. */
	options?: ConnectionOptions;
	/** The worker script. Defaults to `worker.js` beside this module. */
	workerUrl?: string | URL;
	/** Where the worker reads the `.wasm` from. Defaults to the loader's own location. */
	module?: string | URL;
};

/** One embedded engine, hosted in a dedicated worker. */
export declare class WorkerEngine {
	/** Runs one RPC request. */
	execute(data: Uint8Array): Promise<Uint8Array>;
	/** Runs a query, answering with its frames as they are produced. */
	queryStream(data: Uint8Array): Promise<ReadableStream<Uint8Array>>;
	/** The live-query notifications of this connection; one stream per engine. */
	notifications(): ReadableStream<Uint8Array>;
	export(config?: Uint8Array): Promise<string>;
	import(input: string): Promise<void>;
	version(): Promise<string>;
	/** Whether the browser keeps this origin's storage until the user clears it. */
	persisted(): Promise<boolean>;
	/**
	 * Closes the datastore, frees the engine and ends the worker. When this
	 * resolves the storage lock is released.
	 */
	close(): Promise<void>;
	[Symbol.asyncDispose](): Promise<void>;
}

/**
 * Starts a dedicated worker, loads the engine in it and connects to
 * `endpoint`.
 */
export declare function connect(endpoint: string, options?: ConnectOptions): Promise<WorkerEngine>;
