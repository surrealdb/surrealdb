/**
 * Smoke tests for the built `@surrealdb/wasm-native` package.
 *
 * These drive the published artefact — `dist/index.js` and the module beside it
 * — directly, without the JavaScript SDK. The SDK's engine lives in the
 * surrealdb.js repository and depends on this package, so testing through it
 * here would test that repository's code against a build of this one; the
 * `external-sdk-tests` workflow is where that pairing belongs.
 *
 * What is covered is the module's own surface: every exported entry point, the
 * CBOR-in / CBOR-out request channel, the notification stream, the
 * export/import pair, and the freed-engine contract.
 *
 * Requests are encoded with `@surrealdb/cbor` — SurrealDB's own codec, the same
 * one the SDK uses — so these exercise the wire shape a real caller sends
 * rather than a hand-rolled approximation. Run `bun run build` first.
 *
 * Only `mem://` is exercised. `indxdb://` needs IndexedDB, which Bun does not
 * provide; that backend is covered by the browser suite instead.
 */

import { expect, test } from "bun:test";
import { decode, encode } from "@surrealdb/cbor";
import init, { SurrealWasmEngine } from "./dist/index.js";

// The `web` target's default loader fetches the module relative to
// `import.meta.url`. Handing it the bytes keeps this test off that path, which
// is the consumer's to choose and not what is under test here.
await init({ module_or_path: await Bun.file("./dist/index_bg.wasm").arrayBuffer() });

/** One statement's outcome inside a `query` reply. */
type StatementResult<T> = { status: string; result: T };

let nextId = 0;

/**
 * Send one RPC request and return the decoded reply.
 *
 * A success reply is the method's value itself; only a failure is wrapped, in an
 * `{ error }` envelope.
 */
async function rpc(
	engine: SurrealWasmEngine,
	method: string,
	params: unknown[] = [],
): Promise<unknown> {
	const payload = encode({ id: ++nextId, method, params });
	const response = await engine.execute(new Uint8Array(payload));
	return decode(response);
}

/** Send one RPC request, failing the test if it reported an error. */
async function ok(
	engine: SurrealWasmEngine,
	method: string,
	params: unknown[] = [],
): Promise<unknown> {
	const reply = await rpc(engine, method, params);
	const error = (reply as { error?: unknown })?.error;
	expect(error, `${method} should not error`).toBeUndefined();
	return reply;
}

/** Send a `query` and return the statements' results. */
async function query<T>(
	engine: SurrealWasmEngine,
	sql: string,
): Promise<StatementResult<T>[]> {
	return (await ok(engine, "query", [sql])) as StatementResult<T>[];
}

/** Open an in-memory engine with a namespace and database selected. */
async function connect(
	opts?: Parameters<typeof SurrealWasmEngine.connect>[1],
): Promise<SurrealWasmEngine> {
	const engine = await SurrealWasmEngine.connect("mem://", opts);
	await ok(engine, "use", ["test", "test"]);
	return engine;
}

/** One frame of a streaming query answer. */
type StreamFrame = {
	stream: "begin" | "rows" | "value" | "finished" | "end";
	statements?: number;
	index?: number;
	values?: unknown[];
	value?: unknown;
	single?: boolean;
	results?: number;
	error?: unknown;
};

/**
 * Drain a streaming query, returning every frame with the moment it arrived.
 *
 * The timings are what distinguish streaming from a buffered answer delivered in
 * pieces: a frame that arrives before the query has finished could not have been
 * buffered.
 */
async function streamFrames(
	engine: SurrealWasmEngine,
	sql: string,
): Promise<{ frame: StreamFrame; at: number }[]> {
	const payload = encode({ id: ++nextId, method: "query_stream", params: [sql] });
	const stream = await engine.query_stream(new Uint8Array(payload));
	const reader = stream.getReader();
	const started = performance.now();
	const frames: { frame: StreamFrame; at: number }[] = [];

	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		frames.push({
			frame: decode(value as Uint8Array) as StreamFrame,
			at: performance.now() - started,
		});
	}

	return frames;
}

test("reports the engine version", async () => {
	expect(SurrealWasmEngine.version()).toStartWith("3.");

	const engine = await connect();
	expect(await ok(engine, "version")).toMatch(/^surrealdb-/);
	engine.free();
});

test("creates and selects a record", async () => {
	const engine = await connect();

	await query(engine, "CREATE person:tobie SET name = 'Tobie'");
	const [people] = await query<{ name: string }[]>(engine, "SELECT * FROM person");

	expect(people?.status).toBe("OK");
	expect(people?.result).toHaveLength(1);
	expect(people?.result[0]?.name).toBe("Tobie");

	engine.free();
});

test("delivers live query notifications", async () => {
	const engine = await connect();
	const reader = engine.notifications().getReader();

	// `live` names a table, so it has to exist before it can be watched.
	await query(engine, "DEFINE TABLE person");
	expect(await ok(engine, "live", ["person"])).toBeDefined();

	await query(engine, "CREATE person:tobie SET name = 'Tobie'");

	const { done, value } = await reader.read();
	expect(done).toBe(false);

	const notification = decode(value as Uint8Array) as { action: string };
	expect(notification.action).toBe("CREATE");

	await reader.cancel();
	engine.free();
});

test("runs a query under a configured timeout", async () => {
	// Two regressions meet here. The options were `u8`, so the documented
	// 30_000 was rejected outright; and a configured query timeout put the
	// executor on a path that spawned onto a tokio runtime, which does not
	// exist on this target — any timeout at all trapped the instance.
	const engine = await connect({ query_timeout: 30_000, transaction_timeout: 30_000 });
	const [returned] = await query<number>(engine, "RETURN 1");
	expect(returned?.result).toBe(1);
	engine.free();
});

test("creates the configured default namespace and database", async () => {
	const engine = await SurrealWasmEngine.connect("mem://", {
		defaults: { namespace: "custom_ns", database: "custom_db" },
	});

	// Selecting them succeeds only because connecting created them.
	await ok(engine, "use", ["custom_ns", "custom_db"]);
	const [info] = await query<unknown>(engine, "INFO FOR DB");
	expect(info?.status).toBe("OK");

	engine.free();
});

test("accepts every planner strategy the options type declares", async () => {
	for (const strategy of ["best-effort", "compute-only", "all-read-only"] as const) {
		const engine = await connect({ capabilities: { planner_strategy: strategy } });
		const [returned] = await query<number>(engine, "RETURN 1");
		expect(returned?.result, strategy).toBe(1);
		engine.free();
	}
});

test("rejects a malformed capability target instead of aborting", async () => {
	// The module is built with `panic = "abort"`, so this used to take the whole
	// WebAssembly instance down over a typo in a config object.
	expect(
		SurrealWasmEngine.connect("mem://", {
			capabilities: { functions: ["not-a-function-name"] },
		}),
	).rejects.toThrow(/invalid capability target/);
});

test("exports and re-imports the database", async () => {
	const engine = await connect();
	await query(engine, "CREATE person:tobie SET name = 'Tobie'");

	const sql = await engine.export();
	expect(sql).toContain("person:tobie");

	const restored = await connect();
	await restored.import(sql);
	const [people] = await query<{ name: string }[]>(restored, "SELECT * FROM person");
	expect(people?.result[0]?.name).toBe("Tobie");

	engine.free();
	restored.free();
});

test("streams a query as frames that rebuild the buffered result", async () => {
	const engine = await connect();
	await query(engine, "CREATE |person:50| SET n = 1");

	const frames = await streamFrames(engine, "SELECT n FROM person; RETURN 'done';");
	const kinds = frames.map(({ frame }) => frame.stream);

	expect(kinds[0], "the stream opens by announcing itself").toBe("begin");
	expect(frames[0]?.frame.statements).toBe(2);
	expect(kinds.at(-1), "and ends exactly once").toBe("end");
	expect(kinds.filter((kind) => kind === "end")).toHaveLength(1);
	expect(frames.at(-1)?.frame.error).toBeUndefined();
	expect(frames.at(-1)?.frame.results).toBe(2);

	// The 50 rows arrive across several frames, in order, and rebuild the array
	// the buffered `query` would have returned in one piece.
	const rowFrames = frames.filter(({ frame }) => frame.stream === "rows" && frame.index === 0);
	expect(rowFrames.length, "50 rows do not fit in one frame").toBeGreaterThan(1);
	expect(rowFrames.flatMap(({ frame }) => frame.values ?? [])).toHaveLength(50);

	// The second statement is a bare value, so it arrives whole and says so.
	const finished = frames.find(({ frame }) => frame.stream === "finished" && frame.index === 1);
	expect(finished?.frame.single).toBe(true);

	engine.free();
});

test("rows reach JavaScript before the query has finished", async () => {
	const engine = await connect();
	await query(engine, "CREATE |person:50| SET n = 1");

	// The sleep runs after the SELECT, so a buffered answer could not produce a
	// single row until it was over. Streaming delivers them first.
	const frames = await streamFrames(engine, "SELECT n FROM person; SLEEP 2s;");
	const firstRows = frames.find(({ frame }) => frame.stream === "rows");
	const total = frames.at(-1)?.at ?? 0;

	expect(total, "the query really did take the sleep").toBeGreaterThan(2000);
	expect(
		firstRows?.at,
		`the first rows waited for the sleep (${firstRows?.at}ms of ${total}ms)`,
	).toBeLessThan(1000);

	engine.free();
});

test("a query that cannot parse arrives as a terminal frame", async () => {
	const engine = await connect();

	// The failure belongs to no statement, and rejecting across the boundary
	// would flatten it to a string — so it comes back structured, on the one
	// frame an unopened stream produces.
	const frames = await streamFrames(engine, "SELECT * FROM;");

	expect(frames).toHaveLength(1);
	expect(frames[0]?.frame.stream).toBe("end");
	expect(frames[0]?.frame.results).toBe(0);

	const error = frames[0]?.frame.error as { code: number; message: string; kind?: string };
	expect(error?.message).toContain("Parse error");
	expect(error?.kind).toBe("Validation");
	expect(error?.code).toBe(-32000);

	engine.free();
});

test("abandoning a stream mid-flight leaves the engine usable", async () => {
	const engine = await connect();
	await query(engine, "CREATE |person:200| SET n = 1");

	// Cancel the reader while the execution is still running. The driver owns it
	// and has to finish it anyway: it holds an open transaction.
	const payload = encode({
		id: ++nextId,
		method: "query_stream",
		params: ["SELECT n FROM person; SLEEP 5s;"],
	});
	const stream = await engine.query_stream(new Uint8Array(payload));
	const reader = stream.getReader();
	expect((await reader.read()).done).toBe(false);
	await reader.cancel();

	const [one] = await query<number>(engine, "RETURN 1");
	expect(one?.result).toBe(1);

	engine.free();
});

test("cancelling a stream stops an execution the consumer has left behind", async () => {
	const engine = await connect();
	await query(engine, "CREATE |person:200| SET n = 1");

	// The sleep parks the execution with the `CREATE` still ahead of it, which is
	// what makes the stop observable. Without a pause the executor runs ahead of
	// the consumer — it is buffered, not lock-step — so a write that close to the
	// front of the query has already happened by the time a first frame arrives.
	const payload = encode({
		id: ++nextId,
		method: "query_stream",
		params: ["SELECT n FROM person; SLEEP 1s; CREATE person:late SET n = 1;"],
	});
	const stream = await engine.query_stream(new Uint8Array(payload));
	const reader = stream.getReader();
	expect((await reader.read()).done).toBe(false);
	await reader.cancel();

	// Past the sleep the abandoned execution would have waited out.
	await new Promise((resolve) => setTimeout(resolve, 2000));

	const [late] = await query<unknown[]>(engine, "SELECT * FROM person:late");
	expect(late?.result, "the statement past the sleep never ran").toEqual([]);

	engine.free();
});
