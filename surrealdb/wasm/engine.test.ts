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
