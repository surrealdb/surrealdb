// WebSocket / JSON-RPC wire conformance — ported from the Rust suite
// `tests/ws_integration.rs`. These drive the server over the
// raw `json` subprotocol via RpcClient so we can pin the exact JSON-RPC
// envelope shapes, error codes, and live-notification frames that the
// high-level SDK wraps or hides.
//
// Each test spawns its own in-memory server and uses a unique namespace/
// database defined over the same raw connection.
//
// NOT PORTED (and why):
// - session_id_defined / _generic / _both / _invalid / _undefined,
//   session_id via `surreal-id` / `x-request-id` connection headers: the
//   surrealdb.js WebSocket driver — and the harness RpcClient built on the
//   browser `WebSocket` — cannot set arbitrary connection request headers, so
//   the handshake-header behavior is unobservable from JS.
// - detach_connection_session_rejected, websocket_attach_session_cap,
//   multi_session_* : per-connection `attach`/`detach`/`sessions` multiplexing
//   is exercised through the SDK in sessions-multiplex.test.ts.
// - live_query_diff (LIVE SELECT DIFF): text-diff patch payloads are covered
//   indirectly; the raw envelope shape is stable but noisy to pin here.
// - temporary_directory / concurrency: not wire-protocol behavior.
import { afterEach, beforeEach, expect, test } from "bun:test";
import { RpcClient, startServer, type TestServer } from "../src/harness";

// Every test gets its own fresh auth-enabled in-memory server (beforeEach).
// Capability tests that need custom CLI flags start their own dedicated
// server inside the test instead.
let server: TestServer;

beforeEach(async () => {
	server = await startServer();
});

afterEach(async () => {
	await server.stop();
});

let counter = 0;

/** A root-authenticated raw RPC client on a fresh, unique namespace/database. */
async function rootRpc(
	srv: TestServer = server,
): Promise<{ rpc: RpcClient; ns: string; db: string }> {
	const rpc = await RpcClient.connect(srv);
	// signin returns the JWT token for the root system user.
	await rpc.call("signin", [{ user: "root", pass: "root" }]);
	const ns = `ws_ns_${process.pid}_${++counter}`;
	const db = `ws_db_${counter}`;
	// USE selects but does not create; define the ns/db explicitly (as root).
	await rpc.call("query", [`DEFINE NAMESPACE \`${ns}\``]);
	await rpc.call("use", [ns, null]);
	await rpc.call("query", [`DEFINE DATABASE \`${db}\``]);
	await rpc.call("use", [ns, db]);
	return { rpc, ns, db };
}

// Per-statement envelope inside a `query` result array.
interface StmtEnvelope {
	result: unknown;
	status: "OK" | "ERR";
	time: string;
	type: null;
}

// A record-access method whose SIGNUP/SIGNIN key off email+password.
const EMAIL_ACCESS = (session: string, token: string) => `
	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION ${session}, FOR TOKEN ${token};`;

// ---------------------------------------------------------------------------
// Connection-level RPCs (ping / version / signin / signup / invalidate /
// authenticate) — from ws_integration.rs: ping, version, signup, signin,
// invalidate, authenticate.
// ---------------------------------------------------------------------------

test("ping RPC returns an { id, result: null } envelope and needs no auth", async () => {
	const rpc = await RpcClient.connect(server);
	const res = await rpc.rpc("ping", []);
	expect(res.error).toBeUndefined();
	// Ping maps to DbResult::Other(None) → result is JSON null.
	expect(res.result).toBeNull();
	// Envelope carries only id + result (no error).
	expect(Object.keys(res).sort()).toEqual(["id", "result"]);
	await rpc.close();
});

test("version RPC returns the surrealdb-<semver> string without authentication", async () => {
	const rpc = await RpcClient.connect(server);
	const res = await rpc.rpc("version", []);
	expect(res.error).toBeUndefined();
	expect(typeof res.result).toBe("string");
	expect(String(res.result)).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	await rpc.close();
});

test("signup then signin over a record access method each return an HS512 JWT", async () => {
	const { rpc, ns, db } = await rootRpc();
	await rpc.call("query", [EMAIL_ACCESS("24h", "24h")]);

	const signupTok = await rpc.call("signup", [
		{ ns, db, ac: "user", email: "email@email.com", pass: "pass" },
	]);
	expect(typeof signupTok).toBe("string");
	// The JWT header {"typ":"JWT","alg":"HS512"} base64url-encodes to this prefix.
	expect(String(signupTok)).toMatch(/^eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9\./);

	const signinTok = await rpc.call("signin", [
		{ ns, db, ac: "user", email: "email@email.com", pass: "pass" },
	]);
	expect(typeof signinTok).toBe("string");
	expect(String(signinTok)).toMatch(/^eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9\./);
	await rpc.close();
});

test("invalidate RPC drops the authenticated session; a privileged query then fails at the RPC level", async () => {
	const rpc = await RpcClient.connect(server);
	await rpc.call("signin", [{ user: "root", pass: "root" }]);
	// Authenticated: a root-only statement succeeds.
	const before = (await rpc.call("query", ["DEFINE NAMESPACE inv_ns"])) as StmtEnvelope[];
	expect(before[0].status).toBe("OK");

	await rpc.call("invalidate", []);

	// After invalidate the session is anonymous; the privileged statement is
	// rejected as a whole-request RPC error (not a per-statement ERR).
	const after = await rpc.rpc("query", ["DEFINE NAMESPACE inv_ns_two"]);
	expect(after.error?.message).toBe(
		"Anonymous access not allowed: Not enough permissions to perform this action",
	);
	await rpc.close();
});

test("authenticate RPC restores a session on a brand-new connection from a stored token", async () => {
	const rpc1 = await RpcClient.connect(server);
	const token = await rpc1.call("signin", [{ user: "root", pass: "root" }]);
	await rpc1.close();

	// A fresh, unauthenticated connection adopts the token via authenticate.
	const rpc2 = await RpcClient.connect(server);
	await rpc2.call("authenticate", [token]);
	const res = (await rpc2.call("query", ["DEFINE NAMESPACE reauth_ns"])) as StmtEnvelope[];
	expect(res[0].status).toBe("OK");
	await rpc2.close();
});

// ---------------------------------------------------------------------------
// Session parameters — from ws_integration.rs: letset, unset.
// ---------------------------------------------------------------------------

test("let and set RPCs both bind a session variable visible to later queries", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("let", ["let_var", "let_value"]);
	await rpc.call("set", ["set_var", "set_value"]);
	const res = (await rpc.call("query", ["SELECT * FROM $let_var, $set_var"])) as StmtEnvelope[];
	expect(res[0].result).toEqual(["let_value", "set_value"]);
	await rpc.close();
});

test("unset RPC clears a previously let variable (it reads back as null)", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("let", ["let_var", "let_value"]);
	const set = (await rpc.call("query", ["RETURN $let_var"])) as StmtEnvelope[];
	expect(set[0].result).toBe("let_value");

	await rpc.call("unset", ["let_var"]);
	const cleared = (await rpc.call("query", ["RETURN $let_var"])) as StmtEnvelope[];
	expect(cleared[0].result).toBeNull();
	await rpc.close();
});

// ---------------------------------------------------------------------------
// CRUD verb envelope shapes — from ws_integration.rs: select, insert, create,
// update, merge, patch, delete, query. The key contrast being pinned: create
// and record-targeted verbs return a single object, while table-targeted
// select/insert/update/merge/delete return arrays.
// ---------------------------------------------------------------------------

test("select RPC over a table returns an array of matching records", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE tester SET name = 'foo', value = 'bar'"]);
	const res = await rpc.rpc("select", ["tester"]);
	expect(Array.isArray(res.result)).toBe(true);
	const rows = res.result as Array<{ name: string; value: string }>;
	expect(rows).toHaveLength(1);
	expect(rows[0].name).toBe("foo");
	expect(rows[0].value).toBe("bar");
	await rpc.close();
});

test("create RPC on a table returns the single created record (not an array)", async () => {
	const { rpc } = await rootRpc();
	const res = await rpc.rpc("create", ["tester", { value: "bar" }]);
	expect(Array.isArray(res.result)).toBe(false);
	const rec = res.result as { id: string; value: string };
	expect(rec.value).toBe("bar");
	expect(rec.id).toMatch(/^tester:/);
	// And exactly one row now exists.
	const q = (await rpc.call("query", ["SELECT * FROM tester"])) as StmtEnvelope[];
	expect(q[0].result as unknown[]).toHaveLength(1);
	await rpc.close();
});

test("insert RPC returns an array: one element for one object, N for a batch", async () => {
	const { rpc } = await rootRpc();
	const single = await rpc.rpc("insert", ["tester", { name: "foo", value: "bar" }]);
	expect(Array.isArray(single.result)).toBe(true);
	expect(single.result as unknown[]).toHaveLength(1);

	const batch = await rpc.rpc("insert", [
		"tester",
		[
			{ name: "foo", value: "bar" },
			{ name: "foo", value: "bar" },
		],
	]);
	expect(Array.isArray(batch.result)).toBe(true);
	expect(batch.result as unknown[]).toHaveLength(2);

	// Three records total (1 + 2).
	const q = (await rpc.call("query", ["SELECT * FROM tester"])) as StmtEnvelope[];
	expect(q[0].result as unknown[]).toHaveLength(3);
	await rpc.close();
});

test("update RPC replaces record content (previous fields are dropped)", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE tester SET name = 'foo'"]);
	const res = await rpc.rpc("update", ["tester", { value: "bar" }]);
	expect(Array.isArray(res.result)).toBe(true);
	const rows = res.result as Array<Record<string, unknown>>;
	expect(rows).toHaveLength(1);
	expect(rows[0].value).toBe("bar");

	const q = (await rpc.call("query", ["SELECT * FROM tester"])) as StmtEnvelope[];
	const row = (q[0].result as Array<Record<string, unknown>>)[0];
	// `name` was replaced away — absent from the raw JSON payload.
	expect(row.name).toBeUndefined();
	expect(row.value).toBe("bar");
	await rpc.close();
});

test("merge RPC preserves existing fields and adds the merged ones", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE tester SET name = 'foo'"]);
	const res = await rpc.rpc("merge", ["tester", { value: "bar" }]);
	expect(Array.isArray(res.result)).toBe(true);
	const rows = res.result as Array<{ name: string; value: string }>;
	expect(rows).toHaveLength(1);
	expect(rows[0].name).toBe("foo");
	expect(rows[0].value).toBe("bar");
	await rpc.close();
});

test("patch RPC applies a JSON-patch to a record and returns the single patched object", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE tester:id SET name = 'foo'"]);
	const res = await rpc.rpc("patch", [
		"tester:id",
		[
			{ op: "add", path: "value", value: "bar" },
			{ op: "remove", path: "name" },
		],
	]);
	// A record-targeted patch returns a single object.
	expect(Array.isArray(res.result)).toBe(false);
	expect((res.result as { value: string }).value).toBe("bar");

	const q = (await rpc.call("query", ["SELECT * FROM tester"])) as StmtEnvelope[];
	const row = (q[0].result as Array<Record<string, unknown>>)[0];
	expect(row.name).toBeUndefined();
	expect(row.value).toBe("bar");
	await rpc.close();
});

test("delete RPC returns an array for a table target and a single object for a record target", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE tester:id"]);
	const table = await rpc.rpc("delete", ["tester"]);
	expect(Array.isArray(table.result)).toBe(true);
	const rows = table.result as Array<{ id: string }>;
	expect(rows).toHaveLength(1);
	expect(rows[0].id).toBe("tester:id");

	await rpc.call("query", ["CREATE tester:id"]);
	const record = await rpc.rpc("delete", ["tester:id"]);
	expect(Array.isArray(record.result)).toBe(false);
	expect((record.result as { id: string }).id).toBe("tester:id");

	// Table is empty afterwards.
	const q = (await rpc.call("query", ["SELECT * FROM tester"])) as StmtEnvelope[];
	expect(q[0].result as unknown[]).toHaveLength(0);
	await rpc.close();
});

test("query RPC returns one per-statement envelope per statement", async () => {
	const { rpc } = await rootRpc();
	const res = await rpc.rpc("query", ["CREATE tester; SELECT * FROM tester;"]);
	expect(Array.isArray(res.result)).toBe(true);
	const stmts = res.result as StmtEnvelope[];
	expect(stmts).toHaveLength(2);
	expect(stmts[0].status).toBe("OK");
	expect(stmts[1].status).toBe("OK");
	await rpc.close();
});

// ---------------------------------------------------------------------------
// run RPC — from ws_integration.rs: run_functions. Custom fn:: functions and
// builtins, with arity/type errors surfaced as RPC errors. `run` params are
// [name, version|null, args[]].
// ---------------------------------------------------------------------------

test("run RPC invokes custom fn:: functions and builtins; wrong arity/type is an RPC error", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE FUNCTION fn::foo() {RETURN 'fn::foo called';}"]);
	await rpc.call("query", [
		"DEFINE FUNCTION fn::bar($val: string) {RETURN 'fn::bar called with: ' + $val;}",
	]);

	// Zero-arg custom function.
	expect(await rpc.call("run", ["fn::foo", null, []])).toBe("fn::foo called");

	// fn::bar requires exactly one string arg.
	expect((await rpc.rpc("run", ["fn::bar", null, []])).error).toBeDefined(); // too few
	expect((await rpc.rpc("run", ["fn::bar", null, [42]])).error).toBeDefined(); // wrong type
	expect((await rpc.rpc("run", ["fn::bar", null, ["a", "b"]])).error).toBeDefined(); // too many
	expect(await rpc.call("run", ["fn::bar", null, ["string_val"]])).toBe(
		"fn::bar called with: string_val",
	);

	// Builtin functions.
	expect(await rpc.call("run", ["math::abs", null, [42]])).toBe(42);
	expect(await rpc.call("run", ["math::max", null, [[1, 2, 3, 4, 5, 6]]])).toBe(6);
	await rpc.close();
});

// ---------------------------------------------------------------------------
// relate RPC — from ws_integration.rs: relate_rpc. relate params are
// [from, edge, to, content?]; graph traversal reads the edge back.
// ---------------------------------------------------------------------------

test("relate RPC creates an edge with content that graph traversal can read back", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE foo:a, foo:b"]);
	const rel = await rpc.rpc("relate", ["foo:a", "bar", "foo:b", { val: 42 }]);
	expect(rel.error).toBeUndefined();
	// relate returns the single created edge record.
	const edge = rel.result as { id: string; in: string; out: string; val: number };
	expect(edge.in).toBe("foo:a");
	expect(edge.out).toBe("foo:b");
	expect(edge.val).toBe(42);

	// Traverse the edge property and the edge target.
	const val = (await rpc.call("query", ["RETURN foo:a->bar.val"])) as StmtEnvelope[];
	expect(val[0].result).toEqual([42]);
	const to = (await rpc.call("query", ["RETURN foo:a->bar->foo"])) as StmtEnvelope[];
	expect(to[0].result).toEqual(["foo:b"]);
	await rpc.close();
});

// ---------------------------------------------------------------------------
// info RPC — from ws_integration.rs: info. A signed-in record user's info()
// returns that user's own document.
// ---------------------------------------------------------------------------

test("info RPC returns the signed-in record user's own document", async () => {
	const { rpc, ns, db } = await rootRpc();
	await rpc.call("query", ["DEFINE TABLE user PERMISSIONS FULL"]);
	await rpc.call("query", [
		`DEFINE ACCESS user ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET user = $user, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM user WHERE user = $user AND crypto::argon2::compare(pass, $pass) )
			DURATION FOR SESSION 24h;`,
	]);
	await rpc.call("query", [
		"CREATE user CONTENT { user: 'user', pass: crypto::argon2::generate('pass') };",
	]);
	// Sign in as the record user (user/pass ride as $user/$pass variables).
	await rpc.call("signin", [{ user: "user", pass: "pass", ns, db, ac: "user" }]);

	const info = (await rpc.call("info", [])) as { id: string; user: string; pass: string };
	expect(info.user).toBe("user");
	expect(String(info.id)).toMatch(/^user:/);
	expect(info.pass).toBeDefined();
	await rpc.close();
});

// ---------------------------------------------------------------------------
// Session re-authentication — from ws_integration.rs: session_reauthentication.
// authenticate() swaps the whole auth principal in place on one connection.
// ---------------------------------------------------------------------------

test("authenticate RPC swaps the session's auth principal between root and a record user", async () => {
	const { rpc, ns, db } = await rootRpc();
	const rootToken = await rpc.call("signin", [{ user: "root", pass: "root" }]);
	// Root can read INFO FOR ROOT.
	const asRoot = (await rpc.call("query", ["INFO FOR ROOT"])) as StmtEnvelope[];
	expect(asRoot[0].status).toBe("OK");

	await rpc.call("query", [EMAIL_ACCESS("24h", "24h")]);
	await rpc.call("query", [
		`DEFINE TABLE test SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE $access = "user"`,
	]);
	await rpc.call("query", [`CREATE test:1 SET working = "yes"`]);
	const recordToken = await rpc.call("signup", [
		{ ns, db, ac: "user", email: "email@email.com", pass: "pass" },
	]);

	// Become the record user: INFO FOR ROOT is now denied, but the
	// access-gated table is readable.
	await rpc.call("authenticate", [recordToken]);
	const denied = (await rpc.call("query", ["INFO FOR ROOT"])) as StmtEnvelope[];
	expect(denied[0].status).toBe("ERR");
	expect(String(denied[0].result)).toBe(
		"IAM error: Not enough permissions to perform this action",
	);
	const gated = (await rpc.call("query", ["SELECT VALUE working FROM test:1"])) as StmtEnvelope[];
	expect(gated[0].result).toEqual(["yes"]);

	// Re-authenticate back to root; root access is restored.
	await rpc.call("authenticate", [rootToken]);
	const backToRoot = (await rpc.call("query", ["INFO FOR ROOT"])) as StmtEnvelope[];
	expect(backToRoot[0].status).toBe("OK");
	await rpc.close();
});

// ---------------------------------------------------------------------------
// Session expiration — from ws_integration.rs: session_expiration_operations.
// The full per-method matrix on an expired record session.
// ---------------------------------------------------------------------------

test(
	"an expired record session fails data/auth-carrying methods with -32000 while connection methods still work",
	async () => {
		const { rpc, ns, db } = await rootRpc();
		const rootToken = await rpc.call("signin", [{ user: "root", pass: "root" }]);
		// SESSION 1s so the record session expires almost immediately; TOKEN 1d
		// so the token itself stays valid (only the session lapses).
		await rpc.call("query", [EMAIL_ACCESS("1s", "1d")]);
		await rpc.call("query", [
			`DEFINE TABLE test SCHEMALESS PERMISSIONS FOR select, create, update, delete WHERE $access = "user"`,
		]);
		await rpc.call("query", [`CREATE test:1 SET working = "yes"`]);

		const token = await rpc.call("signup", [
			{ ns, db, ac: "user", email: "email@email.com", pass: "pass" },
		]);
		await rpc.call("authenticate", [token]);
		// Immediately authenticated: the gated read works.
		const live = (await rpc.call("query", ["SELECT VALUE working FROM test:1"])) as StmtEnvelope[];
		expect(live[0].result).toEqual(["yes"]);

		// Let the 1s session lapse.
		await Bun.sleep(2000);
		const expired = await rpc.rpc("query", ["SELECT VALUE working FROM test:1"]);
		expect(expired.error?.code).toBe(-32000);
		expect(expired.error?.message).toBe("The session has expired");

		// Methods that MUST reject on an expired session (data + auth-context).
		const denied: Array<[string, unknown[]]> = [
			["let", ["let_var", "let_value"]],
			["set", ["set_var", "set_value"]],
			["info", []],
			["select", ["tester"]],
			["insert", ["tester", { name: "foo", value: "bar" }]],
			["create", ["tester", { value: "bar" }]],
			["update", ["tester", { value: "bar" }]],
			["merge", ["tester", { value: "bar" }]],
			[
				"patch",
				[
					"tester:id",
					[
						{ op: "add", path: "value", value: "bar" },
						{ op: "remove", path: "name" },
					],
				],
			],
			["delete", ["tester"]],
			["live", ["tester"]],
			["kill", ["tester"]],
		];
		for (const [method, params] of denied) {
			const res = await rpc.rpc(method, params);
			expect(res.error?.message).toBe("The session has expired");
		}

		// Methods that MUST still work on an expired session (connection-level).
		// invalidate is last: it also clears the (already expired) auth.
		for (const [method, params] of [
			["use", [ns, db]],
			["ping", []],
			["version", []],
			["invalidate", []],
		] as Array<[string, unknown[]]>) {
			const res = await rpc.rpc(method, params);
			expect(res.error).toBeUndefined();
		}

		// signup/signin refresh the session expiry; each new session lapses again.
		const reSignup = await rpc.rpc("signup", [
			{ ns, db, ac: "user", email: "another@email.com", pass: "pass" },
		]);
		expect(reSignup.error).toBeUndefined();
		await Bun.sleep(2000);
		const expired2 = await rpc.rpc("query", ["SELECT VALUE working FROM test:1"]);
		expect(expired2.error?.code).toBe(-32000);
		expect(expired2.error?.message).toBe("The session has expired");

		const reSignin = await rpc.rpc("signin", [
			{ ns, db, ac: "user", email: "another@email.com", pass: "pass" },
		]);
		expect(reSignin.error).toBeUndefined();
		await Bun.sleep(2000);
		const expired3 = await rpc.rpc("query", ["SELECT VALUE working FROM test:1"]);
		expect(expired3.error?.code).toBe(-32000);
		expect(expired3.error?.message).toBe("The session has expired");

		// authenticate with the non-expiring root token: the session no longer expires.
		const reAuth = await rpc.rpc("authenticate", [rootToken]);
		expect(reAuth.error).toBeUndefined();
		await rpc.close();
	},
	30000,
);

// ---------------------------------------------------------------------------
// RPC capability gating — from ws_integration.rs: rpc_capability. Denied
// methods return -32602 "Method not allowed" (checked before auth), so these
// servers run --unauthenticated to isolate the capability behavior.
// ---------------------------------------------------------------------------

test("--deny-rpc=info blocks only info; every other method still runs", async () => {
	// Own server: --unauthenticated so denials are capability errors, not auth.
	const capServer = await startServer({ args: ["--unauthenticated", "--deny-rpc=info"] });
	try {
		const rpc = await RpcClient.connect(capServer);
		await rpc.rpc("use", ["cap_ns", "cap_db"]);

		const info = await rpc.rpc("info", []);
		expect(info.error?.code).toBe(-32602);
		expect(info.error?.message).toBe("Method not allowed");

		// A representative allowed set still succeeds.
		for (const [method, params] of [
			["ping", []],
			["version", []],
			["let", ["let_var", "let_value"]],
			["set", ["set_var", "set_value"]],
			["query", ["DEFINE TABLE tester"]],
			["select", ["tester"]],
			["insert", ["tester", { name: "foo", value: "bar" }]],
			["create", ["tester", { value: "bar" }]],
			["update", ["tester", { value: "bar" }]],
			["merge", ["tester", { value: "bar" }]],
			["delete", ["tester"]],
			["invalidate", []],
		] as Array<[string, unknown[]]>) {
			const res = await rpc.rpc(method, params);
			expect(res.error).toBeUndefined();
		}
		await rpc.close();
	} finally {
		await capServer.stop();
	}
});

test("--deny-rpc with --allow-rpc=version,use denies everything else with -32602", async () => {
	// Bare --deny-rpc (deny all) is placed BEFORE another --flag so clap's
	// greedy variadic parser does not swallow the trailing `memory` positional.
	const capServer = await startServer({
		args: ["--unauthenticated", "--deny-rpc", "--allow-rpc=version,use"],
	});
	try {
		const rpc = await RpcClient.connect(capServer);

		for (const [method, params] of [
			["query", ["SELECT * FROM 1"]],
			["ping", []],
			["info", []],
			["let", ["let_var", "let_value"]],
			["set", ["set_var", "set_value"]],
			["select", ["tester"]],
			["insert", ["tester", { name: "foo", value: "bar" }]],
			["create", ["tester", { value: "bar" }]],
			["update", ["tester", { value: "bar" }]],
			["merge", ["tester", { value: "bar" }]],
			[
				"patch",
				[
					"tester:id",
					[
						{ op: "add", path: "value", value: "bar" },
						{ op: "remove", path: "name" },
					],
				],
			],
			["delete", ["tester"]],
			["invalidate", []],
		] as Array<[string, unknown[]]>) {
			const res = await rpc.rpc(method, params);
			expect(res.error?.code).toBe(-32602);
			expect(res.error?.message).toBe("Method not allowed");
		}

		// Only version and use are permitted.
		expect((await rpc.rpc("version", [])).error).toBeUndefined();
		expect((await rpc.rpc("use", ["cap_ns", "cap_db"])).error).toBeUndefined();
		await rpc.close();
	} finally {
		await capServer.stop();
	}
});

// ---------------------------------------------------------------------------
// Live queries over RPC — from ws_integration.rs: live_rpc, kill,
// live_query_preserved_on_same_identity_resignin,
// live_query_cleared_on_principal_change. Raw notification frames are pinned
// via RpcClient.notifications.
// ---------------------------------------------------------------------------

test("live RPC delivers a raw CREATE notification frame carrying the live-query id", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE TABLE tester"]);
	const liveId = (await rpc.call("live", ["tester"])) as string;
	expect(typeof liveId).toBe("string");

	await rpc.call("query", ["CREATE tester:id SET name = 'foo'"]);
	const notif = await rpc.notifications.waitFor(
		(n) => n.id === liveId && n.action === "CREATE",
		5000,
	);
	// The raw frame's result is the created record.
	expect((notif.result as { id: string }).id).toBe("tester:id");
	await rpc.close();
});

test("kill RPC returns null, emits a KILLED notification, and stops further delivery", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE TABLE tester"]);
	const liveId = (await rpc.call("live", ["tester"])) as string;

	await rpc.call("query", ["CREATE tester:one SET name = 'one'"]);
	await rpc.notifications.waitFor((n) => n.id === liveId && n.action === "CREATE", 5000);

	const killRes = await rpc.rpc("kill", [liveId]);
	expect(killRes.error).toBeUndefined();
	expect(killRes.result).toBeNull();

	// The kill produces a KILLED frame for that live-query id.
	const killed = await rpc.notifications.waitFor(
		(n) => n.id === liveId && n.action === "KILLED",
		5000,
	);
	expect(killed.action).toBe("KILLED");

	// No further CREATE notifications reach the killed subscription. Scope the
	// silence check to the NEW record so it is not satisfied by the earlier
	// tester:one CREATE still buffered in the collector.
	await rpc.call("query", ["CREATE tester:two SET name = 'two'"]);
	await rpc.notifications.assertSilence(
		(n) => n.action === "CREATE" && (n.result as { id?: string })?.id === "tester:two",
		1000,
	);
	await rpc.close();
});

test("a live query survives re-signin with the same root identity", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE TABLE tester"]);
	const liveId = (await rpc.call("live", ["tester"])) as string;

	// Re-signin as the SAME root identity: the auth principal is unchanged, so
	// the live query must NOT be torn down.
	await rpc.call("signin", [{ user: "root", pass: "root" }]);

	await rpc.call("query", ["CREATE tester:id SET name = 'foo'"]);
	const notif = await rpc.notifications.waitFor(
		(n) => n.id === liveId && n.action === "CREATE",
		5000,
	);
	expect((notif.result as { id: string }).id).toBe("tester:id");
	await rpc.close();
});

test("a live query registered as root is torn down when the connection changes principal (signup)", async () => {
	// Reader registers the LIVE as root, then signs up as a record user on the
	// same connection (Level::Root -> Level::Record). A write from a separate
	// still-root connection must NOT reach the re-authenticated reader.
	const { rpc: reader, ns, db } = await rootRpc();
	await reader.call("query", ["DEFINE TABLE tester"]);
	await reader.call("query", [EMAIL_ACCESS("1d", "1d")]);

	const liveId = (await reader.call("live", ["tester"])) as string;

	// Principal change on the reader's own connection.
	const signup = await reader.rpc("signup", [
		{ ns, db, ac: "user", email: "victim@example.com", pass: "pass" },
	]);
	expect(signup.error).toBeUndefined();

	// Separate root writer on the same server/ns/db.
	const writer = await RpcClient.connect(server);
	await writer.call("signin", [{ user: "root", pass: "root" }]);
	await writer.call("use", [ns, db]);
	await writer.call("query", ["CREATE tester:id SET name = 'foo'"]);

	// The live query, registered before the principal change, must be gone:
	// no notification arrives on the reader within the silence window.
	await reader.notifications.assertSilence((n) => n.id === liveId, 1000);

	await writer.close();
	await reader.close();
});
