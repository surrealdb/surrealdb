// WebSocket transport conformance: the raw JSON-RPC wire protocol plus
// per-connection session semantics.
//
// The wire tests drive the server over the raw `json` subprotocol via RpcClient
// to pin the exact JSON-RPC envelope shapes, error codes, and live-notification
// frames that the high-level SDK wraps or hides. The session tests drive the SDK
// to pin use() / set() / unset(), per-connection parameter isolation, and
// binding scope on a live connection.
//
// Each test spawns its own in-memory server and uses a unique namespace/database.
//
// Not covered (and why):
// - session_id_defined / _generic / _both / _invalid / _undefined,
//   session_id via `surreal-id` / `x-request-id` connection headers: the
//   surrealdb.js WebSocket driver — and the harness RpcClient built on the
//   browser `WebSocket` — cannot set arbitrary connection request headers, so
//   the handshake-header behavior is unobservable from JS.
// - detach_connection_session_rejected, websocket_attach_session_cap,
//   multi_session_* : per-connection `attach`/`detach`/`sessions` multiplexing
//   is exercised through the SDK in sessions.test.ts.
// - live_query_diff (LIVE SELECT DIFF): text-diff patch payloads are covered
//   indirectly; the raw envelope shape is stable but noisy to pin here.
// - temporary_directory / concurrency: not wire-protocol behavior.
import { afterEach, beforeEach, expect, test } from "bun:test";
import { RpcClient, guestClient, rootClient, startServer, type TestServer } from "../src/harness";

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
// Arbitrary-query subject gating — from protocol.rs: the same
// `allows_query_by_subject` gate that fronts /sql also guards the RPC verb
// methods (use / set / query / select / create / insert / update / upsert /
// delete / relate / run / …). This is distinct from --deny-rpc: it keys off the
// session's auth SUBJECT (system = root/ns/db users), so --deny-arbitrary-query
// =system denies query-bearing verbs for a signed-in root user while leaving
// connection-level RPCs untouched. Denial is the same method_not_allowed error
// as --deny-rpc: top-level -32602 "Method not allowed".
// ---------------------------------------------------------------------------

test("--deny-arbitrary-query=system denies query-bearing RPC verbs for a signed-in root, as a top-level -32602", async () => {
	// Own server: the subject gate is scoped to the `system` auth subject, so a
	// root signin is allowed but every query-bearing verb it then issues is not.
	const capServer = await startServer({ args: ["--deny-arbitrary-query=system"] });
	try {
		const rpc = await RpcClient.connect(capServer);
		// signin is a connection/auth verb — NOT gated — and succeeds as root.
		const signin = await rpc.rpc("signin", [{ user: "root", pass: "root" }]);
		expect(signin.error).toBeUndefined();

		// With the gate on, even `use` is denied, so we cannot select ns/db first;
		// assert the denial on each verb from the bare signed-in connection.
		for (const [method, params] of [
			["use", ["cap_ns", "cap_db"]],
			["query", ["SELECT * FROM 1"]],
			["select", ["tester"]],
			["create", ["tester", { value: "bar" }]],
			["insert", ["tester", { name: "foo", value: "bar" }]],
			["update", ["tester", { value: "bar" }]],
			["delete", ["tester"]],
			["relate", ["foo:a", "bar", "foo:b", { val: 42 }]],
			["run", ["math::abs", null, [42]]],
			["set", ["set_var", "set_value"]],
		] as Array<[string, unknown[]]>) {
			const res = await rpc.rpc(method, params);
			// The gate surfaces as a top-level JSON-RPC error, not a per-statement
			// ERR — identical shape to a --deny-rpc denial.
			expect(res.error?.code).toBe(-32602);
			expect(res.error?.message).toBe("Method not allowed");
			expect(res.result).toBeUndefined();
		}
		await rpc.close();
	} finally {
		await capServer.stop();
	}
});

test("--deny-arbitrary-query=system leaves connection-level RPCs (ping / version / signin) working", async () => {
	// The deny is scoped to query-bearing verbs; the connection/auth surface of
	// the protocol stays fully available to a root user.
	const capServer = await startServer({ args: ["--deny-arbitrary-query=system"] });
	try {
		const rpc = await RpcClient.connect(capServer);
		expect((await rpc.rpc("ping", [])).error).toBeUndefined();
		expect((await rpc.rpc("version", [])).error).toBeUndefined();
		// signin (auth verb) still succeeds even though every query verb is denied.
		expect((await rpc.rpc("signin", [{ user: "root", pass: "root" }])).error).toBeUndefined();
		// And repeated after signin — the gate never touches these.
		expect((await rpc.rpc("ping", [])).error).toBeUndefined();
		expect((await rpc.rpc("version", [])).error).toBeUndefined();
		await rpc.close();
	} finally {
		await capServer.stop();
	}
});

test("--allow-arbitrary-query=system (default posture) lets a signed-in root run query verbs", async () => {
	// Contrast: with the system subject allowed, the very verbs denied above run
	// normally once ns/db is selected.
	const capServer = await startServer({ args: ["--allow-arbitrary-query=system"] });
	try {
		const rpc = await RpcClient.connect(capServer);
		await rpc.call("signin", [{ user: "root", pass: "root" }]);
		await rpc.call("query", ["DEFINE NAMESPACE aq_ns"]);
		await rpc.call("use", ["aq_ns", null]);
		await rpc.call("query", ["DEFINE DATABASE aq_db"]);
		await rpc.call("use", ["aq_ns", "aq_db"]);
		const rec = (await rpc.rpc("create", ["tester", { value: "bar" }])).result as {
			id: string;
			value: string;
		};
		expect(rec.value).toBe("bar");
		expect(rec.id).toMatch(/^tester:/);
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

// ---------------------------------------------------------------------------
// Session semantics (SDK-driven): use/set/unset, per-connection isolation,
// binding scope on a live connection.
// ---------------------------------------------------------------------------

test("version() reports a SurrealDB server version", async () => {
	const { db } = await rootClient(server);
	const version = await db.version();
	expect(JSON.stringify(version)).toMatch(/surrealdb/i);
	await db.close();
});

test("use() switches namespace and database on a live connection", async () => {
	const { db, namespace } = await rootClient(server);
	const [before] = await db.query<[string]>("RETURN session::db()").json();

	await db.query("DEFINE DATABASE other_db");
	await db.use({ namespace, database: "other_db" });
	const [after] = await db.query<[string]>("RETURN session::db()").json();

	expect(after).toBe("other_db");
	expect(after).not.toBe(before);
	await db.close();
});

test("set() binds a session parameter visible to later queries; unset() removes it", async () => {
	const { db } = await rootClient(server);

	await db.set("answer", 42);
	const [val] = await db.query<[number]>("RETURN $answer").json();
	expect(val).toBe(42);

	// Session params participate in real queries.
	await db.query("CREATE thing:one SET n = $answer");
	const [rows] = await db.query<[Array<{ n: number }>]>("SELECT n FROM thing:one").json();
	expect(rows[0].n).toBe(42);

	await db.unset("answer");
	const [gone] = await db.query<[unknown]>("RETURN $answer").json();
	expect(gone).toBeUndefined();

	await db.close();
});

test("session parameters are isolated between connections", async () => {
	const { db, namespace, database } = await rootClient(server);
	const other = await guestClient(server, namespace, database);
	await other.signin({ username: "root", password: "root" });
	await other.use({ namespace, database });

	await db.set("private_value", "connection-a-only");

	const [seenByOther] = await other.query<[unknown]>("RETURN $private_value").json();
	expect(seenByOther).toBeUndefined();
	const [seenBySelf] = await db.query<[unknown]>("RETURN $private_value").json();
	expect(seenBySelf).toBe("connection-a-only");

	await other.close();
	await db.close();
});

test("session state (params and auth) survives across many sequential queries", async () => {
	const { db } = await rootClient(server);
	await db.set("counter_base", 100);
	for (let i = 0; i < 25; i++) {
		const [v] = await db.query<[number]>("RETURN $counter_base + $i", { i }).json();
		expect(v).toBe(100 + i);
	}
	const [session] = await db.query<[unknown]>("RETURN session::id() != NONE").json();
	expect(session).toBe(true);
	await db.close();
});

test("query bindings do not leak into session state", async () => {
	const { db } = await rootClient(server);
	const [bound] = await db.query<[string]>("RETURN $ephemeral", { ephemeral: "one-shot" }).json();
	expect(bound).toBe("one-shot");
	// The binding was per-query, not a session parameter.
	const [after] = await db.query<[unknown]>("RETURN $ephemeral").json();
	expect(after).toBeUndefined();
	await db.close();
});

// ---------------------------------------------------------------------------
// insert_relation RPC — params are [table, edgeData]. Unlike `relate` (single
// object), insert_relation returns an array. The edge data must carry `in` and
// `out`; the resulting edge is readable via graph traversal.
// ---------------------------------------------------------------------------

test("insert_relation RPC creates an edge from explicit in/out and returns it in an array", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE person:a, person:b"]);

	const res = await rpc.rpc("insert_relation", [
		"likes",
		{ in: "person:a", out: "person:b", since: 2020 },
	]);
	expect(res.error).toBeUndefined();
	// A single edge object still comes back wrapped in a one-element array.
	expect(Array.isArray(res.result)).toBe(true);
	const rows = res.result as Array<{ id: string; in: string; out: string; since: number }>;
	expect(rows).toHaveLength(1);
	expect(rows[0].in).toBe("person:a");
	expect(rows[0].out).toBe("person:b");
	expect(rows[0].since).toBe(2020);
	expect(rows[0].id).toMatch(/^likes:/);

	// The edge is reachable by traversing the graph from the source record.
	const to = (await rpc.call("query", ["RETURN person:a->likes->person"])) as StmtEnvelope[];
	expect(to[0].result).toEqual(["person:b"]);
	await rpc.close();
});

test("insert_relation RPC inserts a batch and rejects edge data missing in/out", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["CREATE person:a, person:b"]);

	// A batch of edge objects yields one element per edge.
	const batch = await rpc.rpc("insert_relation", [
		"likes",
		[
			{ in: "person:a", out: "person:b" },
			{ in: "person:b", out: "person:a" },
		],
	]);
	expect(batch.error).toBeUndefined();
	expect(batch.result as unknown[]).toHaveLength(2);

	// An explicit `id` on the edge object is honoured.
	const withId = await rpc.rpc("insert_relation", [
		"likes",
		{ id: "likes:custom", in: "person:a", out: "person:b" },
	]);
	expect((withId.result as Array<{ id: string }>)[0].id).toBe("likes:custom");

	// Edge data without `in` fails at the RPC level (relations require both ends).
	const missing = await rpc.rpc("insert_relation", ["likes", { note: "x" }]);
	expect(missing.error?.code).toBe(-32000);
	expect(missing.error?.message).toBe(
		"Cannot execute INSERT statement where property 'in' is: NONE",
	);
	await rpc.close();
});

// ---------------------------------------------------------------------------
// reset RPC — clears the session's auth, USE (ns/db), and params in place,
// without dropping the socket. It returns null and the connection stays usable.
// ---------------------------------------------------------------------------

test("reset RPC clears session params, auth, and USE while keeping the socket usable", async () => {
	const { rpc, ns, db } = await rootRpc();
	await rpc.call("set", ["myvar", "hello"]);
	const before = (await rpc.call("query", ["RETURN $myvar"])) as StmtEnvelope[];
	expect(before[0].result).toBe("hello");

	// reset returns null.
	const resetRes = await rpc.rpc("reset", []);
	expect(resetRes.error).toBeUndefined();
	expect(resetRes.result).toBeNull();

	// Auth is cleared: the connection is now anonymous, so a query is rejected
	// as a whole-request RPC error.
	const anon = await rpc.rpc("query", ["RETURN $myvar"]);
	expect(anon.error?.message).toBe(
		"Anonymous access not allowed: Not enough permissions to perform this action",
	);

	// ping is connection-level and still works — the socket was never dropped.
	expect((await rpc.rpc("ping", [])).result).toBeNull();

	// Re-authenticate and re-select ns/db (reset cleared USE too). The param is
	// gone — it reads back null — confirming reset wiped session variables.
	await rpc.call("signin", [{ user: "root", pass: "root" }]);
	await rpc.call("use", [ns, db]);
	const after = (await rpc.call("query", ["RETURN $myvar"])) as StmtEnvelope[];
	expect(after[0].result).toBeNull();
	await rpc.close();
});

// ---------------------------------------------------------------------------
// revoke RPC — params are [token]. Given an access+refresh token pair from a
// `WITH REFRESH` record access, revoke removes the refresh grant: the refresh
// token can no longer mint new tokens, while the still-unexpired access token
// keeps authenticating. An access-only token cannot be revoked.
// ---------------------------------------------------------------------------

test("revoke RPC invalidates a refresh grant while leaving the access token valid", async () => {
	const { rpc, ns, db } = await rootRpc();
	// A record access that issues a refresh token alongside the access token.
	await rpc.call("query", [
		`DEFINE ACCESS user ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			WITH REFRESH DURATION FOR SESSION 1d, FOR TOKEN 15s;`,
	]);

	// A WITH REFRESH signup returns an { access, refresh } pair, not a bare string.
	const token = (await rpc.call("signup", [
		{ ns, db, ac: "user", email: "a@example.com", pass: "pass" },
	])) as { access: string; refresh: string };
	expect(typeof token.access).toBe("string");
	expect(typeof token.refresh).toBe("string");

	// An access-only token has no refresh component to revoke.
	const accessOnly = await rpc.rpc("revoke", [token.access]);
	expect(accessOnly.error?.code).toBe(-32000);
	expect(accessOnly.error?.message).toBe(
		"Incorrect arguments for function refresh(). Token is an access token, cannot revoke refresh token",
	);

	// Revoking the pair succeeds and returns null.
	const revoked = await rpc.rpc("revoke", [token]);
	expect(revoked.error).toBeUndefined();
	expect(revoked.result).toBeNull();

	// The access token itself is independent and still authenticates.
	const reauth = await rpc.rpc("authenticate", [token.access]);
	expect(reauth.error).toBeUndefined();

	// But the revoked refresh token can no longer mint a fresh token pair.
	const refresh = await rpc.rpc("refresh", [token]);
	expect(refresh.error?.code).toBe(-32002);
	expect(refresh.error?.message).toBe("There was a problem with authentication");
	await rpc.close();
});

// ---------------------------------------------------------------------------
// detach RPC — the durable-session teardown counterpart to `attach`. It keys
// off a per-connection session id supplied via a connection request header. The
// harness RpcClient is built on the browser `WebSocket`, which cannot set
// arbitrary request headers, so detach has no session to act on: it rejects
// with -32603 InvalidParams. The full durable-session teardown path is
// exercised through the SDK in sessions.test.ts.
// ---------------------------------------------------------------------------

test("detach RPC without a connection session id is rejected as invalid params", async () => {
	const { rpc } = await rootRpc();
	const res = await rpc.rpc("detach", []);
	expect(res.error?.code).toBe(-32603);
	expect(res.error?.message).toBe("Expected a session ID");
	expect(res.result).toBeUndefined();
	await rpc.close();
});

test.skip("detach RPC tears down the durable session copy so it cannot be resurrected", async () => {
	// The durable teardown requires a connection session id, delivered by a
	// connection request header the JS WebSocket driver cannot set. Covered by
	// the SDK-driven multi-session tests in sessions.test.ts.
});

// ---------------------------------------------------------------------------
// INFO introspection shape over the wire — the SDK/tooling consume the exact
// object shape a `query` RPC of INFO FOR ... returns. Plain INFO renders each
// definition as its DDL string; the STRUCTURE variant renders typed objects
// (and arrays) instead. User definitions redact their password material either
// way.
// ---------------------------------------------------------------------------

test("INFO FOR TABLE returns DDL-string maps; the STRUCTURE variant returns typed field objects", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", [
		"DEFINE TABLE tbl SCHEMAFULL; DEFINE FIELD f ON tbl TYPE int; DEFINE INDEX idx ON tbl FIELDS f;",
	]);

	// Plain INFO: keyed maps whose values are the definition DDL strings.
	const plain = (await rpc.call("query", ["INFO FOR TABLE tbl"])) as StmtEnvelope[];
	const info = plain[0].result as {
		fields: Record<string, string>;
		indexes: Record<string, string>;
		events: Record<string, unknown>;
		lives: Record<string, unknown>;
		tables: Record<string, unknown>;
	};
	expect(info.fields.f).toBe("DEFINE FIELD f ON tbl TYPE int PERMISSIONS FULL");
	expect(info.indexes.idx).toBe("DEFINE INDEX idx ON tbl FIELDS f");

	// STRUCTURE: the same collections become arrays of typed objects.
	const structured = (await rpc.call("query", [
		"INFO FOR TABLE tbl STRUCTURE",
	])) as StmtEnvelope[];
	const struct = structured[0].result as {
		fields: Array<{ name: string; kind: string; readonly: boolean; permissions: unknown }>;
		indexes: Array<{ name: string; cols: string[] }>;
	};
	expect(Array.isArray(struct.fields)).toBe(true);
	const field = struct.fields.find((x) => x.name === "f")!;
	expect(field.kind).toBe("int");
	expect(field.readonly).toBe(false);
	expect(typeof field.permissions).toBe("object");
	expect(struct.indexes[0].name).toBe("idx");
	expect(struct.indexes[0].cols).toEqual(["f"]);
	await rpc.close();
});

test("INFO FOR USER returns a redacted DDL string; the STRUCTURE variant returns a typed object", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE USER bob ON DATABASE PASSWORD 'secret' ROLES VIEWER"]);

	// Plain INFO: a single DDL string with password material redacted.
	const plain = (await rpc.call("query", ["INFO FOR USER bob"])) as StmtEnvelope[];
	expect(typeof plain[0].result).toBe("string");
	expect(plain[0].result as string).toBe(
		"DEFINE USER bob ON DATABASE PASSHASH '[REDACTED]' PASSSCRAM '[REDACTED]' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE",
	);

	// STRUCTURE: a typed object; the secrets stay redacted string placeholders.
	const structured = (await rpc.call("query", ["INFO FOR USER bob STRUCTURE"])) as StmtEnvelope[];
	const user = structured[0].result as {
		name: string;
		roles: string[];
		hash: string;
		scram: string;
		duration: { session: string | null; token: string };
	};
	expect(user.name).toBe("bob");
	expect(user.roles).toEqual(["VIEWER"]);
	expect(user.hash).toBe("[REDACTED]");
	expect(user.scram).toBe("[REDACTED]");
	expect(user.duration.token).toBe("1h");
	expect(user.duration.session).toBeNull();
	await rpc.close();
});

test("INFO FOR DB STRUCTURE returns typed catalog arrays with typed table entries", async () => {
	const { rpc } = await rootRpc();
	await rpc.call("query", ["DEFINE TABLE person SCHEMAFULL PERMISSIONS FOR select FULL"]);

	const structured = (await rpc.call("query", ["INFO FOR DB STRUCTURE"])) as StmtEnvelope[];
	const db = structured[0].result as {
		tables: Array<{
			name: string;
			schemafull: boolean;
			drop: boolean;
			kind: { kind: string };
			permissions: { create: boolean; select: boolean; update: boolean; delete: boolean };
		}>;
		accesses: unknown[];
		analyzers: unknown[];
		functions: unknown[];
		params: unknown[];
		users: unknown[];
	};
	// Catalog collections are arrays (empty ones included).
	expect(Array.isArray(db.tables)).toBe(true);
	expect(Array.isArray(db.accesses)).toBe(true);
	expect(Array.isArray(db.analyzers)).toBe(true);
	expect(Array.isArray(db.functions)).toBe(true);
	expect(Array.isArray(db.params)).toBe(true);

	const table = db.tables.find((t) => t.name === "person")!;
	expect(table.schemafull).toBe(true);
	expect(table.drop).toBe(false);
	expect(table.kind.kind).toBe("NORMAL");
	// The `select FULL` permission is a boolean-true; the unspecified verbs
	// default to a denying `false` in the STRUCTURE shape.
	expect(table.permissions.select).toBe(true);
	expect(table.permissions.create).toBe(false);
	expect(table.permissions.update).toBe(false);
	expect(table.permissions.delete).toBe(false);
	await rpc.close();
});
