// HTTP endpoint conformance — raw fetch against the server, no SDK.
// Pins observed behavior of the HTTP surface: /health, /version, /status,
// /ready, /sync, the /sql per-statement envelope (auth failures, missing
// namespace, parse errors), /key CRUD plus its body-injection guard,
// /signin + /signup (auth-level inference and record access), /export and
// /import round-trips, and RPC-over-HTTP (query/version, unknown-method
// errors, and session ownership/isolation).
//
// It also pins server-flag behavior: --deny-http / --allow-http route gating
// (including the /sql WebSocket upgrade), the --*-arbitrary-query matrix,
// --client-ip source modes into $session.ip, the startup-import readiness
// gate, --no-identification-headers, the surreal-id session header, and
// bearer / basic-auth level scoping.
//
// Header/API facts:
// - Namespace/database are selected with `surreal-ns` / `surreal-db` headers.
// - Basic auth (`Authorization: Basic ...`) or `Bearer <jwt>` authenticate.
// - Headers select but never create the namespace/database.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, test } from "bun:test";
import { startServer, type TestServer } from "../src/harness";

const ROOT_AUTH = `Basic ${btoa("root:root")}`;

interface HttpOptions {
	/** Authorization header value. Defaults to root basic auth; `null` sends none. */
	auth?: string | null;
	ns?: string;
	db?: string;
	headers?: Record<string, string>;
	body?: string;
}

function buildHeaders(opts: HttpOptions): Record<string, string> {
	const headers: Record<string, string> = { Accept: "application/json", ...opts.headers };
	if (opts.auth !== null) headers.Authorization = opts.auth ?? ROOT_AUTH;
	if (opts.ns) headers["surreal-ns"] = opts.ns;
	if (opts.db) headers["surreal-db"] = opts.db;
	return headers;
}

// Per-statement envelope returned by /sql and /key endpoints.
interface Envelope {
	result: unknown;
	status: "OK" | "ERR";
	time: string;
	type: null;
	kind?: string;
	details?: unknown;
}

let counter = 0;

// ---------------------------------------------------------------------------
// Shared-server group: happy-path endpoint behavior against a single server.
// ---------------------------------------------------------------------------
{
	let server: TestServer;

	beforeAll(async () => {
		server = await startServer();
	});

	afterAll(async () => {
		await server.stop();
	});

	function send(method: string, path: string, opts: HttpOptions = {}): Promise<Response> {
		return fetch(`${server.httpUrl}${path}`, {
			method,
			headers: buildHeaders(opts),
			body: opts.body,
		});
	}

	function sql(query: string, opts: HttpOptions = {}): Promise<Response> {
		return send("POST", "/sql", { ...opts, body: query });
	}

	/** Define a unique namespace/database pair for one test (headers never create them). */
	async function freshNsDb(): Promise<{ ns: string; db: string }> {
		const ns = `http_ns_${process.pid}_${++counter}`;
		const db = `http_db_${counter}`;
		const res = await sql(`DEFINE NAMESPACE \`${ns}\`; USE NS \`${ns}\`; DEFINE DATABASE \`${db}\`;`);
		expect(res.status).toBe(200);
		const results = (await res.json()) as Envelope[];
		for (const r of results) expect(r.status).toBe("OK");
		return { ns, db };
	}

	test("GET /health is 200 with an empty body; GET /version returns the version string", async () => {
		const health = await fetch(`${server.httpUrl}/health`);
		expect(health.status).toBe(200);
		expect(await health.text()).toBe("");
		// The version is also advertised on every response via the surreal-version header.
		expect(health.headers.get("surreal-version")).toMatch(/^surrealdb\/\d+\.\d+\.\d+/);

		const version = await fetch(`${server.httpUrl}/version`);
		expect(version.status).toBe(200);
		expect(version.headers.get("content-type")).toMatch(/^text\/plain/);
		expect(await version.text()).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	});

	test("POST /sql returns a per-statement envelope array; statement errors do not fail the request", async () => {
		const { ns, db } = await freshNsDb();
		const res = await sql(
			`RETURN 1; CREATE person:one SET name = 'a'; THROW 'boom'; SELECT * FROM person;`,
			{ ns, db },
		);
		// One failing statement does NOT change the HTTP status: still 200,
		// with a status:"ERR" envelope in position — and later statements still ran.
		expect(res.status).toBe(200);
		expect(res.headers.get("content-type")).toMatch(/^application\/json/);
		const results = (await res.json()) as Envelope[];
		expect(results).toHaveLength(4);

		for (const r of results) {
			expect(r.status).toMatch(/^(OK|ERR)$/);
			expect(r.time).toMatch(/^\d+(\.\d+)?(ns|µs|ms|s)$/);
			expect(r.type).toBeNull();
		}
		expect(results[0]).toMatchObject({ status: "OK", result: 1 });
		expect(results[1].status).toBe("OK");
		expect(results[1].result).toEqual([{ id: "person:one", name: "a" }]);
		// ERR envelopes carry the message in `result` plus `kind`/`details` fields.
		expect(results[2]).toMatchObject({
			status: "ERR",
			kind: "Thrown",
			result: "An error occurred: boom",
		});
		expect(results[3].status).toBe("OK");
		expect(results[3].result).toEqual([{ id: "person:one", name: "a" }]);
	});

	test("POST /sql auth failures: no auth is 403 (JSON), bad basic auth is 401 (plain text)", async () => {
		const anon = await sql("RETURN 1", { auth: null, ns: "nope", db: "nope" });
		expect(anon.status).toBe(403);
		const anonBody = (await anon.json()) as Record<string, unknown>;
		expect(anonBody).toMatchObject({ code: 403, details: "Forbidden" });
		expect(String(anonBody.information)).toMatch(/Anonymous access not allowed/);

		// Surprising: wrong credentials come back as a bare text/plain body
		// ("The password did not verify") even with Accept: application/json —
		// unlike the anonymous 403, which is a JSON problem document.
		const bad = await sql("RETURN 1", { auth: `Basic ${btoa("root:wrong")}`, ns: "nope", db: "nope" });
		expect(bad.status).toBe(401);
		expect(await bad.text()).toBe("The password did not verify");
	});

	test("POST /sql without namespace headers reports a per-statement ERR, not an HTTP error", async () => {
		// Statements needing a namespace fail per-statement; the request itself is 200.
		const res = await sql("SELECT * FROM person");
		expect(res.status).toBe(200);
		const [result] = (await res.json()) as Envelope[];
		expect(result).toMatchObject({
			status: "ERR",
			kind: "Validation",
			result: "Specify a namespace to use",
		});
		// Headers select but do not create: pointing at an undefined namespace
		// fails with a NotFound error rather than auto-creating it.
		const ghost = await sql("SELECT * FROM person", { ns: "no_such_ns_xyz", db: "no_such_db" });
		expect(ghost.status).toBe(200);
		const [ghostResult] = (await ghost.json()) as Envelope[];
		expect(ghostResult).toMatchObject({ status: "ERR", kind: "NotFound" });
		expect(String(ghostResult.result)).toMatch(/namespace 'no_such_ns_xyz' does not exist/);
	});

	test("POST /sql with unparseable SurrealQL is HTTP 400 with a JSON problem body", async () => {
		const { ns, db } = await freshNsDb();
		const res = await sql("SELECT * FROM person WHERE", { ns, db });
		expect(res.status).toBe(400);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body).toMatchObject({ code: 400, details: "Request problems detected" });
		expect(String(body.information)).toMatch(/Parse error/);
	});

	test("REST /key: POST creates, GET lists and fetches, missing ids read as empty result", async () => {
		const { ns, db } = await freshNsDb();

		// POST /key/:table generates a random record id.
		const created = await send("POST", "/key/fruit", {
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name: "apple", qty: 3 }),
		});
		expect(created.status).toBe(200);
		const [createdEnv] = (await created.json()) as Envelope[];
		expect(createdEnv.status).toBe("OK");
		const [row] = createdEnv.result as Array<{ id: string; name: string; qty: number }>;
		expect(row.id).toMatch(/^fruit:/);
		expect(row).toMatchObject({ name: "apple", qty: 3 });

		// POST /key/:table/:id creates with an explicit id.
		const explicit = await send("POST", "/key/fruit/banana", {
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name: "banana" }),
		});
		const [explicitEnv] = (await explicit.json()) as Envelope[];
		expect(explicitEnv.result).toEqual([{ id: "fruit:banana", name: "banana" }]);

		// GET /key/:table lists every record.
		const list = await send("GET", "/key/fruit", { ns, db });
		expect(list.status).toBe(200);
		const [listEnv] = (await list.json()) as Envelope[];
		expect(listEnv.result as unknown[]).toHaveLength(2);

		// GET /key/:table/:id fetches one; a missing id is still 200 with an empty result.
		const one = await send("GET", "/key/fruit/banana", { ns, db });
		const [oneEnv] = (await one.json()) as Envelope[];
		expect(oneEnv.result).toEqual([{ id: "fruit:banana", name: "banana" }]);

		const missing = await send("GET", "/key/fruit/no_such_id", { ns, db });
		expect(missing.status).toBe(200);
		const [missingEnv] = (await missing.json()) as Envelope[];
		expect(missingEnv).toMatchObject({ status: "OK", result: [] });

		// Unauthenticated access to /key is rejected with 403.
		const anon = await send("GET", "/key/fruit", { ns, db, auth: null });
		expect(anon.status).toBe(403);
	});

	test("REST /key: PUT replaces, PATCH merges, DELETE returns the deleted record", async () => {
		const { ns, db } = await freshNsDb();
		const jsonBody = (body: unknown) => ({
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});

		await send("POST", "/key/veg/carrot", jsonBody({ name: "carrot", qty: 1 }));

		// PUT replaces the whole content (previous fields are dropped).
		const put = await send("PUT", "/key/veg/carrot", jsonBody({ color: "orange" }));
		const [putEnv] = (await put.json()) as Envelope[];
		expect(putEnv.result).toEqual([{ id: "veg:carrot", color: "orange" }]);

		// PATCH with a JSON object merges into the existing content.
		const patch = await send("PATCH", "/key/veg/carrot", jsonBody({ ripe: true }));
		const [patchEnv] = (await patch.json()) as Envelope[];
		expect(patchEnv.result).toEqual([{ id: "veg:carrot", color: "orange", ripe: true }]);

		// Both PUT and PATCH upsert: targeting a missing id creates the record.
		const putNew = await send("PUT", "/key/veg/leek", jsonBody({ color: "green" }));
		const [putNewEnv] = (await putNew.json()) as Envelope[];
		expect(putNewEnv.result).toEqual([{ id: "veg:leek", color: "green" }]);
		const patchNew = await send("PATCH", "/key/veg/potato", jsonBody({ kind: "root" }));
		const [patchNewEnv] = (await patchNew.json()) as Envelope[];
		expect(patchNewEnv.result).toEqual([{ id: "veg:potato", kind: "root" }]);

		// Creating a duplicate id is HTTP 200 with a status:"ERR" AlreadyExists envelope.
		const dup = await send("POST", "/key/veg/carrot", jsonBody({ name: "dupe" }));
		expect(dup.status).toBe(200);
		const [dupEnv] = (await dup.json()) as Envelope[];
		expect(dupEnv).toMatchObject({ status: "ERR", kind: "AlreadyExists" });
		expect(String(dupEnv.result)).toMatch(/already exists/);

		// DELETE /key/:table/:id returns the deleted record (RETURN BEFORE semantics).
		const del = await send("DELETE", "/key/veg/carrot", { ns, db });
		const [delEnv] = (await del.json()) as Envelope[];
		expect(delEnv.result).toEqual([{ id: "veg:carrot", color: "orange", ripe: true }]);

		// DELETE /key/:table clears the table, also returning the deleted rows.
		const delAll = await send("DELETE", "/key/veg", { ns, db });
		const [delAllEnv] = (await delAll.json()) as Envelope[];
		expect(delAllEnv.status).toBe("OK");
		expect(delAllEnv.result as unknown[]).toHaveLength(2);
		const list = await send("GET", "/key/veg", { ns, db });
		const [listEnv] = (await list.json()) as Envelope[];
		expect(listEnv.result).toEqual([]);
	});

	test("POST /signin: root credentials use user/pass keys and yield a token", async () => {
		const ok = await send("POST", "/signin", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ user: "root", pass: "root" }),
		});
		expect(ok.status).toBe(200);
		const body = (await ok.json()) as { code: number; details: string; token: string };
		expect(body.code).toBe(200);
		expect(body.details).toBe("Authentication succeeded");
		// A JWT: three dot-separated base64url segments.
		expect(body.token).toMatch(/^[\w-]+\.[\w-]+\.[\w-]+$/);

		// Surprising: the HTTP endpoint only understands the legacy user/pass keys.
		// The SDK-style {username, password} shape is rejected as bad credentials (401).
		const sdkShape = await send("POST", "/signin", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ username: "root", password: "root" }),
		});
		expect(sdkShape.status).toBe(401);

		const bad = await send("POST", "/signin", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ user: "root", pass: "wrong" }),
		});
		expect(bad.status).toBe(401);
		const badBody = (await bad.json()) as Record<string, unknown>;
		expect(badBody).toMatchObject({ code: 401, details: "Authentication failed" });
	});

	test("POST /signup and /signin: record access uses ns/db/ac keys with top-level variables", async () => {
		const { ns, db } = await freshNsDb();
		await sql(
			`DEFINE TABLE appuser SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
			 DEFINE ACCESS account ON DATABASE TYPE RECORD
				SIGNUP ( CREATE appuser SET email = $email, pass = crypto::argon2::generate($pass) )
				SIGNIN ( SELECT * FROM appuser WHERE email = $email AND crypto::argon2::compare(pass, $pass) );`,
			{ ns, db },
		);

		// Signup variables ride at the top level of the JSON body, next to ns/db/ac.
		const signup = await send("POST", "/signup", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ ns, db, ac: "account", email: "x@y.com", pass: "pw1" }),
		});
		expect(signup.status).toBe(200);
		const signupBody = (await signup.json()) as { code: number; token: string };
		expect(signupBody.code).toBe(200);
		expect(signupBody.token).toMatch(/^[\w-]+\.[\w-]+\.[\w-]+$/);

		const signin = await send("POST", "/signin", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ ns, db, ac: "account", email: "x@y.com", pass: "pw1" }),
		});
		expect(signin.status).toBe(200);
		const { token } = (await signin.json()) as { token: string };

		// The token authenticates /sql via Authorization: Bearer, with $auth populated.
		const who = await sql("RETURN $auth", { auth: `Bearer ${token}`, ns, db });
		expect(who.status).toBe(200);
		const [whoEnv] = (await who.json()) as Envelope[];
		expect(whoEnv.status).toBe("OK");
		expect(String(whoEnv.result)).toMatch(/^appuser:/);

		// Surprising: wrong record-access credentials are 404 ("No record was
		// returned" from the SIGNIN query), not 401.
		const badPass = await send("POST", "/signin", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ ns, db, ac: "account", email: "x@y.com", pass: "wrong" }),
		});
		expect(badPass.status).toBe(404);
		const badPassBody = (await badPass.json()) as Record<string, unknown>;
		expect(String(badPassBody.information)).toMatch(/No record was returned/);

		// A nonexistent access method is a 400 request problem.
		const badAccess = await send("POST", "/signup", {
			auth: null,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ ns, db, ac: "missing", email: "z@y.com", pass: "pw" }),
		});
		expect(badAccess.status).toBe(400);
		const badAccessBody = (await badAccess.json()) as Record<string, unknown>;
		expect(String(badAccessBody.information)).toMatch(/access method does not exist/);
	});

	test("GET /export returns the database as SurrealQL text; unauthenticated export is 403", async () => {
		const { ns, db } = await freshNsDb();
		await sql(
			`DEFINE TABLE exportme SCHEMALESS;
			 CREATE exportme:alpha SET label = 'hello-export';
			 DEFINE ACCESS exp ON DATABASE TYPE RECORD
				SIGNUP ( CREATE exportme ) SIGNIN ( SELECT * FROM exportme );`,
			{ ns, db },
		);

		const res = await send("GET", "/export", { ns, db });
		expect(res.status).toBe(200);
		const text = await res.text();
		expect(text).toContain("OPTION IMPORT;");
		expect(text).toMatch(/DEFINE TABLE exportme/);
		expect(text).toContain("exportme:alpha");
		expect(text).toContain("hello-export");
		// Access JWT keys are redacted in the export text — so a round-tripped
		// import gets literal '[REDACTED]' keys and tokens signed by the source
		// database will not verify on the imported copy.
		expect(text).toMatch(/DEFINE ACCESS exp .* KEY '\[REDACTED\]'/);

		const anon = await send("GET", "/export", { ns, db, auth: null });
		expect(anon.status).toBe(403);
	});

	test("POST /import round-trips an export into a fresh database", async () => {
		const src = await freshNsDb();
		await sql(
			`DEFINE TABLE cargo SCHEMALESS;
			 CREATE cargo:one SET label = 'shipped', qty = 7;
			 CREATE cargo:two SET label = 'pending', qty = 2;`,
			{ ns: src.ns, db: src.db },
		);
		const exported = await (await send("GET", "/export", { ns: src.ns, db: src.db })).text();

		const dst = await freshNsDb();
		const imported = await send("POST", "/import", { ns: dst.ns, db: dst.db, body: exported });
		expect(imported.status).toBe(200);
		// The import response body is an empty statement-result array.
		expect(await imported.json()).toEqual([]);

		const check = await sql("SELECT * FROM cargo ORDER BY id;", { ns: dst.ns, db: dst.db });
		const [checkEnv] = (await check.json()) as Envelope[];
		expect(checkEnv.result).toEqual([
			{ id: "cargo:one", label: "shipped", qty: 7 },
			{ id: "cargo:two", label: "pending", qty: 2 },
		]);
	});

	test("POST /rpc accepts JSON-RPC requests over HTTP", async () => {
		const { ns, db } = await freshNsDb();

		const query = await send("POST", "/rpc", {
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ id: 1, method: "query", params: ["RETURN 40 + 2"] }),
		});
		expect(query.status).toBe(200);
		const queryBody = (await query.json()) as { id: number; result: Envelope[] };
		expect(queryBody.id).toBe(1);
		expect(queryBody.result[0]).toMatchObject({ status: "OK", result: 42 });

		const version = await send("POST", "/rpc", {
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ id: 2, method: "version", params: [] }),
		});
		const versionBody = (await version.json()) as { id: number; result: string };
		expect(versionBody.id).toBe(2);
		expect(versionBody.result).toMatch(/^surrealdb-\d+\.\d+\.\d+/);

		// Unknown methods produce a JSON-RPC error object (code -32601) — still HTTP 200.
		const unknown = await send("POST", "/rpc", {
			ns,
			db,
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ id: 3, method: "bogus", params: [] }),
		});
		expect(unknown.status).toBe(200);
		const unknownBody = (await unknown.json()) as {
			id: number;
			error: { code: number; message: string };
		};
		expect(unknownBody.id).toBe(3);
		expect(unknownBody.error.code).toBe(-32601);
		expect(unknownBody.error.message).toBe("Method not found");
	});
}

// ---------------------------------------------------------------------------
// Per-test-server group: security- and flag-focused cases. Each test spawns
// its OWN server (some with custom startServer args) and stops it in a
// `finally`, so every test sets an explicit timeout to cover server boot.
// ---------------------------------------------------------------------------
{
	function send(
		server: TestServer,
		method: string,
		path: string,
		opts: HttpOptions = {},
	): Promise<Response> {
		return fetch(`${server.httpUrl}${path}`, {
			method,
			headers: buildHeaders(opts),
			body: opts.body,
		});
	}

	function sql(server: TestServer, query: string, opts: HttpOptions = {}): Promise<Response> {
		return send(server, "POST", "/sql", { ...opts, body: query });
	}

	// JSON-RPC-over-HTTP response envelope.
	interface RpcHttpResponse {
		id?: string;
		result?: unknown;
		error?: { code: number; message: string; kind?: string; details?: unknown };
		session?: string;
	}

	/** Create a unique namespace/database pair via root SQL (DDL is not capability-gated). */
	async function defineNsDb(server: TestServer): Promise<{ ns: string; db: string }> {
		const ns = `hp_ns_${process.pid}_${++counter}`;
		const db = `hp_db_${counter}`;
		const res = await sql(
			server,
			`DEFINE NAMESPACE \`${ns}\`; USE NS \`${ns}\`; DEFINE DATABASE \`${db}\`;`,
		);
		expect(res.status).toBe(200);
		const results = (await res.json()) as Envelope[];
		for (const r of results) expect(r.status).toBe("OK");
		return { ns, db };
	}

	/** POST a single JSON-RPC request over HTTP `/rpc` and return the parsed envelope. */
	async function rpcOverHttp(
		server: TestServer,
		body: Record<string, unknown>,
		opts: HttpOptions = {},
	): Promise<RpcHttpResponse> {
		const res = await send(server, "POST", "/rpc", {
			...opts,
			headers: { "Content-Type": "application/json", ...opts.headers },
			body: JSON.stringify(body),
		});
		expect(res.status).toBe(200);
		return (await res.json()) as RpcHttpResponse;
	}

	/** Poll an endpoint until it returns the wanted status, or the attempts run out. */
	async function pollForStatus(
		server: TestServer,
		path: string,
		want: number,
		attempts = 150,
		intervalMs = 100,
	): Promise<boolean> {
		for (let i = 0; i < attempts; i++) {
			try {
				const res = await fetch(`${server.httpUrl}${path}`);
				if (res.status === want) return true;
			} catch {
				// listener not up yet
			}
			await Bun.sleep(intervalMs);
		}
		return false;
	}

	// Headers that request a WebSocket upgrade — used to prove the /sql upgrade path
	// (not just POST) is subject to the same route/subject capability checks.
	const WS_UPGRADE_HEADERS: Record<string, string> = {
		Connection: "Upgrade",
		Upgrade: "websocket",
		"Sec-WebSocket-Version": "13",
		"Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
	};

	// ---------------------------------------------------------------------------
	// /key injection guard
	// ---------------------------------------------------------------------------

	test("POST /key rejects executable bodies (multi-statement / CREATE / function) with no side effect", async () => {
		// Regression for the /key body-injection guard: the REST body must be an
		// inert SurrealQL value (literal/object/array/$param), never an executable
		// statement or function call. Otherwise a deployment that intentionally
		// exposes only the /key route could be used to smuggle arbitrary SurrealQL.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			const jsonHeaders = { "Content-Type": "application/json" };

			// The exact rejection message the tightened parser emits (pin it).
			const REJECT = /expected a single SurrealQL value/;

			// 1. Multi-statement body (the original PoC).
			const multi = await send(server, "POST", "/key/victim_multi", {
				ns,
				db,
				headers: jsonHeaders,
				body: "CREATE pwned:1 SET via = 'key_body'; { name: 'legit_payload' }",
			});
			expect(multi.status).toBe(400);
			expect(String((await multi.json()).information)).toMatch(REJECT);

			// 2. A single executable statement (CREATE) — the case `num_statements() == 1`
			//    alone would not catch.
			const create = await send(server, "POST", "/key/victim_create", {
				ns,
				db,
				headers: jsonHeaders,
				body: "CREATE pwned:2 SET via = 'key_body_create'",
			});
			expect(create.status).toBe(400);
			expect(String((await create.json()).information)).toMatch(REJECT);

			// 3. A single function-call body, rejected even though the fn is side-effect
			//    free — the policy bans the executable shape.
			const fn = await send(server, "POST", "/key/victim_fn", {
				ns,
				db,
				headers: jsonHeaders,
				body: "time::now()",
			});
			expect(fn.status).toBe(400);
			expect(String((await fn.json()).information)).toMatch(REJECT);

			// 4. A normal value (object) body still works — the guard must not regress
			//    legitimate REST usage.
			const okValue = await send(server, "POST", "/key/legit", {
				ns,
				db,
				headers: jsonHeaders,
				body: `{"name": "ok"}`,
			});
			expect(okValue.status).toBe(200);
			const [okEnv] = (await okValue.json()) as Envelope[];
			expect(okEnv.status).toBe("OK");

			// 5. An object referencing a `$param` from the URL query still works.
			const okParam = await send(server, "POST", "/key/legit_params?age=42", {
				ns,
				db,
				headers: jsonHeaders,
				body: "{ age: $age }",
			});
			expect(okParam.status).toBe(200);
			const [paramEnv] = (await okParam.json()) as Envelope[];
			expect(paramEnv.status).toBe("OK");
			expect((paramEnv.result as Array<{ age: number }>)[0].age).toBe(42);

			// No side effect: the `pwned` table must not exist / hold any rows.
			const check = await sql(server, "SELECT * FROM pwned", { ns, db });
			expect(check.status).toBe(200);
			const [checkEnv] = (await check.json()) as Envelope[];
			if (Array.isArray(checkEnv.result)) {
				expect(checkEnv.result).toHaveLength(0);
			}
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// RPC-over-HTTP session security
	// ---------------------------------------------------------------------------

	test("RPC-over-HTTP: the `sessions` method is refused for anonymous and authenticated callers", async () => {
		// The `sessions` method (which would enumerate attached session UUIDs) must
		// never be reachable over HTTP /rpc — not even for an authenticated root
		// caller. It comes back as a JSON-RPC "Method not allowed" error (-32602).
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);

			const anon = await rpcOverHttp(server, { id: "1", method: "sessions" }, { auth: null, ns, db });
			expect(anon.error).toBeDefined();
			expect(anon.result).toBeUndefined();
			expect(anon.error?.code).toBe(-32602);

			const authed = await rpcOverHttp(server, { id: "1b", method: "sessions" }, { ns, db });
			expect(authed.error).toBeDefined();
			expect(authed.result).toBeUndefined();
			expect(authed.error?.code).toBe(-32602);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("RPC-over-HTTP: a leaked session UUID cannot be hijacked (ownership + cross-principal isolation)", async () => {
		// Attaching + signing in a session over HTTP /rpc binds it to the caller's
		// principal. Even a caller who "learns" the session UUID cannot use it
		// without matching credentials, and a *different* authenticated principal
		// cannot target it either. The rejection is always "Session not found",
		// which crucially does not reveal whether the session exists.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			// A second, distinct root principal, for cross-principal isolation.
			const other = await sql(
				server,
				"DEFINE USER other_root ON ROOT PASSWORD 'root' ROLES OWNER",
			);
			expect(other.status).toBe(200);
			const OTHER_AUTH = `Basic ${btoa("other_root:root")}`;

			const victim = "11111111-1111-4111-8111-111111111111";
			const rpc = (body: Record<string, unknown>, opts: HttpOptions = {}) =>
				rpcOverHttp(server, body, { ns, db, ...opts });

			// Legitimate owner attaches and signs in under the victim UUID.
			const attach = await rpc({ id: "a", method: "attach", session: victim });
			expect(attach.error).toBeUndefined();
			const signin = await rpc({
				id: "b",
				method: "signin",
				session: victim,
				params: [{ user: "root", pass: "root" }],
			});
			expect(signin.error).toBeUndefined();

			// Anonymous caller with the UUID in hand — rejected as session_not_found,
			// and it must NOT return a query result.
			const anonHijack = await rpc(
				{ id: "c", method: "query", session: victim, params: ["INFO FOR ROOT"] },
				{ auth: null },
			);
			expect(anonHijack.error).toBeDefined();
			expect(Array.isArray(anonHijack.result)).toBe(false);
			expect(anonHijack.error?.message.toLowerCase()).toContain("session");

			// A different authenticated principal cannot hijack it either.
			const crossPrincipal = await rpc(
				{ id: "d", method: "query", session: victim, params: ["INFO FOR ROOT"] },
				{ auth: OTHER_AUTH },
			);
			expect(crossPrincipal.error).toBeDefined();

			// The legitimate owner keeps working (backwards compatibility).
			const owner = await rpc({
				id: "e",
				method: "query",
				session: victim,
				params: ["INFO FOR ROOT"],
			});
			expect(owner.error).toBeUndefined();
			const ownerEnvs = owner.result as Envelope[];
			expect(ownerEnvs[0].status).toBe("OK");

			// An anonymous query on a random (nonexistent) session is refused the
			// same way — the error shape does not distinguish existent from missing.
			const random = await rpc(
				{
					id: "f",
					method: "query",
					session: "22222222-2222-4222-8222-222222222222",
					params: ["INFO FOR ROOT"],
				},
				{ auth: null },
			);
			expect(random.error).toBeDefined();
		} finally {
			await server.stop();
		}
	}, 30000);

	test("RPC-over-HTTP: concurrent authenticated and anonymous requests stay isolated", async () => {
		// A shared-slot regression would let an unauthenticated request observe an
		// authenticated session. Fire many authed/anon `INFO FOR ROOT` requests at
		// once: every authed one must succeed, every anonymous one must be refused.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			const PAIRS = 12;
			const tasks: Array<Promise<{ authed: boolean; body: RpcHttpResponse }>> = [];
			for (let i = 0; i < PAIRS; i++) {
				tasks.push(
					rpcOverHttp(
						server,
						{ id: `auth-${i}`, method: "query", params: ["INFO FOR ROOT"] },
						{ ns, db },
					).then((body) => ({ authed: true, body })),
				);
				tasks.push(
					rpcOverHttp(
						server,
						{ id: `anon-${i}`, method: "query", params: ["INFO FOR ROOT"] },
						{ auth: null, ns, db },
					).then((body) => ({ authed: false, body })),
				);
			}
			const results = await Promise.all(tasks);
			for (const { authed, body } of results) {
				if (authed) {
					expect(body.error).toBeUndefined();
					const envs = body.result as Envelope[];
					expect(envs[0].status).toBe("OK");
				} else {
					// Anonymous INFO FOR ROOT must never succeed (no session leak).
					expect(body.error).toBeDefined();
					expect(Array.isArray(body.result)).toBe(false);
				}
			}
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// HTTP route capability matrix (--deny-http / --allow-http)
	// ---------------------------------------------------------------------------

	test("--deny-http=sql,export,import denies those routes (incl the /sql WebSocket upgrade) but leaves others", async () => {
		// Denying the SQL HTTP route must ALSO deny the /sql WebSocket upgrade,
		// otherwise the deny is trivially bypassed by switching POST -> WS on the
		// same route.
		const server = await startServer({ args: ["--deny-http=sql,export,import"] });
		try {
			// Denied POST/GET routes -> 403.
			expect((await sql(server, "RETURN 1")).status).toBe(403);
			expect((await send(server, "GET", "/export")).status).toBe(403);
			expect((await send(server, "POST", "/import", { body: "" })).status).toBe(403);

			// The /sql WebSocket upgrade is denied too.
			const wsUpgrade = await send(server, "GET", "/sql", { headers: WS_UPGRADE_HEADERS });
			expect(wsUpgrade.status).toBe(403);

			// Other routes remain reachable (not 403). /key is allowed even though an
			// empty body is a 400 — the point is the ROUTE is not forbidden.
			expect((await send(server, "GET", "/health")).status).toBe(200);
			expect((await send(server, "GET", "/version")).status).toBe(200);
			expect((await send(server, "POST", "/key/test")).status).not.toBe(403);
			expect((await send(server, "POST", "/signin", { body: "{}" })).status).not.toBe(403);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--deny-http --allow-http=rpc,health denies every route except the allow-list", async () => {
		// Bare `--deny-http` (deny all) followed by an explicit allow-list. The bare
		// variadic flag is placed BEFORE another --flag so clap does not swallow the
		// `memory` positional (README harness trap).
		const server = await startServer({ args: ["--deny-http", "--allow-http=rpc,health"] });
		try {
			// Allowed.
			expect((await send(server, "GET", "/health")).status).toBe(200);

			// Denied GET routes.
			for (const route of ["version", "sync", "export"]) {
				expect((await send(server, "GET", `/${route}`)).status).toBe(403);
			}
			// Denied POST routes.
			for (const route of ["sql", "signin", "signup", "key/test", "import"]) {
				expect((await send(server, "POST", `/${route}`, { body: "" })).status).toBe(403);
			}
			// The /sql WebSocket upgrade is denied along with POST /sql.
			const wsUpgrade = await send(server, "GET", "/sql", { headers: WS_UPGRADE_HEADERS });
			expect(wsUpgrade.status).toBe(403);
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// Arbitrary-query capability matrix (--allow/--deny-arbitrary-query)
	// ---------------------------------------------------------------------------

	test("--allow-arbitrary-query=system lets a system (root) caller run a bare value query", async () => {
		const server = await startServer({ args: ["--allow-arbitrary-query=system"] });
		try {
			const res = await sql(server, "123", { ns: "aq_ns", db: "aq_db" });
			expect(res.status).toBe(200);
			const [env] = (await res.json()) as Envelope[];
			expect(env.status).toBe("OK");
			expect(env.result).toBe(123);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--allow-arbitrary-query=record forbids /sql for a system (root) caller (POST and WS upgrade)", async () => {
		// When only `record` users may run arbitrary queries, a system (root) caller
		// is refused the /sql route entirely — and the WS upgrade enforces the same
		// subject-level check as the POST handler.
		const server = await startServer({ args: ["--allow-arbitrary-query=record"] });
		try {
			const res = await sql(server, "123", { ns: "aq_ns", db: "aq_db" });
			expect(res.status).toBe(403);
			expect(String((await res.json()).information)).toContain("The HTTP route 'sql' is forbidden");

			const wsUpgrade = await send(server, "GET", "/sql", { headers: WS_UPGRADE_HEADERS });
			expect(wsUpgrade.status).toBe(403);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--deny-arbitrary-query=* forbids the /sql route entirely (POST and WS upgrade)", async () => {
		const server = await startServer({ args: ["--deny-arbitrary-query=*"] });
		try {
			const res = await sql(server, "123", { ns: "aq_ns", db: "aq_db" });
			expect(res.status).toBe(403);
			expect(String((await res.json()).information)).toContain("The HTTP route 'sql' is forbidden");

			const wsUpgrade = await send(server, "GET", "/sql", { headers: WS_UPGRADE_HEADERS });
			expect(wsUpgrade.status).toBe(403);
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// /signin auth-level inference
	// ---------------------------------------------------------------------------

	test("POST /signin infers the auth level from ns/db and rejects credentials at the wrong level", async () => {
		// The level a `/signin` targets is inferred from which of ns/db are present:
		//   ns+db -> DB level, ns-only -> NS level, neither -> ROOT level.
		// A credential valid at one level fails when the body implies a deeper level.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			// Define one user at each level.
			expect(
				(await sql(server, "DEFINE USER user_db ON DB PASSWORD 'pass_db'", { ns, db })).status,
			).toBe(200);
			expect(
				(await sql(server, "DEFINE USER user_ns ON NS PASSWORD 'pass_ns'", { ns })).status,
			).toBe(200);
			expect((await sql(server, "DEFINE USER user_root ON ROOT PASSWORD 'pass_root'")).status).toBe(
				200,
			);

			const signin = (payload: Record<string, unknown>) =>
				send(server, "POST", "/signin", {
					auth: null,
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify(payload),
				});

			// DB user, ns+db -> DB level -> 200.
			expect((await signin({ ns, db, user: "user_db", pass: "pass_db" })).status).toBe(200);
			// DB user, wrong password -> 401.
			expect((await signin({ ns, db, user: "user_db", pass: "wrong" })).status).toBe(401);

			// NS user with ns+db -> auth attempted at DB level -> 401.
			expect((await signin({ ns, db, user: "user_ns", pass: "pass_ns" })).status).toBe(401);
			// NS user with ns only -> NS level -> 200.
			expect((await signin({ ns, user: "user_ns", pass: "pass_ns" })).status).toBe(200);

			// ROOT user with ns+db -> DB level -> 401.
			expect((await signin({ ns, db, user: "user_root", pass: "pass_root" })).status).toBe(401);
			// ROOT user with ns only -> NS level -> 401.
			expect((await signin({ ns, user: "user_root", pass: "pass_root" })).status).toBe(401);
			// ROOT user with neither -> ROOT level -> 200.
			expect((await signin({ user: "user_root", pass: "pass_root" })).status).toBe(200);
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// --client-ip -> $session.ip
	// ---------------------------------------------------------------------------

	/** Fetch `RETURN session::ip()` over /sql with root auth and the given headers. */
	async function fetchSessionIp(
		server: TestServer,
		extraHeaders: Record<string, string>,
	): Promise<unknown> {
		const res = await sql(server, "RETURN session::ip()", { headers: extraHeaders });
		expect(res.status).toBe(200);
		const [env] = (await res.json()) as Envelope[];
		expect(env.status).toBe("OK");
		return env.result;
	}

	test("--client-ip socket (default) reports the socket peer and ignores forwarding headers", async () => {
		// Default mode: the raw peer address wins. The test client connects from
		// 127.0.0.1, so a spoofed X-Forwarded-For must be ignored.
		const server = await startServer();
		try {
			const ip = await fetchSessionIp(server, { "X-Forwarded-For": "203.0.113.7" });
			expect(ip).toBe("127.0.0.1");
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--client-ip none never attaches an IP even when headers are present", async () => {
		const server = await startServer({ args: ["--client-ip=none"] });
		try {
			const ip = await fetchSessionIp(server, { "X-Forwarded-For": "203.0.113.7" });
			// session::ip() is NONE, serialised as JSON null.
			expect(ip).toBeNull();
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--client-ip forwarding-header modes map the configured header into $session.ip", async () => {
		// The mode is fixed at startup, so every sub-scenario needs its own server.
		// Run them sequentially (one server at a time) to keep this file's peak
		// process count low when the whole suite runs in parallel.
		async function ipForMode(
			mode: string,
			headers: Record<string, string>,
		): Promise<unknown> {
			const server = await startServer({ args: [`--client-ip=${mode}`] });
			try {
				return await fetchSessionIp(server, headers);
			} finally {
				await server.stop();
			}
		}

		expect(await ipForMode("X-Forwarded-For", { "X-Forwarded-For": "203.0.113.7" })).toBe(
			"203.0.113.7",
		);
		// Proxy chain: the raw header value is stored verbatim, not split.
		expect(
			await ipForMode("X-Forwarded-For", {
				"X-Forwarded-For": "203.0.113.7, 198.51.100.1, 192.0.2.1",
			}),
		).toBe("203.0.113.7, 198.51.100.1, 192.0.2.1");
		// No header -> nothing to extract -> NONE (null).
		expect(await ipForMode("X-Forwarded-For", {})).toBeNull();
		expect(await ipForMode("X-Real-IP", { "X-Real-IP": "198.51.100.42" })).toBe("198.51.100.42");
		expect(await ipForMode("CF-Connecting-IP", { "CF-Connecting-IP": "203.0.113.10" })).toBe(
			"203.0.113.10",
		);
		expect(await ipForMode("Fly-Client-IP", { "Fly-Client-IP": "203.0.113.20" })).toBe(
			"203.0.113.20",
		);
		expect(await ipForMode("True-Client-IP", { "True-Client-IP": "203.0.113.30" })).toBe(
			"203.0.113.30",
		);
		// RFC 7239 Forwarded: parse the first element's for= identifier.
		expect(
			await ipForMode("Forwarded", { Forwarded: "for=192.0.2.43;by=203.0.113.43, for=198.51.100.17" }),
		).toBe("192.0.2.43");
		// A Forwarded header without a for= parameter yields no IP.
		expect(await ipForMode("Forwarded", { Forwarded: "by=203.0.113.43;proto=http" })).toBeNull();
	}, 120000);

	// ---------------------------------------------------------------------------
	// Readiness gate
	// ---------------------------------------------------------------------------

	test("/ready is 503 until a startup import completes; queries are gated meanwhile", async () => {
		// A slow startup import keeps the instance "starting": the listener binds
		// (so /status and /health answer) but /ready and query endpoints are gated
		// with 503 until the import finishes.
		const dir = mkdtempSync(join(tmpdir(), "hp-readiness-"));
		const importFile = join(dir, "readiness_import.surql");
		writeFileSync(importFile, "SLEEP 5s;");
		const server = await startServer({ noWait: true, args: [`--import-file=${importFile}`] });
		try {
			// The listener binds before the import completes.
			expect(await pollForStatus(server, "/status", 200)).toBe(true);

			// While the import runs: liveness/reachability OK, readiness gated.
			expect((await fetch(`${server.httpUrl}/status`)).status).toBe(200);
			expect((await fetch(`${server.httpUrl}/health`)).status).toBe(200);
			expect((await fetch(`${server.httpUrl}/ready`)).status).toBe(503);
			// Queries are gated with 503 during the import.
			expect((await sql(server, "INFO FOR ROOT")).status).toBe(503);

			// Once the import completes the instance reports ready and serves queries.
			expect(await pollForStatus(server, "/ready", 200)).toBe(true);
			expect((await fetch(`${server.httpUrl}/health`)).status).toBe(200);
			expect((await sql(server, "INFO FOR ROOT")).status).toBe(200);
		} finally {
			await server.stop();
			rmSync(dir, { recursive: true, force: true });
		}
	}, 60000);

	test("a server with no startup import is ready the moment it binds", async () => {
		// No deferred work -> readiness is set synchronously before binding, so a
		// client that connects immediately (as the SDKs do) is not gated with 503.
		const server = await startServer({ noWait: true });
		try {
			expect(await pollForStatus(server, "/status", 200)).toBe(true);
			// Ready and query-serving the instant it binds.
			expect((await fetch(`${server.httpUrl}/ready`)).status).toBe(200);
			expect((await sql(server, "INFO FOR ROOT")).status).toBe(200);
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// Session-id header + identification headers + /sync
	// ---------------------------------------------------------------------------

	test("the surreal-id header sets the session id; a non-UUID id is rejected with 401", async () => {
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);

			// No header -> a random UUIDv4 session id (contains the "-4" version marker).
			const auto = await sql(server, "SELECT VALUE id FROM $session", { ns, db });
			expect(auto.status).toBe(200);
			const [autoEnv] = (await auto.json()) as Envelope[];
			expect(String((autoEnv.result as string[])[0])).toContain("-4");

			// A valid UUID header pins the session id.
			const pinned = await sql(server, "SELECT VALUE id FROM $session", {
				ns,
				db,
				headers: { "surreal-id": "00000000-0000-4000-8000-000000000000" },
			});
			expect(pinned.status).toBe(200);
			const [pinnedEnv] = (await pinned.json()) as Envelope[];
			expect((pinnedEnv.result as string[])[0]).toBe("00000000-0000-4000-8000-000000000000");

			// A non-UUID surreal-id is rejected outright with 401.
			const bad = await sql(server, "SELECT VALUE id FROM $session", {
				ns,
				db,
				headers: { "surreal-id": "123" },
			});
			expect(bad.status).toBe(401);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--no-identification-headers suppresses the server and surreal-version response headers", async () => {
		// Default: both headers are advertised. With the flag: neither appears.
		const withHeaders = await startServer();
		try {
			const res = await fetch(`${withHeaders.httpUrl}/health`);
			expect(res.headers.get("server")).not.toBeNull();
			expect(res.headers.get("surreal-version")).not.toBeNull();
		} finally {
			await withHeaders.stop();
		}

		const suppressed = await startServer({ args: ["--no-identification-headers"] });
		try {
			const res = await fetch(`${suppressed.httpUrl}/health`);
			expect(res.headers.get("server")).toBeNull();
			expect(res.headers.get("surreal-version")).toBeNull();
		} finally {
			await suppressed.stop();
		}
	}, 45000);

	test('GET /sync returns "Save"; POST /sync returns "Load"', async () => {
		const server = await startServer();
		try {
			const get = await send(server, "GET", "/sync", { auth: null });
			expect(get.status).toBe(200);
			expect(await get.text()).toBe("Save");

			const post = await send(server, "POST", "/sync", { auth: null, body: "" });
			expect(post.status).toBe(200);
			expect(await post.text()).toBe("Load");
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// Basic-auth level scoping + bearer token ns/db
	// ---------------------------------------------------------------------------

	test("basic-auth level scoping: an NS user cannot read ROOT, and DB auth without a namespace is 401", async () => {
		// Basic-auth level is selected by the surreal-auth-ns / surreal-auth-db
		// headers (distinct from the session-selecting surreal-ns / surreal-db).
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			expect((await sql(server, "DEFINE USER nsu ON NS PASSWORD 'p' ROLES OWNER", { ns })).status).toBe(
				200,
			);
			expect(
				(await sql(server, "DEFINE USER dbu ON DB PASSWORD 'p' ROLES OWNER", { ns, db })).status,
			).toBe(200);

			const NS_AUTH = `Basic ${btoa("nsu:p")}`;
			const DB_AUTH = `Basic ${btoa("dbu:p")}`;

			// NS user authenticated at NS level can read its namespace.
			const nsOk = await sql(server, "INFO FOR NS", {
				auth: NS_AUTH,
				headers: { "surreal-ns": ns, "surreal-auth-ns": ns },
			});
			expect(nsOk.status).toBe(200);
			const [nsOkEnv] = (await nsOk.json()) as Envelope[];
			expect(nsOkEnv.status).toBe("OK");

			// The same NS user is not permitted to read ROOT: the request is 200 but
			// the statement fails with an IAM permissions error.
			const nsDenied = await sql(server, "INFO FOR ROOT", {
				auth: NS_AUTH,
				headers: { "surreal-auth-ns": ns },
			});
			expect(nsDenied.status).toBe(200);
			const [nsDeniedEnv] = (await nsDenied.json()) as Envelope[];
			expect(nsDeniedEnv.status).toBe("ERR");
			expect(String(nsDeniedEnv.result)).toBe(
				"IAM error: Not enough permissions to perform this action",
			);

			// A DB-level credential presented WITHOUT a namespace (only surreal-auth-db)
			// is an incomplete auth level and is rejected with 401.
			const dbNoNs = await sql(server, "INFO FOR DB", {
				auth: DB_AUTH,
				headers: { "surreal-auth-db": db },
			});
			expect(dbNoNs.status).toBe(401);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("a bearer token carries its own ns/db, overriding the request headers", async () => {
		// A JWT issued for a DB user embeds that ns/db. Presenting it with DIFFERENT
		// surreal-ns / surreal-db headers must not move the session: session::ns()/
		// session::db() reflect the token's ns/db, not the headers'.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);
			expect(
				(await sql(server, "DEFINE USER bu ON DB PASSWORD 'p' ROLES OWNER", { ns, db })).status,
			).toBe(200);

			// Sign in the DB user (legacy user/pass keys) to get a token.
			const signinRes = await send(server, "POST", "/signin", {
				auth: null,
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ ns, db, user: "bu", pass: "p" }),
			});
			expect(signinRes.status).toBe(200);
			const { token } = (await signinRes.json()) as { token: string };
			expect(token).toMatch(/^[\w-]+\.[\w-]+\.[\w-]+$/);

			// Use the token but point the headers at a different (nonexistent) ns/db.
			const res = await sql(server, "RETURN [session::ns(), session::db()]", {
				auth: `Bearer ${token}`,
				headers: { "surreal-ns": "OTHER_NS", "surreal-db": "OTHER_DB" },
			});
			expect(res.status).toBe(200);
			const [env] = (await res.json()) as Envelope[];
			expect(env.status).toBe("OK");
			expect(env.result).toEqual([ns, db]);
		} finally {
			await server.stop();
		}
	}, 30000);

	// ---------------------------------------------------------------------------
	// Custom API endpoint: /api/{ns}/{db}/{*path} (DEFINE API)
	// ---------------------------------------------------------------------------

	test("/api/{ns}/{db}/{path} serves a DEFINE API handler; the HTTP body is the handler body itself", async () => {
		// The custom-endpoint route takes ns/db from the URL path (not headers) and
		// dispatches to a matching DEFINE API handler for that method.
		const server = await startServer();
		try {
			const { ns, db } = await defineNsDb(server);

			// A handler whose `body` is a structured value must opt into serialization
			// with the api::res::body middleware; the HTTP layer only accepts a None,
			// bytes, or string body straight from the handler.
			await sql(
				server,
				`DEFINE API "/hello" FOR get MIDDLEWARE api::res::body("json")
					THEN { { status: 200, body: { message: "Hello, World!" } } };`,
				{ ns, db },
			);

			const res = await send(server, "GET", `/api/${ns}/${db}/hello`);
			expect(res.status).toBe(200);
			expect(res.headers.get("content-type")).toMatch(/^application\/json/);
			// The response body is the handler's `body` value directly — NOT wrapped in
			// the { status, body, headers } object the handler returns.
			expect(await res.json()).toEqual({ message: "Hello, World!" });

			// Surprising: without a serializing middleware, a structured (object) body
			// is a 500 — the HTTP layer cannot encode it and refuses the response.
			await sql(
				server,
				`DEFINE API "/raw" FOR get THEN { { status: 200, body: { nope: true } } };`,
				{ ns, db },
			);
			const raw = await send(server, "GET", `/api/${ns}/${db}/raw`);
			expect(raw.status).toBe(500);
			expect(String((await raw.json()).description)).toMatch(
				/HTTP API response body must be None, bytes, or string/,
			);

			// A string body needs no middleware: it is returned verbatim as octet-stream.
			await sql(
				server,
				`DEFINE API "/str" FOR get THEN { { status: 200, body: "plain text hi" } };`,
				{ ns, db },
			);
			const str = await send(server, "GET", `/api/${ns}/${db}/str`);
			expect(str.status).toBe(200);
			expect(await str.text()).toBe("plain text hi");

			// Method mismatch (handler is FOR get, called with POST) is a 404, not a 405.
			const wrongMethod = await send(server, "POST", `/api/${ns}/${db}/hello`);
			expect(wrongMethod.status).toBe(404);
		} finally {
			await server.stop();
		}
	}, 30000);

	test("/api routes are per-(ns,db): a handler defined in one tenant is 404 in another", async () => {
		// The API handler is scoped to the ns/db it was defined in. Requesting the
		// same path under a different (existing) ns/db must not find it.
		const server = await startServer();
		try {
			const one = await defineNsDb(server);
			const two = await defineNsDb(server);

			await sql(
				server,
				`DEFINE API "/hello" FOR get MIDDLEWARE api::res::body("json")
					THEN { { status: 200, body: { message: "Hello, World!" } } };`,
				{ ns: one.ns, db: one.db },
			);

			// Defined tenant: found.
			const found = await send(server, "GET", `/api/${one.ns}/${one.db}/hello`);
			expect(found.status).toBe(200);

			// Different existing tenant where it was NOT defined: not found.
			const cross = await send(server, "GET", `/api/${two.ns}/${two.db}/hello`);
			expect(cross.status).toBe(404);
			expect(await cross.text()).toBe("Not found");
		} finally {
			await server.stop();
		}
	}, 30000);

	test("--deny-http=api forbids the custom-endpoint route while leaving other routes reachable", async () => {
		// The /api route is default-ON and gated by RouteTarget::Api; denying it
		// yields the standard forbidden-route 403, but /sql and /health still serve.
		const server = await startServer({ args: ["--deny-http=api"] });
		try {
			const { ns, db } = await defineNsDb(server);
			await sql(
				server,
				`DEFINE API "/hello" FOR get MIDDLEWARE api::res::body("json")
					THEN { { status: 200, body: { message: "Hello, World!" } } };`,
				{ ns, db },
			);

			const denied = await send(server, "GET", `/api/${ns}/${db}/hello`);
			expect(denied.status).toBe(403);
			expect(String((await denied.json()).information)).toBe("The HTTP route 'api' is forbidden");

			// Other routes on the same server are unaffected.
			const query = await sql(server, "RETURN 1", { ns, db });
			expect(query.status).toBe(200);
			const [queryEnv] = (await query.json()) as Envelope[];
			expect(queryEnv).toMatchObject({ status: "OK", result: 1 });
			expect((await send(server, "GET", "/health")).status).toBe(200);
		} finally {
			await server.stop();
		}
	}, 30000);
}
