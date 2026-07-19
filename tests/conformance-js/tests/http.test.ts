// HTTP endpoint conformance — raw fetch against the server, no SDK.
// Pins observed behavior of the server: /health, /version, /sql,
// /key/:table[/:id], /signin, /signup, /export, /import, and RPC-over-HTTP.
//
// Header/API facts:
// - Namespace/database are selected with `surreal-ns` / `surreal-db` headers.
// - Basic auth (`Authorization: Basic ...`) or `Bearer <jwt>` authenticate.
// - Headers select but never create the namespace/database.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { startServer, type TestServer } from "../src/harness";

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

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
