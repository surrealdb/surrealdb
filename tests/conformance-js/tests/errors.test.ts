// Cross-transport error-shape consistency.
//
// The same logical failure is issued over every remote surface SurrealDB
// exposes, and the shape each one reports is pinned side by side. The transports
// deliberately DIVERGE — different HTTP statuses, envelope vs problem-document vs
// GraphQL `errors[]`, and different message text for one underlying cause — and
// the value of this file is documenting that contract, not asserting the shapes
// are identical. Each divergence carries a comment explaining it.
//
// Transports:
//   - WebSocket JSON-RPC (RpcClient): the raw { id, result | error } envelope,
//     with exact JSON-RPC codes and kinds.
//   - HTTP POST /sql: the per-statement envelope array, or a JSON problem
//     document / plain-text body for whole-request failures.
//   - GraphQL POST /graphql: always HTTP 200 for execution/validation, with the
//     failure in the `errors[]` array; transport-level failures are 4xx.
//   - ISO GQL POST /gql: shares /sql's session plumbing and envelope array, but
//     surfaces parse/route failures as a JSON problem document.
//
// Failures covered: (a) unknown method / parse error, (b) missing namespace or
// database, (c) permission denied for an anonymous caller, (d) bad credentials.
//
// One shared server; each test uses a fresh namespace/database (headers select,
// never create). GraphQL requires a per-database `DEFINE CONFIG GRAPHQL AUTO`
// plus at least one table, so the fixture always provisions both.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { RpcClient, rootClient, startServer, type TestServer } from "../src/harness";
import type { Surreal } from "surrealdb";

const ROOT_AUTH = `Basic ${btoa("root:root")}`;
const BAD_AUTH = `Basic ${btoa("root:wrong")}`;

// Per-statement envelope shared by /sql and /gql.
interface Envelope {
	result: unknown;
	status: "OK" | "ERR";
	time: string;
	type: null;
	kind?: string;
	details?: unknown;
}
// JSON problem document returned by whole-request HTTP failures.
interface Problem {
	code: number;
	details: string;
	description: string;
	information: string;
}
interface GqlBody {
	data: unknown;
	errors?: Array<{ message: string; locations?: Array<{ line: number; column: number }> }>;
}

interface Fetched {
	status: number;
	ct: string | null;
	/** Parsed JSON body, or the raw text if the body is not JSON. */
	body: any;
}

interface HttpOpts {
	auth?: string | null;
	/** Omit the surreal-ns header entirely. */
	noNs?: boolean;
	/** Omit the surreal-db header entirely. */
	noDb?: boolean;
}

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

/**
 * A fresh namespace/database with GraphQL configured and two tables: `coin`
 * (default restrictive PERMISSIONS, one row) for the anonymous-denial cases, and
 * `person` (PERMISSIONS FULL, one row) for the parse/missing-context cases.
 */
async function fixture(): Promise<{ db: Surreal; ns: string; dbName: string }> {
	const { db, namespace: ns, database: dbName } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE coin SCHEMAFULL;
		DEFINE FIELD label ON coin TYPE string;
		CREATE coin:one SET label = 'penny';
		DEFINE TABLE person SCHEMAFULL PERMISSIONS FULL;
		DEFINE FIELD name ON person TYPE string;
		CREATE person:one SET name = 'a';
	`);
	return { db, ns, dbName };
}

async function read(res: Response): Promise<Fetched> {
	const text = await res.text();
	let body: unknown;
	try {
		body = JSON.parse(text);
	} catch {
		body = text;
	}
	return { status: res.status, ct: res.headers.get("content-type"), body };
}

function headers(ns: string, db: string, opts: HttpOpts): Record<string, string> {
	const h: Record<string, string> = { Accept: "application/json" };
	if (opts.auth !== null) h.Authorization = opts.auth ?? ROOT_AUTH;
	if (!opts.noNs) h["surreal-ns"] = ns;
	if (!opts.noDb) h["surreal-db"] = db;
	return h;
}

/** HTTP POST /sql (body is the raw SurrealQL). */
async function sql(ns: string, db: string, query: string, opts: HttpOpts = {}): Promise<Fetched> {
	return read(
		await fetch(`${server.httpUrl}/sql`, { method: "POST", headers: headers(ns, db, opts), body: query }),
	);
}

/** HTTP POST /graphql (JSON { query } body; ns/db ride as headers). */
async function graphql(ns: string, db: string, query: string, opts: HttpOpts = {}): Promise<Fetched> {
	const h = headers(ns, db, opts);
	h["Content-Type"] = "application/json";
	return read(
		await fetch(`${server.httpUrl}/graphql`, { method: "POST", headers: h, body: JSON.stringify({ query }) }),
	);
}

/** HTTP POST /gql — ISO GQL (body is the raw MATCH query). */
async function isogql(ns: string, db: string, query: string, opts: HttpOpts = {}): Promise<Fetched> {
	return read(
		await fetch(`${server.httpUrl}/gql`, { method: "POST", headers: headers(ns, db, opts), body: query }),
	);
}

// ---------------------------------------------------------------------------
// (a) Unknown method / parse error
// ---------------------------------------------------------------------------

test("unknown JSON-RPC method is -32601 'Method not found' over both WebSocket and HTTP /rpc", async () => {
	// "Unknown method" only exists on the JSON-RPC method-dispatch surfaces (the
	// WebSocket protocol and HTTP /rpc). /sql, /graphql and /gql have no method
	// name to miss. Both RPC surfaces agree exactly: -32601 "Method not found".
	const { db, ns, dbName } = await fixture();
	const rpc = await RpcClient.connect(server);
	try {
		await rpc.signinRoot();
		const ws = await rpc.rpc("bogus", []);
		expect(ws.result).toBeUndefined();
		expect(ws.error?.code).toBe(-32601);
		expect(ws.error?.message).toBe("Method not found");

		const httpRes = await fetch(`${server.httpUrl}/rpc`, {
			method: "POST",
			headers: { Authorization: ROOT_AUTH, "surreal-ns": ns, "surreal-db": dbName, "Content-Type": "application/json" },
			body: JSON.stringify({ id: 1, method: "bogus", params: [] }),
		});
		expect(httpRes.status).toBe(200);
		const httpBody = (await httpRes.json()) as { error: { code: number; message: string } };
		expect(httpBody.error.code).toBe(-32601);
		expect(httpBody.error.message).toBe("Method not found");
	} finally {
		await rpc.close();
		await db.close();
	}
}, 30000);

test("a malformed query reports a parse error with a transport-specific shape", async () => {
	const { db, ns, dbName } = await fixture();
	const rpc = await RpcClient.connect(server);
	try {
		await rpc.signinRoot();
		await rpc.use(ns, dbName);

		// WebSocket: a parse failure fails the WHOLE request as a top-level RPC
		// error (-32000 "Validation"), NOT a per-statement ERR envelope — unlike
		// the runtime errors below, which arrive inside the result array.
		const ws = await rpc.rpc("query", ["SELECT * FROM person WHERE"]);
		expect(ws.result).toBeUndefined();
		expect(ws.error?.code).toBe(-32000);
		expect(ws.error?.kind).toBe("Validation");
		expect(ws.error?.message).toContain("Parse error:");
		expect(ws.error?.message).toContain("expected an expression");

		// HTTP /sql: a parse failure is a whole-request HTTP 400 problem document
		// (not the 200 + per-statement ERR array that runtime errors get).
		const s = await sql(ns, dbName, "SELECT * FROM person WHERE");
		expect(s.status).toBe(400);
		expect(s.ct).toMatch(/^application\/json/);
		const sBody = s.body as Problem;
		expect(sBody.code).toBe(400);
		expect(sBody.details).toBe("Request problems detected");
		expect(sBody.information).toContain("Parse error:");

		// ISO /gql: same 400 problem-document shape as /sql — the GQL parse error
		// is rendered into `information` verbatim, "Parse error:" prefix included.
		const g = await isogql(ns, dbName, "MATCH (");
		expect(g.status).toBe(400);
		const gBody = g.body as Problem;
		expect(gBody.code).toBe(400);
		expect(gBody.details).toBe("Request problems detected");
		expect(gBody.information).toContain("Parse error:");
		expect(gBody.information).toContain("expected");

		// GraphQL: diverges hardest. A malformed GraphQL document is still HTTP
		// 200 with `data: null` and the failure in `errors[]`. The message is a
		// bare pest-grammar trace carrying `locations` — it does NOT use the
		// "Parse error:" prefix the SurrealQL/GQL transports emit.
		const gql = await graphql(ns, dbName, "{ this is not valid");
		expect(gql.status).toBe(200);
		const gqlBody = gql.body as GqlBody;
		expect(gqlBody.data).toBeNull();
		expect(gqlBody.errors).toHaveLength(1);
		expect(gqlBody.errors![0].message).toContain("expected");
		expect(gqlBody.errors![0].message).not.toContain("Parse error:");
		expect(gqlBody.errors![0].locations).toBeDefined();
	} finally {
		await rpc.close();
		await db.close();
	}
}, 30000);

// ---------------------------------------------------------------------------
// (b) Missing namespace / database
// ---------------------------------------------------------------------------

test("a query with no namespace/database selected reports it per-transport", async () => {
	const { db, ns, dbName } = await fixture();
	// A root connection that never calls `use` — authenticated but with no
	// namespace/database selected.
	const rpc = await RpcClient.connect(server);
	try {
		await rpc.signinRoot();

		// WebSocket: NOT a top-level error. The statement runs and fails as a
		// per-statement ERR envelope, kind "Validation", inside the result array.
		const ws = await rpc.rpc("query", ["SELECT * FROM person"]);
		expect(ws.error).toBeUndefined();
		const wsEnv = (ws.result as Envelope[])[0];
		expect(wsEnv.status).toBe("ERR");
		expect(wsEnv.kind).toBe("Validation");
		expect(wsEnv.result).toBe("Specify a namespace to use");

		// HTTP /sql: identical per-statement ERR, and the request itself is HTTP
		// 200 — a missing namespace is a statement outcome, not a transport error.
		const s = await sql(ns, dbName, "SELECT * FROM person", { noNs: true, noDb: true });
		expect(s.status).toBe(200);
		const sEnv = (s.body as Envelope[])[0];
		expect(sEnv.status).toBe("ERR");
		expect(sEnv.kind).toBe("Validation");
		expect(sEnv.result).toBe("Specify a namespace to use");

		// ISO /gql: same 200 + per-statement Validation ERR as /sql.
		const g = await isogql(ns, dbName, "MATCH (n:person) RETURN n.name", { noNs: true, noDb: true });
		expect(g.status).toBe(200);
		const gEnv = (g.body as Envelope[])[0];
		expect(gEnv.status).toBe("ERR");
		expect(gEnv.kind).toBe("Validation");
		expect(gEnv.result).toBe("Specify a namespace to use");

		// GraphQL: diverges. It resolves ns/db from headers before building the
		// schema, so a missing header is a transport-level HTTP 400 with a
		// header-specific message — never reaching per-statement execution. The
		// db-missing message names the other header.
		const noNs = await graphql(ns, dbName, "{ __typename }", { noNs: true });
		expect(noNs.status).toBe(400);
		expect((noNs.body as GqlBody).errors![0].message).toBe(
			"No namespace specified. Set the `surreal-ns` header on the request.",
		);
		const noDb = await graphql(ns, dbName, "{ __typename }", { noDb: true });
		expect(noDb.status).toBe(400);
		expect((noDb.body as GqlBody).errors![0].message).toBe(
			"No database specified. Set the `surreal-db` header on the request.",
		);
	} finally {
		await rpc.close();
		await db.close();
	}
}, 30000);

// ---------------------------------------------------------------------------
// (c) Permission denied (anonymous / guest)
// ---------------------------------------------------------------------------

test("an anonymous caller reading a permissioned table is denied differently per transport", async () => {
	const { db, ns, dbName } = await fixture();
	// A connected but unauthenticated (anonymous) WebSocket session.
	const rpc = await RpcClient.connect(server);
	try {
		await rpc.use(ns, dbName);

		// WebSocket: a top-level RPC error, code -32002, kind "NotAllowed".
		const ws = await rpc.rpc("query", ["SELECT * FROM coin"]);
		expect(ws.result).toBeUndefined();
		expect(ws.error?.code).toBe(-32002);
		expect(ws.error?.kind).toBe("NotAllowed");
		expect(ws.error?.message).toBe(
			"Anonymous access not allowed: Not enough permissions to perform this action",
		);

		// HTTP /sql: the anonymous query is refused at the route with HTTP 403 and
		// a JSON problem document — the same denial reason in `information`.
		const s = await sql(ns, dbName, "SELECT * FROM coin", { auth: null });
		expect(s.status).toBe(403);
		const sBody = s.body as Problem;
		expect(sBody.code).toBe(403);
		expect(sBody.details).toBe("Forbidden");
		expect(sBody.information).toBe(
			"Anonymous access not allowed: Not enough permissions to perform this action",
		);

		// ISO /gql: identical route-level 403 problem document as /sql.
		const g = await isogql(ns, dbName, "MATCH (n:coin) RETURN n.label", { auth: null });
		expect(g.status).toBe(403);
		const gBody = g.body as Problem;
		expect(gBody.code).toBe(403);
		expect(gBody.details).toBe("Forbidden");
		expect(gBody.information).toBe(
			"Anonymous access not allowed: Not enough permissions to perform this action",
		);

		// GraphQL: diverges. The request is accepted (HTTP 200) and the denial is
		// reached in the resolver, then surfaced in `errors[]` — surprisingly
		// wrapped as an "Internal Error", not a 401/403 and not tagged as a
		// permission failure at the transport level.
		const gql = await graphql(ns, dbName, "{ coins { label } }", { auth: null });
		expect(gql.status).toBe(200);
		const gqlBody = gql.body as GqlBody;
		expect(gqlBody.data).toBeNull();
		expect(gqlBody.errors![0].message).toBe(
			"Internal Error: Failed to execute query plan: Anonymous access not allowed: Not enough permissions to perform this action",
		);
	} finally {
		await rpc.close();
		await db.close();
	}
}, 30000);

// ---------------------------------------------------------------------------
// (d) Unauthenticated (bad credentials)
// ---------------------------------------------------------------------------

test("bad credentials fail with a generic RPC message but a more specific HTTP body", async () => {
	const { db, ns, dbName } = await fixture();
	const rpc = await RpcClient.connect(server);
	try {
		// WebSocket: a wrong-password signin is a top-level -32002 "NotAllowed"
		// with the GENERIC message "There was a problem with authentication" — the
		// RPC auth path deliberately does not disclose why.
		const ws = await rpc.rpc("signin", [{ user: "root", pass: "wrong" }]);
		expect(ws.result).toBeUndefined();
		expect(ws.error?.code).toBe(-32002);
		expect(ws.error?.kind).toBe("NotAllowed");
		expect(ws.error?.message).toBe("There was a problem with authentication");

		// HTTP /sql: bad Basic auth is HTTP 401 with a BARE plain-text body — and
		// it leaks the more specific "The password did not verify", even though
		// Accept: application/json was sent (no JSON problem document here).
		const s = await sql(ns, dbName, "RETURN 1", { auth: BAD_AUTH });
		expect(s.status).toBe(401);
		expect(s.body).toBe("The password did not verify");

		// GraphQL: transport-level Basic auth check fires before the GraphQL
		// layer, so it matches /sql exactly — 401, plain text, same message.
		const gql = await graphql(ns, dbName, "{ __typename }", { auth: BAD_AUTH });
		expect(gql.status).toBe(401);
		expect(gql.body).toBe("The password did not verify");

		// ISO /gql: same transport-level 401 plain-text body as /sql and /graphql.
		const g = await isogql(ns, dbName, "MATCH (n:coin) RETURN n.label", { auth: BAD_AUTH });
		expect(g.status).toBe(401);
		expect(g.body).toBe("The password did not verify");
	} finally {
		await rpc.close();
		await db.close();
	}
}, 30000);
