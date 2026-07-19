// ISO GQL (ISO/IEC 39075, Cypher-like) dialect over the wire.
//
// GQL is enabled by default; `--allow-experimental=gql` remains a harmless
// no-op accepted for backwards compatibility.
//
// Three remote surfaces exist:
//   1. HTTP `POST /gql` — same session/auth plumbing as `/sql`, returns the
//      same per-statement envelope array; binds $vars from URL query params.
//   2. RPC method `gql` (params: [query, vars?]) — works over POST /rpc;
//      the JS SDK (2.0.4) exposes NO way to call it (no .gql(), no raw
//      .rpc()), so these tests drive it with fetch.
//   3. `eval::gql(query, vars?)` inside normal SurrealQL — reachable through
//      the SDK's query(), STILL gated by `--allow-eval-query` (denied for
//      every subject by default, even under --allow-all — that gate is
//      independent of the removed GQL experimental gate).
//
// The SurrealQL parser itself never accepts MATCH — the dialects are fully
// separate.

import { afterAll, beforeAll, expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

/** Server with eval::gql enabled (GQL itself needs no flag). */
let gqlServer: TestServer;
/** Server with stock capabilities — GQL works, but eval::gql is denied. */
let plainServer: TestServer;

beforeAll(async () => {
	[gqlServer, plainServer] = await Promise.all([
		startServer({ args: ["--allow-eval-query=system"] }),
		startServer(),
	]);
});

afterAll(async () => {
	await Promise.all([gqlServer.stop(), plainServer.stop()]);
});

async function rejects(p: Promise<unknown>): Promise<Error> {
	try {
		await p;
	} catch (e) {
		return e as Error;
	}
	throw new Error("expected promise to reject, but it resolved");
}

function rootHeaders(namespace: string, database: string): Record<string, string> {
	return {
		Authorization: `Basic ${btoa("root:root")}`,
		"surreal-ns": namespace,
		"surreal-db": database,
		Accept: "application/json",
	};
}

/** POST a raw GQL query to the /gql HTTP endpoint. */
async function httpGql(
	server: TestServer,
	namespace: string,
	database: string,
	query: string,
	urlParams?: Record<string, string>,
): Promise<{ status: number; body: any }> {
	const qs = urlParams ? `?${new URLSearchParams(urlParams)}` : "";
	const res = await fetch(`${server.httpUrl}/gql${qs}`, {
		method: "POST",
		headers: rootHeaders(namespace, database),
		body: query,
	});
	return { status: res.status, body: await res.json() };
}

/** Call an RPC method over POST /rpc (the SDK exposes no raw-RPC escape hatch). */
async function httpRpc(
	server: TestServer,
	namespace: string,
	database: string,
	method: string,
	params: unknown[],
): Promise<{ status: number; body: any }> {
	const res = await fetch(`${server.httpUrl}/rpc`, {
		method: "POST",
		headers: { ...rootHeaders(namespace, database), "Content-Type": "application/json" },
		body: JSON.stringify({ id: 1, method, params }),
	});
	return { status: res.status, body: await res.json() };
}

// ---------------------------------------------------------------------------
// Dialect separation: the SurrealQL parser never accepts GQL.
// ---------------------------------------------------------------------------

test("SurrealQL query() rejects GQL MATCH (the dialects are fully separate)", async () => {
	const { db } = await rootClient(gqlServer);
	// MATCH is not a SurrealQL statement: the parser reads it as an expression
	// and fails on the parenthesised pattern as a function/constant path.
	const err = await rejects(db.query("MATCH (n:person) RETURN n").collect());
	expect(String(err)).toMatch(/Invalid function\/constant path/);
	await db.close();
});

// ---------------------------------------------------------------------------
// Default capabilities: GQL is available; eval::gql stays behind eval-query.
// ---------------------------------------------------------------------------

test("default caps: POST /gql and RPC gql are available (no experimental flag needed)", async () => {
	const { db, namespace, database } = await rootClient(plainServer);
	await db.query("CREATE dc_thing:1 SET name = 'one'");

	// HTTP endpoint on a stock server: the request executes (200 with an OK
	// envelope) rather than being refused as an experimental capability (403).
	const gql = await httpGql(plainServer, namespace, database, "MATCH (n:dc_thing) RETURN n.name");
	expect(gql.status).toBe(200);
	expect(gql.body[0].status).toBe("OK");
	expect(gql.body[0].result).toEqual([{ "n.name": "one" }]);

	// RPC method likewise executes; no NotAllowed gate error at the top level.
	const rpc = await httpRpc(plainServer, namespace, database, "gql", [
		"MATCH (n:dc_thing) RETURN n.name",
	]);
	expect(rpc.status).toBe(200);
	expect(rpc.body.error).toBeUndefined();
	expect(rpc.body.result[0].status).toBe("OK");

	await db.close();
});

test("default caps: eval::gql is still denied by the eval-query gate", async () => {
	const { db } = await rootClient(plainServer);
	// --allow-eval-query defaults to denied for EVERY subject (even root); this
	// gate is independent of GQL.
	const err = await rejects(db.query(`RETURN eval::gql('MATCH (n:x) RETURN n')`).collect());
	expect(String(err)).toMatch(/Function 'eval::gql' is not allowed to be executed/);
	await db.close();
});

test("eval-query allowed: eval::gql executes the embedded GQL query", async () => {
	// gqlServer has --allow-eval-query=system; with GQL default-on, eval::gql
	// runs the embedded query.
	const { db } = await rootClient(gqlServer);
	await db.query("CREATE eval_person:tobie SET name = 'Tobie'");
	const [rows] = await db
		.query<[Record<string, unknown>]>(`RETURN eval::gql('MATCH (n:eval_person) RETURN n.name')`)
		.json();
	// Quirk pinned deliberately: eval::gql collapses a single-row result to the
	// bare object rather than a one-element array.
	expect(rows).toEqual({ "n.name": "Tobie" });
	await db.close();
});

// ---------------------------------------------------------------------------
// Enabled server: POST /gql executes ISO GQL.
// ---------------------------------------------------------------------------

test("POST /gql: MATCH returns /sql-style envelopes with dotted projection keys", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);
	await db.query(`
		CREATE match_person:tobie SET name = 'Tobie';
		CREATE match_person:jaime SET name = 'Jaime';
	`);

	const { status, body } = await httpGql(
		gqlServer,
		namespace,
		database,
		"MATCH (n:match_person) RETURN n.name ORDER BY n.name",
	);
	expect(status).toBe(200);
	expect(body).toHaveLength(1);
	expect(body[0].status).toBe("OK");
	// Projection keys are the literal GQL expressions, dots included.
	expect(body[0].result).toEqual([{ "n.name": "Jaime" }, { "n.name": "Tobie" }]);

	await db.close();
});

test("POST /gql: relationship traversal over RELATE-created edges", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);
	await db.query(`
		CREATE trav_person:a SET name = 'Alpha';
		CREATE trav_person:b SET name = 'Beta';
		RELATE trav_person:a->trav_knows->trav_person:b;
	`);

	const { status, body } = await httpGql(
		gqlServer,
		namespace,
		database,
		"MATCH (a:trav_person)-[:trav_knows]->(b:trav_person) RETURN a.name, b.name",
	);
	expect(status).toBe(200);
	expect(body[0].status).toBe("OK");
	expect(body[0].result).toEqual([{ "a.name": "Alpha", "b.name": "Beta" }]);

	await db.close();
});

test("POST /gql: $vars are bound from URL query parameters", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);
	await db.query(`
		CREATE qp_person:x SET name = 'Xavier';
		CREATE qp_person:y SET name = 'Yara';
	`);

	// Quirk pinned deliberately: /gql takes bindings from the URL query
	// string (like /sql), not from any request-body framing.
	const { status, body } = await httpGql(
		gqlServer,
		namespace,
		database,
		"MATCH (n:qp_person) WHERE n.name = $name RETURN n.name",
		{ name: "Yara" },
	);
	expect(status).toBe(200);
	expect(body[0].status).toBe("OK");
	expect(body[0].result).toEqual([{ "n.name": "Yara" }]);

	await db.close();
});

test("POST /gql: GQL INSERT persists nodes visible to SurrealQL", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);

	const ins = await httpGql(gqlServer, namespace, database, 'INSERT (:ins_gadget {name: "g1"})');
	expect(ins.status).toBe(200);
	expect(ins.body[0].status).toBe("OK");
	// The INSERT itself reports an empty result set...
	expect(ins.body[0].result).toEqual([]);

	// ...but the node exists, labelled as the table name, in SurrealQL land.
	const [rows] = await db.query<[Array<{ name: string }>]>("SELECT name FROM ins_gadget").json();
	expect(rows).toEqual([{ name: "g1" }]);

	await db.close();
});

test("POST /gql: parse refusals — unlabeled nodes, MATCH-less queries, linear composition", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);

	// A fully unlabeled pattern cannot choose a starting table.
	const unlabeled = await httpGql(gqlServer, namespace, database, "MATCH (n) RETURN n");
	expect(unlabeled.status).toBe(400);
	expect(unlabeled.body.information).toMatch(
		/Cannot choose a starting table for this pattern: label at least one node/,
	);

	// Queries must start with MATCH; bare RETURN is not supported yet.
	const bareReturn = await httpGql(gqlServer, namespace, database, "RETURN 1");
	expect(bareReturn.status).toBe(400);
	expect(bareReturn.body.information).toMatch(
		/A query without a MATCH clause is not supported yet/,
	);

	// Linear query composition (NEXT) is not supported: one query per request.
	const next = await httpGql(
		gqlServer,
		namespace,
		database,
		"MATCH (n:comp_person) RETURN n.name NEXT MATCH (m:comp_person) RETURN m.name",
	);
	expect(next.status).toBe(400);
	expect(next.body.information).toMatch(/Unexpected token `NEXT`, expected the query to end/);

	await db.close();
});

test("POST /gql: anonymous requests are rejected before query execution", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);

	const res = await fetch(`${gqlServer.httpUrl}/gql`, {
		method: "POST",
		headers: {
			"surreal-ns": namespace,
			"surreal-db": database,
			Accept: "application/json",
		},
		body: "MATCH (n:anon_person) RETURN n",
	});
	expect(res.status).toBe(403);
	const body: any = await res.json();
	expect(body.information).toMatch(/Anonymous access not allowed/);

	await db.close();
});

// ---------------------------------------------------------------------------
// Enabled server: RPC method `gql`.
// ---------------------------------------------------------------------------

test("RPC gql: executes with (query, vars) params and returns envelope array", async () => {
	const { db, namespace, database } = await rootClient(gqlServer);
	await db.query(`
		CREATE rpc_person:a SET name = 'Ada';
		CREATE rpc_person:b SET name = 'Bea';
	`);

	const all = await httpRpc(gqlServer, namespace, database, "gql", [
		"MATCH (n:rpc_person) RETURN n.name ORDER BY n.name",
	]);
	expect(all.status).toBe(200);
	expect(all.body.error).toBeUndefined();
	expect(all.body.result[0].status).toBe("OK");
	expect(all.body.result[0].result).toEqual([{ "n.name": "Ada" }, { "n.name": "Bea" }]);

	const bound = await httpRpc(gqlServer, namespace, database, "gql", [
		"MATCH (n:rpc_person) WHERE n.name = $name RETURN n.name",
		{ name: "Bea" },
	]);
	expect(bound.body.result[0].result).toEqual([{ "n.name": "Bea" }]);

	await db.close();
});

// ---------------------------------------------------------------------------
// Enabled server: eval::gql through the SDK — the only SDK-reachable surface.
// ---------------------------------------------------------------------------

test("eval::gql over the SDK: runs GQL inside SurrealQL, with single-row collapse", async () => {
	const { db } = await rootClient(gqlServer);
	await db.query(`
		CREATE eval_person:a SET name = 'A';
		CREATE eval_person:b SET name = 'B';
	`);

	// Multi-row result: an array of projection objects.
	const [rows] = await db
		.query<[Array<Record<string, string>>]>(
			`RETURN eval::gql('MATCH (n:eval_person) RETURN n.name ORDER BY n.name')`,
		)
		.json();
	expect(rows).toEqual([{ "n.name": "A" }, { "n.name": "B" }]);

	// SURPRISING, pinned: a single-row result collapses to the bare object
	// (not a one-element array), so callers cannot distinguish "one row" from
	// "a scalar" by shape alone.
	const [single] = await db
		.query<[Record<string, string>]>(
			`RETURN eval::gql('MATCH (n:eval_person) WHERE n.name = "A" RETURN n.name')`,
		)
		.json();
	expect(single).toEqual({ "n.name": "A" });

	// Bindings are passed as a second argument.
	const [bound] = await db
		.query<[Record<string, string>]>(
			`RETURN eval::gql('MATCH (n:eval_person) WHERE n.name = $name RETURN n.name', { name: 'B' })`,
		)
		.json();
	expect(bound).toEqual({ "n.name": "B" });

	// A GQL INSERT evaluates to NONE (undefined via .json()) but persists.
	const [insertResult] = await db
		.query<[unknown]>(`RETURN eval::gql('INSERT (:eval_widget {name: "w1"})')`)
		.json();
	expect(insertResult).toBeUndefined();
	const [widgets] = await db.query<[unknown[]]>("SELECT name FROM eval_widget").json();
	expect(widgets).toEqual([{ name: "w1" }]);

	await db.close();
});
