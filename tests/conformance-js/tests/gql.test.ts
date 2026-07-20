// ISO GQL (ISO/IEC 39075, Cypher-like) dialect over the wire.
//
// GQL is enabled by default; `--allow-experimental=gql` remains a harmless
// no-op accepted for backwards compatibility.
//
// Three remote surfaces exist:
//   1. HTTP `POST /gql` — same session/auth plumbing as `/sql`, returns the
//      same per-statement envelope array; binds $vars from URL query params.
//      Covers the MATCH surface (multi-hop traversal, edge-property WHERE with
//      a NONE guard, ORDER BY), mutations (INSERT / SET / REMOVE / DELETE),
//      content negotiation (JSON / CBOR / flatbuffers / text-plain 415),
//      body validation, parse-error shapes, anonymous refusal, and route
//      gating (`--deny-http=gql`).
//   2. RPC method `gql` (params: [query, vars?]) — over POST /rpc and over the
//      WebSocket protocol; the JS SDK exposes no way to call it, so these tests
//      drive it with fetch and with a raw/harness WebSocket client. Covers
//      $var binding, the "Validation" parse-error kind, "Method not found",
//      and begin/gql/commit transaction interop via a top-level `txn` field.
//   3. `eval::gql(query, vars?)` inside normal SurrealQL — reachable through
//      the SDK's query(), gated by `--allow-eval-query` (denied by default,
//      independent of GQL availability).
//
// The SurrealQL parser itself never accepts MATCH — the dialects are fully
// separate.

import { afterAll, beforeAll, expect, test } from "bun:test";
import { rootClient, startServer, RpcClient, type TestServer } from "../src/harness";

// ---------------------------------------------------------------------------
// Shared servers (used by the SDK / eval::gql tests)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helpers, fixtures, and shared constants
// ---------------------------------------------------------------------------

const ROOT_AUTH = `Basic ${btoa("root:root")}`;

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
		Authorization: ROOT_AUTH,
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

/** Spawn a server (GQL is on by default), run `fn`, and always stop it. */
async function withGqlServer<T>(fn: (s: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

// Deterministic person/knows graph. Only k12 (A->B, since 2021) and k23
// (B->C, since 2022) satisfy `since > 2020`; k21 (since 2018) is filtered out
// and k31 carries NO `since` property at all, exercising the NONE guard on the
// edge predicate.
const SEED = `
	CREATE person:1 SET name = 'A' RETURN NONE;
	CREATE person:2 SET name = 'B' RETURN NONE;
	CREATE person:3 SET name = 'C' RETURN NONE;
	INSERT RELATION INTO knows [
		{ id: knows:k12, in: person:1, out: person:2, since: 2021 },
		{ id: knows:k21, in: person:2, out: person:1, since: 2018 },
		{ id: knows:k23, in: person:2, out: person:3, since: 2022 },
		{ id: knows:k31, in: person:3, out: person:1 }
	] RETURN NONE;
`;

// Unaliased RETURN items use the verbatim GQL expression text as the column
// name, so rows carry `a.name` / `b.name` keys and ORDER BY references them by
// that same text.
const MATCH_QUERY =
	"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name, b.name ORDER BY a.name";

/** Seed the person/knows graph over the SDK (committed, visible to /gql + RPC). */
async function seedGraph(server: TestServer): Promise<{
	db: Awaited<ReturnType<typeof rootClient>>["db"];
	namespace: string;
	database: string;
}> {
	const { db, namespace, database } = await rootClient(server);
	await db.query(SEED);
	return { db, namespace, database };
}

type RpcError = { code: number; kind?: string; message: string };

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
// Enabled server: RPC method `gql` (POST /rpc).
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

// ---------------------------------------------------------------------------
// HTTP /gql: the richer MATCH surface
// ---------------------------------------------------------------------------

test("POST /gql: multi-hop MATCH with an edge-property WHERE, NONE guard, and ORDER BY", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await seedGraph(server);

		// OPTIONS preflight on the route is answered 200 (CORS), separate from POST.
		const opt = await fetch(`${server.httpUrl}/gql`, {
			method: "OPTIONS",
			headers: rootHeaders(namespace, database),
		});
		expect(opt.status).toBe(200);

		const { status, body } = await httpGql(server, namespace, database, MATCH_QUERY);
		expect(status).toBe(200);
		expect(body[0].status).toBe("OK");
		// k21 (since 2018) filtered; k31 (no `since`) excluded by the NONE guard.
		expect(body[0].result).toEqual([
			{ "a.name": "A", "b.name": "B" },
			{ "a.name": "B", "b.name": "C" },
		]);

		await db.close();
	});
});

// ---------------------------------------------------------------------------
// HTTP /gql: mutations beyond INSERT — MATCH..SET / REMOVE / DELETE
// ---------------------------------------------------------------------------

test("POST /gql: MATCH..SET updates, MATCH..REMOVE drops a field, MATCH..DELETE removes the node", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);
		// Seed one node with an explicit id via SurrealQL so assertions are stable.
		await db.query("CREATE gadget:g1 SET name = 'w1', qty = 1 RETURN NONE");

		// SET writes the property and the RETURN projection reflects the new value.
		const set = await httpGql(
			server,
			namespace,
			database,
			'MATCH (n:gadget) WHERE n.name = "w1" SET n.qty = 5 RETURN n.qty',
		);
		expect(set.status).toBe(200);
		expect(set.body[0].status).toBe("OK");
		expect(set.body[0].result).toEqual([{ "n.qty": 5 }]);

		// REMOVE deletes the `qty` field; the row projects the surviving name.
		const remove = await httpGql(
			server,
			namespace,
			database,
			'MATCH (n:gadget) WHERE n.name = "w1" REMOVE n.qty RETURN n.name',
		);
		expect(remove.status).toBe(200);
		expect(remove.body[0].result).toEqual([{ "n.name": "w1" }]);

		// SurrealQL confirms `qty` is gone but the record still exists.
		const [afterRemove] = await db
			.query<[Array<Record<string, unknown>>]>("SELECT * FROM gadget")
			.json();
		expect(afterRemove).toEqual([{ id: "gadget:g1", name: "w1" }]);

		// DELETE removes the node; the statement reports an empty result set.
		const del = await httpGql(
			server,
			namespace,
			database,
			'MATCH (n:gadget) WHERE n.name = "w1" DELETE n',
		);
		expect(del.status).toBe(200);
		expect(del.body[0].status).toBe("OK");
		expect(del.body[0].result).toEqual([]);

		const [afterDelete] = await db.query<[unknown[]]>("SELECT * FROM gadget").json();
		expect(afterDelete).toEqual([]);

		await db.close();
	});
});

// ---------------------------------------------------------------------------
// HTTP /gql: content negotiation and body validation
// ---------------------------------------------------------------------------

test("POST /gql: Accept negotiation — CBOR/flatbuffers return binary bodies, text/plain is 415", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await seedGraph(server);

		// application/cbor returns a CBOR body: 200, and NOT a JSON array (which
		// would begin with `[` == 0x5b). Full CBOR decoding is left to the Rust
		// format-matrix smoke; the JS suite pins the observable HTTP negotiation.
		const cbor = await fetch(`${server.httpUrl}/gql`, {
			method: "POST",
			headers: { ...rootHeaders(namespace, database), Accept: "application/cbor" },
			body: MATCH_QUERY,
		});
		expect(cbor.status).toBe(200);
		const cborBytes = new Uint8Array(await cbor.arrayBuffer());
		expect(cborBytes.length).toBeGreaterThan(0);
		expect(cborBytes[0]).not.toBe(0x5b);

		// application/vnd.surrealdb.flatbuffers returns the internal binary format.
		const fb = await fetch(`${server.httpUrl}/gql`, {
			method: "POST",
			headers: {
				...rootHeaders(namespace, database),
				Accept: "application/vnd.surrealdb.flatbuffers",
			},
			body: "MATCH (n:person) RETURN n.name",
		});
		expect(fb.status).toBe(200);
		const fbBytes = new Uint8Array(await fb.arrayBuffer());
		expect(fbBytes.length).toBeGreaterThan(0);
		expect(fbBytes[0]).not.toBe(0x5b);

		// An unsupported Accept media type is rejected with 415.
		const plain = await fetch(`${server.httpUrl}/gql`, {
			method: "POST",
			headers: { ...rootHeaders(namespace, database), Accept: "text/plain" },
			body: MATCH_QUERY,
		});
		expect(plain.status).toBe(415);

		await db.close();
	});
});

test("POST /gql: a non-UTF-8 request body is rejected with 400 before parsing", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);

		const res = await fetch(`${server.httpUrl}/gql`, {
			method: "POST",
			headers: rootHeaders(namespace, database),
			body: new Uint8Array([0xff, 0xfe, 0xfd]),
		});
		expect(res.status).toBe(400);
		const body: any = await res.json();
		expect(body.information).toBe("Non UTF-8 request body");

		await db.close();
	});
});

test("POST /gql: an incomplete pattern is a 400 parse error carrying the 'expected' hint", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);

		const { status, body } = await httpGql(server, namespace, database, "MATCH (");
		expect(status).toBe(400);
		// The rendered GQL parse error reaches the wire, including the delimiter hint.
		expect(String(body.information)).toMatch(/Parse error/);
		expect(String(body.information)).toMatch(/expected/);

		await db.close();
	});
});

// ---------------------------------------------------------------------------
// Route gating (GQL itself is enabled by default)
// ---------------------------------------------------------------------------

test("gate: GQL is available on a stock server (no experimental capability needed)", async () => {
	// GQL is enabled by default; a stock server runs /gql rather than
	// refusing it as an experimental capability.
	const server = await startServer();
	try {
		const { db, namespace, database } = await seedGraph(server);

		const { status, body } = await httpGql(server, namespace, database, MATCH_QUERY);
		expect(status).toBe(200);
		expect(body[0].status).toBe("OK");
		// Runs and returns data — the point is it is not gated (403). The exact
		// projection rows are pinned by the dedicated MATCH tests above.
		expect(Array.isArray(body[0].result) && body[0].result.length > 0).toBe(true);

		await db.close();
	} finally {
		await server.stop();
	}
});

test("gate: --deny-http=gql forbids the HTTP route even though GQL is enabled", async () => {
	const server = await startServer({ args: ["--deny-http=gql"] });
	try {
		const { db, namespace, database } = await rootClient(server);

		const { status, body } = await httpGql(server, namespace, database, MATCH_QUERY);
		expect(status).toBe(403);
		expect(body.information).toBe("The HTTP route 'gql' is forbidden");

		// The route still exists (method-level rejection), it is not a 404:
		// a GET is 405 Method Not Allowed rather than Not Found.
		const get = await fetch(`${server.httpUrl}/gql`, {
			method: "GET",
			headers: rootHeaders(namespace, database),
		});
		expect(get.status).toBe(405);

		await db.close();
	} finally {
		await server.stop();
	}
});

// ---------------------------------------------------------------------------
// WebSocket RPC `gql` (raw JSON subprotocol via harness RpcClient)
// ---------------------------------------------------------------------------

test("RPC gql (WebSocket): $vars bind in both WHERE and LIMIT and return an envelope array", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await seedGraph(server);
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			await rpc.use(namespace, database);

			const result = (await rpc.call("gql", [
				"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > $min RETURN a.name, b.name ORDER BY a.name LIMIT $lim",
				{ min: 2020, lim: 1 },
			])) as Array<{ status: string; result: unknown }>;

			expect(result[0].status).toBe("OK");
			// LIMIT $lim = 1 keeps only the first ordered binding.
			expect(result[0].result).toEqual([{ "a.name": "A", "b.name": "B" }]);
		} finally {
			await rpc.close();
			await db.close();
		}
	});
});

test("RPC gql (WebSocket): a parse error surfaces as error kind 'Validation', distinct from the gate", async () => {
	await withGqlServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			await rpc.use(namespace, database);

			const res = await rpc.rpc("gql", ["MATCH ("]);
			expect(res.result).toBeUndefined();
			const err = res.error as RpcError;
			expect(err.code).toBe(-32000);
			// A malformed query is a Validation error — NOT the NotAllowed used by
			// the capability gate, so the two failure classes stay distinguishable.
			expect(err.kind).toBe("Validation");
			expect(err.message).toMatch(/expected/);
		} finally {
			await rpc.close();
			await db.close();
		}
	});
});

test("RPC gql (WebSocket): available on a stock server (no experimental gate)", async () => {
	const server = await startServer();
	try {
		const { db, namespace, database } = await seedGraph(server);
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			await rpc.use(namespace, database);

			// Executes rather than being refused as an experimental capability.
			const result = (await rpc.call("gql", [MATCH_QUERY])) as Array<{ status: string }>;
			expect(result[0].status).toBe("OK");
		} finally {
			await rpc.close();
			await db.close();
		}
	} finally {
		await server.stop();
	}
});

test("RPC (WebSocket): a misspelled 'gqll' method is 'Method not found' (-32601)", async () => {
	await withGqlServer(async (server) => {
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			const res = await rpc.rpc("gqll", []);
			expect(res.result).toBeUndefined();
			const err = res.error as RpcError;
			expect(err.code).toBe(-32601);
			expect(err.message).toBe("Method not found");
		} finally {
			await rpc.close();
		}
	});
});

// ---------------------------------------------------------------------------
// WebSocket RPC `gql`: transaction interop (top-level `txn` envelope field)
//
// RpcClient sends only { id, method, params }; the txn id rides as a sibling
// top-level field, so this drives a raw WebSocket directly (not harness code).
// ---------------------------------------------------------------------------

function rawConnect(server: TestServer): Promise<WebSocket> {
	const ws = new WebSocket(server.url, "json");
	return new Promise((resolve, reject) => {
		ws.addEventListener("open", () => resolve(ws));
		ws.addEventListener("error", () => reject(new Error("raw ws error")));
	});
}

/** Send one raw RPC envelope and resolve with the frame carrying the same id. */
function rawRpc(ws: WebSocket, msg: Record<string, unknown>, timeoutMs = 10000): Promise<any> {
	return new Promise((resolve, reject) => {
		const id = msg.id;
		const timer = setTimeout(() => {
			ws.removeEventListener("message", onMsg);
			reject(new Error(`timed out waiting for RPC id=${String(id)}`));
		}, timeoutMs);
		const onMsg = (ev: MessageEvent) => {
			const frame = JSON.parse(String(ev.data));
			if (frame.id === id) {
				clearTimeout(timer);
				ws.removeEventListener("message", onMsg);
				resolve(frame);
			}
		};
		ws.addEventListener("message", onMsg);
		ws.send(JSON.stringify(msg));
	});
}

test("RPC gql (WebSocket): reads inside an explicit begin/commit accept a top-level txn id", async () => {
	await withGqlServer(async (server) => {
		// Seed person:1 on a separate committed connection.
		const { db, namespace, database } = await rootClient(server);
		await db.query("CREATE person:1 SET name = 'A' RETURN NONE");

		const ws = await rawConnect(server);
		try {
			const signin = await rawRpc(ws, {
				id: 1,
				method: "signin",
				params: [{ user: "root", pass: "root" }],
			});
			expect(signin.error).toBeUndefined();

			const use = await rawRpc(ws, { id: 2, method: "use", params: [namespace, database] });
			expect(use.error).toBeUndefined();

			// begin returns the transaction id as a bare string.
			const begin = await rawRpc(ws, { id: 3, method: "begin" });
			expect(begin.error).toBeUndefined();
			const txn = begin.result as string;
			expect(typeof txn).toBe("string");

			// A GQL read carrying the txn id runs inside that transaction.
			const inTxn = await rawRpc(ws, {
				id: 4,
				method: "gql",
				params: ["MATCH (n:person) RETURN n.name"],
				txn,
			});
			expect(inTxn.error).toBeUndefined();
			expect(inTxn.result[0].status).toBe("OK");
			expect(inTxn.result[0].result).toEqual([{ "n.name": "A" }]);

			const commit = await rawRpc(ws, { id: 5, method: "commit", params: [txn] });
			expect(commit.error).toBeUndefined();
		} finally {
			ws.close();
			await db.close();
		}
	});
});
