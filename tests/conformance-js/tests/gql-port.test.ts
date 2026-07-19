// ISO GQL (ISO/IEC 39075, Cypher-like) dialect over the wire.
//
// GQL is enabled by default. Ports the Rust integration suite
// `tests/gql_integration.rs`
// (surrealdb repo), covering ground BEYOND the existing `tests/gql.test.ts`:
//   - the richer MATCH surface: multi-hop `(a)-[k:knows]->(b)` with a WHERE
//     predicate on the EDGE property plus a NONE-guard row (an edge with no
//     `since` at all), ORDER BY, and multi-row projection;
//   - GQL mutations MATCH..SET / MATCH..REMOVE / MATCH..DELETE (gql.test.ts
//     only covers INSERT);
//   - HTTP content negotiation on /gql (CBOR / flatbuffers binary bodies,
//     text/plain -> 415) and the non-UTF-8 body refusal;
//   - the HTTP incomplete-token parse-error shape (`MATCH (` -> 400 "expected");
//   - the WebSocket RPC `gql` method (raw JSON subprotocol) with $vars bound in
//     both WHERE and LIMIT, its parse-error error KIND ("Validation"), the
//     misspelled-method "Method not found", and begin/gql/commit transaction
//     interop via a top-level `txn` envelope field;
//   - route gating absent from gql.test.ts: `--deny-http=gql` forbids the HTTP
//     route even though GQL itself is enabled.
//
// Per the suite's hard rule each test spawns its own server and gets its ns/db
// from rootClient().

import { expect, test } from "bun:test";
import { rootClient, startServer, RpcClient, type TestServer } from "../src/harness";

// ---------------------------------------------------------------------------
// Fixtures mirrored from gql_integration.rs
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helpers (fetch surface mirrored from gql.test.ts; not shared harness code)
// ---------------------------------------------------------------------------

const ROOT_AUTH = `Basic ${btoa("root:root")}`;

function rootHeaders(namespace: string, database: string): Record<string, string> {
	return {
		Authorization: ROOT_AUTH,
		"surreal-ns": namespace,
		"surreal-db": database,
		Accept: "application/json",
	};
}

/** POST a raw GQL query to /gql; returns status + parsed JSON body. */
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

/** Spawn a server (GQL is on by default), run `fn`, and always stop it. */
async function withGqlServer<T>(fn: (s: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

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
