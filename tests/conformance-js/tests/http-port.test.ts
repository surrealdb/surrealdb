// HTTP endpoint conformance — security-focused cases ported from the Rust
// integration suite `tests/http_integration.rs`. Everything here is driven
// with raw `fetch` (and Bun's
// `WebSocket`-upgrade headers via fetch) — the SDK has no surface for most of
// this. These cases are NOT already covered by `tests/http.test.ts` (which owns
// /health, /version, the /sql envelope, /key CRUD, /signin+/signup happy paths,
// /export, /import, and RPC-over-HTTP happy path).
//
// Each test spawns its OWN server via startServer(...) with unique ns/db and
// unique table names, and stops the server in a `finally`. Bun's default 5s
// per-test timeout is too short for a server boot, so every test sets an
// explicit timeout.
//
// Deliberately LEFT in Rust (binary formats are out of scope for a JS/fetch
// suite): the CBOR / FlatBuffers Accept-negotiation cases in `sql_endpoint`,
// `signup_mal`, and the gzip case in `sql_endpoint_with_compression`.
//
// Ported cases (Rust fn -> test here):
//  - key_endpoint_rejects_executable_body        -> "/key rejects executable bodies ..."
//  - rpc_session_hijack_prevention (sessions)    -> "RPC-over-HTTP: sessions method is refused ..."
//  - rpc_session_hijack_prevention (hijack)      -> "RPC-over-HTTP: a leaked session UUID ..."
//  - rpc_session_isolation_under_concurrency     -> "RPC-over-HTTP: concurrent authed/anon ..."
//  - http_capabilities (deny some)               -> "--deny-http=sql,export,import ..."
//  - http_capabilities (deny all + allow)        -> "--deny-http --allow-http=rpc,health ..."
//  - arbitrary_query_capabilities                -> three "--*-arbitrary-query ..." tests
//  - signin_endpoint                             -> "POST /signin infers the auth level ..."
//  - client_ip_socket                            -> "--client-ip socket (default) ..."
//  - client_ip_none                              -> "--client-ip none ..."
//  - client_ip_extractor / _x_forwarded_for /
//    _forwarded_rfc7239                          -> "--client-ip forwarding-header modes ..."
//  - readiness_gate_during_startup_import        -> "/ready is 503 until a startup import ..."
//  - no_import_server_is_ready_at_bind           -> "a server with no startup import ..."
//  - session_id                                  -> "the surreal-id header ..."
//  - no_server_id_headers                        -> "--no-identification-headers ..."
//  - sync_endpoint                               -> "GET /sync returns Save; POST returns Load"
//  - basic_auth (level scoping subset)           -> "basic-auth level scoping ..."
//  - bearer_auth (ns/db override subset)         -> "a bearer token carries its own ns/db ..."
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
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

// Per-statement envelope returned by /sql and /key endpoints.
interface Envelope {
	result: unknown;
	status: "OK" | "ERR";
	time: string;
	type: null;
	kind?: string;
	details?: unknown;
}

// JSON-RPC-over-HTTP response envelope.
interface RpcHttpResponse {
	id?: string;
	result?: unknown;
	error?: { code: number; message: string; kind?: string; details?: unknown };
	session?: string;
}

let counter = 0;

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
