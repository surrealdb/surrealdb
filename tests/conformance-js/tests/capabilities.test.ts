// Capability enforcement matrix. Every test spawns its own server via
// startServer({ args }) for a specific capability-flag combination and drives
// queries over raw JSON-RPC (RpcClient) to pin the exact capability-denied
// surface the SDK would otherwise wrap.
//
// Behavioral facts (all pinned below):
//  - A capability denial on a *function* or *network target* comes back as a
//    per-statement ERR envelope inside the `query` RPC result:
//      { status: "ERR", kind: "NotAllowed", details: { kind: "Function"|"Target" },
//        result: "<message>" }
//    The `query` RPC itself succeeds (top-level `result`, no top-level `error`).
//  - A GUEST denial (auth on, anonymous, guests not allowed) is different: the
//    whole `query` RPC fails with a TOP-LEVEL JSON-RPC error, code -32002,
//    message "Anonymous access not allowed: Not enough permissions to perform
//    this action".
//  - The server says "Function '<f>' is not allowed to be executed"; we match
//    the shorter substring "Function '<f>' is not allowed".
//  - Scripting is compiled into this binary, so the refusal is always
//    "Scripting functions are not allowed"; we also accept the
//    build-without-scripting message "Embedded functions are not enabled",
//    matching either loosely.
//  - Precedence rule: a target is DENIED whenever any deny-target matches it,
//    regardless of how specific a competing allow-target is; it is ALLOWED only
//    when some allow-target matches AND no deny-target matches. On the wire this
//    is simply "deny wins on match" — see the `deny family / allow specific`
//    test, where
//    a specific `--allow-funcs=string::lowercase` does NOT rescue the function
//    from a broad `--deny-funcs=string`.)
//
// Harness trap (documented in README): variadic capability flags must be passed
// as `--flag=value`, and a BARE variadic flag (`--allow-funcs`, `--deny-net`,
// meaning "all") must never be the LAST arg, or clap greedily swallows the
// trailing `memory` datastore positional and the server misbehaves. We keep
// every bare variadic flag followed by another `--flag`.
import { expect, test } from "bun:test";
import { RpcClient, startServer, type RpcResponse, type TestServer } from "../src/harness";

// Per-statement envelope shape returned inside a `query` RPC result array.
interface StmtEnv {
	status: "OK" | "ERR";
	result: unknown;
	time: string;
	type: null;
	kind?: string;
	details?: { kind?: string; details?: { name?: string } };
}

let counter = 0;

/** Unwrap the first per-statement envelope, asserting the RPC itself succeeded. */
function firstEnv(res: RpcResponse): StmtEnv {
	expect(res.error).toBeUndefined();
	const envs = res.result as StmtEnv[];
	expect(Array.isArray(envs)).toBe(true);
	expect(envs.length).toBeGreaterThanOrEqual(1);
	return envs[0];
}

/**
 * Run a query as authenticated root against a fresh, unique ns/db on `server`.
 * DDL (DEFINE NAMESPACE/DATABASE) is not capability-gated, so this works even
 * under --deny-all.
 */
async function rootQuery(server: TestServer, query: string): Promise<RpcResponse> {
	const c = await RpcClient.connect(server);
	try {
		await c.signinRoot();
		const ns = `caps_ns_${process.pid}_${++counter}`;
		const db = `caps_db_${counter}`;
		await c.call("query", [`DEFINE NAMESPACE \`${ns}\`; USE NS \`${ns}\`; DEFINE DATABASE \`${db}\`;`]);
		await c.use(ns, db);
		return await c.rpc("query", [query]);
	} finally {
		await c.close();
	}
}

/** Run a query as an anonymous guest (no signin, no ns/db selection needed). */
async function guestQuery(server: TestServer, query: string): Promise<RpcResponse> {
	const c = await RpcClient.connect(server);
	try {
		return await c.rpc("query", [query]);
	} finally {
		await c.close();
	}
}

const SCRIPT = "RETURN function() { return '1' };";
/** Either refusal message: scripting-disabled at runtime, or built without scripting. */
const SCRIPT_DENIED = /Scripting functions are not allowed|Embedded functions are not enabled/;

// ---------------------------------------------------------------------------
// Default capabilities: functions run, but net and scripting are denied.
// ---------------------------------------------------------------------------

test("default caps: http::get is refused as a denied NETWORK TARGET", async () => {
	const server = await startServer();
	try {
		const env = firstEnv(await rootQuery(server, "RETURN http::get('http://127.0.0.1/');"));
		expect(env.status).toBe("ERR");
		expect(env.kind).toBe("NotAllowed");
		expect(env.details?.kind).toBe("Target");
		// http://127.0.0.1/ resolves to port 80; the target name carries host:port.
		expect(String(env.result)).toContain("Access to network target '127.0.0.1:80' is not allowed");
	} finally {
		await server.stop();
	}
}, 30000);

test("default caps: embedded scripting is denied", async () => {
	const server = await startServer();
	try {
		const env = firstEnv(await rootQuery(server, SCRIPT));
		expect(env.status).toBe("ERR");
		expect(env.kind).toBe("NotAllowed");
		expect(env.details?.kind).toBe("Scripting");
		expect(String(env.result)).toMatch(SCRIPT_DENIED);
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// --deny-all: functions themselves are denied (http::get denied as a FUNCTION,
// not merely as a network target) and scripting is denied.
// ---------------------------------------------------------------------------

test("--deny-all: http::get is refused as a denied FUNCTION (not a net target)", async () => {
	const server = await startServer({ args: ["--deny-all"] });
	try {
		const env = firstEnv(
			await rootQuery(server, `RETURN http::get('http://127.0.0.1:${server.port}/version');`),
		);
		expect(env.status).toBe("ERR");
		expect(env.kind).toBe("NotAllowed");
		// The distinguishing wire detail: kind Function, not Target.
		expect(env.details?.kind).toBe("Function");
		expect(env.details?.details?.name).toBe("http::get");
		expect(String(env.result)).toContain("Function 'http::get' is not allowed");
	} finally {
		await server.stop();
	}
}, 30000);

test("--deny-all: scripting is denied", async () => {
	const server = await startServer({ args: ["--deny-all"] });
	try {
		const env = firstEnv(await rootQuery(server, SCRIPT));
		expect(env.status).toBe("ERR");
		expect(String(env.result)).toMatch(SCRIPT_DENIED);
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// --allow-all: everything is permitted, INCLUDING anonymous guests. A guest
// (no signin) can run scripting and reach the loopback network via http::get.
// ---------------------------------------------------------------------------

test("--allow-all: an anonymous guest can run scripting and loopback http::get", async () => {
	const server = await startServer({ args: ["--allow-all"] });
	try {
		const script = firstEnv(await guestQuery(server, SCRIPT));
		expect(script.status).toBe("OK");
		expect(script.result).toBe("1");

		const net = firstEnv(
			await guestQuery(server, `RETURN http::get('http://127.0.0.1:${server.port}/version');`),
		);
		expect(net.status).toBe("OK");
		expect(String(net.result)).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// --deny-scripting: scripting denied, everything else default.
// ---------------------------------------------------------------------------

test("--deny-scripting: scripting is denied", async () => {
	const server = await startServer({ args: ["--deny-scripting"] });
	try {
		const env = firstEnv(await rootQuery(server, SCRIPT));
		expect(env.status).toBe("ERR");
		expect(env.details?.kind).toBe("Scripting");
		expect(String(env.result)).toMatch(SCRIPT_DENIED);
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// Function allow/deny precedence matrix.
// ---------------------------------------------------------------------------

test("--deny-all + --allow-funcs (all): builtins run despite the global deny", async () => {
	// Bare --allow-funcs kept BEFORE --deny-all so clap does not eat `memory`.
	const server = await startServer({ args: ["--allow-funcs", "--deny-all"] });
	try {
		const env = firstEnv(await rootQuery(server, "RETURN string::len('123');"));
		expect(env.status).toBe("OK");
		expect(env.result).toBe(3);
	} finally {
		await server.stop();
	}
}, 30000);

test("--deny-all + --allow-funcs=string::len: one family allowed over the global deny", async () => {
	const server = await startServer({ args: ["--deny-all", "--allow-funcs=string::len"] });
	try {
		const env = firstEnv(await rootQuery(server, "RETURN string::len('123');"));
		expect(env.status).toBe("OK");
		expect(env.result).toBe(3);
	} finally {
		await server.stop();
	}
}, 30000);

test("--allow-all + --deny-funcs (all): every function is denied", async () => {
	// Bare --deny-funcs kept BEFORE --allow-all so clap does not eat `memory`.
	const server = await startServer({ args: ["--deny-funcs", "--allow-all"] });
	try {
		const env = firstEnv(await rootQuery(server, "RETURN string::lowercase('SURREALDB');"));
		expect(env.status).toBe("ERR");
		expect(env.details?.kind).toBe("Function");
		expect(String(env.result)).toContain("Function 'string::lowercase' is not allowed");
	} finally {
		await server.stop();
	}
}, 30000);

test("--allow-all + --deny-funcs=string::lowercase: the named fn denied, siblings allowed", async () => {
	const server = await startServer({ args: ["--allow-all", "--deny-funcs=string::lowercase"] });
	try {
		const denied = firstEnv(await rootQuery(server, "RETURN string::lowercase('SURREALDB');"));
		expect(denied.status).toBe("ERR");
		expect(String(denied.result)).toContain("Function 'string::lowercase' is not allowed");

		const allowed = firstEnv(await rootQuery(server, "RETURN string::len('123');"));
		expect(allowed.status).toBe("OK");
		expect(allowed.result).toBe(3);
	} finally {
		await server.stop();
	}
}, 30000);

test("allow family + deny specific: deny wins for the named fn, family siblings still run", async () => {
	const server = await startServer({
		args: ["--allow-funcs=string", "--deny-funcs=string::lowercase"],
	});
	try {
		const denied = firstEnv(await rootQuery(server, "RETURN string::lowercase('SURREALDB');"));
		expect(denied.status).toBe("ERR");
		expect(String(denied.result)).toContain("Function 'string::lowercase' is not allowed");

		const allowed = firstEnv(await rootQuery(server, "RETURN string::len('123');"));
		expect(allowed.status).toBe("OK");
		expect(allowed.result).toBe(3);
	} finally {
		await server.stop();
	}
}, 30000);

test("deny family + allow specific: the family deny still wins — the specific allow does NOT rescue", async () => {
	// The precedence highlight: even though --allow-funcs=string::lowercase is
	// strictly more specific than --deny-funcs=string, BOTH string::lowercase
	// AND string::len are denied. Deny wins whenever any deny-target matches.
	const server = await startServer({
		args: ["--deny-funcs=string", "--allow-funcs=string::lowercase"],
	});
	try {
		const lower = firstEnv(await rootQuery(server, "RETURN string::lowercase('SURREALDB');"));
		expect(lower.status).toBe("ERR");
		expect(String(lower.result)).toContain("Function 'string::lowercase' is not allowed");

		const len = firstEnv(await rootQuery(server, "RETURN string::len('123');"));
		expect(len.status).toBe("ERR");
		expect(String(len.result)).toContain("Function 'string::len' is not allowed");
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// Network allow/deny precedence matrix. Every query targets the server's OWN
// loopback HTTP port (http::get self /version), which succeeds when net is
// allowed and is refused with a NetworkTarget denial otherwise. http::get is
// explicitly re-allowed via --allow-funcs=http::get wherever functions are
// otherwise denied.
// ---------------------------------------------------------------------------

/** http::get against the server's own /version endpoint. */
function selfGet(server: TestServer): string {
	return `RETURN http::get('http://127.0.0.1:${server.port}/version');`;
}

function expectNetDenied(env: StmtEnv, server: TestServer): void {
	expect(env.status).toBe("ERR");
	expect(env.kind).toBe("NotAllowed");
	expect(env.details?.kind).toBe("Target");
	expect(String(env.result)).toContain(
		`Access to network target '127.0.0.1:${server.port}' is not allowed`,
	);
}

test("--deny-all + --allow-net (all) + allow http::get: net allowed generally", async () => {
	// Bare --allow-net is followed by another flag, so `memory` is safe.
	const server = await startServer({
		args: ["--deny-all", "--allow-net", "--allow-funcs=http::get"],
	});
	try {
		const env = firstEnv(await rootQuery(server, selfGet(server)));
		expect(env.status).toBe("OK");
		expect(String(env.result)).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	} finally {
		await server.stop();
	}
}, 30000);

test("--allow-all + --deny-net (all): net denied generally", async () => {
	const server = await startServer({
		args: ["--allow-all", "--deny-net", "--allow-funcs=http::get"],
	});
	try {
		expectNetDenied(firstEnv(await rootQuery(server, selfGet(server))), server);
	} finally {
		await server.stop();
	}
}, 30000);

test("--deny-net (all) + --allow-net=127.0.0.1: denied generally but allowed for the host", async () => {
	const server = await startServer({
		args: ["--deny-net", "--allow-net=127.0.0.1", "--allow-funcs=http::get"],
	});
	try {
		const env = firstEnv(await rootQuery(server, selfGet(server)));
		expect(env.status).toBe("OK");
		expect(String(env.result)).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	} finally {
		await server.stop();
	}
}, 30000);

test("--allow-net=127.0.0.1 + --deny-net=127.0.0.1:80: port-specific deny misses our port", async () => {
	// The deny targets :80 only; our loopback port is random, so the host allow
	// wins and the call succeeds.
	const server = await startServer({
		args: ["--allow-net=127.0.0.1", "--deny-net=127.0.0.1:80", "--allow-funcs=http::get"],
	});
	try {
		const env = firstEnv(await rootQuery(server, selfGet(server)));
		expect(env.status).toBe("OK");
		expect(String(env.result)).toMatch(/^surrealdb-\d+\.\d+\.\d+/);
	} finally {
		await server.stop();
	}
}, 30000);

test("--deny-net=127.0.0.1 + --allow-net=127.0.0.1:80: host deny beats the port-specific allow", async () => {
	// The allow targets :80 only; our port is not :80, so the broad host deny
	// matches and the call is refused — deny wins on match.
	const server = await startServer({
		args: ["--deny-net=127.0.0.1", "--allow-net=127.0.0.1:80", "--allow-funcs=http::get"],
	});
	try {
		expectNetDenied(firstEnv(await rootQuery(server, selfGet(server))), server);
	} finally {
		await server.stop();
	}
}, 30000);

// ---------------------------------------------------------------------------
// Guest access. With auth ENABLED (the harness always passes --user/--pass),
// anonymous queries depend on --allow-guests/--deny-guests. Guest denial is a
// TOP-LEVEL JSON-RPC error (code -32002), not a per-statement ERR envelope.
// ---------------------------------------------------------------------------

test("auth on + --allow-guests: an anonymous query is allowed", async () => {
	const server = await startServer({ args: ["--allow-guests"] });
	try {
		const env = firstEnv(await guestQuery(server, "RETURN 1;"));
		expect(env.status).toBe("OK");
		expect(env.result).toBe(1);
	} finally {
		await server.stop();
	}
}, 30000);

test("auth on + --deny-guests: an anonymous query is refused (top-level -32002)", async () => {
	const server = await startServer({ args: ["--deny-guests"] });
	try {
		const res = await guestQuery(server, "RETURN 1;");
		expect(res.result).toBeUndefined();
		expect(res.error).toBeDefined();
		expect(res.error!.code).toBe(-32002);
		expect(res.error!.message).toContain("Not enough permissions to perform this action");
	} finally {
		await server.stop();
	}
}, 30000);

test("auth on + default caps: anonymous queries are denied by default", async () => {
	// No --allow-guests: guests are off by default when auth is enabled.
	const server = await startServer();
	try {
		const res = await guestQuery(server, "RETURN 1;");
		expect(res.error).toBeDefined();
		expect(res.error!.code).toBe(-32002);
		expect(res.error!.message).toContain("Not enough permissions to perform this action");
	} finally {
		await server.stop();
	}
}, 30000);

test("--unauthenticated + --deny-guests: auth off overrides deny-guests, guest query runs", async () => {
	// With authentication disabled entirely, guest access is always allowed and
	// --deny-guests has no effect.
	const server = await startServer({ args: ["--unauthenticated", "--deny-guests"] });
	try {
		const env = firstEnv(await guestQuery(server, "RETURN 1;"));
		expect(env.status).toBe("OK");
		expect(env.result).toBe(1);
	} finally {
		await server.stop();
	}
}, 30000);
