import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import {
	ROOT_AUTH,
	parseJsonOrSse,
	rawInitialize,
	startServer,
	type TestServer,
} from "../src/harness";

/**
 * Handshake-era revisions this server commits to serving, oldest first. Each is
 * exercised end to end: negotiation, then a real tool call on the negotiated
 * session. Dropping one is a breaking change for the clients pinned to it, so
 * the list is the contract rather than a sample.
 *
 * `2026-07-28` is deliberately absent here: it has no handshake and no session,
 * so it cannot be driven by this flow. It is covered by `stateless.test.ts`.
 */
const SUPPORTED_VERSIONS = ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"] as const;

/**
 * The revision the server names when a client asks for one it does not serve.
 * rmcp answers with the server's own advertised version rather than erroring,
 * so an unknown request must degrade to this instead of failing.
 */
const ADVERTISED_VERSION = "2026-07-28";

/**
 * A version string no revision will ever use. Negotiation must degrade to the
 * advertised version rather than erroring, which is what lets a client probe.
 */
const UNKNOWN_VERSION = "1999-01-01";

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server?.stop();
});

/** Issue a JSON-RPC call against an already-initialized MCP session. */
async function rawCall(
	sessionId: string | null,
	method: string,
	params: Record<string, unknown>,
	protocolVersion: string,
): Promise<any> {
	const headers: Record<string, string> = {
		Authorization: ROOT_AUTH,
		"content-type": "application/json",
		accept: "application/json, text/event-stream",
		"mcp-protocol-version": protocolVersion,
	};
	if (sessionId) headers["mcp-session-id"] = sessionId;
	const res = await fetch(server.mcpUrl, {
		method: "POST",
		headers,
		body: JSON.stringify({ jsonrpc: "2.0", id: 2, method, params }),
	});
	return parseJsonOrSse(await res.text());
}

/** Notify the server that initialization finished, as the lifecycle requires. */
async function sendInitialized(sessionId: string | null, protocolVersion: string): Promise<void> {
	const headers: Record<string, string> = {
		Authorization: ROOT_AUTH,
		"content-type": "application/json",
		accept: "application/json, text/event-stream",
		"mcp-protocol-version": protocolVersion,
	};
	if (sessionId) headers["mcp-session-id"] = sessionId;
	await fetch(server.mcpUrl, {
		method: "POST",
		headers,
		body: JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
	});
}

describe("protocol version negotiation", () => {
	for (const version of SUPPORTED_VERSIONS) {
		test(`negotiates ${version} and echoes it back`, async () => {
			const { status, body, sessionId } = await rawInitialize(server, version);
			expect(status).toBe(200);
			expect(body?.error, `initialize at ${version} must not error`).toBeUndefined();
			expect(body?.result?.protocolVersion).toBe(version);
			expect(sessionId, `${version} must allocate a session id`).toBeTruthy();
		});

		test(`serves a tool call over a ${version} session`, async () => {
			const { body, sessionId } = await rawInitialize(server, version);
			expect(body?.result?.protocolVersion).toBe(version);
			await sendInitialized(sessionId, version);

			const response = await rawCall(
				sessionId,
				"tools/call",
				{ name: "query", arguments: { query: "RETURN 1 + 1" } },
				version,
			);
			expect(response?.error, `tools/call at ${version} must not error`).toBeUndefined();
			expect(JSON.stringify(response?.result)).toContain("2");
		});

		test(`advertises the full tool surface at ${version}`, async () => {
			const { body, sessionId } = await rawInitialize(server, version);
			await sendInitialized(sessionId, version);
			const response = await rawCall(sessionId, "tools/list", {}, version);
			const names = (response?.result?.tools ?? []).map((t: { name: string }) => t.name);
			expect(names.length, `${version} must advertise every tool`).toBe(14);
			expect(names).toContain("query");
			expect(body?.result?.protocolVersion).toBe(version);
		});
	}

	test("downgrades an entirely unknown version rather than erroring", async () => {
		const { status, body } = await rawInitialize(server, UNKNOWN_VERSION);
		expect(status).toBe(200);
		expect(body?.error, "an unknown version must degrade, not error").toBeUndefined();
		expect(body?.result?.protocolVersion).toBe(ADVERTISED_VERSION);
	});

	test(`negotiates ${ADVERTISED_VERSION} when a client asks for it by handshake`, async () => {
		// A client may still send `initialize` while asking for the stateless
		// revision; rmcp answers it and then serves the connection statelessly.
		const { status, body } = await rawInitialize(server, ADVERTISED_VERSION);
		expect(status).toBe(200);
		expect(body?.result?.protocolVersion).toBe(ADVERTISED_VERSION);
	});

	test("declares tools, resources, prompts and completions at every version", async () => {
		for (const version of SUPPORTED_VERSIONS) {
			const { body } = await rawInitialize(server, version);
			const caps = body?.result?.capabilities ?? {};
			expect(caps.tools, `${version} must declare tools`).toBeTruthy();
			expect(caps.resources, `${version} must declare resources`).toBeTruthy();
			expect(caps.prompts, `${version} must declare prompts`).toBeTruthy();
			expect(caps.completions, `${version} must declare completions`).toBeTruthy();
		}
	});
});

describe("session lifecycle", () => {
	/**
	 * Newest revision that still has sessions. Session lifecycle is only
	 * meaningful below 2026-07-28, which removes protocol sessions entirely —
	 * so these cases pin the legacy contract rather than the advertised one.
	 */
	const SESSION_VERSION = "2025-11-25";

	test("a second request reuses the negotiated session", async () => {
		const version = SESSION_VERSION;
		const { sessionId } = await rawInitialize(server, version);
		await sendInitialized(sessionId, version);

		const first = await rawCall(
			sessionId,
			"tools/call",
			{ name: "query", arguments: { query: "DEFINE NAMESPACE reuse_probe" } },
			version,
		);
		expect(first?.error).toBeUndefined();

		const second = await rawCall(
			sessionId,
			"tools/call",
			{ name: "use", arguments: { namespace: "reuse_probe" } },
			version,
		);
		expect(second?.error).toBeUndefined();
		expect(JSON.stringify(second?.result)).toContain("reuse_probe");
	});

	test("a tool call without a session id is rejected", async () => {
		const response = await fetch(server.mcpUrl, {
			method: "POST",
			headers: {
				Authorization: ROOT_AUTH,
				"content-type": "application/json",
				accept: "application/json, text/event-stream",
			},
			body: JSON.stringify({
				jsonrpc: "2.0",
				id: 9,
				method: "tools/call",
				params: { name: "query", arguments: { query: "RETURN 1" } },
			}),
		});
		expect(response.status).toBeGreaterThanOrEqual(400);
	});
});
