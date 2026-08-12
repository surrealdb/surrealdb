import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { startServer, type TestServer } from "../src/harness";
import { STATELESS_VERSION, statelessCall, statelessTool } from "../src/stateless";

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
	// Seed a namespace/database to scope calls into. Bootstrapping goes through
	// the stateless path too, so a failure here is a real failure, not setup noise.
	await statelessTool(server, "query", {
		query: "DEFINE NAMESPACE stateless; USE NS stateless; DEFINE DATABASE main;",
	});
});

afterAll(async () => {
	await server?.stop();
});

describe("stateless protocol", () => {
	test("serves a tool call with no handshake and no session id", async () => {
		const { body, sessionId } = await statelessCall(server, "tools/call", {
			name: "query",
			arguments: { query: "RETURN 1 + 1" },
		});
		expect(body?.error).toBeUndefined();
		expect(JSON.stringify(body?.result)).toContain("2");
		// The revision removes protocol sessions outright.
		expect(sessionId, "a stateless response must not allocate a session id").toBeNull();
	});

	test("every result carries resultType complete", async () => {
		const { body } = await statelessCall(server, "tools/list");
		expect(body?.result?.resultType).toBe("complete");
	});

	test("list results carry the cacheable-result hints", async () => {
		const { body } = await statelessCall(server, "tools/list");
		expect(body?.result).toHaveProperty("ttlMs");
		expect(body?.result).toHaveProperty("cacheScope");
	});

	test("advertises the full tool surface", async () => {
		const { body } = await statelessCall(server, "tools/list");
		const names = (body?.result?.tools ?? []).map((t: { name: string }) => t.name);
		expect(names.length).toBe(14);
		expect(names).toContain("query");
		// `use` stays advertised so the tool list does not vary by protocol era.
		expect(names).toContain("use");
	});

	test("server/discover reports the supported versions and identity", async () => {
		const { body } = await statelessCall(server, "server/discover");
		expect(body?.error).toBeUndefined();
		const payload = JSON.stringify(body?.result);
		expect(payload).toContain(STATELESS_VERSION);
		expect(payload).toContain("2025-11-25");
		expect(payload).toContain("surrealdb");
	});

	test("rejects a request whose _meta omits the required fields", async () => {
		const res = await fetch(server.mcpUrl, {
			method: "POST",
			headers: {
				Authorization: `Basic ${Buffer.from("root:root").toString("base64")}`,
				"content-type": "application/json",
				accept: "application/json, text/event-stream",
				"mcp-protocol-version": STATELESS_VERSION,
				"Mcp-Method": "tools/list",
			},
			body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
		});
		const body = await res.text();
		expect(body).toMatch(/_meta|clientCapabilities|-32602/);
	});
});

describe("stateless scope resolution", () => {
	test("scope passed as tool arguments is honoured", async () => {
		const { structured, isError } = await statelessTool(server, "info", {
			target: "db",
			namespace: "stateless",
			database: "main",
		});
		expect(isError).toBe(false);
		expect(structured).toBeTruthy();
	});

	test("scope passed as surreal-ns / surreal-db headers is honoured", async () => {
		const { structured, isError } = await statelessTool(
			server,
			"info",
			{ target: "db" },
			{ headers: { "surreal-ns": "stateless", "surreal-db": "main" } },
		);
		expect(isError).toBe(false);
		expect(structured).toBeTruthy();
	});

	test("tool arguments win over headers", async () => {
		// Create a table only in the argument-named database, then read it back
		// while the headers point somewhere else. Seeing the table proves the
		// argument, not the header, selected the scope.
		await statelessTool(server, "query", {
			query: "DEFINE NAMESPACE precedence; USE NS precedence; DEFINE DATABASE d; USE DB d; DEFINE TABLE only_here;",
		});
		const { structured } = await statelessTool(
			server,
			"list",
			{ kind: "tables", namespace: "precedence", database: "d" },
			{ headers: { "surreal-ns": "stateless", "surreal-db": "main" } },
		);
		expect(JSON.stringify(structured?.items)).toContain("only_here");
	});

	test("calls are independent: scope does not leak between requests", async () => {
		await statelessTool(server, "info", {
			target: "db",
			namespace: "precedence",
			database: "d",
		});
		// A follow-up naming no scope must not inherit the previous call's.
		const { structured } = await statelessTool(server, "list", {
			kind: "tables",
			namespace: "stateless",
			database: "main",
		});
		expect(JSON.stringify(structured?.items)).not.toContain("only_here");
	});

	test("a full CRUD round-trip works with per-call scope", async () => {
		const scope = { namespace: "stateless", database: "main" };
		await statelessTool(server, "create", {
			...scope,
			target: "widget:one",
			data: { name: "First" },
		});
		const { structured } = await statelessTool(server, "select", {
			...scope,
			target: "widget",
		});
		expect(JSON.stringify(structured?.value)).toContain("First");
	});
});

describe("stateless `use` tool", () => {
	test("is rejected with an explanation rather than silently doing nothing", async () => {
		const { isError, structured } = await statelessTool(server, "use", {
			namespace: "stateless",
			database: "main",
		});
		expect(isError, "`use` cannot work without cross-request state").toBe(true);
		const message = JSON.stringify(structured);
		expect(message).toContain("2026-07-28");
		// The message must name the replacement so an agent can self-correct.
		expect(message).toMatch(/namespace|surreal-ns/);
	});
});
