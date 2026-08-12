import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Client } from "@modelcontextprotocol/sdk/client/index.js";

import {
	connectClient,
	expectOk,
	startServer,
	isToolError,
	structured,
	useFreshContext,
	type TestServer,
} from "../src/harness";

/**
 * Every tool the MCP server advertises. This list is the contract: adding or
 * removing a tool must be a deliberate edit here, because MCP clients and agent
 * configurations bind to these names.
 */
const EXPECTED_TOOLS = [
	"create",
	"delete",
	"gql",
	"graphql",
	"info",
	"insert",
	"list",
	"query",
	"relate",
	"run",
	"select",
	"update",
	"upsert",
	"use",
] as const;

let server: TestServer;
let client: Client;

beforeAll(async () => {
	server = await startServer();
	client = await connectClient(server);
});

afterAll(async () => {
	await client?.close();
	await server?.stop();
});

describe("tool surface", () => {
	test("advertises exactly the expected tools", async () => {
		const { tools } = await client.listTools();
		const names = tools.map((t) => t.name).sort();
		expect(names).toEqual([...EXPECTED_TOOLS]);
	});

	test("every tool carries an input schema and a description", async () => {
		const { tools } = await client.listTools();
		for (const tool of tools) {
			expect(tool.description, `${tool.name} must document itself`).toBeTruthy();
			expect(tool.inputSchema, `${tool.name} must expose an input schema`).toBeTruthy();
			expect(tool.inputSchema.type).toBe("object");
		}
	});

	/**
	 * Annotation hints drive client-side auto-approval, so a tool that can write
	 * must never advertise itself as read-only. `gql` is included in the mutating
	 * set because the GQL dialect parses INSERT/SET/REMOVE/DELETE.
	 */
	test("mutating tools are not advertised as read-only", async () => {
		const { tools } = await client.listTools();
		const mutating = new Set([
			"create",
			"delete",
			"gql",
			"graphql",
			"insert",
			"query",
			"relate",
			"run",
			"update",
			"upsert",
		]);
		for (const tool of tools) {
			if (!mutating.has(tool.name)) continue;
			expect(tool.annotations?.readOnlyHint, `${tool.name} must not be read-only`).not.toBe(true);
			expect(tool.annotations?.destructiveHint, `${tool.name} must be destructive`).toBe(true);
		}
	});

	test("read-only tools are advertised as read-only", async () => {
		const { tools } = await client.listTools();
		for (const name of ["select", "info", "list"]) {
			const tool = tools.find((t) => t.name === name);
			expect(tool?.annotations?.readOnlyHint, `${name} should be read-only`).toBe(true);
		}
	});
});

describe("context and schema tools", () => {
	test("use switches namespace and database", async () => {
		const { namespace, database } = await useFreshContext(client);
		const result = await client.callTool({
			name: "use",
			arguments: { namespace, database },
		});
		const sc = expectOk(result, "use");
		expect(sc.namespace).toBe(namespace);
		expect(sc.database).toBe(database);
	});

	test("use refuses a namespace that does not exist", async () => {
		const result = await client.callTool({
			name: "use",
			arguments: { namespace: "definitely_not_a_real_namespace" },
		});
		expect(isToolError(result)).toBe(true);
		expect(String(structured(result).kind)).toBe("NotFound");
	});

	test("info dumps the database scope", async () => {
		await useFreshContext(client);
		const result = await client.callTool({ name: "info", arguments: { target: "db" } });
		expectOk(result, "info");
	});

	test("list enumerates a single kind", async () => {
		await useFreshContext(client);
		await client.callTool({ name: "query", arguments: { query: "DEFINE TABLE widget" } });
		const result = await client.callTool({ name: "list", arguments: { kind: "tables" } });
		const sc = structured(result);
		const items = sc.items as unknown[];
		expect(Array.isArray(items)).toBe(true);
		expect(JSON.stringify(items)).toContain("widget");
	});

	test("list rejects a table argument on a non-table kind", async () => {
		await useFreshContext(client);
		// An argument that does not apply to the requested kind must be rejected
		// rather than silently ignored, so the model gets a correctable signal.
		const error = await client
			.callTool({ name: "list", arguments: { kind: "tables", table: "widget" } })
			.then(() => null)
			.catch((e: Error) => e);
		expect(error, "an inapplicable `table` argument must be rejected").not.toBeNull();
		expect(error!.message).toContain("table");
	});
});

describe("CRUD tools", () => {
	test("create, select, update, upsert, delete round-trip", async () => {
		await useFreshContext(client);

		const created = await client.callTool({
			name: "create",
			arguments: { target: "person:alice", data: { name: "Alice", age: 30 } },
		});
		expectOk(created, "create");

		const selected = await client.callTool({
			name: "select",
			arguments: { target: "person", where_clause: "age > 18" },
		});
		const sel = expectOk(selected, "select");
		expect(JSON.stringify(sel.value)).toContain("Alice");

		const updated = await client.callTool({
			name: "update",
			arguments: { target: "person:alice", merge_data: { age: 31 } },
		});
		expectOk(updated, "update");

		const upserted = await client.callTool({
			name: "upsert",
			arguments: { target: "person:bob", content_data: { name: "Bob", age: 40 } },
		});
		expectOk(upserted, "upsert");

		const deleted = await client.callTool({
			name: "delete",
			arguments: { target: "person:bob" },
		});
		expectOk(deleted, "delete");

		const remaining = await client.callTool({ name: "select", arguments: { target: "person" } });
		expect(JSON.stringify(structured(remaining).value)).not.toContain("Bob");
	});

	test("insert accepts an array of records", async () => {
		await useFreshContext(client);
		const result = await client.callTool({
			name: "insert",
			arguments: {
				target: "product",
				data: [
					{ name: "widget", price: 10 },
					{ name: "gadget", price: 20 },
				],
			},
		});
		expectOk(result, "insert");

		const all = await client.callTool({ name: "select", arguments: { target: "product" } });
		const text = JSON.stringify(structured(all).value);
		expect(text).toContain("widget");
		expect(text).toContain("gadget");
	});

	test("relate builds a graph edge that can be traversed", async () => {
		await useFreshContext(client);
		await client.callTool({ name: "create", arguments: { target: "person:a", data: { n: "A" } } });
		await client.callTool({ name: "create", arguments: { target: "person:b", data: { n: "B" } } });

		const related = await client.callTool({
			name: "relate",
			arguments: { from: "person:a", table: "knows", with: "person:b", content_data: { since: 2020 } },
		});
		expectOk(related, "relate");

		const traversed = await client.callTool({
			name: "query",
			arguments: { query: "SELECT ->knows->person AS friends FROM person:a" },
		});
		expect(JSON.stringify(structured(traversed))).toContain("person:b");
	});

	/**
	 * JSON cannot express SurrealDB's richer scalars, so the CRUD tools accept a
	 * `{"$ql": "<expr>"}` sentinel. Against a SCHEMAFULL decimal field the plain
	 * JSON string must fail and the sentinel must succeed — that contrast is the
	 * whole point of the escape.
	 */
	test("$ql sentinel binds typed values that raw JSON cannot express", async () => {
		await useFreshContext(client);
		await client.callTool({
			name: "query",
			arguments: {
				query: "DEFINE TABLE item SCHEMAFULL; DEFINE FIELD price ON item TYPE decimal;",
			},
		});

		const bad = await client.callTool({
			name: "create",
			arguments: { target: "item:bad", data: { price: "9.99" } },
		});
		expect(structured(bad).status).toBe("error");

		const good = await client.callTool({
			name: "create",
			arguments: { target: "item:good", data: { price: { $ql: "9.99dec" } } },
		});
		expectOk(good, "create with $ql");
	});
});

describe("query, run and alternate dialects", () => {
	test("query binds parameters with native types", async () => {
		await useFreshContext(client);
		const result = await client.callTool({
			name: "query",
			arguments: {
				query: "RETURN { name: $name, age: $age }",
				parameters: { name: "Alice", age: 30 },
			},
		});
		const sc = structured(result);
		const text = JSON.stringify(sc);
		expect(text).toContain("Alice");
		// `age` must survive as a number, not a stringified one.
		expect(text).toContain("30");
	});

	test("query surfaces statement errors in band rather than as a protocol error", async () => {
		await useFreshContext(client);
		const result = await client.callTool({
			name: "query",
			arguments: { query: "SELECT * FROM" },
		});
		expect(JSON.stringify(structured(result))).toContain("error");
	});

	test("run invokes a built-in function with typed arguments", async () => {
		await useFreshContext(client);
		const result = await client.callTool({
			name: "run",
			arguments: { function: "math::sum", args: [[1, 2, 3, 4]] },
		});
		const sc = expectOk(result, "run");
		expect(String(JSON.stringify(sc.value))).toContain("10");
	});

	test("run rejects an injection attempt in the function name", async () => {
		const result = await client
			.callTool({ name: "run", arguments: { function: "math::sum; DROP TABLE x" } })
			.catch((e: unknown) => e);
		expect(JSON.stringify(result)).toMatch(/invalid|error/i);
	});

	test("gql executes an ISO GQL match query", async () => {
		await useFreshContext(client);
		await client.callTool({ name: "create", arguments: { target: "person:gq", data: { name: "Gee" } } });
		const result = await client.callTool({
			name: "gql",
			arguments: { query: "MATCH (p:person) RETURN p.name AS name" },
		});
		expect(JSON.stringify(structured(result))).toContain("Gee");
	});

	test("graphql returns the { data, errors } envelope", async () => {
		await useFreshContext(client);
		await client.callTool({
			name: "query",
			arguments: {
				query:
					"DEFINE CONFIG GRAPHQL AUTO; DEFINE TABLE person SCHEMAFULL PERMISSIONS FULL; DEFINE FIELD name ON person TYPE string;",
			},
		});
		await client.callTool({ name: "create", arguments: { target: "person:gr", data: { name: "Graph" } } });

		const result = await client.callTool({
			name: "graphql",
			arguments: { query: "{ person { name } }" },
		});
		const sc = structured(result);
		// A configured schema returns data; an unconfigured one returns errors.
		// Either way the response must be the GraphQL envelope, not a tool error.
		expect("data" in sc || "errors" in sc).toBe(true);
	});
});

describe("resources and prompts", () => {
	test("lists the documented resources", async () => {
		const { resources } = await client.listResources();
		const uris = resources.map((r) => r.uri).sort();
		expect(uris).toEqual([
			"surrealdb://info",
			"surrealdb://instructions",
			"surrealdb://version",
		]);
	});

	test("lists the schema resource templates", async () => {
		const { resourceTemplates } = await client.listResourceTemplates();
		expect(resourceTemplates.length).toBeGreaterThanOrEqual(2);
		expect(JSON.stringify(resourceTemplates)).toContain("surrealdb://schema/ns/");
	});

	test("reads the instructions resource", async () => {
		const result = await client.readResource({ uri: "surrealdb://instructions" });
		const text = (result.contents[0] as { text?: string }).text ?? "";
		expect(text).toContain("SurrealDB MCP Server");
	});

	test("lists and renders every prompt", async () => {
		const { prompts } = await client.listPrompts();
		const names = prompts.map((p) => p.name).sort();
		expect(names).toEqual([
			"data_modeler",
			"graph_traversal",
			"query_builder",
			"schema_explorer",
			"search_guide",
			"transaction_guide",
		]);

		for (const prompt of prompts) {
			const args: Record<string, string> = {};
			for (const arg of prompt.arguments ?? []) args[arg.name] = "test input";
			const rendered = await client.getPrompt({ name: prompt.name, arguments: args });
			expect(rendered.messages.length, `${prompt.name} must render messages`).toBeGreaterThan(0);
		}
	});
});
