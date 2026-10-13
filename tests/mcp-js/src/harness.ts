import { spawn, type Subprocess } from "bun";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const BIN = process.env.SURREAL_BIN ?? "surreal";

/** Basic-auth header for the root user the harness starts every server with. */
export const ROOT_AUTH = `Basic ${Buffer.from("root:root").toString("base64")}`;

export interface TestServer {
	port: number;
	/** Base HTTP origin, e.g. `http://127.0.0.1:8000`. */
	httpUrl: string;
	/** Streamable HTTP MCP endpoint. */
	mcpUrl: string;
	proc: Subprocess;
	stop(): Promise<void>;
}

export interface ServerOptions {
	/** Extra CLI arguments, e.g. capability flags a test file needs. */
	args?: string[];
	/** Datastore positional argument. Defaults to "memory". */
	datastore?: string;
	/** Extra environment variables for the server process. */
	env?: Record<string, string>;
}

/** Spawn a fresh in-memory server on a random port and wait until healthy. */
export async function startServer(options: ServerOptions = {}): Promise<TestServer> {
	for (let attempt = 0; attempt < 5; attempt++) {
		const port = 15000 + Math.floor(Math.random() * 20000);
		const proc = spawn(
			[
				BIN,
				"start",
				"--bind",
				`127.0.0.1:${port}`,
				"--user",
				"root",
				"--pass",
				"root",
				"--log",
				"error",
				"--no-banner",
				...(options.args ?? []),
				options.datastore ?? "memory",
			],
			{
				stdout: "ignore",
				stderr: "pipe",
				env: options.env ? { ...process.env, ...options.env } : undefined,
			},
		);
		const httpUrl = `http://127.0.0.1:${port}`;
		const server: TestServer = {
			port,
			httpUrl,
			mcpUrl: `${httpUrl}/mcp`,
			proc,
			async stop() {
				proc.kill();
				await proc.exited;
			},
		};
		if (await waitHealthy(httpUrl, proc)) return server;
		proc.kill();
		await proc.exited;
	}
	throw new Error("could not start surreal server after 5 attempts");
}

async function waitHealthy(httpUrl: string, proc: Subprocess): Promise<boolean> {
	// Each test file spawns its own server, so several debug builds start at
	// once. Startup is the slowest thing this suite does and it degrades under
	// that load, so the deadline covers a loaded machine rather than an idle one.
	const deadline = Date.now() + 30000;
	while (Date.now() < deadline) {
		if (proc.exitCode !== null) return false; // port collision or startup failure
		try {
			const res = await fetch(`${httpUrl}/health`);
			if (res.ok) return true;
		} catch {
			// not up yet
		}
		await Bun.sleep(100);
	}
	return false;
}

/**
 * Connect an MCP client over Streamable HTTP, authenticated as root.
 *
 * The SDK performs the `initialize` handshake during `connect()`, so a
 * resolved client has already negotiated a protocol version with the server.
 */
export async function connectClient(server: TestServer): Promise<Client> {
	const transport = new StreamableHTTPClientTransport(new URL(server.mcpUrl), {
		requestInit: { headers: { Authorization: ROOT_AUTH } },
	});
	const client = new Client(
		{ name: "surrealdb-mcp-conformance", version: "0.0.0" },
		{ capabilities: {} },
	);
	await client.connect(transport);
	return client;
}

/** Text payload of a tool result, joining every text content block. */
export function toolText(result: unknown): string {
	const content = (result as { content?: Array<{ type: string; text?: string }> }).content ?? [];
	return content
		.filter((c) => c.type === "text" && typeof c.text === "string")
		.map((c) => c.text as string)
		.join("\n");
}

/** Structured payload of a tool result. */
export function structured(result: unknown): Record<string, unknown> {
	const sc = (result as { structuredContent?: Record<string, unknown> }).structuredContent;
	if (!sc) throw new Error(`tool result had no structuredContent: ${JSON.stringify(result).slice(0, 400)}`);
	return sc;
}

/** True when a tool reported an in-band failure. */
export function isToolError(result: unknown): boolean {
	return (result as { isError?: boolean }).isError === true;
}

/**
 * Assert a tool call succeeded in-band, returning its structured payload.
 *
 * Two envelope families are in play. Tools that map onto SurrealQL statements
 * (`query`, `select`, the CRUD family, `run`) wrap their payload as
 * `{status, value, ...}`. Tools that do not (`use`, `list`, `graphql`) return
 * their payload directly and signal failure only through `isError`. Both are
 * accepted here so callers can assert success uniformly.
 */
export function expectOk(result: unknown, label: string): Record<string, unknown> {
	const sc = structured(result);
	if (isToolError(result)) {
		throw new Error(`${label} returned a tool error: ${JSON.stringify(sc).slice(0, 500)}`);
	}
	if ("status" in sc && sc.status !== "ok") {
		throw new Error(`${label} expected status=ok, got: ${JSON.stringify(sc).slice(0, 500)}`);
	}
	return sc;
}

let nsCounter = 0;

/**
 * Create a unique namespace/database and switch the MCP session onto it via
 * the `use` tool, returning the pair. Namespaces are not auto-provisioned by
 * `use`, so they are defined through the raw `query` tool first.
 */
export async function useFreshContext(
	client: Client,
): Promise<{ namespace: string; database: string }> {
	const namespace = `ns_${process.pid}_${++nsCounter}`;
	const database = `db_${nsCounter}`;
	await client.callTool({
		name: "query",
		arguments: { query: `DEFINE NAMESPACE \`${namespace}\`` },
	});
	await client.callTool({ name: "use", arguments: { namespace } });
	await client.callTool({
		name: "query",
		arguments: { query: `DEFINE DATABASE \`${database}\`` },
	});
	await client.callTool({ name: "use", arguments: { namespace, database } });
	return { namespace, database };
}

/**
 * Send a raw JSON-RPC `initialize` to the MCP endpoint, bypassing the SDK so
 * the test can request protocol versions the SDK itself does not support.
 *
 * Returns the parsed JSON-RPC envelope plus the allocated session id, if any.
 * The endpoint may answer as JSON or as a single-event SSE stream depending on
 * the negotiated version, so both framings are unwrapped here.
 */
export async function rawInitialize(
	server: TestServer,
	protocolVersion: string,
): Promise<{ status: number; body: any; sessionId: string | null }> {
	const res = await fetch(server.mcpUrl, {
		method: "POST",
		headers: {
			Authorization: ROOT_AUTH,
			"content-type": "application/json",
			accept: "application/json, text/event-stream",
		},
		body: JSON.stringify({
			jsonrpc: "2.0",
			id: 1,
			method: "initialize",
			params: {
				protocolVersion,
				capabilities: {},
				clientInfo: { name: "raw-conformance", version: "0.0.0" },
			},
		}),
	});
	const text = await res.text();
	return {
		status: res.status,
		body: parseJsonOrSse(text),
		sessionId: res.headers.get("mcp-session-id"),
	};
}

/** Parse a response body that may be plain JSON or an SSE `data:` stream. */
export function parseJsonOrSse(text: string): any {
	try {
		return JSON.parse(text);
	} catch {
		// fall through to SSE framing
	}
	let last: unknown = null;
	for (const line of text.split("\n")) {
		const trimmed = line.startsWith("data:") ? line.slice(5).trim() : "";
		if (!trimmed) continue;
		try {
			last = JSON.parse(trimmed);
		} catch {
			// keep-alive or retry frame
		}
	}
	return last;
}
