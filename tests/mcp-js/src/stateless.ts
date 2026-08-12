import { ROOT_AUTH, parseJsonOrSse, type TestServer } from "./harness";

/** The stateless protocol revision this server implements. */
export const STATELESS_VERSION = "2026-07-28";

/**
 * Issue a single stateless JSON-RPC request.
 *
 * There is no handshake and no session id: the request carries its own
 * protocol version, client identity and capabilities in `_meta`, and the
 * `Mcp-Method` / `Mcp-Name` headers so a gateway can route on them without
 * parsing the body. That envelope is the whole point of the revision, so the
 * tests build it explicitly rather than hiding it behind a client.
 */
export async function statelessCall(
	server: TestServer,
	method: string,
	params: Record<string, unknown> = {},
	options: { headers?: Record<string, string>; auth?: string | null } = {},
): Promise<any> {
	const headers: Record<string, string> = {
		"content-type": "application/json",
		accept: "application/json, text/event-stream",
		"mcp-protocol-version": STATELESS_VERSION,
		"Mcp-Method": method,
		...(options.headers ?? {}),
	};
	if (options.auth !== null) headers.Authorization = options.auth ?? ROOT_AUTH;
	if (method === "tools/call" && typeof params.name === "string") {
		headers["Mcp-Name"] = params.name;
	}

	const res = await fetch(server.mcpUrl, {
		method: "POST",
		headers,
		body: JSON.stringify({
			jsonrpc: "2.0",
			id: Math.floor(Math.random() * 1_000_000),
			method,
			params: {
				...params,
				_meta: {
					"io.modelcontextprotocol/protocolVersion": STATELESS_VERSION,
					"io.modelcontextprotocol/clientCapabilities": {},
					"io.modelcontextprotocol/clientInfo": {
						name: "surrealdb-mcp-conformance",
						version: "0.0.0",
					},
				},
			},
		}),
	});
	return { status: res.status, sessionId: res.headers.get("mcp-session-id"), body: parseJsonOrSse(await res.text()) };
}

/** Call a tool statelessly and return the parsed structured payload. */
export async function statelessTool(
	server: TestServer,
	name: string,
	args: Record<string, unknown> = {},
	options: { headers?: Record<string, string> } = {},
): Promise<{ result: any; structured: any; isError: boolean }> {
	const { body } = await statelessCall(server, "tools/call", { name, arguments: args }, options);
	if (body?.error) {
		throw new Error(`tools/call ${name} returned a protocol error: ${JSON.stringify(body.error)}`);
	}
	const result = body?.result;
	return {
		result,
		structured: result?.structuredContent,
		isError: result?.isError === true,
	};
}
