import { spawn, type Subprocess } from "bun";
import { Surreal } from "surrealdb";

const BIN = process.env.SURREAL_BIN ?? "surreal";

export interface TestServer {
	port: number;
	url: string;
	httpUrl: string;
	proc: Subprocess;
	stop(): Promise<void>;
}

export interface ServerOptions {
	/** Extra CLI arguments, e.g. capability flags a test file needs. */
	args?: string[];
	/**
	 * Datastore positional argument. Defaults to "memory". Examples:
	 * "memory", "memory?versioned=true", `surrealkv://${dir}/db`.
	 */
	datastore?: string;
	/** Extra environment variables for the server process. */
	env?: Record<string, string>;
	/**
	 * When true, return as soon as the process is spawned WITHOUT waiting for
	 * /health to go green. Needed by readiness-gate tests that must observe the
	 * server before it finishes an import. `stop()` still works; `httpUrl` is
	 * immediately usable for polling /ready.
	 */
	noWait?: boolean;
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
			url: `ws://127.0.0.1:${port}/rpc`,
			httpUrl,
			proc,
			async stop() {
				proc.kill();
				await proc.exited;
			},
		};
		if (options.noWait) {
			// Give the process a moment to bind the socket, but do not await health.
			await Bun.sleep(200);
			if (proc.exitCode !== null) {
				continue; // port collision; retry
			}
			return server;
		}
		const healthy = await waitHealthy(httpUrl, proc);
		if (healthy) return server;
		proc.kill();
		await proc.exited;
	}
	throw new Error("could not start surreal server after 5 attempts");
}

async function waitHealthy(httpUrl: string, proc: Subprocess): Promise<boolean> {
	const deadline = Date.now() + 15000;
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

let nsCounter = 0;

/** A connected root client on a unique namespace/database pair. */
export async function rootClient(
	server: TestServer,
): Promise<{ db: Surreal; namespace: string; database: string }> {
	const namespace = `ns_${process.pid}_${++nsCounter}`;
	const database = `db_${nsCounter}`;
	const db = new Surreal();
	await db.connect(server.url, {
		authentication: { username: "root", password: "root" },
	});
	// Connecting with ns/db selected does not create them; define explicitly.
	await db.query(`DEFINE NAMESPACE \`${namespace}\``);
	await db.use({ namespace });
	await db.query(`DEFINE DATABASE \`${database}\``);
	await db.use({ namespace, database });
	return { db, namespace, database };
}

/** A second, independent connection to the same namespace/database. */
export async function guestClient(
	server: TestServer,
	namespace: string,
	database: string,
): Promise<Surreal> {
	const db = new Surreal();
	await db.connect(server.url, { namespace, database });
	return db;
}

/** Collects async events (e.g. live-query notifications) for ordered assertion. */
export class EventCollector<T> {
	private events: T[] = [];
	private waiters: Array<{
		pred: (e: T) => boolean;
		resolve: (e: T) => void;
	}> = [];

	push(event: T) {
		this.events.push(event);
		for (let i = 0; i < this.waiters.length; i++) {
			if (this.waiters[i].pred(event)) {
				const [w] = this.waiters.splice(i, 1);
				w.resolve(event);
				return;
			}
		}
	}

	all(): readonly T[] {
		return this.events;
	}

	/** Resolve when a matching event arrives (including one already received). */
	waitFor(pred: (e: T) => boolean, timeoutMs = 5000): Promise<T> {
		const existing = this.events.find(pred);
		if (existing) return Promise.resolve(existing);
		return new Promise((resolve, reject) => {
			const waiter = { pred, resolve: (e: T) => { clearTimeout(timer); resolve(e); } };
			const timer = setTimeout(() => {
				const idx = this.waiters.indexOf(waiter);
				if (idx >= 0) this.waiters.splice(idx, 1);
				reject(
					new Error(
						`timed out after ${timeoutMs}ms waiting for event; saw ${this.events.length}: ${JSON.stringify(this.events).slice(0, 500)}`,
					),
				);
			}, timeoutMs);
			this.waiters.push(waiter);
		});
	}

	/** Assert no event matching `pred` arrives within the window. */
	async assertSilence(pred: (e: T) => boolean, windowMs = 750): Promise<void> {
		await Bun.sleep(windowMs);
		const hit = this.events.find(pred);
		if (hit) {
			throw new Error(`expected silence but saw: ${JSON.stringify(hit).slice(0, 500)}`);
		}
	}
}

/** Unwrap the first result of a query response, asserting it succeeded. */
export function first<T>(results: T[]): T {
	if (results.length === 0) throw new Error("empty query response");
	return results[0];
}

/**
 * A raw JSON-RPC WebSocket client for the `json` subprotocol. Unlike the SDK,
 * this exposes the wire envelope verbatim — exact error codes/messages, raw
 * notification frames, and request-id correlation — for tests that must pin
 * protocol details the SDK wraps or hides. It does NOT speak CBOR; the server's
 * default CBOR path is covered by the SDK-driven tests, and the surviving Rust
 * format-matrix smoke covers JSON-vs-CBOR negotiation.
 */
export class RpcClient {
	private ws: WebSocket;
	private nextId = 1;
	private pending = new Map<string, { resolve: (v: RpcResponse) => void }>();
	readonly notifications = new EventCollector<RpcNotification>();
	private ready: Promise<void>;

	private constructor(url: string) {
		this.ws = new WebSocket(url, "json");
		this.ready = new Promise((resolve, reject) => {
			this.ws.addEventListener("open", () => resolve());
			this.ws.addEventListener("error", () => reject(new Error("ws error")));
		});
		this.ws.addEventListener("message", (ev) => {
			const msg = JSON.parse(String(ev.data));
			if (msg.id !== undefined && this.pending.has(String(msg.id))) {
				const p = this.pending.get(String(msg.id))!;
				this.pending.delete(String(msg.id));
				p.resolve(msg);
			} else if (msg.result?.action || msg.result?.id) {
				// live notification frame: { result: { id, action, result/value } }
				this.notifications.push(msg.result as RpcNotification);
			}
		});
	}

	static async connect(server: TestServer): Promise<RpcClient> {
		const c = new RpcClient(server.url);
		await c.ready;
		return c;
	}

	/** Send an RPC and resolve with the raw response envelope (result OR error). */
	rpc(method: string, params: unknown[] = []): Promise<RpcResponse> {
		const id = String(this.nextId++);
		const frame = JSON.stringify({ id, method, params });
		return new Promise((resolve) => {
			this.pending.set(id, { resolve });
			this.ws.send(frame);
		});
	}

	/** Send an RPC and assert it succeeded, returning `result`. */
	async call(method: string, params: unknown[] = []): Promise<unknown> {
		const res = await this.rpc(method, params);
		if (res.error) {
			throw new Error(`RPC ${method} errored: ${res.error.code} ${res.error.message}`);
		}
		return res.result;
	}

	async signinRoot(): Promise<void> {
		await this.call("signin", [{ user: "root", pass: "root" }]);
	}

	async use(namespace: string, database: string): Promise<void> {
		await this.call("use", [namespace, database]);
	}

	async close(): Promise<void> {
		this.ws.close();
	}
}

export interface RpcResponse {
	id?: string;
	result?: unknown;
	error?: { code: number; message: string };
}

export interface RpcNotification {
	id: string; // live query id
	action?: string;
	result?: unknown;
	value?: unknown;
	[k: string]: unknown;
}
