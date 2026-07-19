import { afterAll, beforeAll, expect, test } from "bun:test";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

test("version() reports a SurrealDB server version", async () => {
	const { db } = await rootClient(server);
	const version = await db.version();
	expect(JSON.stringify(version)).toMatch(/surrealdb/i);
	await db.close();
});

test("use() switches namespace and database on a live connection", async () => {
	const { db, namespace } = await rootClient(server);
	const [before] = await db.query<[string]>("RETURN session::db()").json();

	await db.query("DEFINE DATABASE other_db");
	await db.use({ namespace, database: "other_db" });
	const [after] = await db.query<[string]>("RETURN session::db()").json();

	expect(after).toBe("other_db");
	expect(after).not.toBe(before);
	await db.close();
});

test("set() binds a session parameter visible to later queries; unset() removes it", async () => {
	const { db } = await rootClient(server);

	await db.set("answer", 42);
	const [val] = await db.query<[number]>("RETURN $answer").json();
	expect(val).toBe(42);

	// Session params participate in real queries.
	await db.query("CREATE thing:one SET n = $answer");
	const [rows] = await db.query<[Array<{ n: number }>]>("SELECT n FROM thing:one").json();
	expect(rows[0].n).toBe(42);

	await db.unset("answer");
	const [gone] = await db.query<[unknown]>("RETURN $answer").json();
	expect(gone).toBeUndefined();

	await db.close();
});

test("session parameters are isolated between connections", async () => {
	const { db, namespace, database } = await rootClient(server);
	const other = await guestClient(server, namespace, database);
	await other.signin({ username: "root", password: "root" });
	await other.use({ namespace, database });

	await db.set("private_value", "connection-a-only");

	const [seenByOther] = await other.query<[unknown]>("RETURN $private_value").json();
	expect(seenByOther).toBeUndefined();
	const [seenBySelf] = await db.query<[unknown]>("RETURN $private_value").json();
	expect(seenBySelf).toBe("connection-a-only");

	await other.close();
	await db.close();
});

test("session state (params and auth) survives across many sequential queries", async () => {
	const { db } = await rootClient(server);
	await db.set("counter_base", 100);
	for (let i = 0; i < 25; i++) {
		const [v] = await db.query<[number]>("RETURN $counter_base + $i", { i }).json();
		expect(v).toBe(100 + i);
	}
	const [session] = await db.query<[unknown]>("RETURN session::id() != NONE").json();
	expect(session).toBe(true);
	await db.close();
});

test("query bindings do not leak into session state", async () => {
	const { db } = await rootClient(server);
	const [bound] = await db.query<[string]>("RETURN $ephemeral", { ephemeral: "one-shot" }).json();
	expect(bound).toBe("one-shot");
	// The binding was per-query, not a session parameter.
	const [after] = await db.query<[unknown]>("RETURN $ephemeral").json();
	expect(after).toBeUndefined();
	await db.close();
});
