import { afterAll, beforeAll, expect, test } from "bun:test";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Computed/materialized view tables — DEFINE TABLE v AS SELECT ... FROM src.
// A view's rows are derived from its source query and maintained automatically,
// so the view is read-only: it reflects the source, direct writes are refused,
// and its table PERMISSIONS gate reads exactly like an ordinary table (a denied
// SELECT returns the normal empty result, never a "this is a view" signal).

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

async function rejects(p: Promise<unknown>): Promise<Error> {
	try {
		await p;
	} catch (e) {
		return e as Error;
	}
	throw new Error("expected promise to reject, but it resolved");
}

const VIEW_ERROR =
	"Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); " +
	"view tables are read-only and their records are computed from the source query";

test("a view reflects the source rows it projects, and tracks source changes", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		DEFINE TABLE person SCHEMALESS;
		CREATE person:ada SET name = 'Ada', age = 36;
		CREATE person:bob SET name = 'Bob', age = 12;
		DEFINE TABLE adults AS SELECT name FROM person WHERE age >= 18;
	`);

	// Only Ada qualifies; the view id mirrors the source record id.
	const [initial] = await db.query<[Array<{ id: string; name: string }>]>(
		"SELECT * FROM adults ORDER BY id",
	).json();
	expect(initial).toEqual([{ id: "adults:ada", name: "Ada" }]);

	// The view is maintained: promoting Bob into the predicate adds his row.
	await db.query("UPDATE person:bob SET age = 20");
	const [after] = await db.query<[Array<{ id: string; name: string }>]>(
		"SELECT * FROM adults ORDER BY id",
	).json();
	expect(after).toEqual([
		{ id: "adults:ada", name: "Ada" },
		{ id: "adults:bob", name: "Bob" },
	]);

	await db.close();
});

test("a grouped view exposes the aggregated rows of its source", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		DEFINE TABLE sale SCHEMALESS;
		CREATE sale SET team = 'a', amt = 10;
		CREATE sale SET team = 'a', amt = 15;
		CREATE sale SET team = 'b', amt = 7;
		DEFINE TABLE totals AS
			SELECT team, math::sum(amt) AS total, count() AS count FROM sale GROUP BY team;
	`);

	const [rows] = await db.query<[Array<{ team: string; total: number; count: number }>]>(
		"SELECT team, total, count FROM totals ORDER BY team",
	).json();
	expect(rows).toEqual([
		{ team: "a", total: 25, count: 2 },
		{ team: "b", total: 7, count: 1 },
	]);

	await db.close();
});

test("direct writes to a view are rejected as read-only", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		DEFINE TABLE person SCHEMALESS;
		CREATE person:ada SET name = 'Ada', age = 36;
		CREATE person:bob SET name = 'Bob', age = 12;
		DEFINE TABLE adults AS SELECT name FROM person WHERE age >= 18;
	`);

	// Every mutating verb against the view fails with the same view-is-read-only
	// error, at owner level where table PERMISSIONS do not intervene.
	const writes = [
		"CREATE adults:manual SET name = 'Manual'",
		"INSERT INTO adults { id: adults:manual2, name: 'Manual2' }",
		"UPSERT adults:manual SET name = 'Manual'",
		"UPDATE adults SET name = 'changed'",
		"DELETE adults",
		"RELATE person:ada->adults->person:bob",
	];
	for (const w of writes) {
		const err = await rejects(db.query(w).collect());
		expect(err.message).toContain(VIEW_ERROR);
	}

	// The view still holds only its single computed row.
	const [rows] = await db.query<[Array<{ id: string; name: string }>]>(
		"SELECT * FROM adults",
	).json();
	expect(rows).toEqual([{ id: "adults:ada", name: "Ada" }]);

	await db.close();
});

test("a record user without select permission sees no view rows rather than a view-ness leak", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE TABLE user SCHEMALESS
			PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
		DEFINE TABLE person SCHEMALESS;
		CREATE person:ada SET name = 'Ada', age = 36;
		DEFINE TABLE adults AS SELECT name FROM person WHERE age >= 18
			PERMISSIONS FOR select WHERE name = 'nobody';
	`);

	// Owner bypasses PERMISSIONS and sees the computed view row.
	const [rootRows] = await db.query<[Array<{ id: string; name: string }>]>(
		"SELECT * FROM adults",
	).json();
	expect(rootRows).toEqual([{ id: "adults:ada", name: "Ada" }]);

	// A record user whose select predicate matches nothing gets the ordinary empty
	// permission outcome — the read is filtered by PERMISSIONS before the view's
	// read-only nature is ever reached, so it never leaks that `adults` is a view.
	const alice = await guestClient(server, namespace, database);
	await alice.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "alice@example.com", pass: "alice-pw" },
	});
	const [denied] = await alice.query<[unknown[]]>("SELECT * FROM adults").json();
	expect(denied).toEqual([]);

	await alice.close();
	await db.close();
});
