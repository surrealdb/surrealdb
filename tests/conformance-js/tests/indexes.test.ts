import { afterAll, beforeAll, expect, test } from "bun:test";
import { InternalError } from "surrealdb";
import { rootClient, startServer, type TestServer } from "../src/harness";

// DEFINE INDEX ... UNIQUE enforcement — the uniqueness constraint surface as
// observed through the SDK. Covers single-field, composite, and array-element
// collision, that UPDATE (not just CREATE) is guarded, and that a plain
// (non-UNIQUE) index imposes no constraint. Pins the exact violation message
// and the SDK error class the server surfaces.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

// A uniqueness violation surfaces as an InternalError whose message is the
// server's "Database index ... already contains ..." string. This helper runs a
// query expected to violate a constraint and returns the rejection.
async function expectReject(db: { query: (q: string) => { collect: () => Promise<unknown> } }, q: string): Promise<Error> {
	const err = await db
		.query(q)
		.collect()
		.then(() => null)
		.catch((e) => e as Error);
	if (err === null) throw new Error(`expected "${q}" to reject, but it succeeded`);
	return err;
}

test("single-field UNIQUE rejects a duplicate and does not create the row", async () => {
	const { db } = await rootClient(server);

	await db.query("DEFINE INDEX email_idx ON TABLE user FIELDS email UNIQUE").collect();
	const [made] = (await db.query("CREATE user:1 SET email = 'a@b.com'").json()) as [
		Array<{ id: string; email: string }>,
	];
	expect(made).toEqual([{ id: "user:1", email: "a@b.com" }]);

	const err = await expectReject(db, "CREATE user:2 SET email = 'a@b.com'");
	expect(err).toBeInstanceOf(InternalError);
	expect(err.message).toBe(
		"Database index `email_idx` already contains 'a@b.com', with record `user:1`",
	);

	// The rejected record was not persisted.
	const [rows] = (await db.query("SELECT VALUE id FROM user").json()) as [string[]];
	expect(rows.map(String)).toEqual(["user:1"]);

	await db.close();
}, 30000);

test("composite UNIQUE rejects a duplicate pair but allows a differing field", async () => {
	const { db } = await rootClient(server);

	await db.query("DEFINE INDEX ab_idx ON TABLE pair FIELDS a, b UNIQUE").collect();
	await db.query("CREATE pair:1 SET a = 'x', b = 'y'").collect();

	// Identical (a, b) pair collides on the composite key.
	const err = await expectReject(db, "CREATE pair:2 SET a = 'x', b = 'y'");
	expect(err).toBeInstanceOf(InternalError);
	expect(err.message).toBe(
		"Database index `ab_idx` already contains ['x', 'y'], with record `pair:1`",
	);

	// Differing in exactly one of the two fields yields a distinct key and is allowed.
	const [diffB] = (await db.query("CREATE pair:3 SET a = 'x', b = 'z'").json()) as [
		Array<{ id: string }>,
	];
	expect(diffB.map((r) => r.id)).toEqual(["pair:3"]);
	const [diffA] = (await db.query("CREATE pair:4 SET a = 'w', b = 'y'").json()) as [
		Array<{ id: string }>,
	];
	expect(diffA.map((r) => r.id)).toEqual(["pair:4"]);

	await db.close();
}, 30000);

test("array-field UNIQUE indexes each element, so overlapping arrays collide", async () => {
	const { db } = await rootClient(server);

	// A UNIQUE index on an array field indexes every element independently rather
	// than the array as a whole: two records collide when they share ANY element,
	// even if the arrays as a whole differ. The message names the single
	// overlapping element (not the array).
	await db.query("DEFINE INDEX tags_idx ON TABLE thing FIELDS tags UNIQUE").collect();
	await db.query("CREATE thing:1 SET tags = ['one', 'two']").collect();

	const err = await expectReject(db, "CREATE thing:2 SET tags = ['two', 'three']");
	expect(err).toBeInstanceOf(InternalError);
	expect(err.message).toBe("Database index `tags_idx` already contains 'two', with record `thing:1`");

	// Wholly non-overlapping arrays share no element and are permitted.
	const [ok] = (await db.query("CREATE thing:3 SET tags = ['four', 'five']").json()) as [
		Array<{ id: string }>,
	];
	expect(ok.map((r) => r.id)).toEqual(["thing:3"]);

	await db.close();
}, 30000);

test("UPDATE that would introduce a duplicate value also rejects", async () => {
	const { db } = await rootClient(server);

	await db.query("DEFINE INDEX email_idx ON TABLE acct FIELDS email UNIQUE").collect();
	await db
		.query("CREATE acct:1 SET email = 'one@b.com'; CREATE acct:2 SET email = 'two@b.com';")
		.collect();

	// Moving acct:2 onto acct:1's value violates the constraint.
	const err = await expectReject(db, "UPDATE acct:2 SET email = 'one@b.com'");
	expect(err).toBeInstanceOf(InternalError);
	expect(err.message).toBe(
		"Database index `email_idx` already contains 'one@b.com', with record `acct:1`",
	);

	// acct:2 retains its original value.
	const [email] = (await db.query("SELECT VALUE email FROM acct:2").json()) as [string[]];
	expect(email).toEqual(["two@b.com"]);

	await db.close();
}, 30000);

test("a non-UNIQUE index permits duplicate values", async () => {
	const { db } = await rootClient(server);

	await db.query("DEFINE INDEX name_idx ON TABLE member FIELDS name").collect();
	await db
		.query("CREATE member:1 SET name = 'Sam'; CREATE member:2 SET name = 'Sam';")
		.collect();

	const [ids] = (await db.query("SELECT VALUE id FROM member ORDER BY id").json()) as [string[]];
	expect(ids.map(String)).toEqual(["member:1", "member:2"]);

	await db.close();
}, 30000);
