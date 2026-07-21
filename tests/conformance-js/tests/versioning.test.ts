import { expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

// Temporal / time-travel conformance — the VERSION clause reading historical
// record state on a versioned datastore. Every test runs against its own fresh
// server started with the versioned in-memory datastore, so version history is
// isolated per test. The exhaustive spec lives in
// language-tests/tests/reproductions/*_version_*.surql.

// Each test owns a versioned server; version history must not leak between them.
async function withVersionedServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer({ datastore: "memory?versioned=true" });
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

test("SELECT ... VERSION returns the pre-mutation row while a plain SELECT shows the current row", async () => {
	await withVersionedServer(async (server) => {
		const { db } = await rootClient(server);

		// The timestamp is captured between the CREATE and the UPDATE, with a
		// SLEEP on each side so both commits land strictly outside the captured
		// instant — versioned reads resolve against commit timestamps, so a
		// boundary-exact instant is racy.
		const results = (await db
			.query(
				`CREATE person:tobie SET name = 'Tobie', age = 1;
				 SLEEP 50ms;
				 LET $t = time::now();
				 SLEEP 50ms;
				 UPDATE person:tobie SET name = 'Changed', age = 2;
				 SELECT * FROM person:tobie;
				 SELECT * FROM person:tobie VERSION $t;`,
			)
			.json()) as Array<Array<{ id: string; name: string; age: number }>>;

		const current = results[5];
		const historical = results[6];
		expect(current).toEqual([{ id: "person:tobie", name: "Changed", age: 2 }]);
		expect(historical).toEqual([{ id: "person:tobie", name: "Tobie", age: 1 }]);

		await db.close();
	});
}, 30000);

test("SELECT ... VERSION reads a row that has since been deleted", async () => {
	await withVersionedServer(async (server) => {
		const { db } = await rootClient(server);

		const results = (await db
			.query(
				`CREATE thing:1 SET v = 'orig';
				 SLEEP 50ms;
				 LET $t = time::now();
				 SLEEP 50ms;
				 DELETE thing:1;
				 SELECT * FROM thing;
				 SELECT * FROM thing VERSION $t;`,
			)
			.json()) as Array<Array<{ id: string; v: string }>>;

		const current = results[5];
		const historical = results[6];
		// The row is gone at HEAD but the version read still returns its
		// pre-deletion state.
		expect(current).toEqual([]);
		expect(historical).toEqual([{ id: "thing:1", v: "orig" }]);

		await db.close();
	});
}, 30000);

test("VERSION on an aggregate query counts the rows present at that version", async () => {
	await withVersionedServer(async (server) => {
		const { db } = await rootClient(server);

		const results = (await db
			.query(
				`CREATE c:1; CREATE c:2;
				 SLEEP 50ms;
				 LET $t = time::now();
				 SLEEP 50ms;
				 CREATE c:3;
				 SELECT count() FROM c GROUP ALL;
				 SELECT count() FROM c GROUP ALL VERSION $t;`,
			)
			.json()) as Array<Array<{ count: number }>>;

		expect(results[6]).toEqual([{ count: 3 }]);
		expect(results[7]).toEqual([{ count: 2 }]);

		await db.close();
	});
}, 30000);

test("VERSION on a subquery source is rejected; placing it inside the subquery is accepted", async () => {
	await withVersionedServer(async (server) => {
		const { db } = await rootClient(server);

		await db.query("CREATE c:1; CREATE c:2;").collect();

		// A VERSION clause on a statement whose FROM target is a subquery is a
		// query-level rejection, not a runtime result.
		const boom = db
			.query('SELECT count() FROM (SELECT * FROM c) GROUP ALL VERSION d"2020-01-01T00:00:00Z"')
			.collect()
			.then(() => null)
			.catch((e) => e as Error);
		const err = await boom;
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toContain(
			"VERSION clause cannot be used with a subquery source. Place the VERSION clause inside the subquery instead.",
		);

		await db.close();
	});
}, 30000);
