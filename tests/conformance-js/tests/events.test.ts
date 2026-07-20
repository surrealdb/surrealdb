import { expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

// DEFINE EVENT trigger conformance — the CREATE/UPDATE/DELETE side effects a
// table event fires, and the exact shapes of the $event / $before / $after
// context params visible inside the event body. Events run inline as part of
// the triggering mutation, so an audit row written by the event is already
// present by the time the mutation call returns. Pins observed behaviour; the
// exhaustive spec lives in language-tests/*.surql.
//
// A SELECT against a table that has never been created errors ("The table 'x'
// does not exist"), so the audit table is DEFINEd up front wherever a test
// reads it before the event has had a chance to write the first row.

// Each test runs against its own fresh server (unique ns/db) via withServer().
async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

test("CREATE trigger writes an audit row with $event = 'CREATE' and $after data", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE EVENT audit ON TABLE thing WHEN $event = 'CREATE' THEN (
					CREATE audit SET action = $event, thing = $after.id, name = $after.name
				);`,
			)
			.collect();

		await db.query("CREATE thing:one SET name = 'first'").collect();

		// The event fired inline: the audit row already exists.
		const [rows] = (await db
			.query("SELECT action, thing, name FROM audit")
			.json()) as [Array<{ action: string; thing: string; name: string }>];
		expect(rows).toEqual([{ action: "CREATE", thing: "thing:one", name: "first" }]);

		await db.close();
	});
}, 30000);

test("UPDATE trigger sees both $before (old) and $after (new) field values", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE TABLE audit;
				 DEFINE EVENT audit ON TABLE thing WHEN $event = 'UPDATE' THEN (
					CREATE audit SET action = $event, was = $before.n, now = $after.n
				);`,
			)
			.collect();

		await db.query("CREATE thing:one SET n = 1").collect();
		// The CREATE does not match the WHEN filter, so no audit yet.
		const [afterCreate] = (await db.query("SELECT VALUE id FROM audit").json()) as [unknown[]];
		expect(afterCreate).toEqual([]);

		await db.query("UPDATE thing:one SET n = 2").collect();

		const [rows] = (await db
			.query("SELECT action, was, now FROM audit")
			.json()) as [Array<{ action: string; was: number; now: number }>];
		expect(rows).toEqual([{ action: "UPDATE", was: 1, now: 2 }]);

		await db.close();
	});
}, 30000);

test("DELETE trigger exposes the deleted record in $before, and $after is NONE", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE EVENT audit ON TABLE thing WHEN $event = 'DELETE' THEN (
					CREATE audit SET action = $event, gone = $before.id, name = $before.name, after = $after
				);`,
			)
			.collect();

		await db.query("CREATE thing:one SET name = 'doomed'").collect();
		await db.query("DELETE thing:one").collect();

		const [rows] = (await db
			.query("SELECT action, gone, name, after FROM audit")
			.json()) as [Array<{ action: string; gone: string; name: string; after: unknown }>];
		// $before holds the record as it was; $after has no value on a DELETE,
		// so .json() maps the SurrealQL NONE to undefined (the key is absent).
		expect(rows).toHaveLength(1);
		expect(rows[0].action).toBe("DELETE");
		expect(rows[0].gone).toBe("thing:one");
		expect(rows[0].name).toBe("doomed");
		expect(rows[0].after).toBeUndefined();

		// And the record itself is really gone.
		const [remaining] = (await db.query("SELECT VALUE id FROM thing").json()) as [unknown[]];
		expect(remaining).toEqual([]);

		await db.close();
	});
}, 30000);

test("full $before / $after / $event shapes across CREATE, UPDATE, DELETE", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		// A single unfiltered (WHEN true) event that snapshots the whole context
		// into a log row keyed by the action, mirroring the language-test spec.
		// $this is captured into $doc first: inside the nested CREATE, a bare
		// $this rebinds to the log record being created, so it must be grabbed
		// before the CREATE to observe the triggering document.
		await db
			.query(
				`DEFINE EVENT snap ON TABLE test WHEN true THEN {
					LET $doc = $this;
					CREATE type::record('log', $event) SET
						this = $doc, value = $value, before = $before, after = $after;
				};`,
			)
			.collect();

		await db.query("CREATE test:1 SET num = 1").collect();
		await db.query("UPSERT test:1 SET num = 2").collect();
		await db.query("DELETE test:1").collect();

		const [logs] = (await db.query("SELECT * FROM log ORDER BY id").json()) as [
			Array<{
				id: string;
				this?: unknown;
				value?: unknown;
				before?: unknown;
				after?: unknown;
			}>,
		];
		const byId = Object.fromEntries(logs.map((l) => [String(l.id), l]));

		// CREATE: no $before; $after / $this / $value are the new record.
		expect(byId["log:CREATE"].before).toBeUndefined();
		expect(byId["log:CREATE"].after).toEqual({ id: "test:1", num: 1 });
		expect(byId["log:CREATE"].this).toEqual({ id: "test:1", num: 1 });
		expect(byId["log:CREATE"].value).toEqual({ id: "test:1", num: 1 });

		// UPDATE: $before is the old record, $after the new one; $this/$value
		// track the new (post-mutation) document.
		expect(byId["log:UPDATE"].before).toEqual({ id: "test:1", num: 1 });
		expect(byId["log:UPDATE"].after).toEqual({ id: "test:1", num: 2 });
		expect(byId["log:UPDATE"].this).toEqual({ id: "test:1", num: 2 });
		expect(byId["log:UPDATE"].value).toEqual({ id: "test:1", num: 2 });

		// DELETE: $after has no value; $before / $this / $value hold the record
		// as it stood before removal.
		expect(byId["log:DELETE"].after).toBeUndefined();
		expect(byId["log:DELETE"].before).toEqual({ id: "test:1", num: 2 });
		expect(byId["log:DELETE"].this).toEqual({ id: "test:1", num: 2 });
		expect(byId["log:DELETE"].value).toEqual({ id: "test:1", num: 2 });

		await db.close();
	});
}, 30000);

test("WHEN filter fires only for the matching subset of events", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		// Only DELETEs are audited; CREATE and UPDATE must not write a row.
		await db
			.query(
				`DEFINE TABLE audit;
				 DEFINE EVENT only_delete ON TABLE thing WHEN $event = 'DELETE' THEN (
					CREATE audit SET subject = $before.id
				);`,
			)
			.collect();

		await db.query("CREATE thing:one SET n = 1").collect();
		await db.query("UPDATE thing:one SET n = 2").collect();
		// No audit rows yet — neither CREATE nor UPDATE matched the WHEN.
		const [beforeDelete] = (await db.query("SELECT VALUE id FROM audit").json()) as [unknown[]];
		expect(beforeDelete).toEqual([]);

		await db.query("DELETE thing:one").collect();

		const [afterDelete] = (await db.query("SELECT VALUE subject FROM audit").json()) as [string[]];
		expect(afterDelete.map(String)).toEqual(["thing:one"]);

		await db.close();
	});
}, 30000);
