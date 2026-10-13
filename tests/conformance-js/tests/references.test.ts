import { afterAll, beforeAll, expect, test } from "bun:test";
import { Table, type LiveMessage } from "surrealdb";
import {
	EventCollector,
	RpcClient,
	rootClient,
	startServer,
	type TestServer,
} from "../src/harness";

// Record-reference conformance — `DEFINE FIELD f ON child TYPE record<parent>
// REFERENCE ON DELETE {CASCADE|REJECT|UNSET}`. Pins how deleting a referenced
// parent propagates to the referencing child, the live-notification stream it
// produces, and the documented rejection message. Everything stays within a
// single namespace/database. The exhaustive spec lives in
// language-tests/tests/language/reference/*.surql.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

/** Pump a live subscription's messages into a collector in the background. */
function pump(sub: AsyncIterable<LiveMessage>, into: EventCollector<LiveMessage>) {
	(async () => {
		try {
			for await (const msg of sub) into.push(msg);
		} catch {
			// subscription closed with the connection — fine for tests
		}
	})();
}

test("ON DELETE CASCADE removes the referencing child rows", async () => {
	const { db } = await rootClient(server);
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE record<parent> REFERENCE ON DELETE CASCADE;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;
			 CREATE child:c2 SET parent = parent:p1;`,
		)
		.collect();

	// Deleting the parent cascades: every child that referenced it is removed.
	await db.query("DELETE parent:p1").collect();
	const [children] = (await db.query("SELECT * FROM child").json()) as [unknown[]];
	expect(children).toEqual([]);

	await db.close();
}, 30000);

test("ON DELETE CASCADE delivers a DELETE notification for each removed child", async () => {
	const { db } = await rootClient(server);
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE record<parent> REFERENCE ON DELETE CASCADE;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;
			 CREATE child:c2 SET parent = parent:p1;`,
		)
		.collect();

	const events = new EventCollector<LiveMessage>();
	const sub = await db.live(new Table("child"));
	pump(sub, events);

	// Deleting the parent fires one child-table DELETE per cascade-removed child.
	await db.query("DELETE parent:p1");
	await events.waitFor((e) => e.action === "DELETE" && String(e.recordId) === "child:c1");
	await events.waitFor((e) => e.action === "DELETE" && String(e.recordId) === "child:c2");

	const deletes = events
		.all()
		.filter((e) => e.action === "DELETE")
		.map((e) => String(e.recordId))
		.sort();
	expect(deletes).toEqual(["child:c1", "child:c2"]);

	await sub.kill();
	await db.close();
}, 30000);

test("ON DELETE REJECT refuses deleting a still-referenced parent", async () => {
	const { db } = await rootClient(server);
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE record<parent> REFERENCE ON DELETE REJECT;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;`,
		)
		.collect();

	// The delete is refused with the documented message naming both records.
	const boom = db
		.query("DELETE parent:p1")
		.collect()
		.then(() => null)
		.catch((e) => e as Error);
	const err = await boom;
	expect(err).toBeInstanceOf(Error);
	expect((err as Error).message).toContain(
		"Cannot delete `parent:p1` as it is referenced by `child:c1` with an ON DELETE REJECT clause",
	);

	// The parent and its referencing child both survive the refused delete.
	const [parent] = (await db.query("SELECT * FROM parent").json()) as [Array<{ id: string }>];
	expect(parent).toEqual([{ id: "parent:p1" }]);
	const [child] = (await db.query("SELECT * FROM child").json()) as [
		Array<{ id: string; parent: string }>,
	];
	expect(child).toEqual([{ id: "child:c1", parent: "parent:p1" }]);

	await db.close();
}, 30000);

test("ON DELETE REJECT in a non-transactional batch still returns the prior statement results", async () => {
	// A fresh ns/db to run the raw multi-statement batch against.
	const { db, namespace, database } = await rootClient(server);
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE record<parent> REFERENCE ON DELETE REJECT;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;`,
		)
		.collect();

	// The raw wire client exposes the per-statement envelope the SDK collapses
	// into a single throw. Outside an explicit transaction the statements run
	// independently: the CREATE before the rejected DELETE keeps its committed
	// result, the DELETE reports the reference error, and the CREATE after it
	// still runs.
	const rpc = await RpcClient.connect(server);
	await rpc.signinRoot();
	await rpc.use(namespace, database);
	const res = await rpc.rpc("query", [
		"CREATE other:o1 SET n = 1; DELETE parent:p1; CREATE other:o2 SET n = 2;",
	]);
	const statements = res.result as Array<{ status: string; result: unknown }>;
	expect(statements).toHaveLength(3);

	expect(statements[0].status).toBe("OK");
	expect(statements[0].result).toEqual([{ id: "other:o1", n: 1 }]);

	expect(statements[1].status).toBe("ERR");
	expect(statements[1].result).toBe(
		"Cannot delete `parent:p1` as it is referenced by `child:c1` with an ON DELETE REJECT clause",
	);

	expect(statements[2].status).toBe("OK");
	expect(statements[2].result).toEqual([{ id: "other:o2", n: 2 }]);

	await rpc.close();
	await db.close();
}, 30000);

test("ON DELETE UNSET clears the reference field on the child instead of deleting it", async () => {
	const { db } = await rootClient(server);
	// UNSET writes NONE back into the reference field, so the field must accept
	// NONE — an `option<record<parent>>`.
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE option<record<parent>> REFERENCE ON DELETE UNSET;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;`,
		)
		.collect();

	// Deleting the parent keeps the child but unsets its reference field.
	await db.query("DELETE parent:p1").collect();
	const [children] = (await db.query("SELECT * FROM child").json()) as [
		Array<{ id: string; parent?: unknown }>,
	];
	expect(children).toEqual([{ id: "child:c1" }]);
	expect(children[0].parent).toBeUndefined();

	await db.close();
}, 30000);

test("ON DELETE UNSET on a non-optional reference field refuses the delete", async () => {
	const { db } = await rootClient(server);
	// A mandatory `record<parent>` field cannot hold the NONE that UNSET writes,
	// so the parent delete fails coercion and both records are preserved.
	await db
		.query(
			`DEFINE FIELD parent ON child TYPE record<parent> REFERENCE ON DELETE UNSET;
			 CREATE parent:p1;
			 CREATE child:c1 SET parent = parent:p1;`,
		)
		.collect();

	const boom = db
		.query("DELETE parent:p1")
		.collect()
		.then(() => null)
		.catch((e) => e as Error);
	const err = await boom;
	expect(err).toBeInstanceOf(Error);
	expect((err as Error).message).toContain(
		"An error occurred while updating references for `parent:p1`: Couldn't coerce value for field `parent` of `child:c1`: Expected `record<parent>` but found `NONE`",
	);

	const [child] = (await db.query("SELECT * FROM child").json()) as [
		Array<{ id: string; parent: string }>,
	];
	expect(child).toEqual([{ id: "child:c1", parent: "parent:p1" }]);

	await db.close();
}, 30000);
