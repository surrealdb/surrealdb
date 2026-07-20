import { afterAll, beforeAll, expect, test } from "bun:test";
import { Table, type Uuid, type LiveMessage } from "surrealdb";
import {
	EventCollector,
	guestClient,
	rootClient,
	startServer,
	type TestServer,
} from "../src/harness";

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

test("live query delivers CREATE, UPDATE, and DELETE notifications in order", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE person SCHEMALESS");

	const events = new EventCollector<LiveMessage>();
	const sub = await db.live(new Table("person"));
	pump(sub, events);

	await db.query("CREATE person:tobie SET name = 'Tobie'");
	const created = await events.waitFor((e) => e.action === "CREATE");
	expect(String(created.recordId)).toBe("person:tobie");
	expect(created.value.name).toBe("Tobie");

	await db.query("UPDATE person:tobie SET name = 'Tobie MH'");
	const updated = await events.waitFor((e) => e.action === "UPDATE");
	expect(updated.value.name).toBe("Tobie MH");

	await db.query("DELETE person:tobie");
	const deleted = await events.waitFor((e) => e.action === "DELETE");
	expect(String(deleted.recordId)).toBe("person:tobie");

	// Order is CREATE, UPDATE, DELETE.
	const actions = events.all().map((e) => e.action);
	expect(actions).toEqual(["CREATE", "UPDATE", "DELETE"]);

	await sub.kill();
	await db.close();
});

test("notifications cross connections: a second client's writes are delivered", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE TABLE city SCHEMALESS");

	const events = new EventCollector<LiveMessage>();
	const sub = await db.live(new Table("city"));
	pump(sub, events);

	// Writer is an entirely separate authenticated connection.
	const writer = await guestClient(server, namespace, database);
	await writer.signin({ username: "root", password: "root" });
	await writer.use({ namespace, database });
	await writer.query("CREATE city:london SET population = 9000000");

	const created = await events.waitFor((e) => e.action === "CREATE");
	expect(String(created.recordId)).toBe("city:london");

	await sub.kill();
	await writer.close();
	await db.close();
});

test("kill() stops delivery; subsequent writes produce no notifications", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE order SCHEMALESS");

	const events = new EventCollector<LiveMessage>();
	const sub = await db.live(new Table("order"));
	pump(sub, events);

	await db.query("CREATE order:one SET total = 10");
	await events.waitFor((e) => e.action === "CREATE");
	expect(sub.isAlive).toBe(true);

	await sub.kill();
	expect(sub.isAlive).toBe(false);

	await db.query("CREATE order:two SET total = 20");
	await events.assertSilence(
		(e) => e.action === "CREATE" && String(e.recordId) === "order:two",
	);

	await db.close();
});

test("live notifications respect the subscriber's row-level permissions", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET email = $email )
			SIGNIN ( SELECT * FROM user WHERE email = $email )
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
		DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	`);

	// A record user subscribes to `post`.
	const subscriber = await guestClient(server, namespace, database);
	await subscriber.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "watcher@example.com" },
	});
	const [me] = await subscriber.query<[string]>("RETURN $auth").json();

	const events = new EventCollector<LiveMessage>();
	const sub = await subscriber.live(new Table("post"));
	pump(sub, events);

	// Root creates one post owned by the subscriber, one owned by nobody.
	await db.query("CREATE post:mine SET title = 'visible', owner = <record>$owner", {
		owner: String(me),
	});
	await db.query("CREATE post:other SET title = 'hidden', owner = user:someone_else");

	const visible = await events.waitFor((e) => String(e.recordId) === "post:mine");
	expect(visible.action).toBe("CREATE");
	expect(visible.value.title).toBe("visible");

	// The post the subscriber cannot SELECT must never be delivered.
	await events.assertSilence((e) => String(e.recordId) === "post:other");

	await sub.kill();
	await subscriber.close();
	await db.close();
});

test("two subscribers on one table each receive their own stream", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE TABLE event SCHEMALESS");

	const a = new EventCollector<LiveMessage>();
	const b = new EventCollector<LiveMessage>();
	const subA = await db.live(new Table("event"));
	pump(subA, a);

	const other = await guestClient(server, namespace, database);
	await other.signin({ username: "root", password: "root" });
	await other.use({ namespace, database });
	const subB = await other.live(new Table("event"));
	pump(subB, b);

	await db.query("CREATE event:launch SET at = time::now()");

	const gotA = await a.waitFor((e) => e.action === "CREATE", 10000);
	const gotB = await b.waitFor((e) => e.action === "CREATE", 10000);
	expect(String(gotA.recordId)).toBe("event:launch");
	expect(String(gotB.recordId)).toBe("event:launch");
	// Distinct live query ids — two real subscriptions, not a shared one.
	expect(String(gotA.queryId)).not.toBe(String(gotB.queryId));

	await subA.kill();
	await subB.kill();
	await other.close();
	await db.close();
}, 25000);

test("live query WHERE filter delivers only matching records", async () => {
	const { db } = await rootClient(server);
	await db.query("DEFINE TABLE metric SCHEMALESS");

	// A WHERE-filtered LIVE SELECT run through query() returns the live-query
	// UUID; liveOf() attaches an unmanaged subscription to that id.
	const [uuid] = await db.query<[Uuid]>("LIVE SELECT * FROM metric WHERE n > 10");

	const events = new EventCollector<LiveMessage>();
	const sub = await db.liveOf(uuid);
	pump(sub, events);

	// A matching record is delivered.
	await db.query("CREATE metric:high SET n = 20");
	const matched = await events.waitFor((e) => e.action === "CREATE");
	expect(String(matched.recordId)).toBe("metric:high");
	expect(matched.value.n).toBe(20);

	// A non-matching record is filtered out server-side.
	await db.query("CREATE metric:low SET n = 5");
	await events.assertSilence((e) => String(e.recordId) === "metric:low");

	await sub.kill();
	await db.close();
}, 30000);

test("live notifications respect row-level permissions on UPDATE", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET email = $email )
			SIGNIN ( SELECT * FROM user WHERE email = $email )
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
		DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	`);

	const subscriber = await guestClient(server, namespace, database);
	await subscriber.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "updater@example.com" },
	});
	const [me] = await subscriber.query<[string]>("RETURN $auth").json();

	const events = new EventCollector<LiveMessage>();
	const sub = await subscriber.live(new Table("post"));
	pump(sub, events);

	await db.query("CREATE post:mine SET title = 'mine', owner = <record>$owner", {
		owner: String(me),
	});
	await db.query("CREATE post:other SET title = 'other', owner = user:someone_else");
	await events.waitFor((e) => String(e.recordId) === "post:mine");

	// An UPDATE to a row the subscriber can SELECT is delivered.
	await db.query("UPDATE post:mine SET title = 'mine v2'");
	const updated = await events.waitFor(
		(e) => e.action === "UPDATE" && String(e.recordId) === "post:mine",
	);
	expect(updated.value.title).toBe("mine v2");

	// An UPDATE to a row the subscriber cannot SELECT is never delivered.
	await db.query("UPDATE post:other SET title = 'other v2'");
	await events.assertSilence(
		(e) => e.action === "UPDATE" && String(e.recordId) === "post:other",
	);

	await sub.kill();
	await subscriber.close();
	await db.close();
}, 30000);

test("live notifications respect row-level permissions on DELETE", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE user SET email = $email )
			SIGNIN ( SELECT * FROM user WHERE email = $email )
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
		DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	`);

	const subscriber = await guestClient(server, namespace, database);
	await subscriber.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "deleter@example.com" },
	});
	const [me] = await subscriber.query<[string]>("RETURN $auth").json();

	const events = new EventCollector<LiveMessage>();
	const sub = await subscriber.live(new Table("post"));
	pump(sub, events);

	await db.query("CREATE post:mine SET title = 'mine', owner = <record>$owner", {
		owner: String(me),
	});
	await db.query("CREATE post:other SET title = 'other', owner = user:someone_else");
	await events.waitFor((e) => String(e.recordId) === "post:mine");

	// A DELETE of a row the subscriber can SELECT is delivered.
	await db.query("DELETE post:mine");
	const deleted = await events.waitFor(
		(e) => e.action === "DELETE" && String(e.recordId) === "post:mine",
	);
	expect(String(deleted.recordId)).toBe("post:mine");

	// A DELETE of a row the subscriber cannot SELECT is never delivered.
	await db.query("DELETE post:other");
	await events.assertSilence(
		(e) => e.action === "DELETE" && String(e.recordId) === "post:other",
	);

	await sub.kill();
	await subscriber.close();
	await db.close();
}, 30000);
