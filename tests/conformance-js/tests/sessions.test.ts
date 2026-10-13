import { afterAll, beforeAll, expect, test } from "bun:test";
import { Table, type LiveMessage } from "surrealdb";
import { EventCollector, rootClient, startServer, type TestServer } from "../src/harness";

// Multiplexed sessions: several server-side sessions (each with its own
// auth, namespace/database selection, and parameters) sharing ONE WebSocket
// connection, via the SDK's newSession()/forkSession()/sessions() API.

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

const RECORD_ACCESS_SETUP = `
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email )
		SIGNIN ( SELECT * FROM user WHERE email = $email )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

test("sessions() lists multiplexed sessions; the default session is not listed", async () => {
	const { db } = await rootClient(server);

	// The primary session is the connection default: it has no session id of
	// its own (`db.session` is undefined) and sessions() starts empty.
	expect(db.session).toBeUndefined();
	expect(await db.sessions()).toHaveLength(0);

	const a = await db.newSession();
	const b = await db.newSession();
	expect(a.isValid).toBe(true);
	expect(b.isValid).toBe(true);
	expect(String(a.session)).not.toBe(String(b.session));

	const listed = (await db.sessions()).map(String);
	expect(listed).toHaveLength(2);
	expect(listed).toContain(String(a.session));
	expect(listed).toContain(String(b.session));

	await a.closeSession();
	await b.closeSession();
	expect(await db.sessions()).toHaveLength(0);

	await db.close();
});

test("newSession() starts anonymous and unselected, not a clone of the primary session", async () => {
	const { db, namespace, database } = await rootClient(server);

	const fresh = await db.newSession();
	// No namespace/database inherited from the (root, ns/db-selected) primary.
	expect(fresh.namespace).toBeUndefined();
	expect(fresh.database).toBeUndefined();

	// And no auth inherited either: anonymous access is rejected by default.
	const err = await rejects(fresh.query("RETURN 1").collect());
	expect(String(err)).toMatch(/anonymous|not allowed|permissions/i);

	// The session becomes usable after its own signin + use.
	await fresh.signin({ username: "root", password: "root" });
	await fresh.use({ namespace, database });
	const [one] = await fresh.query<[number]>("RETURN 1").json();
	expect(one).toBe(1);

	await fresh.closeSession();
	await db.close();
});

test("two sessions on one connection hold different ns/db selections independently", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE DATABASE mx_db_a; DEFINE DATABASE mx_db_b;");

	const a = await db.newSession();
	const b = await db.newSession();
	await a.signin({ username: "root", password: "root" });
	await b.signin({ username: "root", password: "root" });
	await a.use({ namespace, database: "mx_db_a" });
	await b.use({ namespace, database: "mx_db_b" });

	// Each server-side session reports its own selection and its own id.
	// (Note: `RETURN a, b` is not valid SurrealQL — array form required.)
	const [[dbA, idA]] = await a.query<[[string, string]]>("RETURN [session::db(), session::id()]").json();
	const [[dbB, idB]] = await b.query<[[string, string]]>("RETURN [session::db(), session::id()]").json();
	expect(dbA).toBe("mx_db_a");
	expect(dbB).toBe("mx_db_b");
	expect(idA).not.toBe(idB);

	// Same table name, different databases: writes land in separate databases.
	await a.query("CREATE item:one SET origin = 'a'");
	await b.query("CREATE item:one SET origin = 'b'");
	const [rowsA] = await a.query<[Array<{ origin: string }>]>("SELECT origin FROM item").json();
	const [rowsB] = await b.query<[Array<{ origin: string }>]>("SELECT origin FROM item").json();
	expect(rowsA).toEqual([{ origin: "a" }]);
	expect(rowsB).toEqual([{ origin: "b" }]);

	// The primary session's selection was never touched — its database never
	// even saw the table. (SELECT from a never-created table rejects with
	// "table does not exist" rather than returning [].)
	const [primaryDb] = await db.query<[string]>("RETURN session::db()").json();
	expect(primaryDb).toBe(database);
	const err = await rejects(db.query("SELECT * FROM item").collect());
	expect(String(err)).toMatch(/table 'item' does not exist/i);

	await a.closeSession();
	await b.closeSession();
	await db.close();
});

test("session parameters set() on one session are invisible to siblings and the primary", async () => {
	const { db, namespace, database } = await rootClient(server);

	const a = await db.newSession();
	const b = await db.newSession();
	for (const s of [a, b]) {
		await s.signin({ username: "root", password: "root" });
		await s.use({ namespace, database });
	}

	await db.set("who", "primary");
	await a.set("who", "session-a");
	await b.set("who", "session-b");

	const [seenA] = await a.query<[string]>("RETURN $who").json();
	const [seenB] = await b.query<[string]>("RETURN $who").json();
	const [seenP] = await db.query<[string]>("RETURN $who").json();
	expect(seenA).toBe("session-a");
	expect(seenB).toBe("session-b");
	expect(seenP).toBe("primary");

	// unset() is scoped the same way.
	await a.unset("who");
	const [goneA] = await a.query<[unknown]>("RETURN $who").json();
	const [stillB] = await b.query<[string]>("RETURN $who").json();
	expect(goneA).toBeUndefined();
	expect(stillB).toBe("session-b");

	await a.closeSession();
	await b.closeSession();
	await db.close();
});

test("forkSession() clones auth, ns/db, and params — then diverges independently", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.set("legacy", "from-primary");

	const fork = await db.forkSession();
	// Inherited: namespace, database, parameters, and root authentication.
	expect(fork.namespace).toBe(namespace);
	expect(fork.database).toBe(database);
	const [inherited] = await fork.query<[string]>("RETURN $legacy").json();
	expect(inherited).toBe("from-primary");
	const [rootInfo] = await fork.query<[unknown]>("INFO FOR ROOT").json();
	expect(rootInfo).toBeDefined();

	// But it is a distinct server-side session...
	const [forkId] = await fork.query<[string]>("RETURN session::id()").json();
	const [primaryId] = await db.query<[string]>("RETURN session::id()").json();
	expect(forkId).not.toBe(primaryId);

	// ...and its state is a copy, not a reference: divergence does not leak back.
	await fork.set("legacy", "changed-in-fork");
	const [primarySees] = await db.query<[string]>("RETURN $legacy").json();
	expect(primarySees).toBe("from-primary");

	await fork.closeSession();
	await db.close();
});

test("root and record-user sessions coexist on one connection, each with its own visibility", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	// A record user signs up inside a multiplexed session; the primary session
	// on the SAME connection stays root the whole time.
	const member = await db.newSession();
	const tokens = await member.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "member@example.com" },
	});
	expect(tokens.access).toBeString();

	// Root also creates a second user the record user must not see.
	await db.query("CREATE user SET email = 'stranger@example.com'");

	// Interleaved on one socket: root sees both rows, the record user sees
	// exactly their own ($auth-filtered), and each session reports its own kind.
	const [allRows] = await db.query<[unknown[]]>("SELECT * FROM user").json();
	expect(allRows).toHaveLength(2);
	const [memberRows] = await member.query<[Array<{ email: string }>]>("SELECT * FROM user").json();
	expect(memberRows).toHaveLength(1);
	expect(memberRows[0].email).toBe("member@example.com");

	const [memberAuth] = await member.query<[string]>("RETURN $auth").json();
	expect(String(memberAuth)).toMatch(/^user:/);
	const [primaryAuth] = await db.query<[unknown]>("RETURN $auth").json();
	expect(primaryAuth).toBeUndefined(); // system users have no record $auth

	await member.closeSession();
	await db.close();
});

test("closeSession() disposes one session without affecting its sibling or the primary", async () => {
	const { db, namespace, database } = await rootClient(server);

	const a = await db.newSession();
	const b = await db.newSession();
	for (const s of [a, b]) {
		await s.signin({ username: "root", password: "root" });
		await s.use({ namespace, database });
	}
	const closedId = String(a.session);

	await a.closeSession();
	expect(a.isValid).toBe(false);
	expect(b.isValid).toBe(true);

	// The server no longer knows the session: queries fail with Session not found.
	const err = await rejects(a.query("RETURN 1").collect());
	expect(String(err)).toMatch(/session not found/i);

	// The sibling and the primary session are untouched.
	const [fromB] = await b.query<[number]>("RETURN 2").json();
	expect(fromB).toBe(2);
	const [fromPrimary] = await db.query<[number]>("RETURN 3").json();
	expect(fromPrimary).toBe(3);

	const remaining = (await db.sessions()).map(String);
	expect(remaining).not.toContain(closedId);
	expect(remaining).toContain(String(b.session));

	await b.closeSession();
	await db.close();
});

test("invalidate() de-authenticates one session but not its sibling or the primary", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	const s1 = await db.newSession();
	const s2 = await db.newSession();
	await s1.signup({ namespace, database, access: "account", variables: { email: "one@example.com" } });
	await s2.signup({ namespace, database, access: "account", variables: { email: "two@example.com" } });
	const [auth2Before] = await s2.query<[string]>("RETURN $auth").json();

	await s1.invalidate();

	// s1 is anonymous again (rejected by default) — but still a live session:
	// invalidate() clears auth without disposing, so it stays in sessions().
	const err = await rejects(s1.query("RETURN $auth").collect());
	expect(String(err)).toMatch(/anonymous|not allowed|permissions/i);
	expect(s1.isValid).toBe(true);
	expect((await db.sessions()).map(String)).toContain(String(s1.session));

	// The sibling record user and the root primary session keep their auth.
	const [auth2After] = await s2.query<[string]>("RETURN $auth").json();
	expect(String(auth2After)).toBe(String(auth2Before));
	const [rootInfo] = await db.query<[unknown]>("INFO FOR ROOT").json();
	expect(rootInfo).toBeDefined();

	await s1.closeSession();
	await s2.closeSession();
	await db.close();
});

test(
	"a live subscription belongs to its session: closeSession() stops delivery, sibling unaffected",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query("DEFINE TABLE watched SCHEMALESS");

		const watcher = await db.newSession();
		await watcher.signin({ username: "root", password: "root" });
		await watcher.use({ namespace, database });

		const events = new EventCollector<LiveMessage>();
		const sub = await watcher.live(new Table("watched"));
		sub.subscribe((m) => events.push(m));

		// A write from the PRIMARY session (same socket) reaches the watcher session.
		await db.query("CREATE watched:before SET v = 1");
		const before = await events.waitFor((e) => String(e.recordId) === "watched:before");
		expect(before.action).toBe("CREATE");

		await watcher.closeSession();

		// The live query died with its session: no further notifications.
		await db.query("CREATE watched:after SET v = 2");
		await events.assertSilence((e) => String(e.recordId) === "watched:after", 1500);

		// The subscription is no longer alive: its owning session was disposed.
		expect(sub.isAlive).toBe(false);

		// The primary session's own queries are unaffected throughout.
		const [rows] = await db.query<[unknown[]]>("SELECT * FROM watched").json();
		expect(rows).toHaveLength(2);

		await db.close();
	},
	20000,
);
