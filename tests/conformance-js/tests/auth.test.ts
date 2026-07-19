import { afterAll, beforeAll, expect, test } from "bun:test";
import { Surreal } from "surrealdb";
import { rootClient, guestClient, startServer, type TestServer } from "../src/harness";

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

test("root signin over the wire succeeds and grants root-level access", async () => {
	const { db } = await rootClient(server);
	const [ok] = await db.query("INFO FOR ROOT").json();
	expect(ok).toBeDefined();
	await db.close();
});

test("root signin with a wrong password is rejected", async () => {
	const db = new Surreal();
	const err = await rejects(
		db.connect(server.url, {
			namespace: "nope",
			database: "nope",
			authentication: { username: "root", password: "wrong-password" },
		}),
	);
	expect(String(err)).toMatch(/authentication|credentials|InvalidAuth|failed/i);
	await db.close();
});

test("database user with VIEWER role can read but not write", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE USER dbviewer ON DATABASE PASSWORD 'viewer-pass-1' ROLES VIEWER;
		DEFINE TABLE widget SCHEMALESS;
		CREATE widget:one SET name = 'first';
	`);

	const viewer = new Surreal();
	await viewer.connect(server.url, { namespace, database });
	await viewer.signin({ namespace, database, username: "dbviewer", password: "viewer-pass-1" });

	const [rows] = await viewer.query<[unknown[]]>("SELECT * FROM widget").json();
	expect(rows).toHaveLength(1);

	// Observed 3.x behavior: a role-denied data write is SILENTLY filtered —
	// the statement succeeds with an empty result and no record is created.
	// (Schema-level operations by contrast reject loudly; see the editor test.)
	// Spec-pinned here; flagged as a wart-adjudication candidate.
	const [created] = await viewer.query<[unknown[]]>("CREATE widget:two SET name = 'second'").json();
	expect(created).toHaveLength(0);
	const [check] = await db.query<[unknown[]]>("SELECT * FROM widget:two").json();
	expect(check).toHaveLength(0);

	await viewer.close();
	await db.close();
});

test("database user with EDITOR role can write data but not change schema", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE USER dbeditor ON DATABASE PASSWORD 'editor-pass-1' ROLES EDITOR;
		DEFINE TABLE gadget SCHEMALESS;
	`);

	const editor = new Surreal();
	await editor.connect(server.url, { namespace, database });
	await editor.signin({ namespace, database, username: "dbeditor", password: "editor-pass-1" });

	const [created] = await editor.query<[unknown[]]>("CREATE gadget:one SET n = 1").json();
	expect(created).toHaveLength(1);

	const err = await rejects(editor.query("DEFINE USER sneaky ON DATABASE PASSWORD 'x' ROLES OWNER").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError/i);

	await editor.close();
	await db.close();
});

const RECORD_ACCESS_SETUP = `
	DEFINE TABLE user SCHEMALESS
		PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

test("record access: signup creates the user and yields a working token", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	const client = await guestClient(server, namespace, database);
	const tokens = await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "alice@example.com", pass: "alice-pw" },
	});
	expect(tokens.access).toBeString();

	// $auth is populated and points at the created user record.
	const [auth] = await client.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^user:/);

	await client.close();
	await db.close();
});

test("record access: signin authenticates an existing record user", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	const a = await guestClient(server, namespace, database);
	await a.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "bob@example.com", pass: "bob-pw" },
	});
	await a.close();

	const b = await guestClient(server, namespace, database);
	const tokens = await b.signin({
		namespace,
		database,
		access: "account",
		variables: { email: "bob@example.com", pass: "bob-pw" },
	});
	expect(tokens.access).toBeString();

	const err = await (async () => {
		const c = await guestClient(server, namespace, database);
		try {
			await c.signin({
				namespace,
				database,
				access: "account",
				variables: { email: "bob@example.com", pass: "wrong" },
			});
			return null;
		} catch (e) {
			return e as Error;
		} finally {
			await c.close();
		}
	})();
	expect(err).not.toBeNull();

	await b.close();
	await db.close();
});

test("record users only see rows their table permissions allow", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	const alice = await guestClient(server, namespace, database);
	await alice.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "a@example.com", pass: "a-pw" },
	});
	const bob = await guestClient(server, namespace, database);
	await bob.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "b@example.com", pass: "b-pw" },
	});

	// Root sees both users; each record user sees exactly their own row.
	const [allRows] = await db.query<[unknown[]]>("SELECT * FROM user").json();
	expect(allRows).toHaveLength(2);
	const [aliceRows] = await alice.query<[Array<{ email: string }>]>("SELECT * FROM user").json();
	expect(aliceRows).toHaveLength(1);
	expect(aliceRows[0].email).toBe("a@example.com");
	const [bobRows] = await bob.query<[Array<{ email: string }>]>("SELECT * FROM user").json();
	expect(bobRows).toHaveLength(1);
	expect(bobRows[0].email).toBe("b@example.com");

	await alice.close();
	await bob.close();
	await db.close();
});

test("authenticate() with a token authorizes a fresh connection; invalidate() clears it", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(RECORD_ACCESS_SETUP);

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "carol@example.com", pass: "carol-pw" },
	});
	await signer.close();

	const fresh = await guestClient(server, namespace, database);
	await fresh.authenticate(tokens.access);
	const [auth] = await fresh.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^user:/);

	await fresh.invalidate();
	// The session is anonymous again, and anonymous access is denied by default.
	const err = await rejects(fresh.query("RETURN $auth").collect());
	expect(String(err)).toMatch(/anonymous|not allowed|permissions/i);

	await fresh.close();
	await db.close();
});

test(
	"sessions expire after DURATION FOR SESSION elapses",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(`
			DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
			DEFINE ACCESS shortlived ON DATABASE TYPE RECORD
				SIGNUP ( CREATE user SET email = $email )
				SIGNIN ( SELECT * FROM user WHERE email = $email )
				DURATION FOR TOKEN 15m, FOR SESSION 1s;
		`);

		const client = await guestClient(server, namespace, database);
		await client.signup({
			namespace,
			database,
			access: "shortlived",
			variables: { email: "brief@example.com" },
		});

		// Immediately after signin the session works.
		const [auth] = await client.query<[string]>("RETURN $auth").json();
		expect(String(auth)).toMatch(/^user:/);

		// Poll until the session is rejected as expired (bounded, flake-tolerant).
		const deadline = Date.now() + 10000;
		let expired = false;
		while (Date.now() < deadline) {
			await Bun.sleep(500);
			try {
				await client.query("SELECT * FROM user").collect();
			} catch (e) {
				expect(String(e)).toMatch(/expired|invalid.*session|authentication/i);
				expired = true;
				break;
			}
		}
		expect(expired).toBe(true);

		await client.close();
		await db.close();
	},
	20000,
);
