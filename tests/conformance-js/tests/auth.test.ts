import { afterAll, beforeAll, expect, test } from "bun:test";
import { Surreal } from "surrealdb";
import { rootClient, guestClient, startServer, RpcClient, type TestServer } from "../src/harness";

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

// §4 — access-method types beyond RECORD: BEARER grants, external JWT, AUTHENTICATE.

// Mint an HS256 JWT with WebCrypto (no JWT dependency is available in this suite).
function base64url(input: string | Uint8Array): string {
	const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
	let bin = "";
	for (const b of bytes) bin += String.fromCharCode(b);
	return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function mintHs256(payload: Record<string, unknown>, secret: string): Promise<string> {
	const header = base64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
	const body = base64url(JSON.stringify(payload));
	const signingInput = `${header}.${body}`;
	const key = await crypto.subtle.importKey(
		"raw",
		new TextEncoder().encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(signingInput));
	return `${signingInput}.${base64url(new Uint8Array(sig))}`;
}

test("bearer access: a GRANT yields a usable key that signs in at the user's role", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 4w, FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
	`);

	// The GRANT returns a grant object; the one-time secret key lives at grant.key.
	// Shape: { ac, creation, expiration, grant: { id, key }, id, subject: { user }, type: "bearer" }.
	// The key is formatted "surreal-bearer-<id>-<secret>".
	const [grant] = await db
		.query<[{ ac: string; type: string; subject: { user: string }; grant: { id: string; key: string } }]>(
			"ACCESS api ON DATABASE GRANT FOR USER tobie",
		)
		.json();
	expect(grant.ac).toBe("api");
	expect(grant.type).toBe("bearer");
	expect(grant.subject).toEqual({ user: "tobie" });
	expect(grant.grant.id).toBeString();
	expect(grant.grant.key).toStartWith("surreal-bearer-");

	const client = await guestClient(server, namespace, database);
	const tokens = await client.signin({ namespace, database, access: "api", key: grant.grant.key });
	expect(tokens.access).toBeString();

	// The bearer subject is a system user, not a record — $auth is NONE, but the
	// session carries the access name and the user's role (EDITOR).
	const [auth] = await client.query<[unknown]>("RETURN $auth").json();
	expect(auth).toBeUndefined();
	const [session] = await client
		.query<[{ ac: string; tk: { AC: string; RL: string[] } }]>("RETURN $session")
		.json();
	expect(session.ac).toBe("api");
	expect(session.tk.RL).toEqual(["EDITOR"]);

	// EDITOR powers: INFO FOR DB reads fine; an owner-only schema write is rejected.
	const [info] = await client.query<[unknown]>("INFO FOR DB").json();
	expect(info).toBeDefined();
	const err = await rejects(client.query("DEFINE USER sneaky ON DATABASE PASSWORD 'x' ROLES OWNER").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError/i);

	await client.close();
	await db.close();
});

test("bearer access: a bogus key is rejected", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 4w, FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
	`);

	const client = await guestClient(server, namespace, database);
	const err = await rejects(
		client.signin({
			namespace,
			database,
			access: "api",
			key: "surreal-bearer-AAAAAAAAAAAA-notarealsecretkeyvalue00",
		}),
	);
	expect(String(err)).toMatch(/authentication|not allowed|invalid/i);

	await client.close();
	await db.close();
});

test("jwt access: an HS256 token with a roles claim authenticates at that role", async () => {
	const { db, namespace, database } = await rootClient(server);
	const secret = "a-shared-secret-that-is-long-enough";
	await db.query(`DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS256 KEY '${secret}';`);

	const now = Math.floor(Date.now() / 1000);
	const jwt = await mintHs256(
		{ ns: namespace, db: database, ac: "token", rl: ["Owner"], iat: now, exp: now + 3600 },
		secret,
	);

	const client = await guestClient(server, namespace, database);
	await client.authenticate(jwt);

	// A JWT user-level token maps to a system role, not a record: $auth is NONE.
	// The claims are echoed under $session.tk and the access name under $access.
	const [auth] = await client.query<[unknown]>("RETURN $auth").json();
	expect(auth).toBeUndefined();
	const [access] = await client.query<[string]>("RETURN $access").json();
	expect(access).toBe("token");
	const [session] = await client
		.query<[{ ac: string; tk: { AC: string; RL: string[] } }]>("RETURN $session")
		.json();
	expect(session.ac).toBe("token");
	expect(session.tk.RL).toEqual(["Owner"]);

	// Owner powers: a schema write succeeds (DEFINE returns NONE -> undefined).
	const [defined] = await client.query<[unknown]>("DEFINE TABLE t1 SCHEMALESS").json();
	expect(defined).toBeUndefined();
	const [tables] = await client.query<[{ tables: Record<string, unknown> }]>("INFO FOR DB").json();
	expect(tables.tables).toHaveProperty("t1");

	await client.close();
	await db.close();
});

test("jwt access: a token with no roles claim silently authenticates as VIEWER", async () => {
	const { db, namespace, database } = await rootClient(server);
	const secret = "a-shared-secret-that-is-long-enough";
	await db.query(`DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS256 KEY '${secret}';`);

	const now = Math.floor(Date.now() / 1000);
	// Same token as above but with the rl claim omitted entirely.
	const jwt = await mintHs256(
		{ ns: namespace, db: database, ac: "token", iat: now, exp: now + 3600 },
		secret,
	);

	const client = await guestClient(server, namespace, database);
	await client.authenticate(jwt);

	// Observed: a valid token missing rl authenticates rather than being rejected,
	// and lands at VIEWER — reads succeed but any write is denied.
	const [info] = await client.query<[unknown]>("INFO FOR DB").json();
	expect(info).toBeDefined();
	const err = await rejects(client.query("DEFINE TABLE t2 SCHEMALESS").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError/i);

	await client.close();
	await db.close();
});

test("jwt access: a token signed with the wrong key is rejected", async () => {
	const { db, namespace, database } = await rootClient(server);
	const secret = "a-shared-secret-that-is-long-enough";
	await db.query(`DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS256 KEY '${secret}';`);

	const now = Math.floor(Date.now() / 1000);
	const jwt = await mintHs256(
		{ ns: namespace, db: database, ac: "token", rl: ["Owner"], iat: now, exp: now + 3600 },
		"a-different-secret-that-is-also-long-enough",
	);

	const client = await guestClient(server, namespace, database);
	const err = await rejects(client.authenticate(jwt));
	expect(String(err)).toMatch(/authentication|not allowed|invalid|token/i);

	await client.close();
	await db.close();
});

const AUTHENTICATE_ACCESS_SETUP = `
	DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, enabled = $enabled )
		SIGNIN ( SELECT * FROM user WHERE email = $email )
		AUTHENTICATE { IF !$auth.enabled { THROW 'account disabled' }; RETURN $auth; }
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

test("authenticate clause: signin succeeds for an enabled account and is rejected for a disabled one", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(AUTHENTICATE_ACCESS_SETUP);

	// The AUTHENTICATE clause runs on signup too, so a disabled account cannot even
	// be created through the access method — seed the rows directly as root instead.
	await db.query(`
		CREATE user SET email = 'enabled@example.com', enabled = true;
		CREATE user SET email = 'disabled@example.com', enabled = false;
	`);

	// $auth inside AUTHENTICATE is the record returned by SIGNIN; a false enabled
	// flag makes the clause THROW and the signin fails with the thrown message.
	const denied = await guestClient(server, namespace, database);
	const err = await rejects(
		denied.signin({
			namespace,
			database,
			access: "account",
			variables: { email: "disabled@example.com" },
		}),
	);
	expect(String(err)).toMatch(/account disabled/i);
	await denied.close();

	const allowed = await guestClient(server, namespace, database);
	const tokens = await allowed.signin({
		namespace,
		database,
		access: "account",
		variables: { email: "enabled@example.com" },
	});
	expect(tokens.access).toBeString();
	const [auth] = await allowed.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^user:/);

	await allowed.close();
	await db.close();
});

test("authenticate clause: a THROW blocks signup as well as signin", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(AUTHENTICATE_ACCESS_SETUP);

	// Because AUTHENTICATE runs on the freshly-created record during signup, a
	// disabled signup is rejected with the same thrown message.
	const client = await guestClient(server, namespace, database);
	const err = await rejects(
		client.signup({
			namespace,
			database,
			access: "account",
			variables: { email: "newbie@example.com", enabled: false },
		}),
	);
	expect(String(err)).toMatch(/account disabled/i);

	// An enabled signup passes the clause and yields a token.
	const tokens = await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "welcome@example.com", enabled: true },
	});
	expect(tokens.access).toBeString();

	await client.close();
	await db.close();
});

// §6 — auth LEVEL: system access (BEARER/JWT) and system users at ROOT and
// NAMESPACE scope, plus how the signin level is inferred from what is supplied.
// Every scenario stays within one namespace/database; only the auth LEVEL varies.

test("namespace bearer access: a wire signin yields a key that authorizes at the namespace level", async () => {
	const { db, namespace } = await rootClient(server);
	await db.query(`
		DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER DURATION FOR GRANT 4w, FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE USER nsbear ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
	`);

	// Grant shape mirrors the database-level bearer grant; the one-time key lives
	// at grant.key and is formatted "surreal-bearer-<id>-<secret>".
	const [grant] = await db
		.query<[{ ac: string; type: string; subject: { user: string }; grant: { id: string; key: string } }]>(
			"ACCESS api ON NAMESPACE GRANT FOR USER nsbear",
		)
		.json();
	expect(grant.ac).toBe("api");
	expect(grant.type).toBe("bearer");
	expect(grant.subject).toEqual({ user: "nsbear" });
	expect(grant.grant.key).toStartWith("surreal-bearer-");

	// Namespace bearer signin over the wire carries { ns, ac, key } with NO db key.
	// (The signin dispatch routes on which of ns/db/ac are present, so a db key
	// here — even a null one — would misroute the request; see the SDK test below.)
	const rc = await RpcClient.connect(server);
	const res = await rc.rpc("signin", [{ ns: namespace, ac: "api", key: grant.grant.key }]);
	expect(res.error).toBeUndefined();
	const token = res.result as string;
	expect(token).toBeString();
	await rc.close();

	// The issued token authorizes a fresh connection at the namespace level:
	// INFO FOR NS reads fine, the session carries the access name and EDITOR role,
	// and reaching up to the root level is denied.
	const client = new Surreal();
	await client.connect(server.url, { namespace });
	await client.authenticate(token);
	const [nsInfo] = await client.query<[unknown]>("INFO FOR NS").json();
	expect(nsInfo).toBeDefined();
	const [session] = await client
		.query<[{ ac: string; ns: string; tk: { RL: string[] } }]>("RETURN $session")
		.json();
	expect(session.ac).toBe("api");
	expect(session.ns).toBe(namespace);
	expect(session.tk.RL).toEqual(["EDITOR"]);
	const err = await rejects(client.query("INFO FOR ROOT").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError|IAM/i);
	await client.close();
	await db.close();
});

test("namespace bearer signin authenticates through the SDK", async () => {
	// A namespace bearer key authenticates at the namespace level: signing in with
	// { namespace, access, key } and no database reaches the namespace-access path.
	const { db, namespace } = await rootClient(server);
	await db.query(`
		DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER DURATION FOR GRANT 4w, FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE USER nsbear ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
	`);
	const [grant] = await db
		.query<[{ grant: { key: string } }]>("ACCESS api ON NAMESPACE GRANT FOR USER nsbear")
		.json();

	const client = new Surreal();
	await client.connect(server.url, { namespace });
	const tokens = await client.signin({ namespace, access: "api", key: grant.grant.key });
	expect(tokens.access).toBeString();
	const [nsInfo] = await client.query<[unknown]>("INFO FOR NS").json();
	expect(nsInfo).toBeDefined();
	await client.close();
	await db.close();
});

test("root bearer access signin grants root access", async () => {
	// A root bearer key authenticates at the root level: signing in with
	// { access, key } and no namespace/database reaches the root-access path and
	// yields a session with root-level reach.
	const { db } = await rootClient(server);
	await db.query(`
		DEFINE ACCESS api ON ROOT TYPE BEARER FOR USER DURATION FOR GRANT 4w, FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE USER rootbear ON ROOT PASSWORD 'secret' ROLES OWNER;
	`);
	const [grant] = await db
		.query<[{ grant: { key: string } }]>("ACCESS api ON ROOT GRANT FOR USER rootbear")
		.json();
	const client = new Surreal();
	await client.connect(server.url);
	await client.signin({ access: "api", key: grant.grant.key });
	const [rootInfo] = await client.query<[unknown]>("INFO FOR ROOT").json();
	expect(rootInfo).toBeDefined();
	await client.close();
	await db.close();
});

test("root JWT access: a claimless-scope token authenticates at the root level", async () => {
	const { db } = await rootClient(server);
	const secret = "a-shared-secret-that-is-long-enough";
	await db.query(`DEFINE ACCESS token ON ROOT TYPE JWT ALGORITHM HS256 KEY '${secret}';`);

	// A root token carries no ns/db claim; the access name and roles come from the
	// claims. Authenticate over a connection with no namespace/database selected.
	const now = Math.floor(Date.now() / 1000);
	const jwt = await mintHs256({ ac: "token", rl: ["Owner"], iat: now, exp: now + 3600 }, secret);

	const client = new Surreal();
	await client.connect(server.url);
	await client.authenticate(jwt);

	const [rootInfo] = await client.query<[unknown]>("INFO FOR ROOT").json();
	expect(rootInfo).toBeDefined();
	// The session is root-scoped: it echoes the access name and role and carries
	// neither a namespace nor a database.
	const [session] = await client
		.query<[{ ac: string; ns?: string; db?: string; tk: { RL: string[] } }]>("RETURN $session")
		.json();
	expect(session.ac).toBe("token");
	expect(session.ns).toBeUndefined();
	expect(session.db).toBeUndefined();
	expect(session.tk.RL).toEqual(["Owner"]);

	await client.close();
	await db.close();
});

test("namespace JWT access: an ns-scoped token authenticates at the namespace level", async () => {
	const { db, namespace } = await rootClient(server);
	const secret = "a-shared-secret-that-is-long-enough";
	await db.query(`DEFINE ACCESS token ON NAMESPACE TYPE JWT ALGORITHM HS256 KEY '${secret}';`);

	// A namespace token carries an ns claim but no db claim.
	const now = Math.floor(Date.now() / 1000);
	const jwt = await mintHs256(
		{ ns: namespace, ac: "token", rl: ["Owner"], iat: now, exp: now + 3600 },
		secret,
	);

	const client = new Surreal();
	await client.connect(server.url, { namespace });
	await client.authenticate(jwt);

	const [nsInfo] = await client.query<[unknown]>("INFO FOR NS").json();
	expect(nsInfo).toBeDefined();
	const [session] = await client
		.query<[{ ac: string; ns: string; db?: string; tk: { RL: string[] } }]>("RETURN $session")
		.json();
	expect(session.ac).toBe("token");
	expect(session.ns).toBe(namespace);
	expect(session.db).toBeUndefined();
	expect(session.tk.RL).toEqual(["Owner"]);
	// The token is namespace-scoped, so it cannot reach the root level.
	const err = await rejects(client.query("INFO FOR ROOT").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError|IAM/i);

	await client.close();
	await db.close();
});

test("a namespace OWNER cannot read root info or define a root user", async () => {
	const { db, namespace } = await rootClient(server);
	await db.query("DEFINE USER nsowner ON NAMESPACE PASSWORD 'ns-pass' ROLES OWNER");

	const client = new Surreal();
	await client.connect(server.url, { namespace });
	await client.signin({ namespace, username: "nsowner", password: "ns-pass" });

	// Full authority within the namespace, but nothing above it: reading root info
	// and defining a root-level user are both rejected loudly as IAM violations.
	const [nsInfo] = await client.query<[unknown]>("INFO FOR NS").json();
	expect(nsInfo).toBeDefined();
	const infoErr = await rejects(client.query("INFO FOR ROOT").collect());
	expect(String(infoErr)).toMatch(/not allowed|permission|IamError|IAM/i);
	const defineErr = await rejects(
		client.query("DEFINE USER escalated ON ROOT PASSWORD 'x' ROLES OWNER").collect(),
	);
	expect(String(defineErr)).toMatch(/not allowed|permission|IamError|IAM/i);

	await client.close();
	await db.close();
});

test("a database OWNER cannot read namespace info", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE USER dbowner ON DATABASE PASSWORD 'db-pass' ROLES OWNER");

	const client = new Surreal();
	await client.connect(server.url, { namespace, database });
	await client.signin({ namespace, database, username: "dbowner", password: "db-pass" });

	// Full authority within the database, but reading the enclosing namespace's
	// info is rejected as an IAM violation.
	const [dbInfo] = await client.query<[unknown]>("INFO FOR DB").json();
	expect(dbInfo).toBeDefined();
	const err = await rejects(client.query("INFO FOR NS").collect());
	expect(String(err)).toMatch(/not allowed|permission|IamError|IAM/i);

	await client.close();
	await db.close();
});

test("signin level is inferred from the scope supplied, not the user's own level", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE USER leveluser ON DATABASE PASSWORD 'lvl-pass' ROLES OWNER");

	// Supplying the database scope the user was defined at authenticates at DB level.
	const full = new Surreal();
	await full.connect(server.url, { namespace, database });
	await full.signin({ namespace, database, username: "leveluser", password: "lvl-pass" });
	const [dbInfo] = await full.query<[unknown]>("INFO FOR DB").json();
	expect(dbInfo).toBeDefined();
	await full.close();

	// The same credentials are NOT honored at a different (lower-scoped) level: the
	// server looks the user up at the requested level and finds none, so a
	// namespace-scoped signin with a database user is refused — never silently
	// elevated to a namespace session.
	const nsScoped = new Surreal();
	await nsScoped.connect(server.url, { namespace });
	const nsErr = await rejects(nsScoped.signin({ namespace, username: "leveluser", password: "lvl-pass" }));
	expect(String(nsErr)).toMatch(/problem with authentication|not allowed|authentication/i);
	await nsScoped.close();

	// Likewise a root-scoped signin (no ns/db supplied) with the database user fails.
	const rootScoped = new Surreal();
	await rootScoped.connect(server.url);
	const rootErr = await rejects(
		rootScoped.signin({ username: "leveluser", password: "lvl-pass" } as never),
	);
	expect(String(rootErr)).toMatch(/problem with authentication|not allowed|authentication/i);
	await rootScoped.close();

	await db.close();
});

// §7 — system-user DURATION: DEFINE USER ... DURATION FOR SESSION / FOR TOKEN
// enforcement for root/namespace/database users (distinct from record-access
// session expiry). Every scenario stays within one namespace/database.

test(
	"a system database user session expires after DURATION FOR SESSION elapses",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(
			"DEFINE USER shortlived ON DATABASE PASSWORD 'shortlived-pass' ROLES OWNER DURATION FOR SESSION 1s",
		);

		const client = new Surreal();
		await client.connect(server.url, { namespace, database });
		await client.signin({
			namespace,
			database,
			username: "shortlived",
			password: "shortlived-pass",
		});

		// Immediately after signin the session works at its OWNER role.
		const [info] = await client.query<[unknown]>("INFO FOR DB").json();
		expect(info).toBeDefined();

		// Poll until the session is rejected as expired (bounded, flake-tolerant).
		// The default token duration outlives the poll window, so the SDK's own
		// token-expiry renewal never fires; the server rejects the live session
		// itself once DURATION FOR SESSION lapses.
		const deadline = Date.now() + 10000;
		let expired = false;
		while (Date.now() < deadline) {
			await Bun.sleep(500);
			try {
				await client.query("INFO FOR DB").collect();
			} catch (e) {
				expect(String(e)).toMatch(/session has expired/i);
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

test(
	"a system user's live session outlives its shorter token; the expired token cannot re-authenticate",
	async () => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(
			"DEFINE USER dualdur ON DATABASE PASSWORD 'dualdur-pass' ROLES OWNER DURATION FOR TOKEN 1s, FOR SESSION 1h",
		);

		// Sign in over the raw wire: the SDK renews (and, lacking a refresh token,
		// invalidates) a password session the moment its token lapses, which would
		// mask the server's own token/session split. The raw connection does not.
		const rc = await RpcClient.connect(server);
		await rc.use(namespace, database);
		const token = (await rc.call("signin", [
			{ ns: namespace, db: database, user: "dualdur", pass: "dualdur-pass" },
		])) as string;
		expect(token).toStartWith("eyJ");

		// Poll a FRESH connection authenticating with the issued token until the
		// server rejects it as expired — DURATION FOR TOKEN governs the token, and
		// the rejection surfaces as "The token has expired".
		const deadline = Date.now() + 10000;
		let tokenErr: { code: number; message: string } | undefined;
		while (Date.now() < deadline) {
			await Bun.sleep(400);
			const fresh = await RpcClient.connect(server);
			await fresh.use(namespace, database);
			const res = await fresh.rpc("authenticate", [token]);
			await fresh.close();
			if (res.error) {
				tokenErr = res.error;
				break;
			}
		}
		expect(tokenErr).toBeDefined();
		expect(tokenErr!.message).toMatch(/token has expired/i);

		// The already-established session is governed by DURATION FOR SESSION, not
		// FOR TOKEN: it keeps executing queries after its token has lapsed.
		const res = await rc.rpc("query", ["INFO FOR DB"]);
		expect(res.error).toBeUndefined();
		const rows = res.result as Array<{ status: string }>;
		expect(rows[0].status).toBe("OK");

		await rc.close();
		await db.close();
	},
	20000,
);
