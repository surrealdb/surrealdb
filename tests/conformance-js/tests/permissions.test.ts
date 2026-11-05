import { afterAll, beforeAll, expect, test } from "bun:test";
import { RecordId } from "surrealdb";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Permission-reduction conformance through a real record-user session: signup /
// signin mints a token, the token drives an authenticated RPC session, and the
// server reduces every read and write against the row-level table permissions
// and field-level select permissions before the document is CBOR-encoded back
// over the SDK. These pin the observed end-to-end behavior of that path — field
// redaction on select, computed-field stripping, and per-action write denial —
// which the synthetic-$auth language tests never exercise. All scenarios stay
// inside a single namespace/database.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

// §1 — FIELD-LEVEL SELECT PERMISSIONS
//
// `pass` is redacted for everyone (FOR select NONE), `premium_note` is gated on
// a predicate over a sibling field, and `display` is a computed VALUE field that
// is also redacted (FOR select NONE). A record user reads their own row; root
// reads the same rows with full visibility.
const FIELD_PERM_SETUP = `
	DEFINE TABLE user SCHEMALESS
		PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
	DEFINE FIELD email ON user TYPE string;
	DEFINE FIELD tier ON user TYPE string;
	DEFINE FIELD pass ON user TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD premium_note ON user TYPE string PERMISSIONS FOR select WHERE tier = 'gold';
	DEFINE FIELD display ON user TYPE string VALUE string::uppercase(email) PERMISSIONS FOR select NONE;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass), tier = $tier, premium_note = 'members-only' )
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

test("field select NONE and a computed VALUE field are redacted from a record user's own row", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(FIELD_PERM_SETUP);

	const alice = await guestClient(server, namespace, database);
	try {
		await alice.signup({
			namespace,
			database,
			access: "account",
			variables: { email: "alice@example.com", pass: "alice-pw", tier: "gold" },
		});

		const [rows] = await alice
			.query<[Array<Record<string, unknown>>]>("SELECT * FROM user")
			.json();
		expect(rows).toHaveLength(1);
		const row = rows[0];

		// Allowed plain fields are present.
		expect(row.email).toBe("alice@example.com");
		expect(row.tier).toBe("gold");

		// FOR select NONE fields are absent from the decoded document entirely —
		// the key is dropped, not nulled. This holds for the stored `pass` field
		// and for the computed VALUE field `display`.
		expect("pass" in row).toBe(false);
		expect("display" in row).toBe(false);

		// The gated field IS visible here because the predicate (tier = 'gold')
		// holds for this row.
		expect(row.premium_note).toBe("members-only");
	} finally {
		await alice.close();
		await db.close();
	}
});

test("a field gated by FOR select WHERE is dropped when the predicate does not hold", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(FIELD_PERM_SETUP);

	const bob = await guestClient(server, namespace, database);
	try {
		await bob.signup({
			namespace,
			database,
			access: "account",
			variables: { email: "bob@example.com", pass: "bob-pw", tier: "free" },
		});

		const [rows] = await bob
			.query<[Array<Record<string, unknown>>]>("SELECT * FROM user")
			.json();
		expect(rows).toHaveLength(1);
		const row = rows[0];

		// Plain fields present; the always-redacted fields absent.
		expect(row.email).toBe("bob@example.com");
		expect(row.tier).toBe("free");
		expect("pass" in row).toBe(false);
		expect("display" in row).toBe(false);

		// tier is 'free', so the FOR select WHERE tier = 'gold' predicate fails
		// and the field is dropped for this row.
		expect("premium_note" in row).toBe(false);
	} finally {
		await bob.close();
		await db.close();
	}
});

test("root sees every field the record-user select permissions redact", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(FIELD_PERM_SETUP);

	const alice = await guestClient(server, namespace, database);
	await alice.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "root-view@example.com", pass: "pw", tier: "free" },
	});
	await alice.close();

	try {
		const [rows] = await db
			.query<[Array<Record<string, unknown>>]>("SELECT * FROM user")
			.json();
		expect(rows).toHaveLength(1);
		const row = rows[0];

		// Root bypasses field select permissions: the redacted-for-users fields
		// are all present, including the argon2 hash and the computed field.
		expect(row.email).toBe("root-view@example.com");
		expect(typeof row.pass).toBe("string");
		expect(String(row.pass)).toStartWith("$argon2");
		expect(row.display).toBe("ROOT-VIEW@EXAMPLE.COM");
		// Gated field is visible to root regardless of the predicate result.
		expect(row.premium_note).toBe("members-only");
	} finally {
		await db.close();
	}
});

// §5 — RECORD-USER WRITE DENIAL PER ACTION (owner-scoped, same-tenant)
//
// `resource` rows are owned via an `owner = $auth` predicate on select, update
// and delete, and creation is forbidden outright (FOR create NONE). Record users
// authenticate against the `user` access; root seeds the resource rows because
// the record users themselves cannot create them.
const WRITE_DENIAL_SETUP = `
	DEFINE TABLE user SCHEMALESS
		PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
	DEFINE TABLE resource SCHEMALESS
		PERMISSIONS FOR select, update, delete WHERE owner = $auth FOR create NONE;
`;

// Sign a record user up and return { client, id } where id is the RecordId of
// the created `user` row (its $auth).
async function signupUser(
	namespace: string,
	database: string,
	email: string,
): Promise<{ client: Awaited<ReturnType<typeof guestClient>>; id: RecordId }> {
	const client = await guestClient(server, namespace, database);
	await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email, pass: `${email}-pw` },
	});
	const [id] = await client.query<[RecordId]>("RETURN $auth");
	return { client, id };
}

test("create under FOR create NONE is silently filtered: empty result, no row, no error", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(WRITE_DENIAL_SETUP);

	const { client: alice, id: aliceId } = await signupUser(namespace, database, "a@example.com");
	try {
		// The record user attempts to create a row it would own. There is no
		// thrown error: the statement resolves with an empty result set.
		const [created] = await alice
			.query<[unknown[]]>("CREATE resource:mine SET owner = $auth, label = 'x'")
			.json();
		expect(created).toEqual([]);

		// And nothing was persisted — root confirms the row does not exist.
		const [check] = await db.query<[unknown[]]>("SELECT * FROM resource:mine").json();
		expect(check).toEqual([]);

		// (aliceId is used below to prove the owner predicate itself works.)
		expect(aliceId).toBeInstanceOf(RecordId);
	} finally {
		await alice.close();
		await db.close();
	}
});

test("update/delete of another record user's row is silently filtered; the user's own row succeeds", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(WRITE_DENIAL_SETUP);

	const { client: alice, id: aliceId } = await signupUser(namespace, database, "alice@example.com");
	const { client: bob, id: bobId } = await signupUser(namespace, database, "bob@example.com");

	try {
		// Root seeds one resource per user (record users cannot create them).
		await db.query(
			"CREATE resource:alice SET owner = $alice, label = 'orig-a'; CREATE resource:bob SET owner = $bob, label = 'orig-b'",
			{ alice: aliceId, bob: bobId },
		);

		// Alice updating Bob's row: the owner predicate excludes the target, so
		// the update matches nothing and returns an empty result with no error.
		const [crossUpdate] = await alice
			.query<[unknown[]]>("UPDATE resource:bob SET label = 'hacked'")
			.json();
		expect(crossUpdate).toEqual([]);

		// Alice deleting Bob's row: same silent filtering.
		const [crossDelete] = await alice
			.query<[unknown[]]>("DELETE resource:bob RETURN BEFORE")
			.json();
		expect(crossDelete).toEqual([]);

		// Bob's row is untouched by either attempt.
		const [bobRow] = await db
			.query<[Array<{ label: string }>]>("SELECT * FROM resource:bob")
			.json();
		expect(bobRow).toHaveLength(1);
		expect(bobRow[0].label).toBe("orig-b");

		// Alice updating her OWN row succeeds and returns the mutated record.
		const [ownUpdate] = await alice
			.query<[Array<{ label: string }>]>("UPDATE resource:alice SET label = 'mine-now'")
			.json();
		expect(ownUpdate).toHaveLength(1);
		expect(ownUpdate[0].label).toBe("mine-now");

		// Alice deleting her OWN row succeeds and removes it.
		const [ownDelete] = await alice
			.query<[Array<unknown>]>("DELETE resource:alice RETURN BEFORE")
			.json();
		expect(ownDelete).toHaveLength(1);
		const [gone] = await db.query<[unknown[]]>("SELECT * FROM resource:alice").json();
		expect(gone).toEqual([]);
	} finally {
		await alice.close();
		await bob.close();
		await db.close();
	}
});
