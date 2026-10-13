import { afterAll, beforeAll, expect, test } from "bun:test";
import type { Surreal } from "surrealdb";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Refresh-token rotation.
//
// Server syntax discovered: `DEFINE ACCESS ... TYPE RECORD ... WITH REFRESH`
// enables refresh tokens. The refresh-token lifetime is the GRANT duration
// (`DURATION FOR GRANT ...`, default 4w2d), and issued refresh tokens are
// stored server-side as bearer grants inspectable with
// `ACCESS <name> ON DATABASE SHOW ALL` and revocable with
// `ACCESS <name> ON DATABASE REVOKE GRANT <id>`.
//
// SDK redemption API: `db.authenticate({ access, refresh })` — when a refresh
// token is present the SDK issues the `refresh` RPC and returns brand-new
// Tokens. There is no public `revoke` on the Surreal class (it exists only on
// the engine/protocol layer), so revocation is exercised via SurrealQL.

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

/** Access definition with refresh tokens enabled. */
function refreshAccess(table: string): string {
	return `
		DEFINE TABLE ${table} SCHEMALESS
			PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE ${table} SET email = $email, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM ${table} WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			WITH REFRESH
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
	`;
}

/** Access definition WITHOUT refresh tokens. */
function plainAccess(table: string): string {
	return `
		DEFINE TABLE ${table} SCHEMALESS
			PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE ${table} SET email = $email, pass = crypto::argon2::generate($pass) )
			SIGNIN ( SELECT * FROM ${table} WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
			DURATION FOR TOKEN 15m, FOR SESSION 12h;
	`;
}

test("WITH REFRESH: signup and signin both yield a refresh token; the definition gains a GRANT duration", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_a"));

	// The stored definition normalizes WITH REFRESH and adds the default
	// grant (refresh-token) duration of 4w2d alongside an issuer key.
	const [info] = await db
		.query<[{ accesses: Record<string, string> }]>("INFO FOR DB")
		.json();
	expect(info.accesses.account).toContain("WITH REFRESH");
	expect(info.accesses.account).toContain("DURATION FOR GRANT 4w2d");

	const client = await guestClient(server, namespace, database);
	const up = await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "a@example.com", pass: "a-pw" },
	});
	expect(up.access).toBeString();
	expect(up.refresh).toBeString();
	// Refresh tokens are opaque bearer keys, not JWTs.
	expect(up.refresh).toMatch(/^surreal-refresh-/);

	const dupe = await guestClient(server, namespace, database);
	const inn = await dupe.signin({
		namespace,
		database,
		access: "account",
		variables: { email: "a@example.com", pass: "a-pw" },
	});
	expect(inn.refresh).toBeString();
	expect(inn.refresh).not.toBe(up.refresh);

	// Every signup/signin mints its own bearer grant, visible to root.
	const [grants] = await db
		.query<[Array<{ type: string; subject: { record: string }; revocation: unknown }>]>(
			"ACCESS account ON DATABASE SHOW ALL",
		)
		.json();
	expect(grants).toHaveLength(2);
	for (const g of grants) {
		expect(g.type).toBe("bearer");
		expect(String(g.subject.record)).toMatch(/^person_a:/);
		// SurrealQL NONE surfaces as undefined through .json().
		expect(g.revocation).toBeUndefined();
	}

	await client.close();
	await dupe.close();
	await db.close();
});

test("without WITH REFRESH: no refresh token is issued and no grant is recorded", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(plainAccess("person_b"));

	const client = await guestClient(server, namespace, database);
	const up = await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "b@example.com", pass: "b-pw" },
	});
	expect(up.access).toBeString();
	expect(up.refresh).toBeUndefined();

	const inn = await client.signin({
		namespace,
		database,
		access: "account",
		variables: { email: "b@example.com", pass: "b-pw" },
	});
	expect(inn.refresh).toBeUndefined();

	const [grants] = await db.query<[unknown[]]>("ACCESS account ON DATABASE SHOW ALL").json();
	expect(grants).toHaveLength(0);

	await client.close();
	await db.close();
});

test("redeeming a refresh token on a fresh connection yields new, working, rotated tokens", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_c"));

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "c@example.com", pass: "c-pw" },
	});
	await signer.close();

	const fresh = await guestClient(server, namespace, database);
	const next = await fresh.authenticate({ access: tokens.access, refresh: tokens.refresh });

	// Both tokens rotate: a new access JWT and a new refresh token.
	expect(next.access).toBeString();
	expect(next.access).not.toBe(tokens.access);
	expect(next.refresh).toBeString();
	expect(next.refresh).not.toBe(tokens.refresh);

	// The refreshed session is authenticated as the same record user.
	const [auth] = await fresh.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^person_c:/);

	// The new tokens themselves work on yet another connection.
	const third = await guestClient(server, namespace, database);
	await third.authenticate(next.access);
	const [auth2] = await third.query<[string]>("RETURN $auth").json();
	expect(String(auth2)).toBe(String(auth));

	await third.close();
	await fresh.close();
	await db.close();
});

test("rotation is single-use: redeeming the same refresh token twice fails", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_d"));

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "d@example.com", pass: "d-pw" },
	});
	await signer.close();

	const a = await guestClient(server, namespace, database);
	const next = await a.authenticate({ access: tokens.access, refresh: tokens.refresh });
	expect(next.refresh).toBeString();

	// Second redemption of the ORIGINAL refresh token is rejected: the old
	// grant was consumed (revoked) by the first redemption. The error is
	// deliberately generic (no revoked/expired oracle for token guessing).
	const b = await guestClient(server, namespace, database);
	const err = await rejects(b.authenticate({ access: tokens.access, refresh: tokens.refresh }));
	expect(String(err)).toMatch(/problem with authentication/i);

	// The replacement refresh token from the first redemption still works.
	const c = await guestClient(server, namespace, database);
	const third = await c.authenticate({ access: next.access, refresh: next.refresh });
	expect(third.refresh).toBeString();

	await a.close();
	await b.close();
	await c.close();
	await db.close();
});

test("rotation does not invalidate the old access JWT before its expiry", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_e"));

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "e@example.com", pass: "e-pw" },
	});
	await signer.close();

	const a = await guestClient(server, namespace, database);
	await a.authenticate({ access: tokens.access, refresh: tokens.refresh });

	// Observed 3.x behavior: access tokens are stateless JWTs. Rotating the
	// refresh token does NOT revoke the previously-issued access token — it
	// keeps authenticating new connections until its own `exp`.
	const old = await guestClient(server, namespace, database);
	await old.authenticate(tokens.access);
	const [auth] = await old.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^person_e:/);

	await old.close();
	await a.close();
	await db.close();
});

test("root can revoke a refresh grant: redemption fails afterwards", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_f"));

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "f@example.com", pass: "f-pw" },
	});
	await signer.close();

	// The grant id is embedded in the refresh token: surreal-refresh-<id>-<key>.
	const [grants] = await db
		.query<[Array<{ id: string }>]>("ACCESS account ON DATABASE SHOW ALL")
		.json();
	expect(grants).toHaveLength(1);
	const grantId = grants[0].id;
	expect(tokens.refresh).toContain(grantId);

	// Observed quirk: unlike SHOW ALL (flat array of grants), REVOKE GRANT
	// returns its grants wrapped in an EXTRA array level: [[grant]].
	const [revoked] = await db
		.query<[Array<Array<{ revocation: string }>>]>(
			`ACCESS account ON DATABASE REVOKE GRANT ${grantId}`,
		)
		.json();
	expect(revoked).toHaveLength(1);
	expect(revoked[0][0].revocation).toBeString();

	const fresh = await guestClient(server, namespace, database);
	const err = await rejects(
		fresh.authenticate({ access: tokens.access, refresh: tokens.refresh }),
	);
	expect(String(err)).toMatch(/problem with authentication/i);

	// ...but the stateless access JWT still works after grant revocation.
	await fresh.authenticate(tokens.access);
	const [auth] = await fresh.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^person_f:/);

	await fresh.close();
	await db.close();
});

test("refresh tokens expire with DURATION FOR GRANT", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE TABLE person_g SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
		DEFINE ACCESS account ON DATABASE TYPE RECORD
			SIGNUP ( CREATE person_g SET email = $email )
			SIGNIN ( SELECT * FROM person_g WHERE email = $email )
			WITH REFRESH
			DURATION FOR GRANT 1s, FOR TOKEN 15m, FOR SESSION 12h;
	`);

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "g@example.com" },
	});
	expect(tokens.refresh).toBeString();
	await signer.close();

	// Redemption consumes a refresh token, so we cannot poll by redeeming
	// (the second attempt would fail for reuse, not expiry). Instead poll
	// the server clock past the grant's own expiration, then redeem once.
	const [grants] = await db
		.query<[Array<{ expiration: string }>]>("ACCESS account ON DATABASE SHOW ALL")
		.json();
	expect(grants).toHaveLength(1);
	const deadline = Date.now() + 10000;
	let past = false;
	while (Date.now() < deadline) {
		const [now] = await db
			.query<[boolean]>("RETURN time::now() > <datetime> $exp", { exp: grants[0].expiration })
			.json();
		if (now) {
			past = true;
			break;
		}
		await Bun.sleep(250);
	}
	expect(past).toBe(true);

	const fresh = await guestClient(server, namespace, database);
	const err = await rejects(
		fresh.authenticate({ access: tokens.access, refresh: tokens.refresh }),
	);
	// Same deliberately generic error as reuse/revocation.
	expect(String(err)).toMatch(/problem with authentication/i);

	await fresh.close();
	await db.close();
}, 20000);

test("REMOVE ACCESS kills refresh redemption", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_h"));

	const signer = await guestClient(server, namespace, database);
	const tokens = await signer.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "h@example.com", pass: "h-pw" },
	});
	await signer.close();

	await db.query("REMOVE ACCESS account ON DATABASE");

	const fresh = await guestClient(server, namespace, database);
	const err = await rejects(
		fresh.authenticate({ access: tokens.access, refresh: tokens.refresh }),
	);
	// Unlike reuse/revocation/expiry (generic message), a missing access
	// definition is reported specifically — pinned as observed.
	expect(String(err)).toMatch(/access method does not exist/i);

	await fresh.close();
	await db.close();
});

test("invalidate() clears the session but does not revoke the refresh grant server-side", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(refreshAccess("person_i"));

	const client = await guestClient(server, namespace, database);
	const tokens = await client.signup({
		namespace,
		database,
		access: "account",
		variables: { email: "i@example.com", pass: "i-pw" },
	});

	await client.invalidate();
	const err = await rejects(client.query("RETURN $auth").collect());
	expect(String(err)).toMatch(/anonymous|not allowed|permissions/i);

	// Observed 3.x behavior: invalidate() is session-scoped only. The refresh
	// grant survives, so a saved refresh token can still mint fresh tokens
	// afterwards. Revocation requires ACCESS ... REVOKE GRANT (see above).
	const fresh = await guestClient(server, namespace, database);
	const next = await fresh.authenticate({ access: tokens.access, refresh: tokens.refresh });
	expect(next.refresh).toBeString();
	const [auth] = await fresh.query<[string]>("RETURN $auth").json();
	expect(String(auth)).toMatch(/^person_i:/);

	await fresh.close();
	await client.close();
	await db.close();
});

// Grant purging.
//
// `ACCESS <ac> ON DATABASE PURGE EXPIRED, REVOKED [FOR <grace>]` deletes bearer
// grants that are expired and/or revoked. The purge is gated by a grace window:
// a grant is only removed when `(now - expiration|revocation)` (compared at
// whole-second granularity) is STRICTLY GREATER than the grace duration. The
// grace defaults to 0s when no `FOR` clause is given, so a grant expired or
// revoked within the current second is retained and only becomes purgeable once
// the server clock has advanced past it.
//
// PURGE returns a flat array of the purged grants (keys redacted), whereas
// REVOKE GRANT wraps its grants in an extra array level.

/** Bearer access for a database user, with a configurable grant lifetime. */
function bearerAccess(grantDuration: string): string {
	return `
		DEFINE USER api_user ON DATABASE PASSWORD 'api-pw' ROLES EDITOR;
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER
			DURATION FOR GRANT ${grantDuration};
	`;
}

/** Issue one bearer grant and return its id. */
async function issueGrant(db: Surreal): Promise<string> {
	const [g] = await db
		.query<[{ id: string }]>("ACCESS api ON DATABASE GRANT FOR USER api_user")
		.json();
	return g.id;
}

/** Poll the server clock until `when` is at least `bufferSecs` in its past. */
async function waitPastServerTime(db: Surreal, when: string, bufferSecs: number): Promise<void> {
	const deadline = Date.now() + 15000;
	while (Date.now() < deadline) {
		const [past] = await db
			.query<[boolean]>(`RETURN time::now() > (<datetime> $t + ${bufferSecs}s)`, { t: when })
			.json();
		if (past) return;
		await Bun.sleep(200);
	}
	throw new Error("server clock did not advance past the expected time");
}

test("PURGE EXPIRED, REVOKED removes expired and revoked grants while an active grant remains", async () => {
	const { db } = await rootClient(server);
	await db.query(bearerAccess("2s"));

	// One grant we revoke explicitly; one we let expire on its own.
	const revokedId = await issueGrant(db);
	const expiredId = await issueGrant(db);
	await db.query(`ACCESS api ON DATABASE REVOKE GRANT ${revokedId}`);

	// Wait until the expiring grant is comfortably past its own expiration so
	// the whole-second grace test (`> 0s`) is satisfied.
	const [shown] = await db
		.query<[Array<{ id: string; expiration: string }>]>("ACCESS api ON DATABASE SHOW ALL")
		.json();
	const expiring = shown.find((g) => g.id === expiredId);
	expect(expiring).toBeDefined();
	await waitPastServerTime(db, expiring!.expiration, 2);

	// A fresh grant issued now is still active and must survive the purge.
	const activeId = await issueGrant(db);

	// The explicit FOR 0s defeats the default grace; both stale grants go.
	const [purged] = await db
		.query<[Array<{ id: string }>]>("ACCESS api ON DATABASE PURGE EXPIRED, REVOKED FOR 0s")
		.json();
	expect(purged.map((g) => g.id).sort()).toEqual([revokedId, expiredId].sort());

	const [remaining] = await db
		.query<[Array<{ id: string }>]>("ACCESS api ON DATABASE SHOW ALL")
		.json();
	expect(remaining).toHaveLength(1);
	expect(remaining[0].id).toBe(activeId);

	await db.close();
}, 30000);

test("PURGE REVOKED without FOR keeps a just-revoked grant, then removes it once past the grace", async () => {
	const { db } = await rootClient(server);
	// Long grant lifetime so nothing expires on its own during the test.
	await db.query(bearerAccess("1h"));
	const id = await issueGrant(db);

	// Revoke and purge in a single batched request: the revocation timestamp
	// and the purge clock land in the same wall-clock second, so
	// `(now - revocation)` is 0 and the strict `> grace` test (default grace
	// 0s) does not match. The revoked grant is therefore retained.
	const [, purgeRes, showRes] = await db
		.query<[unknown, unknown[], Array<{ id: string; revocation: string }>]>(
			`ACCESS api ON DATABASE REVOKE GRANT ${id};
			 ACCESS api ON DATABASE PURGE REVOKED;
			 ACCESS api ON DATABASE SHOW ALL`,
		)
		.json();
	expect(purgeRes).toHaveLength(0);
	expect(showRes).toHaveLength(1);
	expect(showRes[0].id).toBe(id);
	// The grant is flagged revoked even though it survived the no-grace purge.
	expect(showRes[0].revocation).toBeString();

	// Once the revocation is safely in the server's past, an explicit short
	// grace removes it.
	await waitPastServerTime(db, showRes[0].revocation, 2);
	const [purged] = await db
		.query<[Array<{ id: string }>]>("ACCESS api ON DATABASE PURGE REVOKED FOR 0s")
		.json();
	expect(purged).toHaveLength(1);
	expect(purged[0].id).toBe(id);

	const [after] = await db.query<[unknown[]]>("ACCESS api ON DATABASE SHOW ALL").json();
	expect(after).toHaveLength(0);

	await db.close();
}, 30000);
