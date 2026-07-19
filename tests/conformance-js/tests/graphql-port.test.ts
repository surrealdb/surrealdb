// GraphQL endpoint conformance — ported from the Rust integration suite
// `tests/graphql_integration.rs` (65 tests). Driven with raw `fetch` against
// POST /graphql (JSON body {query, variables}; Basic root:root auth; ns/db via
// surreal-ns / surreal-db headers) plus RpcClient for the `graphql` RPC method.
//
// GraphQL is default-on (no capability/experimental flag); the only gate is
// per-database `DEFINE CONFIG GRAPHQL AUTO` (or TABLES INCLUDE ...).
//
// This file deliberately does NOT re-cover what graphql.test.ts already pins
// (unconfigured/empty 400s, basic introspection SCHEMAFULL vs SCHEMALESS, simple
// list/filter/order/limit/aggregate-count, basic create/update/delete, DDL
// add-a-table regeneration, TABLES INCLUDE, anonymous denial, missing headers,
// --deny-http). It ports the higher-value cases: error-message SAFETY,
// per-operation PERMISSIONS through the resolver, relations / record-links,
// filtering / cursor-pagination correctness, schema-generation edges, DDL cache
// invalidation, and the `graphql` RPC surface.
//
// HARD RULE compliance: each test spawns its own server, uses a unique ns/db via
// rootClient(), closes every client, and stops the server.

import { afterEach, expect, test } from "bun:test";
import {
	RpcClient,
	rootClient,
	startServer,
	type ServerOptions,
	type TestServer,
} from "../src/harness";
import type { Surreal } from "surrealdb";

const ROOT_AUTH = `Basic ${Buffer.from("root:root").toString("base64")}`;
// Server startup (~1-2s) is inside each test, so bun's 5s default is too tight.
const TIMEOUT = 30000;

interface GqlError {
	message: string;
	locations?: Array<{ line: number; column: number }>;
}
interface GqlBody {
	data?: any;
	errors?: GqlError[];
	[k: string]: unknown;
}
interface GqlResult {
	status: number;
	body: GqlBody;
}

interface GqlOpts {
	/** Authorization header value; defaults to root Basic. `null` sends none. */
	auth?: string | null;
	variables?: Record<string, unknown>;
	/** Override the surreal-ns header (defaults to the test ns). */
	ns?: string;
	/** Override the surreal-db header (defaults to the test db). */
	db?: string;
	/** Omit the ns header entirely. */
	noNs?: boolean;
	/** Omit the db header entirely. */
	noDb?: boolean;
}

/** A GraphQL request context bound to one server + one ns/db. */
interface Ctx {
	server: TestServer;
	db: Surreal;
	ns: string;
	dbName: string;
	/** POST a GraphQL query/mutation (string body or a raw JSON payload). */
	gql(query: string, opts?: GqlOpts): Promise<GqlResult>;
	gqlRaw(payload: unknown, opts?: GqlOpts): Promise<GqlResult>;
	/** POST /signup with a JSON body; returns the token (raw fetch, not SDK). */
	signup(body: Record<string, unknown>): Promise<{ status: number; token: string; body: any }>;
}

// Track servers/clients so a thrown assertion still tears everything down.
let openServers: TestServer[] = [];
let openClients: Surreal[] = [];

afterEach(async () => {
	for (const c of openClients) {
		try {
			await c.close();
		} catch {
			/* already closed */
		}
	}
	for (const s of openServers) {
		try {
			await s.stop();
		} catch {
			/* already stopped */
		}
	}
	openClients = [];
	openServers = [];
});

/** Start a fresh server + root client on a unique ns/db, and hand back a Ctx. */
async function withGql(fn: (ctx: Ctx) => Promise<void>, opts?: ServerOptions): Promise<void> {
	const server = await startServer(opts);
	openServers.push(server);
	const { db, namespace, database } = await rootClient(server);
	openClients.push(db);

	const headersFor = (o: GqlOpts): Record<string, string> => {
		const headers: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json",
		};
		if (!o.noNs) headers["surreal-ns"] = o.ns ?? namespace;
		if (!o.noDb) headers["surreal-db"] = o.db ?? database;
		const auth = o.auth === undefined ? ROOT_AUTH : o.auth;
		if (auth !== null) headers.Authorization = auth;
		return headers;
	};

	const ctx: Ctx = {
		server,
		db,
		ns: namespace,
		dbName: database,
		async gqlRaw(payload, opts = {}) {
			const res = await fetch(`${server.httpUrl}/graphql`, {
				method: "POST",
				headers: headersFor(opts),
				body: JSON.stringify(payload),
			});
			return { status: res.status, body: (await res.json()) as GqlBody };
		},
		gql(query, opts = {}) {
			return ctx.gqlRaw({ query, variables: opts.variables }, opts);
		},
		async signup(body) {
			const res = await fetch(`${server.httpUrl}/signup`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(body),
			});
			const json = (await res.json()) as any;
			return { status: res.status, token: json?.token, body: json };
		},
	};

	await fn(ctx);
}

// A record-access definition used across the permission/auth tests. Mirrors the
// Rust suite's `DEFINE ACCESS user ...` block.
const USER_ACCESS = `
	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 60s, FOR TOKEN 1d;
	DEFINE TABLE user SCHEMAFULL
		PERMISSIONS FOR select, create, update, delete WHERE id = $auth;
	DEFINE FIELD email ON user TYPE string;
	DEFINE FIELD pass ON user TYPE string;
`;

// ---------------------------------------------------------------------------
// (1) ERROR-MESSAGE SAFETY  — highest priority
// ---------------------------------------------------------------------------

test(
	"invalid Basic auth on /graphql is 401 with a generic message (no query/crypto leak)",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`DEFINE CONFIG GRAPHQL AUTO;${USER_ACCESS}`);
			// Wrong credentials at the transport layer: 401, generic body.
			const res = await fetch(`${c.server.httpUrl}/graphql`, {
				method: "POST",
				headers: {
					"surreal-ns": c.ns,
					"surreal-db": c.dbName,
					"Content-Type": "application/json",
					Authorization: `Basic ${Buffer.from("invalid:invalid").toString("base64")}`,
				},
				body: JSON.stringify({ query: "{ __typename }" }),
			});
			expect(res.status).toBe(401);
			const text = await res.text();
			expect(text).toContain("There was a problem with authentication");
			// No leak of the SIGNIN query text or password-hash internals.
			expect(text).not.toContain("SELECT");
			expect(text).not.toContain("argon2");
			expect(text).not.toContain("crypto");
		});
	},
	TIMEOUT,
);

test(
	"signIn/signUp mutation errors are generic — no SELECT text, no argon2 detail, no access-method leak",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE item SCHEMAFULL;
				DEFINE FIELD name ON item TYPE string;
				DEFINE FIELD price ON item TYPE float;
			`);

			// Wrong credentials via GraphQL mutation: HTTP 200 + generic error.
			const wrong = await c.gql(
				`mutation { signIn(access: "user", variables: { email: "nobody@test.com", pass: "wrong" }) }`,
			);
			expect(wrong.status).toBe(200);
			expect(Array.isArray(wrong.body.errors)).toBe(true);
			const wrongMsg = wrong.body.errors![0].message;
			expect(wrongMsg).toContain("problem with authentication");
			expect(wrongMsg).not.toContain("SELECT");
			expect(wrongMsg).not.toContain("FROM user");
			expect(wrongMsg).not.toContain("argon2");
			expect(wrongMsg).not.toContain("crypto");

			// Non-existent access method: still generic — must not confirm/deny
			// which access methods exist.
			const noAccess = await c.gql(
				`mutation { signIn(access: "nonexistent_access", variables: { email: "x", pass: "y" }) }`,
			);
			expect(noAccess.status).toBe(200);
			expect(Array.isArray(noAccess.body.errors)).toBe(true);
			expect(noAccess.body.errors![0].message).toContain("problem with authentication");

			// Invalid record id to _get must not leak a parser backtrace.
			const badId = await c.gql(`{ _get(id: "not_a_valid_id") { id } }`);
			expect(badId.status).toBe(200);
			if (Array.isArray(badId.body.errors)) {
				const m = badId.body.errors[0].message;
				expect(m).not.toContain("ParseError");
				expect(m).not.toContain("backtrace");
			}
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// (2) PER-OPERATION PERMISSIONS through the resolver — highest priority
// ---------------------------------------------------------------------------

test(
	"mutation permissions: create allowed/denied, author-only update, silently-true denied delete, deleteMany=0",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE article SCHEMAFULL
					PERMISSIONS
						FOR select WHERE $auth != NONE
						FOR create WHERE $auth != NONE
						FOR update WHERE author = $auth.id
						FOR delete WHERE author = $auth.id;
				DEFINE FIELD title ON article TYPE string;
				DEFINE FIELD content ON article TYPE string;
				DEFINE FIELD author ON article TYPE record<user>;
				DEFINE TABLE secret SCHEMAFULL PERMISSIONS NONE;
				DEFINE FIELD data ON secret TYPE string;
			`);

			const { token } = await c.signup({
				ns: c.ns,
				db: c.dbName,
				ac: "user",
				email: "alice@example.com",
				pass: "secret123",
			});
			const bearer = `Bearer ${token}`;

			// The user's own record id (needed for author checks).
			const me = await c.gql(`{ users { id } }`, { auth: bearer });
			expect(me.body.errors).toBeUndefined();
			const userId: string = me.body.data.users[0].id;

			// 1. Authenticated user CAN create an article.
			const created = await c.gql(
				`mutation { createArticle(data: { title: "My Post", content: "Hello world", author: "${userId}" }) { id title author { id } } }`,
				{ auth: bearer },
			);
			expect(created.body.errors).toBeUndefined();
			expect(created.body.data.createArticle.title).toBe("My Post");
			expect(typeof created.body.data.createArticle.id).toBe("string");

			// 2. PERMISSIONS NONE table: user create is silently denied -> null (no error).
			const secretDenied = await c.gql(
				`mutation { createSecret(data: { data: "top secret" }) { id data } }`,
				{ auth: bearer },
			);
			expect(secretDenied.status).toBe(200);
			expect(secretDenied.body.data.createSecret).toBeNull();

			// 3. Root CAN create on the locked table.
			const secretRoot = await c.gql(
				`mutation { createSecret(data: { data: "classified" }) { id data } }`,
			);
			expect(secretRoot.body.errors).toBeUndefined();
			expect(typeof secretRoot.body.data.createSecret.id).toBe("string");

			// Seed two articles as root: one owned by alice, one by a fake user.
			await c.db.query(`
				CREATE article:alice_post SET title = "Alice's article", content = "Original", author = ${userId};
				CREATE article:other_post SET title = "Other article", content = "Not mine", author = user:fake;
			`);

			// 4a. Author CAN update her own article.
			const upOwn = await c.gql(
				`mutation { updateArticle(id: "alice_post", data: { title: "Updated title" }) { id title } }`,
				{ auth: bearer },
			);
			expect(upOwn.body.errors).toBeUndefined();
			expect(upOwn.body.data.updateArticle.title).toBe("Updated title");

			// 4b. Author CANNOT update another user's article -> null.
			const upOther = await c.gql(
				`mutation { updateArticle(id: "other_post", data: { title: "Hacked" }) { id title } }`,
				{ auth: bearer },
			);
			expect(upOther.body.data.updateArticle).toBeNull();

			// 5a. SURPRISING: a permission-denied delete returns `true` (no error),
			// even though nothing is deleted — the engine silently ignores it.
			const delOther = await c.gql(`mutation { deleteArticle(id: "other_post") }`, {
				auth: bearer,
			});
			expect(delOther.body.errors).toBeUndefined();
			expect(delOther.body.data.deleteArticle).toBe(true);
			// The record still exists.
			const [survivors] = await c.db
				.query<[unknown[]]>("SELECT * FROM article:other_post")
				.json();
			expect(survivors).toHaveLength(1);

			// 5b. Author CAN delete her own article.
			const delOwn = await c.gql(`mutation { deleteArticle(id: "alice_post") }`, {
				auth: bearer,
			});
			expect(delOwn.body.errors).toBeUndefined();

			// 6. deleteMany on a PERMISSIONS NONE table returns 0 and deletes nothing.
			await c.db.query(`CREATE secret:s1 SET data = "s1"; CREATE secret:s2 SET data = "s2";`);
			const delSecrets = await c.gql(`mutation { deleteSecrets }`, { auth: bearer });
			expect(delSecrets.body.data.deleteSecrets).toBe(0);
			const [remaining] = await c.db
				.query<[Array<{ count: number }>]>("SELECT count() FROM secret GROUP ALL")
				.json();
			expect(remaining[0].count).toBeGreaterThanOrEqual(2);
		});
	},
	TIMEOUT,
);

test(
	"relation field resolution respects relation-table PERMISSIONS (each user sees only their own edges)",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE post SCHEMAFULL
					PERMISSIONS FOR select WHERE $auth != NONE
					FOR create, update, delete WHERE $auth != NONE;
				DEFINE FIELD title ON post TYPE string;
				DEFINE TABLE likes TYPE RELATION FROM user TO post SCHEMAFULL
					PERMISSIONS FOR select WHERE in = $auth.id
					FOR create, update, delete WHERE in = $auth.id;
				DEFINE FIELD rating ON likes TYPE int;
				CREATE post:p1 SET title = "First Post";
				CREATE post:p2 SET title = "Second Post";
			`);

			const alice = await c.signup({
				ns: c.ns,
				db: c.dbName,
				ac: "user",
				email: "alice@test.com",
				pass: "pass123",
			});
			const bob = await c.signup({
				ns: c.ns,
				db: c.dbName,
				ac: "user",
				email: "bob@test.com",
				pass: "pass123",
			});
			const aAuth = `Bearer ${alice.token}`;
			const bAuth = `Bearer ${bob.token}`;

			const aliceId: string = (await c.gql(`{ users { id } }`, { auth: aAuth })).body.data
				.users[0].id;
			const bobId: string = (await c.gql(`{ users { id } }`, { auth: bAuth })).body.data
				.users[0].id;

			await c.db.query(`
				RELATE ${aliceId}->likes->post:p1 SET rating = 5;
				RELATE ${bobId}->likes->post:p2 SET rating = 3;
			`);

			const aView = await c.gql(`{ users { id likes { rating } } }`, { auth: aAuth });
			expect(aView.body.errors).toBeUndefined();
			expect(aView.body.data.users[0].likes).toEqual([{ rating: 5 }]);

			const bView = await c.gql(`{ users { id likes { rating } } }`, { auth: bAuth });
			expect(bView.body.data.users[0].likes).toEqual([{ rating: 3 }]);

			// Root sees all likes.
			const rootView = await c.gql(`{ likes { rating } }`);
			expect(rootView.body.data.likes).toHaveLength(2);
		});
	},
	TIMEOUT,
);

test(
	"upsert mutations respect PERMISSIONS: denied upsert returns null and leaves the record unchanged",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE locked SCHEMAFULL
					PERMISSIONS
						FOR select WHERE $auth != NONE
						FOR create, update, delete NONE;
				DEFINE FIELD name ON locked TYPE string;
				CREATE locked:existing SET name = "Original";
			`);

			const { token } = await c.signup({
				ns: c.ns,
				db: c.dbName,
				ac: "user",
				email: "alice@test.com",
				pass: "pass123",
			});
			const bearer = `Bearer ${token}`;

			// Upsert to a NEW id: denied -> null.
			const upNew = await c.gql(
				`mutation { upsertLocked(id: "new_record", data: { name: "Hacked" }) { id name } }`,
				{ auth: bearer },
			);
			expect(upNew.status).toBe(200);
			expect(upNew.body.data.upsertLocked).toBeNull();

			// Upsert an EXISTING id: still denied -> null.
			const upExisting = await c.gql(
				`mutation { upsertLocked(id: "existing", data: { name: "Modified" }) { id name } }`,
				{ auth: bearer },
			);
			expect(upExisting.body.data.upsertLocked).toBeNull();

			// The record is untouched.
			const [rows] = await c.db
				.query<[Array<{ name: string }>]>("SELECT name FROM locked:existing")
				.json();
			expect(rows[0].name).toBe("Original");

			// Root CAN upsert.
			const upRoot = await c.gql(
				`mutation { upsertLocked(id: "existing", data: { name: "Root Modified" }) { id name } }`,
			);
			expect(upRoot.body.errors).toBeUndefined();
			expect(upRoot.body.data.upsertLocked.name).toBe("Root Modified");
		});
	},
	TIMEOUT,
);

test(
	"record-access read permissions filter the GraphQL list ($auth.email = email) for record tokens vs root",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE foo SCHEMAFULL PERMISSIONS FOR select WHERE $auth.email = email;
				DEFINE FIELD email ON foo TYPE string;
				DEFINE FIELD val ON foo TYPE int;
				CREATE foo:1 SET val = 42, email = "user@email.com";
				CREATE foo:2 SET val = 43, email = "other@email.com";
			`);

			// Root sees both rows.
			const asRoot = await c.gql(`{ foos { id val } }`);
			expect(asRoot.body.data.foos).toEqual([
				{ id: "foo:1", val: 42 },
				{ id: "foo:2", val: 43 },
			]);

			// A signed-up user sees only the row whose email matches $auth.email.
			const { token } = await c.signup({
				ns: c.ns,
				db: c.dbName,
				ac: "user",
				email: "user@email.com",
				pass: "pass",
			});
			const partial = await c.gql(`{ foos { id val } }`, { auth: `Bearer ${token}` });
			expect(partial.body.errors).toBeUndefined();
			expect(partial.body.data.foos).toEqual([{ id: "foo:1", val: 42 }]);
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// GraphQL auth mutations (signIn / signUp) surface
// ---------------------------------------------------------------------------

test(
	"auth mutations: signUp/signIn return JWTs that authenticate; introspected as String! with access:String!/variables:JSON!",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				${USER_ACCESS}
				DEFINE TABLE post SCHEMAFULL
					PERMISSIONS FOR select WHERE $auth != NONE
					FOR create, update, delete WHERE $auth != NONE;
				DEFINE FIELD title ON post TYPE string;
				DEFINE FIELD content ON post TYPE string;
			`);

			// Mutation type exposes signIn and signUp.
			const mut = await c.gql(`{ __type(name: "Mutation") { fields { name } } }`);
			const mutNames = (mut.body.data.__type.fields as Array<{ name: string }>).map(
				(f) => f.name,
			);
			expect(mutNames).toContain("signIn");
			expect(mutNames).toContain("signUp");

			// signUp returns a 3-part JWT.
			const su = await c.gql(
				`mutation { signUp(access: "user", variables: { email: "alice@example.com", pass: "secret123" }) }`,
			);
			expect(su.body.errors).toBeUndefined();
			const signupToken: string = su.body.data.signUp;
			expect(signupToken.split(".")).toHaveLength(3);

			// The signup token authenticates a query.
			const authed = await c.gql(`{ posts { id } }`, { auth: `Bearer ${signupToken}` });
			expect(authed.body.errors).toBeUndefined();

			// signIn returns a working JWT.
			const si = await c.gql(
				`mutation { signIn(access: "user", variables: { email: "alice@example.com", pass: "secret123" }) }`,
			);
			expect(si.body.errors).toBeUndefined();
			const signinToken: string = si.body.data.signIn;
			expect(signinToken.split(".")).toHaveLength(3);

			await c.db.query(`CREATE post:1 SET title = "Hello", content = "World"`);
			const posts = await c.gql(`{ posts { id title content } }`, {
				auth: `Bearer ${signinToken}`,
			});
			expect(posts.body.data.posts).toHaveLength(1);
			expect(posts.body.data.posts[0].title).toBe("Hello");

			// Argument + return-type introspection.
			const argIntro = await c.gql(`{
				__type(name: "Mutation") {
					fields { name args { name type { name kind ofType { name } } } type { name kind ofType { name } } }
				}
			}`);
			const fields = argIntro.body.data.__type.fields as any[];
			const signIn = fields.find((f) => f.name === "signIn");
			expect(signIn.type).toEqual({ name: null, kind: "NON_NULL", ofType: { name: "String" } });
			const access = signIn.args.find((a: any) => a.name === "access");
			expect(access.type).toEqual({ name: null, kind: "NON_NULL", ofType: { name: "String" } });
			const variables = signIn.args.find((a: any) => a.name === "variables");
			expect(variables.type).toEqual({ name: null, kind: "NON_NULL", ofType: { name: "JSON" } });
		});
	},
	TIMEOUT,
);

test(
	"a signin-only access method exposes signIn but NOT signUp in the Mutation type",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE ACCESS readonly_user ON DATABASE TYPE RECORD
					SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
					DURATION FOR SESSION 60s, FOR TOKEN 1d;
				DEFINE TABLE user SCHEMAFULL;
				DEFINE FIELD email ON user TYPE string;
				DEFINE FIELD pass ON user TYPE string;
			`);
			const mut = await c.gql(`{ __type(name: "Mutation") { fields { name } } }`);
			const names = (mut.body.data.__type.fields as Array<{ name: string }>).map((f) => f.name);
			expect(names).toContain("signIn");
			expect(names).not.toContain("signUp");
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// (3) AUTO-GENERATED CRUD SURFACE — relations, record links, functions
// ---------------------------------------------------------------------------

test(
	"relations: outgoing (likes) and incoming (likes_in) fields, ordering, limit, list context, introspection",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE person SCHEMAFULL;
				DEFINE FIELD name ON person TYPE string;
				DEFINE TABLE post SCHEMAFULL;
				DEFINE FIELD title ON post TYPE string;
				DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFULL;
				DEFINE FIELD rating ON likes TYPE int;
				CREATE person:alice SET name = "Alice";
				CREATE person:bob SET name = "Bob";
				CREATE post:p1 SET title = "First Post";
				CREATE post:p2 SET title = "Second Post";
				RELATE person:alice->likes->post:p1 SET rating = 5;
				RELATE person:alice->likes->post:p2 SET rating = 3;
				RELATE person:bob->likes->post:p1 SET rating = 4;
			`);

			// Outgoing relation, ordered by rating asc.
			const outgoing = await c.gql(
				`{ person(id: "alice") { id name likes(order: {asc: rating}) { rating } } }`,
			);
			expect(outgoing.body.data.person.id).toBe("person:alice");
			expect(outgoing.body.data.person.likes).toEqual([{ rating: 3 }, { rating: 5 }]);

			// Incoming relation on post (likes_in): both alice(5) and bob(4).
			const incoming = await c.gql(`{ post(id: "p1") { title likes_in { rating } } }`);
			const inRatings = (incoming.body.data.post.likes_in as Array<{ rating: number }>)
				.map((l) => l.rating)
				.sort();
			expect(inRatings).toEqual([4, 5]);

			// Relation field with limit.
			const limited = await c.gql(
				`{ person(id: "alice") { likes(limit: 1, order: {desc: rating}) { rating } } }`,
			);
			expect(limited.body.data.person.likes).toEqual([{ rating: 5 }]);

			// List context: per-person like counts.
			const list = await c.gql(`{ persons(order: {asc: name}) { name likes { rating } } }`);
			expect(list.body.data.persons[0].name).toBe("Alice");
			expect(list.body.data.persons[0].likes).toHaveLength(2);
			expect(list.body.data.persons[1].name).toBe("Bob");
			expect(list.body.data.persons[1].likes).toHaveLength(1);

			// Introspection exposes the relation fields.
			const pIntro = await c.gql(`{ __type(name: "person") { fields { name } } }`);
			const pNames = (pIntro.body.data.__type.fields as Array<{ name: string }>).map(
				(f) => f.name,
			);
			expect(pNames).toEqual(expect.arrayContaining(["id", "name", "likes"]));
			const postIntro = await c.gql(`{ __type(name: "post") { fields { name } } }`);
			const postNames = (postIntro.body.data.__type.fields as Array<{ name: string }>).map(
				(f) => f.name,
			);
			expect(postNames).toEqual(expect.arrayContaining(["id", "title", "likes_in"]));
		});
	},
	TIMEOUT,
);

test(
	"record links dereference to the target table type with nested sub-field selection",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE department SCHEMAFULL;
				DEFINE FIELD name ON department TYPE string;
				DEFINE FIELD location ON department TYPE string;
				DEFINE TABLE employee SCHEMAFULL;
				DEFINE FIELD name ON employee TYPE string;
				DEFINE FIELD dept ON employee TYPE record<department>;
				CREATE department:eng SET name = "Engineering", location = "Building A";
				CREATE department:mkt SET name = "Marketing", location = "Building B";
				CREATE employee:e1 SET name = "Alice", dept = department:eng;
				CREATE employee:e2 SET name = "Bob", dept = department:mkt;
				CREATE employee:e3 SET name = "Charlie", dept = department:eng;
			`);

			const list = await c.gql(
				`{ employees(order: {asc: name}) { name dept { id name location } } }`,
			);
			expect(list.body.data.employees).toEqual([
				{ name: "Alice", dept: { id: "department:eng", name: "Engineering", location: "Building A" } },
				{ name: "Bob", dept: { id: "department:mkt", name: "Marketing", location: "Building B" } },
				{ name: "Charlie", dept: { id: "department:eng", name: "Engineering", location: "Building A" } },
			]);

			const single = await c.gql(`{ employee(id: "e2") { name dept { name location } } }`);
			expect(single.body.data.employee).toEqual({
				name: "Bob",
				dept: { name: "Marketing", location: "Building B" },
			});

			// The dept field is a NON_NULL-wrapped named object type.
			const intro = await c.gql(`{ __type(name: "employee") { fields { name type { name kind } } } }`);
			const dept = (intro.body.data.__type.fields as any[]).find((f) => f.name === "dept");
			expect(dept.type.kind).toBe("NON_NULL");
		});
	},
	TIMEOUT,
);

test(
	"DEFINE FUNCTION surfaces as fn_* Query fields returning records and scalars",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE foo SCHEMAFULL;
				DEFINE FIELD val ON foo TYPE int;
				CREATE foo:1 SET val = 86;
				DEFINE FUNCTION fn::num() -> int { RETURN 42; };
				DEFINE FUNCTION fn::double($x: int) -> int { RETURN $x * 2; };
				DEFINE FUNCTION fn::foo() -> record<foo> { RETURN foo:1; };
				DEFINE FUNCTION fn::record() -> record { RETURN foo:1; };
			`);

			const recs = await c.gql(`{ fn_foo { id val } fn_record { id ...on foo { val } } }`);
			expect(recs.body.errors).toBeUndefined();
			expect(recs.body.data).toEqual({
				fn_foo: { id: "foo:1", val: 86 },
				fn_record: { id: "foo:1", val: 86 },
			});

			const scalars = await c.gql(`{ fn_num fn_double(x: 21) }`);
			expect(scalars.body.data).toEqual({ fn_num: 42, fn_double: 42 });
		});
	},
	TIMEOUT,
);

test(
	"option<record<T>> (Kind::Either) fields settable via mutation as a string id; null variant round-trips",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE team SCHEMAFULL;
				DEFINE FIELD name ON team TYPE string;
				DEFINE TABLE player SCHEMAFULL;
				DEFINE FIELD name ON player TYPE string;
				DEFINE FIELD squad ON player TYPE option<record<team>>;
				CREATE team:red SET name = "Red Team";
			`);

			const withSquad = await c.gql(
				`mutation { createPlayer(data: { name: "Alice", squad: "team:red" }) { id name squad { id name } } }`,
			);
			expect(withSquad.body.errors).toBeUndefined();
			expect(withSquad.body.data.createPlayer.name).toBe("Alice");
			expect(withSquad.body.data.createPlayer.squad).toEqual({ id: "team:red", name: "Red Team" });

			const noSquad = await c.gql(
				`mutation { createPlayer(data: { name: "Bob" }) { id name squad { id name } } }`,
			);
			expect(noSquad.body.errors).toBeUndefined();
			expect(noSquad.body.data.createPlayer.name).toBe("Bob");
			expect(noSquad.body.data.createPlayer.squad).toBeNull();
		});
	},
	TIMEOUT,
);

test(
	"COMPUTED and READONLY fields are excluded from Create/Update/Upsert input types but still read back",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE product SCHEMAFULL;
				DEFINE FIELD price ON product TYPE int;
				DEFINE FIELD tax ON product TYPE int COMPUTED math::floor(price * 0.20);
				DEFINE FIELD sku ON product TYPE string READONLY VALUE "fixed-sku";
			`);

			const intro = await c.gql(`{
				create: __type(name: "CreateProductInput") { inputFields { name } }
				update: __type(name: "UpdateProductInput") { inputFields { name } }
				upsert: __type(name: "UpsertProductInput") { inputFields { name } }
			}`);
			expect(intro.body.errors).toBeUndefined();
			for (const which of ["create", "update", "upsert"] as const) {
				const names = (intro.body.data[which].inputFields as Array<{ name: string }>).map(
					(f) => f.name,
				);
				expect(names).not.toContain("tax");
				expect(names).not.toContain("sku");
				expect(names).toContain("price");
			}

			await c.db.query("CREATE product:p1 SET price = 100");
			const read = await c.gql(`{ product(id: "p1") { id price tax sku } }`);
			expect(read.body.data.product).toEqual({
				id: "product:p1",
				price: 100,
				tax: 20,
				sku: "fixed-sku",
			});
		});
	},
	TIMEOUT,
);

test(
	"computed table views expose Query fields but NO mutation fields; base tables keep their mutations",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE temperature SCHEMAFULL;
				DEFINE FIELD city ON temperature TYPE string;
				DEFINE FIELD value ON temperature TYPE int;
				DEFINE TABLE city_avg AS SELECT city, math::mean(value) AS avg FROM temperature GROUP BY city;
				CREATE temperature SET city = "London", value = 10;
				CREATE temperature SET city = "London", value = 20;
				CREATE temperature SET city = "Paris", value = 30;
			`);

			const view = await c.gql(`{ cityAvgs { id } }`);
			expect(view.body.errors).toBeUndefined();
			expect((view.body.data.cityAvgs as unknown[]).length).toBeGreaterThanOrEqual(2);

			const mut = await c.gql(`{ __schema { mutationType { fields { name } } } }`);
			const names = (mut.body.data.__schema.mutationType.fields as Array<{ name: string }>).map(
				(f) => f.name,
			);
			for (const muty of ["createCity_avg", "updateCity_avg", "deleteCity_avg", "upsertCity_avg"]) {
				expect(names).not.toContain(muty);
			}
			expect(names).toContain("createTemperature");
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// (4) FILTERING / ORDERING / CURSOR PAGINATION
// ---------------------------------------------------------------------------

test(
	"list filters: where-alias, eq/ne/gt/lt/gte/lte, string ops, in, implicit AND, not, or, datetime",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE product SCHEMAFULL;
				DEFINE FIELD name ON product TYPE string;
				DEFINE FIELD price ON product TYPE float;
				DEFINE FIELD quantity ON product TYPE int;
				DEFINE FIELD created ON product TYPE datetime;
				CREATE product:1 SET name = "Alpha Widget", price = 9.99, quantity = 100, created = d"2024-01-15T00:00:00Z";
				CREATE product:2 SET name = "Beta Widget", price = 19.99, quantity = 50, created = d"2024-03-20T00:00:00Z";
				CREATE product:3 SET name = "Gamma Tool", price = 29.99, quantity = 200, created = d"2024-06-01T00:00:00Z";
				CREATE product:4 SET name = "Delta Tool", price = 4.99, quantity = 10, created = d"2024-09-10T00:00:00Z";
				CREATE product:5 SET name = "Epsilon Widget", price = 49.99, quantity = 0, created = d"2025-01-05T00:00:00Z";
			`);

			const count = async (filter: string): Promise<number> => {
				const res = await c.gql(`{ products(filter: ${filter}) { id } }`);
				expect(res.body.errors).toBeUndefined();
				return (res.body.data.products as unknown[]).length;
			};

			// `where` is an alias for `filter`.
			const whereAlias = await c.gql(`{ products(where: { name: { eq: "Alpha Widget" } }) { id } }`);
			expect(whereAlias.body.data.products).toEqual([{ id: "product:1" }]);

			expect(await count(`{ name: { ne: "Alpha Widget" } }`)).toBe(4);
			expect(await count(`{ quantity: { gt: 50 } }`)).toBe(2);
			expect(await count(`{ price: { gte: 19.99 } }`)).toBe(3);
			expect(await count(`{ price: { lte: 9.99 } }`)).toBe(2);
			expect(await count(`{ name: { contains: "Widget" } }`)).toBe(3);

			const startsWith = await c.gql(`{ products(filter: { name: { startsWith: "Delta" } }) { id } }`);
			expect(startsWith.body.data.products).toEqual([{ id: "product:4" }]);

			expect(await count(`{ name: { endsWith: "Tool" } }`)).toBe(2);
			expect(await count(`{ name: { regex: "^(Alpha|Gamma)" } }`)).toBe(2);
			expect(await count(`{ name: { in: ["Alpha Widget", "Delta Tool"] } }`)).toBe(2);
			expect(await count(`{ quantity: { in: [100, 200] } }`)).toBe(2);

			// Implicit AND across fields.
			const andFields = await c.gql(
				`{ products(filter: { name: { contains: "Widget" }, price: { lt: 10 } }) { id } }`,
			);
			expect(andFields.body.data.products).toEqual([{ id: "product:1" }]);

			// Implicit AND on the same field.
			expect(await count(`{ price: { gte: 10, lte: 30 } }`)).toBe(2);
			// not / or.
			expect(await count(`{ not: { name: { contains: "Widget" } } }`)).toBe(2);
			expect(await count(`{ or: [{ price: { lt: 5 } }, { price: { gt: 40 } }] }`)).toBe(2);
			// datetime gt (exclusive of the boundary record).
			expect(await count(`{ created: { gt: "2024-06-01T00:00:00Z" } }`)).toBe(2);
		});
	},
	TIMEOUT,
);

test(
	"function-call filter predicate: builtin string::len and a user-defined fn::high",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE post SCHEMAFULL;
				DEFINE FIELD title ON post TYPE string;
				DEFINE FIELD score ON post TYPE int;
				DEFINE FUNCTION fn::high($n: int) -> bool { RETURN $n >= 10; };
				CREATE post:a SET title = "Hi", score = 5;
				CREATE post:b SET title = "Hello", score = 12;
				CREATE post:c SET title = "Greetings", score = 20;
			`);

			const builtin = await c.gql(
				`{ posts(filter: { title: { call: { fn: "string::len", op: gte, value: 5 } } }, order: { asc: title }) { id } }`,
			);
			expect(builtin.body.errors).toBeUndefined();
			expect((builtin.body.data.posts as Array<{ id: string }>).map((p) => p.id)).toEqual([
				"post:c",
				"post:b",
			]);

			const udf = await c.gql(
				`{ posts(filter: { score: { call: { fn: "fn::high", op: eq, value: true } } }, order: { asc: title }) { id } }`,
			);
			expect(udf.body.errors).toBeUndefined();
			expect((udf.body.data.posts as Array<{ id: string }>).map((p) => p.id)).toEqual([
				"post:c",
				"post:b",
			]);
		});
	},
	TIMEOUT,
);

test(
	"table_aggregate: overall stats, groupBy rows, groupBy+filter, aggregate-row introspection",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE product SCHEMAFULL;
				DEFINE FIELD name ON product TYPE string;
				DEFINE FIELD category ON product TYPE string;
				DEFINE FIELD price ON product TYPE float;
				DEFINE FIELD qty ON product TYPE int;
				CREATE product:a SET name = "Apple", category = "fruit", price = 1.0, qty = 10;
				CREATE product:b SET name = "Banana", category = "fruit", price = 0.5, qty = 20;
				CREATE product:c SET name = "Carrot", category = "veggie", price = 0.8, qty = 30;
				CREATE product:d SET name = "Daikon", category = "veggie", price = 1.2, qty = 5;
			`);

			const overall = await c.gql(`{
				products_aggregate { count price_min price_max price_sum price_avg qty_min qty_max qty_sum qty_avg }
			}`);
			expect(overall.body.errors).toBeUndefined();
			const row = overall.body.data.products_aggregate[0];
			expect(row.count).toBe(4);
			expect(row.price_min).toBe(0.5);
			expect(row.price_max).toBe(1.2);
			expect(row.qty_sum).toBe(65);
			expect(Math.abs(Number(row.qty_avg) - 16.25)).toBeLessThan(1e-6);

			const grouped = await c.gql(`{ products_aggregate(groupBy: [category]) { category count price_avg } }`);
			const byCat = Object.fromEntries(
				(grouped.body.data.products_aggregate as any[]).map((r) => [r.category, r]),
			);
			expect(byCat.fruit.count).toBe(2);
			expect(Math.abs(Number(byCat.fruit.price_avg) - 0.75)).toBeLessThan(1e-6);
			expect(byCat.veggie.count).toBe(2);
			expect(Math.abs(Number(byCat.veggie.price_avg) - 1.0)).toBeLessThan(1e-6);

			const filtered = await c.gql(
				`{ products_aggregate(filter: { price: { gt: 0.6 } }, groupBy: [category]) { category count } }`,
			);
			const total = (filtered.body.data.products_aggregate as any[]).reduce(
				(s, r) => s + Number(r.count),
				0,
			);
			expect(total).toBe(3);

			const intro = await c.gql(`{ __type(name: "product_aggregate_row") { fields { name } } }`);
			const names = (intro.body.data.__type.fields as Array<{ name: string }>).map((f) => f.name);
			for (const req of ["count", "price_min", "price_max", "price_sum", "price_avg", "qty_avg", "category"]) {
				expect(names).toContain(req);
			}
		});
	},
	TIMEOUT,
);

test(
	"cursor pagination (forward): <plural>Connection(first, after) walks Relay edges/pageInfo to exhaustion",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE box SCHEMAFULL;
				DEFINE FIELD label ON box TYPE string;
				CREATE box:b1 SET label = "one";
				CREATE box:b2 SET label = "two";
				CREATE box:b3 SET label = "three";
				CREATE box:b4 SET label = "four";
				CREATE box:b5 SET label = "five";
			`);

			const page1 = await c.gql(
				`{ boxesConnection(first: 2) { edges { cursor node { id label } } pageInfo { hasNextPage endCursor } } }`,
			);
			expect(page1.body.errors).toBeUndefined();
			const p1 = page1.body.data.boxesConnection;
			expect((p1.edges as any[]).map((e) => e.node.id)).toEqual(["box:b1", "box:b2"]);
			expect(p1.pageInfo.hasNextPage).toBe(true);
			const c1: string = p1.pageInfo.endCursor;
			expect(c1.length).toBeGreaterThan(0);

			const page2 = await c.gql(
				`{ boxesConnection(first: 2, after: "${c1}") { edges { node { id } } pageInfo { hasNextPage endCursor } } }`,
			);
			const p2 = page2.body.data.boxesConnection;
			expect((p2.edges as any[]).map((e) => e.node.id)).toEqual(["box:b3", "box:b4"]);
			expect(p2.pageInfo.hasNextPage).toBe(true);
			const c2: string = p2.pageInfo.endCursor;

			const page3 = await c.gql(
				`{ boxesConnection(first: 2, after: "${c2}") { edges { node { id } } pageInfo { hasNextPage } } }`,
			);
			const p3 = page3.body.data.boxesConnection;
			expect((p3.edges as any[]).map((e) => e.node.id)).toEqual(["box:b5"]);
			expect(p3.pageInfo.hasNextPage).toBe(false);
		});
	},
	TIMEOUT,
);

test(
	"cursor pagination (backward): last/before, totalCount (independent + filtered), first+last mutual exclusion",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE crate SCHEMAFULL;
				DEFINE FIELD label ON crate TYPE string;
				CREATE crate:c1 SET label = "one";
				CREATE crate:c2 SET label = "two";
				CREATE crate:c3 SET label = "three";
				CREATE crate:c4 SET label = "four";
				CREATE crate:c5 SET label = "five";
			`);

			// last:2 from the tail -> ascending [c4, c5], hasPreviousPage true.
			const tail = await c.gql(
				`{ cratesConnection(last: 2) { edges { node { id } } pageInfo { hasNextPage hasPreviousPage startCursor endCursor } } }`,
			);
			expect(tail.body.errors).toBeUndefined();
			const t = tail.body.data.cratesConnection;
			expect((t.edges as any[]).map((e) => e.node.id)).toEqual(["crate:c4", "crate:c5"]);
			expect(t.pageInfo.hasPreviousPage).toBe(true);
			expect(t.pageInfo.hasNextPage).toBe(false);
			const startCursor: string = t.pageInfo.startCursor;

			// One more page backwards.
			const back = await c.gql(
				`{ cratesConnection(last: 2, before: "${startCursor}") { edges { node { id } } pageInfo { hasNextPage hasPreviousPage } } }`,
			);
			const b = back.body.data.cratesConnection;
			expect((b.edges as any[]).map((e) => e.node.id)).toEqual(["crate:c2", "crate:c3"]);
			expect(b.pageInfo.hasPreviousPage).toBe(true);
			expect(b.pageInfo.hasNextPage).toBe(true);

			// totalCount is an independent count().
			const tc = await c.gql(`{ cratesConnection(first: 2) { totalCount } }`);
			expect(tc.body.data.cratesConnection.totalCount).toBe(5);

			// totalCount honours the same filter.
			const tcf = await c.gql(
				`{ cratesConnection(first: 10, filter: { label: { in: ["one", "two", "three"] } }) { totalCount edges { node { id } } } }`,
			);
			expect(tcf.body.data.cratesConnection.totalCount).toBe(3);
			expect(tcf.body.data.cratesConnection.edges).toHaveLength(3);

			// Mixing first + last is rejected.
			const both = await c.gql(`{ cratesConnection(first: 2, last: 2) { edges { node { id } } } }`);
			const msg = both.body.errors?.[0].message.toLowerCase() ?? "";
			expect(msg).toContain("first");
			expect(msg).toContain("last");
		});
	},
	TIMEOUT,
);

test(
	"cursor pageInfo is Relay-correct: hasNextPage/hasPreviousPage driven by a probe, not by cursor presence",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE box SCHEMAFULL;
				DEFINE FIELD label ON box TYPE string;
				CREATE box:b1 SET label = "one";
				CREATE box:b2 SET label = "two";
				CREATE box:b3 SET label = "three";
				CREATE box:b4 SET label = "four";
				CREATE box:b5 SET label = "five";
			`);

			// Forward page 1: no `after` -> hasPreviousPage false.
			const fwd1 = await c.gql(
				`{ boxesConnection(first: 2) { pageInfo { hasNextPage hasPreviousPage endCursor } } }`,
			);
			expect(fwd1.body.data.boxesConnection.pageInfo.hasNextPage).toBe(true);
			expect(fwd1.body.data.boxesConnection.pageInfo.hasPreviousPage).toBe(false);
			const after: string = fwd1.body.data.boxesConnection.pageInfo.endCursor;

			// Forward page 2: records exist before -> hasPreviousPage true.
			const fwd2 = await c.gql(
				`{ boxesConnection(first: 2, after: "${after}") { pageInfo { hasNextPage hasPreviousPage } } }`,
			);
			expect(fwd2.body.data.boxesConnection.pageInfo.hasPreviousPage).toBe(true);

			// Backward from tail: last:2, no `before` -> hasNextPage false.
			const tail = await c.gql(
				`{ boxesConnection(last: 2) { pageInfo { hasNextPage hasPreviousPage startCursor } } }`,
			);
			expect(tail.body.data.boxesConnection.pageInfo.hasNextPage).toBe(false);
			expect(tail.body.data.boxesConnection.pageInfo.hasPreviousPage).toBe(true);
			const before: string = tail.body.data.boxesConnection.pageInfo.startCursor;

			// Backward with `before` one in from the tail -> hasNextPage true.
			const step = await c.gql(
				`{ boxesConnection(last: 2, before: "${before}") { pageInfo { hasNextPage hasPreviousPage } } }`,
			);
			expect(step.body.data.boxesConnection.pageInfo.hasNextPage).toBe(true);
		});
	},
	TIMEOUT,
);

test(
	"cursor edge cases: invalid cursor errors, cross-table cursor rejected, `order:` not advertised on Connection",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE box SCHEMAFULL;
				DEFINE FIELD label ON box TYPE string;
				DEFINE TABLE coin SCHEMAFULL;
				DEFINE FIELD value ON coin TYPE int;
				CREATE box:b1 SET label = "one";
				CREATE box:b2 SET label = "two";
				CREATE coin:c1 SET value = 1;
				CREATE coin:c2 SET value = 2;
			`);

			// Garbage cursor -> explicit "invalid cursor" error (no silent page 1).
			const bad = await c.gql(
				`{ boxesConnection(first: 2, after: "not-a-real-cursor") { edges { node { id } } } }`,
			);
			expect((bad.body.errors?.[0].message ?? "").toLowerCase()).toContain("invalid cursor");

			// A real box cursor must not work on the coin connection.
			const boxCursor: string = (
				await c.gql(`{ boxesConnection(first: 1) { pageInfo { endCursor } } }`)
			).body.data.boxesConnection.pageInfo.endCursor;
			const cross = await c.gql(
				`{ coinsConnection(first: 1, after: "${boxCursor}") { edges { node { id } } } }`,
			);
			expect(cross.body.errors?.[0].message ?? "").toContain("cursor table mismatch");

			// `order:` is not a valid argument on Connection (id-keyed cursors).
			const order = await c.gql(
				`{ boxesConnection(first: 1, order: { asc: label }) { edges { node { id } } } }`,
			);
			const orderMsg = (order.body.errors?.[0].message ?? "").toLowerCase();
			expect(orderMsg).toContain("unknown argument");
			expect(orderMsg).toContain("order");
		});
	},
	TIMEOUT,
);

test(
	"id filters: `in` list, inclusive/exclusive `range`, and the oversize `id.in` cap rejection",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE thing;
				CREATE thing:1; CREATE thing:2; CREATE thing:3; CREATE thing:4; CREATE thing:5;
			`);

			const inList = await c.gql(`{ things(filter: { id: { in: ["thing:1","thing:3"] } }) { id } }`);
			expect(inList.body.errors).toBeUndefined();
			expect(inList.body.data.things).toHaveLength(2);

			const inclusive = await c.gql(
				`{ things(filter: { id: { range: { from: "thing:2", to: "thing:4", inclusive: true } } }) { id } }`,
			);
			expect(inclusive.body.data.things).toHaveLength(3);

			const exclusive = await c.gql(
				`{ things(filter: { id: { range: { from: "thing:2", to: "thing:4" } } }) { id } }`,
			);
			expect(exclusive.body.data.things).toHaveLength(2);

			// 1001 ids — one over the 1000 cap -> rejected (cannot synthesise an unbounded OR chain).
			const ids = Array.from({ length: 1001 }, (_, i) => `"thing:${i + 1}"`).join(",");
			const oversize = await c.gql(`{ things(filter: { id: { in: [${ids}] } }) { id } }`);
			expect(oversize.body.errors?.[0].message ?? "").toContain("`id.in` accepts at most");
		});
	},
	TIMEOUT,
);

test(
	"relation-count predicate in the table filter, both directions, plus a plain _aggregate smoke",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE person;
				DEFINE TABLE email;
				DEFINE TABLE sent TYPE RELATION IN person OUT email;
				CREATE person:alice; CREATE person:bob; CREATE person:carol;
				CREATE email:e1; CREATE email:e2; CREATE email:e3; CREATE email:e4;
				RELATE person:alice->sent->email:e1;
				RELATE person:alice->sent->email:e2;
				RELATE person:alice->sent->email:e3;
				RELATE person:bob->sent->email:e4;
			`);

			const outgoing = await c.gql(`{ persons(filter: { sent: { count: { gt: 2 } } }) { id } }`);
			expect(outgoing.body.errors).toBeUndefined();
			expect(outgoing.body.data.persons).toEqual([{ id: "person:alice" }]);

			const incoming = await c.gql(`{ emails(filter: { sent_in: { count: { gte: 1 } } }) { id } }`);
			expect(incoming.body.data.emails).toHaveLength(4);

			const agg = await c.gql(`{ persons_aggregate { count } }`);
			expect(agg.body.data.persons_aggregate).toEqual([{ count: 3 }]);
		});
	},
	TIMEOUT,
);

test(
	"batched HTTP: POSTing an array of operations returns a parallel array of responses",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE coin SCHEMAFULL;
				DEFINE FIELD value ON coin TYPE int;
				CREATE coin:c1 SET value = 1;
				CREATE coin:c2 SET value = 2;
			`);

			const res = await c.gqlRaw([
				{ query: "{ coins { id value } }" },
				{ query: '{ coin(id: "c1") { id value } }' },
			]);
			expect(res.status).toBe(200);
			const arr = res.body as unknown as Array<{ data: any }>;
			expect(Array.isArray(arr)).toBe(true);
			expect(arr).toHaveLength(2);
			expect(arr[0].data.coins).toHaveLength(2);
			expect(arr[1].data.coin.id).toBe("coin:c1");
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// (5) SCHEMA GENERATION, ALIASES, COLLISIONS, INTROSPECTION, DESCRIPTIONS
// ---------------------------------------------------------------------------

test(
	"GRAPHQL_ALIAS renames the GraphQL surface for tables, fields and functions",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE customer_account GRAPHQL_ALIAS "Customer";
				DEFINE FIELD first_name ON customer_account TYPE string GRAPHQL_ALIAS "firstName";
				DEFINE FIELD last_name ON customer_account TYPE string GRAPHQL_ALIAS "lastName";
				DEFINE FUNCTION fn::ping() -> string { RETURN "pong"; } GRAPHQL_ALIAS "ping";
				CREATE customer_account:alice SET first_name = "Alice", last_name = "Smith";
			`);

			// Table alias -> Customer/customers pair, aliased fields.
			const list = await c.gql(`{ customers { id firstName lastName } }`);
			expect(list.body.errors).toBeUndefined();
			expect(list.body.data.customers).toHaveLength(1);
			expect(list.body.data.customers[0].firstName).toBe("Alice");
			expect(list.body.data.customers[0].lastName).toBe("Smith");

			// Field alias works inside filters.
			const filtered = await c.gql(`{ customers(filter: { firstName: { eq: "Alice" } }) { id } }`);
			expect(filtered.body.data.customers).toHaveLength(1);

			// Singular fetch uses the aliased `Customer`.
			const single = await c.gql(`{ Customer(id: "alice") { id firstName } }`);
			expect(single.body.data.Customer.firstName).toBe("Alice");

			// Mutation uses the aliased capitalisation.
			const created = await c.gql(
				`mutation { createCustomer(data: { firstName: "Bob", lastName: "Jones" }) { id firstName } }`,
			);
			expect(created.body.data.createCustomer.firstName).toBe("Bob");

			// Function alias.
			const ping = await c.gql(`{ ping }`);
			expect(ping.body.data.ping).toBe("pong");
		});
	},
	TIMEOUT,
);

test(
	"invalid GRAPHQL_ALIAS is rejected at DEFINE-time with a clear DDL error",
	async () => {
		await withGql(async (c) => {
			// An alias with a space is not a valid GraphQL identifier. Validated at
			// DEFINE-time: the /sql response names GRAPHQL_ALIAS in the error. (Driven
			// over raw /sql, matching the Rust source, since the failure is a DDL-time
			// validation, not a GraphQL request.)
			const res = await fetch(`${c.server.httpUrl}/sql`, {
				method: "POST",
				headers: {
					Authorization: ROOT_AUTH,
					"surreal-ns": c.ns,
					"surreal-db": c.dbName,
					Accept: "application/json",
				},
				body: `DEFINE TABLE person SCHEMAFULL;
					 DEFINE FIELD first_name ON person TYPE string GRAPHQL_ALIAS "first name";`,
			});
			expect(await res.text()).toContain("GRAPHQL_ALIAS");
		});
	},
	TIMEOUT,
);

test(
	"schema-name collisions are rejected at build time: Apollo plural clash and PageInfo built-in clash",
	async () => {
		// Two tables collapsing to the same plural query name.
		await withGql(async (c) => {
			await c.db.query(`DEFINE CONFIG GRAPHQL AUTO; DEFINE TABLE store; DEFINE TABLE stores;`);
			const res = await c.gql(`{ __typename }`);
			expect((res.body.errors?.[0].message ?? "").toLowerCase()).toContain("collision");
		});

		// A table whose name collides with the built-in PageInfo helper type.
		await withGql(async (c) => {
			await c.db.query(`DEFINE CONFIG GRAPHQL AUTO; DEFINE TABLE PageInfo;`);
			const res = await c.gql(`{ __typename }`);
			const msg = res.body.errors?.[0].message ?? "";
			expect(msg).toContain("PageInfo");
			expect(msg).toContain("built-in");
		});
	},
	TIMEOUT,
);

test(
	"schema cache invalidates on DDL: changing a field type is reflected on the next request",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE test;
				DEFINE FIELD test_field ON test TYPE string;
			`);

			const typeOf = async (): Promise<string> => {
				const res = await c.gql(
					`{ __type(name: "test") { fields { name type { kind name ofType { name } } } } }`,
				);
				const f = (res.body.data.__type.fields as any[]).find((x) => x.name === "test_field");
				return f.type.kind === "NON_NULL" ? f.type.ofType.name : f.type.name;
			};

			expect(await typeOf()).toBe("String");
			await c.db.query("DEFINE FIELD OVERWRITE test_field ON test TYPE int;");
			expect(await typeOf()).toBe("Int");
		});
	},
	TIMEOUT,
);

test(
	"schema generation produces valid identifiers for nested-object and array<record> filter type names",
	async () => {
		await withGql(async (c) => {
			// #7034: a record-typed field inside an object literal.
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE bar SCHEMAFULL;
				DEFINE TABLE foo SCHEMAFULL;
				DEFINE FIELD bar ON foo TYPE { bar: record<foo> };
			`);
			const nested = await c.gql(`{
				__type(name: "foo") { fields { name type { kind name ofType { kind name } } } }
				inner: __type(name: "foo_bar") { fields { name type { kind name ofType { kind name } } } }
			}`);
			expect(nested.body.errors).toBeUndefined();
			const bar = (nested.body.data.__type.fields as any[]).find((f) => f.name === "bar");
			const referenced = bar.type.kind === "NON_NULL" ? bar.type.ofType.name : bar.type.name;
			expect(referenced).toBe("foo_bar");
			const innerBar = (nested.body.data.inner.fields as any[]).find((f) => f.name === "bar");
			const target = innerBar.type.kind === "NON_NULL" ? innerBar.type.ofType.name : innerBar.type.name;
			expect(target).toBe("foo");

			// #4999: nested object + array<record> filters generate legal type names.
			await c.db.query(`
				DEFINE TABLE parent;
				DEFINE TABLE child;
				DEFINE FIELD children ON parent TYPE option<array<record<child>>>;
				DEFINE FIELD nested ON parent TYPE object;
				DEFINE FIELD nested.field1 ON parent TYPE bool;
				DEFINE FIELD nested.field2 ON parent TYPE int;
			`);
			const all = await c.gql(`{ __schema { types { name } } }`);
			expect(all.body.errors).toBeUndefined();
			const names = (all.body.data.__schema.types as Array<{ name: string }>)
				.map((t) => t.name)
				.filter((n): n is string => typeof n === "string");
			expect(names).toContain("parent_nested");
			expect(names.some((n) => n.startsWith("_filter_list_"))).toBe(true);
			for (const n of names) {
				expect(n).not.toMatch(/[[\].]/);
			}
		});
	},
	TIMEOUT,
);

test(
	"INTROSPECTION NONE blocks __schema/__type while data queries keep working; re-enabling restores it",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE person SCHEMAFULL;
				DEFINE FIELD name ON person TYPE string;
				DEFINE FIELD age ON person TYPE int;
				CREATE person:1 SET name = 'Alice', age = 30;
			`);

			// Introspection works by default.
			expect((await c.gql(`{ __schema { queryType { fields { name } } } }`)).body.errors).toBeUndefined();

			// Disable it.
			await c.db.query(`DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION NONE;`);
			const blockedSchema = await c.gql(`{ __schema { queryType { fields { name } } } }`);
			expect(blockedSchema.body.data?.__schema == null || Array.isArray(blockedSchema.body.errors)).toBe(
				true,
			);
			const blockedType = await c.gql(`{ __type(name: "person") { name } }`);
			expect(blockedType.body.data?.__type == null || Array.isArray(blockedType.body.errors)).toBe(true);

			// Data queries still work with introspection off.
			const data = await c.gql(`{ persons { id name age } }`);
			expect(data.body.errors).toBeUndefined();
			expect(Array.isArray(data.body.data.persons)).toBe(true);

			// Re-enable.
			await c.db.query(`DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION AUTO;`);
			expect((await c.gql(`{ __schema { queryType { fields { name } } } }`)).body.errors).toBeUndefined();

			// The setting round-trips through INFO FOR DB (set NONE again, then read).
			await c.db.query(`DEFINE CONFIG OVERWRITE GRAPHQL AUTO INTROSPECTION NONE;`);
			const [info] = await c.db.query<[any]>("INFO FOR DB").json();
			expect(String(info.configs.GraphQL)).toContain("INTROSPECTION NONE");
		});
	},
	TIMEOUT,
);

test(
	"DEPTH / COMPLEXITY limits reject deep / wide queries; raising the limits admits them; round-trips via INFO",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO DEPTH 3 COMPLEXITY 10;
				DEFINE TABLE person SCHEMAFULL;
				DEFINE FIELD name ON person TYPE string;
				DEFINE FIELD age ON person TYPE int;
				DEFINE TABLE post SCHEMAFULL;
				DEFINE FIELD title ON post TYPE string;
				DEFINE FIELD author ON post TYPE record<person>;
				DEFINE TABLE comment SCHEMAFULL;
				DEFINE FIELD text ON comment TYPE string;
				DEFINE FIELD post ON comment TYPE record<post>;
				CREATE person:1 SET name = 'Alice', age = 30;
				CREATE post:1 SET title = 'Hello', author = person:1;
				CREATE comment:1 SET text = 'Nice', post = post:1;
			`);

			// Shallow query is fine.
			expect((await c.gql(`{ persons { id name } }`)).body.errors).toBeUndefined();

			// Too deep -> depth-limit error.
			const deep = await c.gql(`{ comments { text post { title author { name age } } } }`);
			expect((deep.body.errors?.[0].message ?? "")).toContain("too deep");

			// Too wide -> complexity error.
			const wide = await c.gql(`{
				persons { id name age }
				posts { id title }
				comments { id text }
				p2: persons { id name age }
			}`);
			const wideMsg = wide.body.errors?.[0].message ?? "";
			expect(/too complex|complexity/.test(wideMsg)).toBe(true);

			// Raise the limits; both queries now pass.
			await c.db.query(`DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 10 COMPLEXITY 100;`);
			expect(
				(await c.gql(`{ comments { text post { title author { name } } } }`)).body.errors,
			).toBeUndefined();

			// Round-trip DEPTH/COMPLEXITY through INFO FOR DB.
			await c.db.query(`DEFINE CONFIG OVERWRITE GRAPHQL AUTO DEPTH 5 COMPLEXITY 50;`);
			const [info] = await c.db.query<[any]>("INFO FOR DB").json();
			const cfg = String(info.configs.GraphQL);
			expect(cfg).toContain("DEPTH 5");
			expect(cfg).toContain("COMPLEXITY 50");
		});
	},
	TIMEOUT,
);

test(
	"SurrealQL COMMENTs surface as GraphQL descriptions; GRAPHQL_DEPRECATED annotates descriptions",
	async () => {
		await withGql(async (c) => {
			await c.db.query(`
				DEFINE CONFIG GRAPHQL AUTO;
				DEFINE TABLE person SCHEMAFULL COMMENT "Person records";
				DEFINE FIELD name ON person TYPE string COMMENT "Person display name";
				DEFINE FIELD age ON person TYPE int COMMENT "Person age";
				DEFINE TABLE legacy SCHEMAFULL GRAPHQL_DEPRECATED "table-gone";
				DEFINE FIELD old_name ON legacy TYPE string GRAPHQL_DEPRECATED "field-gone";
				DEFINE FUNCTION fn::old_fn() -> bool { RETURN true; } GRAPHQL_DEPRECATED "function-gone";
			`);

			const res = await c.gql(`{
				q: __type(name: "Query") { fields { name description } }
				personType: __type(name: "person") { fields { name description } }
				legacyType: __type(name: "legacy") { fields { name description } }
			}`);
			expect(res.body.errors).toBeUndefined();

			// COMMENT -> descriptions.
			const qFields = res.body.data.q.fields as any[];
			expect(qFields.find((f) => f.name === "person").description).toBe("Person records");
			const personFields = res.body.data.personType.fields as any[];
			expect(personFields.find((f) => f.name === "name").description).toBe("Person display name");
			expect(personFields.find((f) => f.name === "age").description).toBe("Person age");

			// GRAPHQL_DEPRECATED -> "[Deprecated: ...]" appended to descriptions.
			const legacyFields = res.body.data.legacyType.fields as any[];
			expect(legacyFields.find((f) => f.name === "old_name").description).toContain(
				"[Deprecated: field-gone]",
			);
			expect(qFields.find((f) => f.name === "legacies").description).toContain(
				"[Deprecated: table-gone]",
			);
			expect(qFields.find((f) => f.name === "fn_old_fn").description).toContain(
				"[Deprecated: function-gone]",
			);
		});
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// (6) The `graphql` RPC method (raw JSON-RPC via RpcClient)
// ---------------------------------------------------------------------------

test(
	"graphql RPC method: not-configured errors cleanly, then query + variables/operation both resolve",
	async () => {
		const server = await startServer();
		openServers.push(server);
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			const ns = `gqlrpc_ns_${process.pid}`;
			const dbName = `gqlrpc_db_${process.pid}`;
			await rpc.call("query", [
				`DEFINE NAMESPACE \`${ns}\`; USE NS \`${ns}\`; DEFINE DATABASE \`${dbName}\`;`,
			]);
			await rpc.use(ns, dbName);

			// Before any GraphQL config: clean "not configured" error, no panic.
			await rpc.call("query", ["DEFINE TABLE foo SCHEMALESS"]);
			const unconfigured = await rpc.rpc("graphql", ["query{ __typename }"]);
			expect(unconfigured.error).toBeDefined();
			expect(String(unconfigured.error!.message).toLowerCase()).toContain("configured");

			// Configure + seed.
			await rpc.call("query", [
				`DEFINE TABLE foo SCHEMAFULL;
				 DEFINE FIELD val ON foo TYPE int;
				 CREATE foo:1 SET val = 42 RETURN NONE;
				 CREATE foo:2 SET val = 43 RETURN NONE;
				 DEFINE CONFIG GRAPHQL AUTO;`,
			]);

			// Plain query.
			const q = (await rpc.call("graphql", ["query{ foos { id val } }"])) as any;
			expect(q.errors == null).toBe(true);
			expect(q.data.foos).toEqual([
				{ id: "foo:1", val: 42 },
				{ id: "foo:2", val: 43 },
			]);

			// Variables + named operation (2nd and 3rd RPC args).
			const v = (await rpc.call("graphql", [
				"query Foos($n: Int) { foos(limit: $n) { id } }",
				{ n: 1 },
				"Foos",
			])) as any;
			expect(v.errors == null).toBe(true);
			expect(v.data.foos).toEqual([{ id: "foo:1" }]);
		} finally {
			await rpc.close();
		}
	},
	TIMEOUT,
);

test(
	"graphql RPC method is blocked by --deny-rpc=graphql (per-method kill switch)",
	async () => {
		const server = await startServer({ args: ["--deny-rpc=graphql"] });
		openServers.push(server);
		const rpc = await RpcClient.connect(server);
		try {
			await rpc.signinRoot();
			const ns = `deny_ns_${process.pid}`;
			const dbName = `deny_db_${process.pid}`;
			await rpc.call("query", [
				`DEFINE NAMESPACE \`${ns}\`; USE NS \`${ns}\`; DEFINE DATABASE \`${dbName}\`;`,
			]);
			await rpc.use(ns, dbName);

			const res = await rpc.rpc("graphql", ["query{ __typename }"]);
			expect(res.error).toBeDefined();
			// The method is refused by the RPC capability; message names the method
			// or the denial (kind is NotAllowed on the wire).
			const msg = String(res.error!.message);
			expect(/not allowed|method|graphql/i.test(msg)).toBe(true);
		} finally {
			await rpc.close();
		}
	},
	TIMEOUT,
);

// ---------------------------------------------------------------------------
// graphql-transport-ws subscriptions — skipped (no SDK surface)
// ---------------------------------------------------------------------------

// Ported from graphql_integration.rs::subscriptions_live_query_stream (and the
// two _shape variants). surrealdb.js 2.0.4 exposes NO GraphQL subscription
// surface: the SDK's GraphQL support is HTTP query/mutation only. The Rust test
// drives a raw `graphql-transport-ws` WebSocket upgrade against ws://<addr>/graphql
// (connection_init -> connection_ack -> subscribe -> next), which is outside the
// SDK-as-driver contract of this suite. Left as test.skip; this is the
// acceptance test to enable once the SDK grows a GraphQL subscription API (or a
// stable raw-ws helper lands in the harness).
test.skip("graphql-transport-ws subscription streams live-query events (no SDK surface)", async () => {
	// Intentionally empty: no supported SDK path. See the note above.
});
