import { afterAll, beforeAll, expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

// GraphQL endpoint conformance.
//
// Availability: the /graphql route is compiled in and allowed by DEFAULT —
// no capability flag, no experimental flag ('surreal start --help'
// has no --allow-experimental at all). The only gate is per-database:
// `DEFINE CONFIG GRAPHQL AUTO` (or `TABLES INCLUDE ...`) must exist, else
// every request is HTTP 400 "GraphQL has not been configured for this
// database". Route-level denial is still possible via `--deny-http=graphql`
// (equals form required — see the last test).

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

const ROOT_AUTH = `Basic ${Buffer.from("root:root").toString("base64")}`;

interface GqlError {
	message: string;
	locations?: Array<{ line: number; column: number }>;
}

interface GqlResult {
	status: number;
	body: { data?: unknown; errors?: GqlError[] } & Record<string, unknown>;
}

/** POST a GraphQL query. `auth: null` sends no Authorization header. */
async function gql(
	srv: TestServer,
	namespace: string,
	database: string,
	query: string,
	opts: { auth?: string | null; variables?: Record<string, unknown> } = {},
): Promise<GqlResult> {
	const headers: Record<string, string> = {
		"surreal-ns": namespace,
		"surreal-db": database,
		"Content-Type": "application/json",
		Accept: "application/json",
	};
	const auth = opts.auth === undefined ? ROOT_AUTH : opts.auth;
	if (auth !== null) headers.Authorization = auth;
	const res = await fetch(`${srv.httpUrl}/graphql`, {
		method: "POST",
		headers,
		body: JSON.stringify({ query, variables: opts.variables }),
	});
	return { status: res.status, body: (await res.json()) as GqlResult["body"] };
}

test("unconfigured database: HTTP 400 with a clear not-configured error", async () => {
	const { db, namespace, database } = await rootClient(server);
	// No DEFINE CONFIG GRAPHQL on this database.
	const res = await gql(server, namespace, database, "{ __typename }");
	expect(res.status).toBe(400);
	expect(res.body.data).toBeNull();
	expect(res.body.errors).toHaveLength(1);
	expect(res.body.errors?.[0].message).toBe("GraphQL has not been configured for this database");
	await db.close();
});

test("CONFIG GRAPHQL AUTO on an empty database: schema generation fails loudly", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE CONFIG GRAPHQL AUTO");
	// Configured but zero tables/functions: still 400, with an explicit reason.
	const res = await gql(server, namespace, database, "{ __typename }");
	expect(res.status).toBe(400);
	expect(res.body.errors?.[0].message).toBe(
		"Error generating schema: no items found in database: GraphQL requires at least one table or function",
	);
	await db.close();
});

test("introspection: SCHEMAFULL fields are typed, SCHEMALESS tables expose only id", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE movie SCHEMAFULL;
		DEFINE FIELD title ON movie TYPE string;
		DEFINE FIELD year ON movie TYPE int;
		DEFINE TABLE extra SCHEMALESS;
	`);

	const typ = await gql(
		server,
		namespace,
		database,
		'{ __type(name: "movie") { name fields { name type { kind ofType { name } } } } }',
	);
	expect(typ.status).toBe(200);
	const fields = (typ.body.data as any).__type.fields as Array<{
		name: string;
		type: { kind: string; ofType: { name: string } | null };
	}>;
	const byName = Object.fromEntries(fields.map((f) => [f.name, f]));
	// All declared fields are NON_NULL-wrapped; id maps to ID, int to Int, string to String.
	expect(byName.id.type).toEqual({ kind: "NON_NULL", ofType: { name: "ID" } });
	expect(byName.year.type).toEqual({ kind: "NON_NULL", ofType: { name: "Int" } });
	expect(byName.title.type).toEqual({ kind: "NON_NULL", ofType: { name: "String" } });

	// A SCHEMALESS table still appears under AUTO, but with only the id field —
	// undeclared fields are invisible to GraphQL.
	const loose = await gql(server, namespace, database, '{ __type(name: "extra") { fields { name } } }');
	expect((loose.body.data as any).__type.fields).toEqual([{ name: "id" }]);

	// Per table the Query root grows five fields: plural list, singular-by-id,
	// aggregate, relay-style connection — plus a shared table-agnostic _get.
	const root = await gql(server, namespace, database, "{ __schema { queryType { fields { name } } } }");
	const names = ((root.body.data as any).__schema.queryType.fields as Array<{ name: string }>).map(
		(f) => f.name,
	);
	for (const expected of ["movies", "movie", "movies_aggregate", "moviesConnection", "_get"]) {
		expect(names).toContain(expected);
	}
	await db.close();
});

test("list queries: order, filter, limit arguments and aggregate count", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE album SCHEMAFULL;
		DEFINE FIELD title ON album TYPE string;
		DEFINE FIELD year ON album TYPE int;
		CREATE album:a SET title = 'Abbey Road', year = 1969;
		CREATE album:b SET title = 'Kid A', year = 2000;
		CREATE album:c SET title = 'Blackstar', year = 2016;
	`);

	const ordered = await gql(server, namespace, database, "{ albums(order: {asc: year}) { title year } }");
	expect(ordered.status).toBe(200);
	expect((ordered.body.data as any).albums).toEqual([
		{ title: "Abbey Road", year: 1969 },
		{ title: "Kid A", year: 2000 },
		{ title: "Blackstar", year: 2016 },
	]);

	// Numeric comparison filter (also accepted under the alias arg `where`).
	const filtered = await gql(server, namespace, database, "{ albums(filter: {year: {gt: 1990}}, order: {asc: year}) { title } }");
	expect((filtered.body.data as any).albums).toEqual([{ title: "Kid A" }, { title: "Blackstar" }]);

	// String operator filter: eq/ne/contains/startsWith/endsWith/regex/in/matches/call.
	const contains = await gql(server, namespace, database, '{ albums(filter: {title: {contains: "lack"}}) { title } }');
	expect((contains.body.data as any).albums).toEqual([{ title: "Blackstar" }]);

	const limited = await gql(server, namespace, database, "{ albums(limit: 1, order: {desc: year}) { title } }");
	expect((limited.body.data as any).albums).toEqual([{ title: "Blackstar" }]);

	// Aggregate returns a LIST of group rows even without groupBy.
	const agg = await gql(server, namespace, database, "{ albums_aggregate { count } }");
	expect((agg.body.data as any).albums_aggregate).toEqual([{ count: 3 }]);

	await db.close();
});

test("singular lookup takes the BARE record key; a full `table:key` id silently returns null", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE song SCHEMAFULL;
		DEFINE FIELD name ON song TYPE string;
		CREATE song:hey SET name = 'Hey Jude';
	`);

	// id values RENDER as "song:hey" ...
	const list = await gql(server, namespace, database, "{ songs { id } }");
	expect((list.body.data as any).songs).toEqual([{ id: "song:hey" }]);

	// ... but the singular field only accepts the bare key. Surprising: the id
	// string the API itself returns does NOT round-trip into `song(id:)` — it
	// yields data.song = null with NO error rather than a lookup failure.
	const bare = await gql(server, namespace, database, '{ song(id: "hey") { id name } }');
	expect((bare.body.data as any).song).toEqual({ id: "song:hey", name: "Hey Jude" });
	const full = await gql(server, namespace, database, '{ song(id: "song:hey") { id name } }');
	expect(full.status).toBe(200);
	expect((full.body.data as any).song).toBeNull();
	expect(full.body.errors).toBeUndefined();

	// The table-agnostic _get field is the opposite: it REQUIRES the full id.
	const got = await gql(server, namespace, database, '{ _get(id: "song:hey") { id } }');
	expect((got.body.data as any)._get).toEqual({ id: "song:hey" });

	// _get against a nonexistent table leaks an internal execution error.
	const bad = await gql(server, namespace, database, '{ _get(id: "nosuch:x") { id } }');
	expect(bad.body.errors?.[0].message).toBe(
		"Internal Error: Query execution failed: The table 'nosuch' does not exist",
	);
	await db.close();
});

test("mutations: createX / updateX / deleteX write through to the database", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE robot SCHEMAFULL;
		DEFINE FIELD name ON robot TYPE string;
		DEFINE FIELD legs ON robot TYPE int;
	`);

	// Mutation root offers camelCase create/update/upsert/delete in singular
	// and plural forms per table.
	const root = await gql(server, namespace, database, "{ __schema { mutationType { fields { name } } } }");
	const names = ((root.body.data as any).__schema.mutationType.fields as Array<{ name: string }>).map(
		(f) => f.name,
	);
	expect(names.sort()).toEqual([
		"createRobot",
		"createRobots",
		"deleteRobot",
		"deleteRobots",
		"updateRobot",
		"updateRobots",
		"upsertRobot",
		"upsertRobots",
	]);

	const created = await gql(
		server,
		namespace,
		database,
		'mutation { createRobot(data: {name: "bender", legs: 2}) { id name legs } }',
	);
	const rec = (created.body.data as any).createRobot;
	expect(rec.name).toBe("bender");
	expect(rec.legs).toBe(2);
	expect(rec.id).toMatch(/^robot:/);
	// Visible through SurrealQL on the same database.
	const [rows] = await db.query<[Array<{ name: string }>]>("SELECT * FROM robot").json();
	expect(rows).toHaveLength(1);
	expect(rows[0].name).toBe("bender");

	// update/delete take the BARE key (same convention as the singular query).
	const key = rec.id.slice("robot:".length);
	const updated = await gql(server, namespace, database, `mutation { updateRobot(id: "${key}", data: {legs: 6}) { legs } }`);
	expect((updated.body.data as any).updateRobot).toEqual({ legs: 6 });

	// delete returns a bare Boolean (no selection set allowed).
	const deleted = await gql(server, namespace, database, `mutation { deleteRobot(id: "${key}") }`);
	expect((deleted.body.data as any).deleteRobot).toBe(true);
	const [after] = await db.query<[unknown[]]>("SELECT * FROM robot").json();
	expect(after).toHaveLength(0);

	await db.close();
});

test("DDL changes regenerate the schema without touching the config", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE plant SCHEMAFULL;
		DEFINE FIELD name ON plant TYPE string;
	`);
	// Warm the schema cache.
	const warm = await gql(server, namespace, database, "{ plants { id } }");
	expect(warm.status).toBe(200);

	// A brand-new table becomes queryable immediately — the schema cache is
	// fingerprinted against the catalog, so DDL invalidates it automatically.
	await db.query(`
		DEFINE TABLE stone SCHEMAFULL;
		DEFINE FIELD kind ON stone TYPE string;
		CREATE stone:s1 SET kind = 'granite';
	`);
	const res = await gql(server, namespace, database, "{ stones { id kind } }");
	expect(res.status).toBe(200);
	expect((res.body.data as any).stones).toEqual([{ id: "stone:s1", kind: "granite" }]);
	await db.close();
});

test("TABLES INCLUDE restricts the schema; excluded tables are unknown fields", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL TABLES INCLUDE cat;
		DEFINE TABLE cat SCHEMAFULL;
		DEFINE FIELD name ON cat TYPE string;
		DEFINE TABLE dog SCHEMAFULL;
		DEFINE FIELD name ON dog TYPE string;
		CREATE cat:c SET name = 'miso';
		CREATE dog:d SET name = 'rex';
	`);

	const cats = await gql(server, namespace, database, "{ cats { name } }");
	expect((cats.body.data as any).cats).toEqual([{ name: "miso" }]);

	// The excluded table is absent from the schema entirely — a standard
	// GraphQL validation error with locations, HTTP 200.
	const dogs = await gql(server, namespace, database, "{ dogs { name } }");
	expect(dogs.status).toBe(200);
	expect(dogs.body.data).toBeNull();
	expect(dogs.body.errors?.[0].message).toBe('Unknown field "dogs" on type "Query".');
	expect(dogs.body.errors?.[0].locations).toEqual([{ line: 1, column: 3 }]);
	await db.close();
});

test("anonymous request: HTTP 200 with a permissions error in the errors array", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query(`
		DEFINE CONFIG GRAPHQL AUTO;
		DEFINE TABLE coin SCHEMAFULL;
		DEFINE FIELD label ON coin TYPE string;
		CREATE coin:one SET label = 'penny';
	`);

	// Without --allow-guests, execution is denied per-field, not at transport
	// level: the HTTP status is 200 and the denial is a GraphQL error wrapped
	// as "Internal Error" (surprising taxonomy — a permissions denial is not
	// an internal error, and there is no 401).
	const res = await gql(server, namespace, database, "{ coins { label } }", { auth: null });
	expect(res.status).toBe(200);
	expect(res.body.data).toBeNull();
	expect(res.body.errors?.[0].message).toBe(
		"Internal Error: Failed to execute query plan: Anonymous access not allowed: Not enough permissions to perform this action",
	);
	await db.close();
});

test("missing surreal-ns / surreal-db headers: HTTP 400 with guidance", async () => {
	const { db, namespace, database } = await rootClient(server);
	await db.query("DEFINE CONFIG GRAPHQL AUTO");

	const noNs = await fetch(`${server.httpUrl}/graphql`, {
		method: "POST",
		headers: { Authorization: ROOT_AUTH, "Content-Type": "application/json" },
		body: JSON.stringify({ query: "{ __typename }" }),
	});
	expect(noNs.status).toBe(400);
	const nsBody = (await noNs.json()) as { errors: GqlError[] };
	expect(nsBody.errors[0].message).toBe(
		"No namespace specified. Set the `surreal-ns` header on the request.",
	);

	const noDb = await fetch(`${server.httpUrl}/graphql`, {
		method: "POST",
		headers: {
			Authorization: ROOT_AUTH,
			"surreal-ns": namespace,
			"Content-Type": "application/json",
		},
		body: JSON.stringify({ query: "{ __typename }" }),
	});
	expect(noDb.status).toBe(400);
	const dbBody = (await noDb.json()) as { errors: GqlError[] };
	expect(dbBody.errors[0].message).toBe(
		"No database specified. Set the `surreal-db` header on the request.",
	);
	await db.close();
});

test(
	"--deny-http=graphql gates the route with 403 (equals form required)",
	async () => {
		// NOTE: pass variadic capability flags as --flag=value. The bare form
		// ("--deny-http", "graphql") makes clap's variadic parser swallow the
		// trailing "memory" positional the harness appends, and startup fails
		// with: invalid value 'memory' for '--deny-http'.
		const denied = await startServer({ args: ["--deny-http=graphql"] });
		try {
			const res = await fetch(`${denied.httpUrl}/graphql`, {
				method: "POST",
				headers: {
					Authorization: ROOT_AUTH,
					"surreal-ns": "anyns",
					"surreal-db": "anydb",
					"Content-Type": "application/json",
				},
				body: JSON.stringify({ query: "{ __typename }" }),
			});
			expect(res.status).toBe(403);
			const body = (await res.json()) as Record<string, unknown>;
			expect(body).toEqual({
				code: 403,
				details: "Forbidden",
				description: "Not allowed to do this.",
				information: "The HTTP route 'graphql' is forbidden",
			});
		} finally {
			await denied.stop();
		}
	},
	30000,
);
