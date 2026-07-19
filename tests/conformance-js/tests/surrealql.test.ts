import { afterAll, beforeAll, expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

// SurrealQL dialect conformance — the query-language surface itself, executed
// through the normal `query` path, kept deliberately simple and parallel to
// graphql.test.ts / gql.test.ts (dialect surface) and distinct from
// surrealql-wire.test.ts (CBOR rich-type / multi-statement wire boundary).
// A representative smoke of the core statements and clauses, not exhaustive —
// the exhaustive spec lives in language-tests/*.surql.

let server: TestServer;

beforeAll(async () => {
	server = await startServer();
});

afterAll(async () => {
	await server.stop();
});

test("CREATE returns the created record with its fields and id", async () => {
	const { db } = await rootClient(server);
	const [rows] = await db
		.query<[Array<{ id: unknown; name: string; age: number }>]>(
			"CREATE person:tobie SET name = 'Tobie', age = 38",
		)
		.json();
	expect(rows).toHaveLength(1);
	expect(rows[0].name).toBe("Tobie");
	expect(rows[0].age).toBe(38);
	expect(String(rows[0].id)).toBe("person:tobie");
	await db.close();
});

test("SELECT with WHERE, ORDER BY, LIMIT, START", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		CREATE t:1 SET n = 3;
		CREATE t:2 SET n = 1;
		CREATE t:3 SET n = 2;
		CREATE t:4 SET n = 5;
	`);
	const [rows] = await db
		.query<[Array<{ n: number }>]>(
			"SELECT n FROM t WHERE n > 1 ORDER BY n ASC LIMIT 2 START 1",
		)
		.json();
	// n>1 → [2,3,5] ordered; START 1 skips the 2; LIMIT 2 → [3,5]
	expect(rows.map((r) => r.n)).toEqual([3, 5]);
	await db.close();
});

test("SELECT VALUE projects a flat array", async () => {
	const { db } = await rootClient(server);
	await db.query("CREATE v:1 SET n = 10; CREATE v:2 SET n = 20;");
	const [vals] = await db.query<[number[]]>("SELECT VALUE n FROM v ORDER BY n").json();
	expect(vals).toEqual([10, 20]);
	await db.close();
});

test("GROUP BY with aggregate functions", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		CREATE sale SET team = 'a', amt = 10;
		CREATE sale SET team = 'a', amt = 15;
		CREATE sale SET team = 'b', amt = 7;
	`);
	const [rows] = await db
		.query<[Array<{ team: string; total: number; count: number }>]>(
			"SELECT team, math::sum(amt) AS total, count() AS count FROM sale GROUP BY team ORDER BY team",
		)
		.json();
	expect(rows).toEqual([
		{ team: "a", total: 25, count: 2 },
		{ team: "b", total: 7, count: 1 },
	]);
	await db.close();
});

test("UPDATE replaces via SET; UPSERT creates then updates", async () => {
	const { db } = await rootClient(server);
	await db.query("CREATE doc:1 SET a = 1, b = 2");
	const [upd] = await db.query<[Array<{ a: number; b: number }>]>("UPDATE doc:1 SET a = 100").json();
	expect(upd[0]).toEqual({ a: 100, b: 2, id: expect.anything() } as never);

	// UPSERT on a missing id creates it…
	const [made] = await db.query<[Array<{ x: number }>]>("UPSERT doc:2 SET x = 7").json();
	expect(made[0].x).toBe(7);
	// …and on an existing id updates it.
	const [again] = await db.query<[Array<{ x: number }>]>("UPSERT doc:2 SET x = 8").json();
	expect(again[0].x).toBe(8);
	await db.close();
});

test("DELETE removes records; RETURN BEFORE yields the prior value", async () => {
	const { db } = await rootClient(server);
	await db.query("CREATE gone:1 SET v = 'x'");
	const [before] = await db
		.query<[Array<{ v: string }>]>("DELETE gone:1 RETURN BEFORE")
		.json();
	expect(before[0].v).toBe("x");
	const [after] = await db.query<[unknown[]]>("SELECT * FROM gone").json();
	expect(after).toHaveLength(0);
	await db.close();
});

test("INSERT accepts a batch and returns all created records", async () => {
	const { db } = await rootClient(server);
	const [rows] = await db
		.query<[unknown[]]>("INSERT INTO item [{ id: item:1, n: 1 }, { id: item:2, n: 2 }]")
		.json();
	expect(rows).toHaveLength(2);
	await db.close();
});

test("RELATE creates a graph edge traversable with ->", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		CREATE person:a SET name = 'A';
		CREATE person:b SET name = 'B';
		RELATE person:a->knows->person:b SET since = 2020;
	`);
	// forward traversal to the neighbour's field
	const [names] = await db
		.query<[string[]]>("SELECT VALUE ->knows->person.name FROM person:a")
		.json();
	expect(names.flat()).toContain("B");
	// edge carries its content
	const [edge] = await db.query<[Array<{ since: number }>]>("SELECT since FROM knows").json();
	expect(edge[0].since).toBe(2020);
	await db.close();
});

test("record ranges select an inclusive id span", async () => {
	const { db } = await rootClient(server);
	await db.query("CREATE r:1; CREATE r:2; CREATE r:3; CREATE r:4; CREATE r:5;");
	const [ids] = await db.query<[string[]]>("SELECT VALUE id FROM r:2..=4").json();
	expect([...ids].map(String).sort()).toEqual(["r:2", "r:3", "r:4"]);
	await db.close();
});

test("FOR loop iterates and writes records", async () => {
	const { db } = await rootClient(server);
	await db.query("FOR $i IN [1, 2, 3] { CREATE loop SET n = $i }");
	const [ns] = await db.query<[number[]]>("SELECT VALUE n FROM loop ORDER BY n").json();
	expect(ns).toEqual([1, 2, 3]);
	await db.close();
});

test("LET binds a query-local variable usable downstream", async () => {
	const { db } = await rootClient(server);
	const [, , out] = await db
		.query<[unknown, unknown, number]>("LET $x = 21; LET $y = $x * 2; RETURN $y")
		.json();
	expect(out).toBe(42);
	await db.close();
});

test("IF/ELSE evaluates as an expression", async () => {
	const { db } = await rootClient(server);
	const [r] = await db.query<[string]>("RETURN IF 3 > 2 THEN 'yes' ELSE 'no' END").json();
	expect(r).toBe("yes");
	await db.close();
});

test("built-in functions and method chaining", async () => {
	const { db } = await rootClient(server);
	const [res] = await db
		.query<[{ upper: string; len: number; sum: number; rounded: number }]>(
			`RETURN {
				upper: string::uppercase('hi'),
				len: [1,2,3].len(),
				sum: math::sum([1,2,3,4]),
				rounded: math::round(3.7)
			}`,
		)
		.json();
	expect(res).toEqual({ upper: "HI", len: 3, sum: 10, rounded: 4 });
	await db.close();
});

test("type casts coerce values", async () => {
	const { db } = await rootClient(server);
	const [res] = await db
		.query<[{ i: number; s: string; b: boolean }]>(
			"RETURN { i: <int> '42', s: <string> 7, b: <bool> 'true' }",
		)
		.json();
	expect(res.i).toBe(42);
	expect(res.s).toBe("7");
	expect(res.b).toBe(true);
	await db.close();
});

test("FETCH inlines a linked record", async () => {
	const { db } = await rootClient(server);
	await db.query(`
		CREATE author:1 SET name = 'Ada';
		CREATE book:1 SET title = 'Notes', author = author:1;
	`);
	const [rows] = await db
		.query<[Array<{ author: { name: string } }>]>("SELECT * FROM book:1 FETCH author")
		.json();
	expect(rows[0].author.name).toBe("Ada");
	await db.close();
});

test("a failing statement does not abort earlier successes outside a transaction", async () => {
	const { db } = await rootClient(server);
	// duplicate id in the middle; the first CREATE persists, the call rejects.
	const boom = db
		.query("CREATE dup:1 SET n = 1; CREATE dup:1 SET n = 2;")
		.collect()
		.then(() => null)
		.catch((e) => e as Error);
	expect(await boom).toBeInstanceOf(Error);
	const [rows] = await db.query<[Array<{ n: number }>]>("SELECT VALUE n FROM dup").json();
	expect(rows).toEqual([1]);
	await db.close();
});
