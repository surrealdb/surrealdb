import { afterAll, beforeAll, expect, test } from "bun:test";
import { RecordId } from "surrealdb";
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

// The query-result cases at the end run against their own fresh server via
// withServer(); the language-surface tests above share the beforeAll server
// (each with a unique ns/db).
async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

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

test("query bindings: CREATE ... SET name = $name is readable back, and a bound record id resolves", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);

		const [created] = (await db
			.query("CREATE user:john SET name = $name", { name: "John Doe" })
			.json()) as [Array<{ id: string; name: string }>];
		expect(created).toEqual([{ id: "user:john", name: "John Doe" }]);

		const [selected] = (await db.query("SELECT name FROM user:john").json()) as [
			Array<{ name: string }>,
		];
		expect(selected).toEqual([{ name: "John Doe" }]);

		// A record id supplied as a binding resolves as the FROM target.
		const [byRid] = (await db
			.query("SELECT * FROM $record_id", { record_id: new RecordId("user", "john") })
			.json()) as [Array<{ id: string; name: string }>];
		expect(byRid).toEqual([{ id: "user:john", name: "John Doe" }]);

		await db.close();
	});
});

test("SELECT ... ORDER BY DESC honors START and LIMIT", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:john SET name = 'John';
				 CREATE user:zoey SET name = 'Zoey';
				 CREATE user:amos SET name = 'Amos';
				 CREATE user:jane SET name = 'Jane';`,
			)
			.collect();

		const names = async (q: string) => {
			const [rows] = (await db.query(q).json()) as [Array<{ name: string }>];
			return rows.map((r) => r.name);
		};

		expect(await names("SELECT name FROM user ORDER BY name DESC")).toEqual([
			"Zoey",
			"John",
			"Jane",
			"Amos",
		]);
		expect(await names("SELECT name FROM user ORDER BY name DESC START 1 LIMIT 2")).toEqual([
			"John",
			"Jane",
		]);
		expect(await names("SELECT name FROM user ORDER BY name DESC START 1")).toEqual([
			"John",
			"Jane",
			"Amos",
		]);
		// START past the end yields nothing.
		expect(await names("SELECT name FROM user ORDER BY name DESC START 4")).toEqual([]);
		expect(await names("SELECT name FROM user ORDER BY name DESC LIMIT 2")).toEqual([
			"Zoey",
			"John",
		]);

		await db.close();
	});
});

test("record-id range SELECTs honor inclusive and exclusive bounds", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query("CREATE user:amos; CREATE user:jane; CREATE user:john; CREATE user:zoey;")
			.collect();

		const ids = async (q: string) => {
			const [rows] = (await db.query(q).json()) as [Array<{ id: string }>];
			return rows.map((r) => r.id);
		};

		expect(await ids("SELECT id FROM user:..")).toEqual([
			"user:amos",
			"user:jane",
			"user:john",
			"user:zoey",
		]);
		// `..john` is exclusive of the upper bound; `..=john` is inclusive.
		expect(await ids("SELECT id FROM user:..john")).toEqual(["user:amos", "user:jane"]);
		expect(await ids("SELECT id FROM user:..=john")).toEqual([
			"user:amos",
			"user:jane",
			"user:john",
		]);
		// The lower bound is inclusive by default.
		expect(await ids("SELECT id FROM user:jane..")).toEqual([
			"user:jane",
			"user:john",
			"user:zoey",
		]);
		expect(await ids("SELECT id FROM user:jane..john")).toEqual(["user:jane"]);
		expect(await ids("SELECT id FROM user:jane..=john")).toEqual(["user:jane", "user:john"]);

		await db.close();
	});
});

test("FETCH resolves linked record ids into inline objects", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE tag:rs SET name = 'Rust';
				 CREATE tag:go SET name = 'Golang';
				 CREATE tag:js SET name = 'JavaScript';
				 CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
				 CREATE person:jaime SET tags = [tag:js];`,
			)
			.collect();

		const [all] = (await db.query("SELECT * FROM person ORDER BY id FETCH tags").json()) as [
			Array<{ id: string; tags: Array<{ id: string; name: string }> }>,
		];
		expect(all).toEqual([
			{ id: "person:jaime", tags: [{ id: "tag:js", name: "JavaScript" }] },
			{
				id: "person:tobie",
				tags: [
					{ id: "tag:rs", name: "Rust" },
					{ id: "tag:go", name: "Golang" },
					{ id: "tag:js", name: "JavaScript" },
				],
			},
		]);

		// LIMIT 1 (default id order) fetches only the first person, tags inlined.
		const [limited] = (await db.query("SELECT * FROM person LIMIT 1 FETCH tags").json()) as [
			Array<{ id: string; tags: Array<{ id: string; name: string }> }>,
		];
		expect(limited).toEqual([
			{ id: "person:jaime", tags: [{ id: "tag:js", name: "JavaScript" }] },
		]);

		await db.close();
	});
});

test("DELETE ... range returns the deleted rows and leaves the rest", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:amos SET name = 'Amos';
				 CREATE user:jane SET name = 'Jane';
				 CREATE user:john SET name = 'John';
				 CREATE user:zoey SET name = 'Zoey';`,
			)
			.collect();

		// DELETE over a [jane, zoey) range returns the two deleted rows.
		const [deleted] = (await db.query("DELETE user:jane..zoey RETURN BEFORE").json()) as [
			Array<{ id: string; name: string }>,
		];
		expect(deleted).toEqual([
			{ id: "user:jane", name: "Jane" },
			{ id: "user:john", name: "John" },
		]);

		// The rows outside the range survive.
		const [remaining] = (await db.query("SELECT * FROM user ORDER BY id").json()) as [
			Array<{ id: string; name: string }>,
		];
		expect(remaining).toEqual([
			{ id: "user:amos", name: "Amos" },
			{ id: "user:zoey", name: "Zoey" },
		]);

		await db.close();
	});
});

test("typed field coercion: TYPE decimal stores and returns a decimal", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE TABLE foo;
				 DEFINE FIELD bar ON foo TYPE decimal;
				 CREATE foo:x CONTENT { bar: 42.69 };`,
			)
			.collect();

		// The field is stored as a decimal (not a float): type::of reports
		// "decimal" and .json() renders the value as its exact string form.
		const [rows] = (await db
			.query("SELECT bar, type::of(bar) AS t FROM foo:x")
			.json()) as [Array<{ bar: string; t: string }>];
		expect(rows).toEqual([{ bar: "42.69", t: "decimal" }]);

		await db.close();
	});
});

test("UPDATE ... CONTENT replaces the whole record body", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE user:a SET name = 'A', extra = 1;
				 CREATE user:b SET name = 'B', extra = 2;`,
			)
			.collect();

		// CONTENT is a full replace: pre-existing fields not in the new content
		// (here `extra`) are dropped from every affected row.
		const [updated] = (await db.query("UPDATE user CONTENT { name: 'X' } RETURN AFTER").json()) as [
			Array<Record<string, unknown>>,
		];
		expect(updated).toEqual([
			{ id: "user:a", name: "X" },
			{ id: "user:b", name: "X" },
		]);

		await db.close();
	});
});

test("DEFINE FIELD ASSERT rejects a violating write and admits a conforming one", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query("DEFINE FIELD age ON person TYPE int ASSERT $value >= 0")
			.collect();

		// A value that fails the assertion aborts the statement with the
		// field/record/constraint triple.
		const boom = db
			.query("CREATE person:test SET age = -1")
			.collect()
			.then(() => null)
			.catch((e) => e as Error);
		const err = await boom;
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toContain(
			"Found -1 for field `age`, with record `person:test`, but field must conform to: $value >= 0",
		);

		// A conforming value is stored unchanged.
		const [rows] = (await db.query("CREATE person:ok SET age = 5").json()) as [
			Array<{ id: string; age: number }>,
		];
		expect(rows).toEqual([{ id: "person:ok", age: 5 }]);

		await db.close();
	});
});

test("DEFINE FIELD READONLY refuses an update after creation", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE TABLE person SCHEMAFULL;
				 DEFINE FIELD code ON person TYPE string READONLY;
				 DEFINE FIELD name ON person TYPE string;`,
			)
			.collect();

		// The readonly field is settable on the initial CREATE.
		const [created] = (await db
			.query("CREATE person:test SET code = 'abc', name = 'A'")
			.json()) as [Array<{ id: string; code: string; name: string }>];
		expect(created).toEqual([{ id: "person:test", code: "abc", name: "A" }]);

		// Any subsequent write that changes it is refused.
		const boom = db
			.query("UPDATE person:test SET code = 'xyz'")
			.collect()
			.then(() => null)
			.catch((e) => e as Error);
		const err = await boom;
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toContain(
			"Found changed value for field `code`, with record `person:test`, but field is readonly",
		);

		// The stored value is untouched, and a non-readonly field still updates.
		const [after] = (await db
			.query("UPDATE person:test SET name = 'B'")
			.json()) as [Array<{ id: string; code: string; name: string }>];
		expect(after).toEqual([{ id: "person:test", code: "abc", name: "B" }]);

		await db.close();
	});
});

test("DEFINE FIELD VALUE computes the field from other fields on write", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`DEFINE FIELD name ON t TYPE string;
				 DEFINE FIELD slug ON t VALUE string::lowercase(name);`,
			)
			.collect();

		// The computed field ignores any supplied value and derives from `name`.
		const [created] = (await db
			.query("CREATE t:1 SET name = 'Hello World', slug = 'ignored'")
			.json()) as [Array<{ id: string; name: string; slug: string }>];
		expect(created).toEqual([
			{ id: "t:1", name: "Hello World", slug: "hello world" },
		]);

		// It recomputes when the source field changes on update.
		const [updated] = (await db
			.query("UPDATE t:1 SET name = 'GOODBYE'")
			.json()) as [Array<{ id: string; name: string; slug: string }>];
		expect(updated).toEqual([{ id: "t:1", name: "GOODBYE", slug: "goodbye" }]);

		await db.close();
	});
});

test("DEFINE FIELD DEFAULT fills a missing field on CREATE", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query("DEFINE FIELD status ON t DEFAULT 'active';")
			.collect();

		// Omitting the field takes the default.
		const [defaulted] = (await db.query("CREATE t:1 SET n = 1").json()) as [
			Array<{ id: string; n: number; status: string }>,
		];
		expect(defaulted).toEqual([{ id: "t:1", n: 1, status: "active" }]);

		// An explicit value overrides the default.
		const [explicit] = (await db
			.query("CREATE t:2 SET n = 2, status = 'archived'")
			.json()) as [Array<{ id: string; n: number; status: string }>];
		expect(explicit).toEqual([{ id: "t:2", n: 2, status: "archived" }]);

		await db.close();
	});
});

test("multi-hop forward traversal follows the edge chain across nodes", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE person:a SET name = 'A';
				 CREATE person:b SET name = 'B';
				 CREATE person:c SET name = 'C';
				 RELATE person:a->knows->person:b;
				 RELATE person:b->knows->person:c;`,
			)
			.collect();

		// Two hops from a reach c.
		const [reached] = (await db
			.query("SELECT VALUE ->knows->person->knows->person.name FROM person:a")
			.json()) as [string[][]];
		expect(reached).toEqual([["C"]]);

		await db.close();
	});
});

test("reverse traversal follows edges inbound", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE person:a SET name = 'A';
				 CREATE person:b SET name = 'B';
				 CREATE person:c SET name = 'C';
				 RELATE person:a->knows->person:b;
				 RELATE person:b->knows->person:c;`,
			)
			.collect();

		// One reverse hop from c reaches b.
		const [oneBack] = (await db
			.query("SELECT VALUE <-knows<-person.name FROM person:c")
			.json()) as [string[][]];
		expect(oneBack).toEqual([["B"]]);

		// Two reverse hops from c reach a.
		const [twoBack] = (await db
			.query("SELECT VALUE <-knows<-person<-knows<-person.name FROM person:c")
			.json()) as [string[][]];
		expect(twoBack).toEqual([["A"]]);

		await db.close();
	});
});

test("bidirectional traversal reaches neighbours on either side", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE person:a SET name = 'A';
				 CREATE person:b SET name = 'B';
				 CREATE person:c SET name = 'C';
				 RELATE person:a->knows->person:b;
				 RELATE person:b->knows->person:c;`,
			)
			.collect();

		// Each `<->knows` from b matches both its inbound (a->b) and outbound
		// (b->c) edge, and the trailing `<->person` resolves BOTH endpoints of
		// every matched edge — so b itself surfaces twice alongside a and c.
		const [network] = (await db
			.query("SELECT VALUE <->knows<->person.name FROM person:b")
			.json()) as [string[][]];
		expect(network).toHaveLength(1);
		expect([...network[0]].sort()).toEqual(["A", "B", "B", "C"]);

		await db.close();
	});
});
