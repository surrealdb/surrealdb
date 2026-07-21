import { expect, test } from "bun:test";
import { guestClient, rootClient, startServer, type TestServer } from "../src/harness";

// Search subsystem conformance — full-text search (analyzers, the @@ / @N@
// match operators, search::score / search::highlight) and vector KNN over an
// HNSW index (<|K,EF|>, vector::distance::knn / ::euclidean). Each case runs
// against its own fresh server on a unique ns/db; the exhaustive spec lives in
// language-tests/*.surql. Observed shapes (BM25 scores, KNN ordering) are
// pinned here.
//
// The permission section pins that index-backed search paths (@@ FTS and <|K|>
// KNN) are still gated by table and field PERMISSIONS when driven by a real
// record-user session (db.signin on a DEFINE ACCESS TYPE RECORD): a matched row
// the session may not SELECT is filtered out, and a field the session may not
// read is redacted.

async function withServer<T>(fn: (server: TestServer) => Promise<T>): Promise<T> {
	const server = await startServer();
	try {
		return await fn(server);
	} finally {
		await server.stop();
	}
}

// --- Full-text search -------------------------------------------------------

test("@@ match operator filters rows to those the analyzer matches", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE book:1 SET title = 'The Rust Programming Language';
				 CREATE book:2 SET title = 'Learning Go';
				 CREATE book:3 SET title = 'Rust in Action';
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON TABLE book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
			)
			.collect();

		const [rows] = (await db
			.query("SELECT VALUE id FROM book WHERE title @@ 'rust' ORDER BY id")
			.json()) as [string[]];
		expect(rows.map(String)).toEqual(["book:1", "book:3"]);

		// A term absent from every document matches nothing.
		const [none] = (await db
			.query("SELECT VALUE id FROM book WHERE title @@ 'python'")
			.json()) as [string[]];
		expect(none).toEqual([]);

		await db.close();
	});
}, 30000);

test("search::score(n) with the @n@ matchref returns a positive BM25 relevance", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		// "rust" appears in only two of six documents (a minority) so its IDF is
		// positive — see the majority-term test below for why that matters.
		await db
			.query(
				`CREATE book:1 SET title = 'Rust rust rust systems programming';
				 CREATE book:2 SET title = 'A note on Rust';
				 CREATE book:3 SET title = 'Cooking with vegetables';
				 CREATE book:4 SET title = 'Gardening basics';
				 CREATE book:5 SET title = 'History of Rome';
				 CREATE book:6 SET title = 'Baking bread at home';
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON TABLE book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
			)
			.collect();

		const [rows] = (await db
			.query(
				`SELECT VALUE { id: id, score: search::score(1) }
				 FROM book WHERE title @1@ 'rust' ORDER BY score DESC`,
			)
			.json()) as [Array<{ id: string; score: number }>];

		// Only the two documents containing "rust" survive the match filter.
		expect(rows.map((r) => String(r.id))).toEqual(["book:1", "book:2"]);
		// Every surviving row carries a finite positive relevance score, and the
		// document with more term occurrences (book:1) ranks first.
		for (const r of rows) {
			expect(typeof r.score).toBe("number");
			expect(r.score).toBeGreaterThan(0);
		}
		expect(rows[0].score).toBeGreaterThan(rows[1].score);

		await db.close();
	});
}, 30000);

test("BM25 clamps a majority-term IDF to a zero score", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		// The scorer uses the Robertson IDF ln((N-n+0.5)/(n+0.5)); a term present
		// in more than half the documents yields a negative IDF that is clamped
		// to zero. Here "rust" is in two of three documents, so every matched row
		// scores exactly 0 even though the rows still match and are returned.
		await db
			.query(
				`CREATE book:1 SET title = 'Rust rust rust systems programming';
				 CREATE book:2 SET title = 'A note on Rust';
				 CREATE book:3 SET title = 'Cooking with vegetables';
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON TABLE book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
			)
			.collect();

		const [rows] = (await db
			.query(
				`SELECT VALUE { id: id, score: search::score(1) }
				 FROM book WHERE title @1@ 'rust' ORDER BY id`,
			)
			.json()) as [Array<{ id: string; score: number }>];
		expect(rows.map((r) => String(r.id))).toEqual(["book:1", "book:2"]);
		expect(rows.map((r) => r.score)).toEqual([0, 0]);

		await db.close();
	});
}, 30000);

test("search::highlight wraps the matched terms in the surrounding markers", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE book:1 SET title = 'The Rust Programming Language';
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON TABLE book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
			)
			.collect();

		const [rows] = (await db
			.query(
				"SELECT VALUE search::highlight('<b>', '</b>', 1) FROM book WHERE title @1@ 'rust'",
			)
			.json()) as [string[]];
		// The matched term is wrapped in place; the rest of the title is intact.
		expect(rows).toEqual(["The <b>Rust</b> Programming Language"]);

		await db.close();
	});
}, 30000);

test("search::offsets(n) returns matched-term char spans keyed by the indexed field position", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE book:1 SET title = 'The Rust Programming Language and more rust';
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON TABLE book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
			)
			.collect();

		// The value decodes over the wire as an OBJECT, not a flat span array. Its
		// keys are numeric strings for the position of the matched field within the
		// index's FIELDS list — a single-field FULLTEXT index always keys under "0",
		// independent of the @1@ matchref. Each value is the list of {s, e} half-open
		// char offsets of every occurrence of the matched term, ordered by start.
		const [single] = (await db
			.query("SELECT VALUE search::offsets(1) FROM book WHERE title @1@ 'rust'")
			.json()) as [Array<Record<string, Array<{ s: number; e: number }>>>];
		expect(single).toEqual([
			{
				"0": [
					{ s: 4, e: 8 },
					{ s: 39, e: 43 },
				],
			},
		]);

		// A multi-term query collapses every matched term's occurrences into the same
		// field-keyed bucket, merged and sorted by start position.
		const [multi] = (await db
			.query("SELECT VALUE search::offsets(1) FROM book WHERE title @1@ 'rust programming'")
			.json()) as [Array<Record<string, Array<{ s: number; e: number }>>>];
		expect(multi).toEqual([
			{
				"0": [
					{ s: 4, e: 8 },
					{ s: 9, e: 20 },
					{ s: 39, e: 43 },
				],
			},
		]);

		await db.close();
	});
}, 30000);

// --- Vector KNN (HNSW) ------------------------------------------------------

test("HNSW <|K|> returns the K nearest neighbours in ascending-distance order", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE pt:1 SET embedding = [0.1, 0.1, 0.1];
				 CREATE pt:2 SET embedding = [0.2, 0.2, 0.2];
				 CREATE pt:3 SET embedding = [0.9, 0.9, 0.9];
				 CREATE pt:4 SET embedding = [5.0, 5.0, 5.0];
				 DEFINE INDEX vec ON TABLE pt FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;`,
			)
			.collect();

		const [rows] = (await db
			.query(
				`SELECT VALUE { id: id, dist: vector::distance::knn() }
				 FROM pt WHERE embedding <|2,100|> [0.1, 0.1, 0.1] ORDER BY dist`,
			)
			.json()) as [Array<{ id: string; dist: number }>];

		// K=2 nearest to the query point [0.1,0.1,0.1]: pt:1 (itself, dist 0)
		// then pt:2, ordered by ascending distance.
		expect(rows).toHaveLength(2);
		expect(rows.map((r) => String(r.id))).toEqual(["pt:1", "pt:2"]);
		expect(rows[0].dist).toBeCloseTo(0, 5);
		expect(rows[0].dist).toBeLessThanOrEqual(rows[1].dist);

		await db.close();
	});
}, 30000);

test("vector::distance::euclidean reads the exact distance to the query vector", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE pt:1 SET embedding = [0.0, 0.0, 0.0];
				 CREATE pt:2 SET embedding = [3.0, 4.0, 0.0];
				 DEFINE INDEX vec ON TABLE pt FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;`,
			)
			.collect();

		const [rows] = (await db
			.query(
				`SELECT VALUE { id: id, dist: vector::distance::euclidean(embedding, [0.0, 0.0, 0.0]) }
				 FROM pt WHERE embedding <|2,100|> [0.0, 0.0, 0.0] ORDER BY dist`,
			)
			.json()) as [Array<{ id: string; dist: number }>];

		expect(rows.map((r) => String(r.id))).toEqual(["pt:1", "pt:2"]);
		expect(rows[0].dist).toBeCloseTo(0, 5);
		// [3,4,0] is at euclidean distance 5 from the origin.
		expect(rows[1].dist).toBeCloseTo(5, 5);

		await db.close();
	});
}, 30000);

test("KNN <|K|> with a bound-param query vector matches the inline float-array literal", async () => {
	await withServer(async (server) => {
		const { db } = await rootClient(server);
		await db
			.query(
				`CREATE pt:1 SET embedding = [0.1, 0.1, 0.1];
				 CREATE pt:2 SET embedding = [0.2, 0.2, 0.2];
				 CREATE pt:3 SET embedding = [0.9, 0.9, 0.9];
				 CREATE pt:4 SET embedding = [5.0, 5.0, 5.0];
				 DEFINE INDEX vec ON TABLE pt FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;`,
			)
			.collect();

		const KNN = `SELECT VALUE { id: id, dist: vector::distance::knn() }
			 FROM pt WHERE embedding <|2,100|>`;

		// Inline float-array literal drives the KNN operator directly.
		const [inline] = (await db
			.query(`${KNN} [0.1, 0.1, 0.1] ORDER BY dist`)
			.json()) as [Array<{ id: string; dist: number }>];

		// A JS number[] bound as a query parameter resolves to the same query vector,
		// so the K=2 neighbours and their distances are identical to the literal form.
		const [bound] = (await db
			.query(`${KNN} $q ORDER BY dist`, { q: [0.1, 0.1, 0.1] })
			.json()) as [Array<{ id: string; dist: number }>];

		expect(inline.map((r) => String(r.id))).toEqual(["pt:1", "pt:2"]);
		expect(bound.map((r) => String(r.id))).toEqual(inline.map((r) => String(r.id)));
		expect(bound.map((r) => r.dist)).toEqual(inline.map((r) => r.dist));
		expect(bound[0].dist).toBeCloseTo(0, 5);

		await db.close();
	});
}, 30000);

// --- Search results honour record-user permissions (§7) ---------------------

// A DATABASE RECORD access whose users each own only their own `user` row. Rows
// in the searched tables link back to a user via an `owner` field, and the
// tables grant SELECT only WHERE owner = $auth — so the same @@ / <|K|> query
// yields different rows for different signed-in record users.
const RECORD_ACCESS = `
	DEFINE TABLE user SCHEMALESS
		PERMISSIONS FOR select, update, delete WHERE id = $auth FOR create NONE;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNUP ( CREATE user SET email = $email )
		SIGNIN ( SELECT * FROM user WHERE email = $email )
		DURATION FOR TOKEN 15m, FOR SESSION 12h;
`;

test("FTS @@ over an owner-scoped table filters matches to the signed-in record user", async () => {
	await withServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(
			RECORD_ACCESS +
				`DEFINE TABLE book SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
				 DEFINE FIELD owner ON book TYPE record<user>;
				 DEFINE FIELD title ON book TYPE string;
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
		);

		const alice = await guestClient(server, namespace, database);
		await alice.signup({ namespace, database, access: "account", variables: { email: "alice@example.com" } });
		const bob = await guestClient(server, namespace, database);
		await bob.signup({ namespace, database, access: "account", variables: { email: "bob@example.com" } });

		// Two matching books ("rust") owned by different users, plus a non-match
		// owned by alice. Owner is resolved from the user table (record ids do not
		// round-trip as bindable RecordIds through .json()).
		await db.query(`
			LET $alice = (SELECT VALUE id FROM ONLY user WHERE email = 'alice@example.com' LIMIT 1);
			LET $bob = (SELECT VALUE id FROM ONLY user WHERE email = 'bob@example.com' LIMIT 1);
			CREATE book:1 SET title = 'Rust programming', owner = $alice;
			CREATE book:2 SET title = 'Rust in action', owner = $bob;
			CREATE book:3 SET title = 'Cooking food', owner = $alice;
		`);

		// Root has no row filter and sees every matching book.
		const [rootRows] = (await db
			.query("SELECT VALUE id FROM book WHERE title @@ 'rust' ORDER BY id")
			.json()) as [string[]];
		expect(rootRows.map(String)).toEqual(["book:1", "book:2"]);

		// Alice's matching book survives; Bob's equally-matching book:2 is filtered
		// out by the row permission even though the FTS index matched it.
		const [aliceRows] = (await alice
			.query("SELECT VALUE id FROM book WHERE title @@ 'rust' ORDER BY id")
			.json()) as [string[]];
		expect(aliceRows.map(String)).toEqual(["book:1"]);

		await alice.close();
		await bob.close();
		await db.close();
	});
}, 30000);

test("HNSW <|K|> KNN filters the K nearest to the rows the record user may SELECT", async () => {
	await withServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(
			RECORD_ACCESS +
				`DEFINE TABLE pt SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
				 DEFINE FIELD owner ON pt TYPE record<user>;
				 DEFINE FIELD embedding ON pt TYPE array<float>;
				 DEFINE INDEX vec ON pt FIELDS embedding HNSW DIMENSION 3 DIST EUCLIDEAN;`,
		);

		const alice = await guestClient(server, namespace, database);
		await alice.signup({ namespace, database, access: "account", variables: { email: "alice@example.com" } });
		const bob = await guestClient(server, namespace, database);
		await bob.signup({ namespace, database, access: "account", variables: { email: "bob@example.com" } });

		// Bob owns the point nearest the query [0,0,0]; alice owns the next three.
		await db.query(`
			LET $alice = (SELECT VALUE id FROM ONLY user WHERE email = 'alice@example.com' LIMIT 1);
			LET $bob = (SELECT VALUE id FROM ONLY user WHERE email = 'bob@example.com' LIMIT 1);
			CREATE pt:1 SET embedding = [0.1, 0.1, 0.1], owner = $bob;
			CREATE pt:2 SET embedding = [0.2, 0.2, 0.2], owner = $alice;
			CREATE pt:3 SET embedding = [0.3, 0.3, 0.3], owner = $alice;
			CREATE pt:4 SET embedding = [0.4, 0.4, 0.4], owner = $alice;
		`);

		// Root's K=2 nearest to the origin are pt:1 then pt:2.
		const [rootRows] = (await db
			.query("SELECT VALUE id FROM pt WHERE embedding <|2,100|> [0.0, 0.0, 0.0] ORDER BY id")
			.json()) as [string[]];
		expect(rootRows.map(String)).toEqual(["pt:1", "pt:2"]);

		// The KNN operator selects the K nearest candidates and THEN applies the row
		// permission — it does not backfill to K after filtering. So alice's K=2
		// search picks {pt:1, pt:2} and drops bob's closer pt:1, leaving a single
		// row. Critically pt:1 (a closer neighbour alice may not SELECT) never
		// leaks, and pt:3 is not pulled in to refill the K slot.
		const [aliceK2] = (await alice
			.query("SELECT VALUE id FROM pt WHERE embedding <|2,100|> [0.0, 0.0, 0.0] ORDER BY id")
			.json()) as [string[]];
		expect(aliceK2.map(String)).toEqual(["pt:2"]);

		// Widening K to 3 selects {pt:1, pt:2, pt:3}; filtering bob's pt:1 leaves
		// alice's two nearest owned points.
		const [aliceK3] = (await alice
			.query("SELECT VALUE id FROM pt WHERE embedding <|3,100|> [0.0, 0.0, 0.0] ORDER BY id")
			.json()) as [string[]];
		expect(aliceK3.map(String)).toEqual(["pt:2", "pt:3"]);

		await alice.close();
		await bob.close();
		await db.close();
	});
}, 30000);

test("FTS @@ match redacts a FOR select NONE field for the record user", async () => {
	await withServer(async (server) => {
		const { db, namespace, database } = await rootClient(server);
		await db.query(
			RECORD_ACCESS +
				`DEFINE TABLE book SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
				 DEFINE FIELD owner ON book TYPE record<user>;
				 DEFINE FIELD title ON book TYPE string;
				 DEFINE FIELD secret ON book TYPE string PERMISSIONS FOR select NONE;
				 DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
				 DEFINE INDEX ft ON book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;`,
		);

		const alice = await guestClient(server, namespace, database);
		await alice.signup({ namespace, database, access: "account", variables: { email: "alice@example.com" } });

		await db.query(`
			LET $alice = (SELECT VALUE id FROM ONLY user WHERE email = 'alice@example.com' LIMIT 1);
			CREATE book:1 SET title = 'Rust programming', secret = 'top-secret', owner = $alice;
		`);

		// Alice owns and may SELECT the matched row, so it is returned — but `secret`
		// carries FOR select NONE and is omitted from her projection entirely.
		const [aliceRow] = (await alice
			.query("SELECT id, title, secret FROM book WHERE title @@ 'rust'")
			.json()) as [Array<{ id: string; title: string; secret?: string }>];
		expect(aliceRow).toHaveLength(1);
		expect(String(aliceRow[0].id)).toBe("book:1");
		expect(aliceRow[0].title).toBe("Rust programming");
		expect(aliceRow[0].secret).toBeUndefined();

		// Root reads the same matched row with the redacted field intact.
		const [rootRow] = (await db
			.query("SELECT id, title, secret FROM book WHERE title @@ 'rust'")
			.json()) as [Array<{ id: string; secret?: string }>];
		expect(rootRow).toHaveLength(1);
		expect(rootRow[0].secret).toBe("top-secret");

		await alice.close();
		await db.close();
	});
}, 30000);
