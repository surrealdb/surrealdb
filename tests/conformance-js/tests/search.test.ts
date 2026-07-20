import { expect, test } from "bun:test";
import { rootClient, startServer, type TestServer } from "../src/harness";

// Search subsystem conformance — full-text search (analyzers, the @@ / @N@
// match operators, search::score / search::highlight) and vector KNN over an
// HNSW index (<|K,EF|>, vector::distance::knn / ::euclidean). Each case runs
// against its own fresh server on a unique ns/db; the exhaustive spec lives in
// language-tests/*.surql. Observed shapes (BM25 scores, KNN ordering) are
// pinned here.

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
