#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! The exact prefilter tier scores allow-list members from the vectors the
//! HNSW or DiskANN index stores whenever nothing has to be evaluated per
//! record. These tests pin that it answers exactly what the record-scoring
//! path and a brute-force scan answer, over compacted and uncompacted index
//! state.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio_util::sync::CancellationToken;

async fn run(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<Value> {
	let mut results = ds.execute(sql, session, None).await?;
	Ok(results.pop().expect("one statement result").result?)
}

/// A deterministic 6-dimensional vector with small integer components, so
/// several records share a vector and distance ties occur.
fn vector(seed: u64) -> String {
	let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
	let mut components = Vec::with_capacity(6);
	for _ in 0..6 {
		x ^= x >> 29;
		x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
		components.push(format!("{}f", (x >> 33) % 5));
	}
	format!("[{}]", components.join(", "))
}

/// Runs the same KNN, filtered on `category`, three ways and asserts identical rows, in
/// identical order, with identical distances:
///
/// * the covered filter alone — the exact tier scoring from index vectors;
/// * the covered filter plus an uncovered conjunct every row satisfies — the exact tier scoring
///   from the fetched records, since the conjunct must be evaluated per record;
/// * brute force over the table.
async fn assert_paths_agree(
	ds: &Datastore,
	session: &Session,
	category: &str,
	query: &str,
	k: usize,
) -> anyhow::Result<()> {
	let index = format!(
		"SELECT id, vector::distance::knn() AS d FROM pts
		 WHERE category = '{category}' AND point <|{k},40|> {query} ORDER BY d, id"
	);
	let plan = run(ds, session, &format!("EXPLAIN ANALYZE {index}")).await?;
	let Value::String(plan) = plan else {
		anyhow::bail!("unexpected EXPLAIN result: {plan:?}")
	};
	assert!(plan.contains("prefilter_tier: exact"), "expected the exact tier:\n{plan}");
	let index = run(ds, session, &index).await?.to_sql();
	let records = run(
		ds,
		session,
		&format!(
			"SELECT id, vector::distance::knn() AS d FROM pts
			 WHERE category = '{category}' AND always = true AND point <|{k},40|> {query} ORDER BY d, id"
		),
	)
	.await?
	.to_sql();
	let brute = run(
		ds,
		session,
		&format!(
			"SELECT id, vector::distance::knn() AS d FROM pts
			 WHERE category = '{category}' AND point <|{k},EUCLIDEAN|> {query} ORDER BY d, id"
		),
	)
	.await?
	.to_sql();
	// A write transaction reads the index's doc-ID → elements mapping through
	// itself, next to a write of its own.
	let mut in_txn = ds
		.execute(
			&format!(
				"BEGIN; UPSERT marker:1; SELECT id, vector::distance::knn() AS d FROM pts
				 WHERE category = '{category}' AND point <|{k},40|> {query} ORDER BY d, id; COMMIT;"
			),
			session,
			None,
		)
		.await?;
	// One response per statement: BEGIN, UPSERT, SELECT, COMMIT.
	let in_txn = in_txn.remove(2).result?.to_sql();
	assert_eq!(index, in_txn, "read-only vs write transaction, k = {k}, q = {query}");
	assert_eq!(index, records, "index vectors vs record vectors, k = {k}, q = {query}");
	assert_eq!(index, brute, "index vectors vs brute force, k = {k}, q = {query}");
	if k == 0 || category == NO_CATEGORY {
		assert_eq!(index, "[]", "nothing to offer, k = {k}, category = {category}");
	}
	Ok(())
}

/// A category no record has: its allow-list is empty.
const NO_CATEGORY: &str = "none";

/// Each round also asks for no neighbour and filters on an empty allow-list,
/// which every path answers with no rows.
async fn assert_round(ds: &Datastore, session: &Session, round: u64) -> anyhow::Result<()> {
	for q in 0..4 {
		let query = vector(10_000 + round * 100 + q);
		for k in [0, 1, 7, 500] {
			assert_paths_agree(ds, session, "a", &query, k).await?;
		}
		assert_paths_agree(ds, session, NO_CATEGORY, &query, 7).await?;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_exact_tier_from_index_vectors_matches_record_scoring() -> anyhow::Result<()> {
	exact_tier_matches_record_scoring("HNSW DIMENSION 6 DIST EUCLIDEAN").await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_exact_tier_from_index_vectors_matches_record_scoring() -> anyhow::Result<()> {
	exact_tier_matches_record_scoring("DISKANN DIMENSION 6 DIST EUCLIDEAN TYPE F32").await
}

/// Runs [`assert_paths_agree`] over an index defined by `index`, through a
/// sequence of writes and compactions.
async fn exact_tier_matches_record_scoring(index: &str) -> anyhow::Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		&format!(
			"DEFINE FIELD category ON pts TYPE string;
			 DEFINE INDEX idx_category ON pts FIELDS category;
			 DEFINE INDEX ix_pt ON pts FIELDS point {index};"
		),
	)
	.await?;
	let categories = ["a", "b", "c"];
	let mut inserts = Vec::new();
	for i in 0..300u64 {
		inserts.push(format!(
			"{{ id: pts:{i}, point: {}, category: '{}', always: true }}",
			vector(i),
			categories[(i % 3) as usize]
		));
	}
	// Allow-list members the index holds no vector for: one with no `point`
	// at all and one whose `point` is NULL. Every path skips them.
	inserts.push("{ id: pts:900, category: 'a', always: true }".to_owned());
	inserts.push("{ id: pts:901, point: NULL, category: 'a', always: true }".to_owned());
	run(&ds, &session, &format!("INSERT INTO pts [{}];", inserts.join(", "))).await?;
	let compact = || {
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
	};

	// Every vector is still pending.
	assert_round(&ds, &session, 0).await?;
	compact().await?;
	// Every vector is compacted into the graph.
	assert_round(&ds, &session, 1).await?;

	// Moves between vectors and in and out of the allow-list, deletions and
	// re-creations, queried before and after they are compacted.
	run(
		&ds,
		&session,
		&format!(
			"UPDATE pts:0 SET point = {}; UPDATE pts:3 SET category = 'b';
			 UPDATE pts:4 SET category = 'a'; DELETE pts:6; DELETE pts:9;
			 UPDATE pts:12 SET point = {};",
			vector(1_000),
			vector(1_001),
		),
	)
	.await?;
	assert_round(&ds, &session, 2).await?;
	compact().await?;
	assert_round(&ds, &session, 3).await?;
	run(
		&ds,
		&session,
		&format!(
			"CREATE pts:6 SET point = {}, category = 'a', always = true;
			 UPDATE pts:15 SET point = {};",
			vector(1_002),
			vector(1_003),
		),
	)
	.await?;
	assert_round(&ds, &session, 4).await?;
	compact().await?;
	assert_round(&ds, &session, 5).await?;
	Ok(())
}

/// The index flattens a record's nested vector arrays into one vector, in
/// order. The exact tier reads them the same way whether it scores the
/// members from the index or from their records, and so does brute force.
async fn nested_vector_arrays_score_as_the_index_flattens_them(index: &str) -> anyhow::Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		&format!(
			"DEFINE FIELD category ON pts TYPE string;
			 DEFINE INDEX idx_category ON pts FIELDS category;
			 DEFINE INDEX ix_pt ON pts FIELDS point {index};
			 CREATE pts:1 SET point = [[0f, 0f], [0f, 1f]], category = 'a', always = true;
			 CREATE pts:2 SET point = [5f, 5f, 5f, 5f], category = 'a', always = true;
			 CREATE pts:3 SET point = [[1f], [1f, 1f], [1f]], category = 'a', always = true;"
		),
	)
	.await?;
	let want = "[{ d: 1f, id: pts:1 }, { d: 2f, id: pts:3 }, { d: 10f, id: pts:2 }]";
	for state in ["pending", "compacted"] {
		if state == "compacted" {
			Datastore::index_compaction(
				Arc::clone(&ds),
				Duration::from_secs(1),
				CancellationToken::new(),
			)
			.await?;
		}
		for (path, filter, knn) in [
			("index", "", "3,40"),
			("records", " AND always = true", "3,40"),
			("brute force", "", "3,EUCLIDEAN"),
		] {
			let got = run(
				&ds,
				&session,
				&format!(
					"SELECT id, vector::distance::knn() AS d FROM pts
					 WHERE category = 'a'{filter} AND point <|{knn}|> [0f, 0f, 0f, 0f] ORDER BY d, id"
				),
			)
			.await?
			.to_sql();
			assert_eq!(got, want, "{index}, {state}, {path}");
		}
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_nested_vector_arrays_score_as_the_index_flattens_them() -> anyhow::Result<()> {
	nested_vector_arrays_score_as_the_index_flattens_them("HNSW DIMENSION 4 DIST EUCLIDEAN").await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_nested_vector_arrays_score_as_the_index_flattens_them() -> anyhow::Result<()> {
	nested_vector_arrays_score_as_the_index_flattens_them(
		"DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32",
	)
	.await
}

/// When an uncovered residual (`admitted = true`) filters out a subset of the
/// allow-list, the exact tier scores candidates from the index's stored vectors
/// first and screens records in nearest-first distance order until `k`
/// candidates pass the residual, matching brute force across batch boundaries.
#[tokio::test(flavor = "multi_thread")]
async fn hnsw_exact_tier_with_selective_residual_screens_in_distance_order() -> anyhow::Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		"DEFINE FIELD category ON pts TYPE string;
		 DEFINE INDEX idx_category ON pts FIELDS category;
		 DEFINE INDEX ix_pt ON pts FIELDS point HNSW DIMENSION 6 DIST EUCLIDEAN;",
	)
	.await?;
	let mut inserts = Vec::new();
	for i in 0..120u64 {
		// Only every 5th record passes the uncovered residual (`admitted = true`),
		// forcing `screen_scored_candidates` to step past rejected closer vectors
		// across multiple screening batches.
		let admitted = i % 5 == 0;
		inserts.push(format!(
			"{{ id: pts:{i}, point: {}, category: 'a', admitted: {admitted} }}",
			vector(i),
		));
	}
	run(&ds, &session, &format!("INSERT INTO pts [{}];", inserts.join(", "))).await?;

	for state in ["pending", "compacted"] {
		if state == "compacted" {
			Datastore::index_compaction(
				Arc::clone(&ds),
				Duration::from_secs(1),
				CancellationToken::new(),
			)
			.await?;
		}
		for k in [1usize, 5, 15, 30] {
			let query = vector(42);
			let screened = run(
				&ds,
				&session,
				&format!(
					"SELECT id, vector::distance::knn() AS d FROM pts
					 WHERE category = 'a' AND admitted = true AND point <|{k},40|> {query}
					 ORDER BY d, id"
				),
			)
			.await?
			.to_sql();
			let brute = run(
				&ds,
				&session,
				&format!(
					"SELECT id, vector::distance::knn() AS d FROM pts
					 WHERE category = 'a' AND admitted = true AND point <|{k},EUCLIDEAN|> {query}
					 ORDER BY d, id"
				),
			)
			.await?
			.to_sql();
			assert_eq!(screened, brute, "state = {state}, k = {k}");
		}
	}

	// Explicit regression check when the nearest passing neighbour (`pts:z`)
	// sorts after a farther passing neighbour (`pts:a`) in `RecordId` storage
	// key order: `k = 1` must admit `pts:z`, not `pts:a`.
	run(
		&ds,
		&session,
		"INSERT INTO pts [
			{ id: pts:a, point: [10f, 10f, 10f, 10f, 10f, 10f], category: 'rev', admitted: true },
			{ id: pts:m, point: [5f, 5f, 5f, 5f, 5f, 5f], category: 'rev', admitted: false },
			{ id: pts:z, point: [0f, 0f, 0f, 0f, 0f, 0f], category: 'rev', admitted: true }
		];",
	)
	.await?;
	let rev_top1 = run(
		&ds,
		&session,
		"SELECT VALUE id FROM pts
		 WHERE category = 'rev' AND admitted = true AND point <|1, 40|> [0f, 0f, 0f, 0f, 0f, 0f]",
	)
	.await?
	.to_sql();
	assert_eq!(rev_top1, "[pts:z]");
	let rev_top2 = run(
		&ds,
		&session,
		"SELECT VALUE id FROM pts
		 WHERE category = 'rev' AND admitted = true AND point <|2, 40|> [0f, 0f, 0f, 0f, 0f, 0f]
		 ORDER BY vector::distance::knn() ASC",
	)
	.await?
	.to_sql();
	assert_eq!(rev_top2, "[pts:z, pts:a]");
	Ok(())
}

/// `ORDER BY vector::distance::<metric>(field, vector) LIMIT k` is answered by
/// a KNN scan of the HNSW index on `field` only when the query vector answers
/// one value however often it is evaluated: the scan evaluates it once, where
/// the sort evaluates it per row. A vector that draws entropy keeps the sort.
#[tokio::test]
async fn order_by_distance_moves_to_the_index_only_for_a_stable_vector() -> anyhow::Result<()> {
	let ds = Datastore::new("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		"DEFINE NAMESPACE test; DEFINE DATABASE test;
		 DEFINE INDEX ix_pt ON pts FIELDS point HNSW DIMENSION 6 DIST EUCLIDEAN TYPE F32;",
	)
	.await?;
	let inserts: Vec<String> =
		(0..50u64).map(|i| format!("{{ id: pts:{i}, point: {} }}", vector(i))).collect();
	run(&ds, &session, &format!("INSERT INTO pts [{}];", inserts.join(", "))).await?;

	for (vector, pushed) in [
		("[1f, 1f, 1f, 1f, 1f, 1f]", true),
		("$q", true),
		("[rand::float(), 1f, 1f, 1f, 1f, 1f]", false),
		("array::map([1, 2, 3, 4, 5, 6], |$v| rand::float())", false),
		("<array<float, 6>> $q", true),
	] {
		let plan = run(
			&ds,
			&session,
			&format!(
				"LET $q = [1f, 1f, 1f, 1f, 1f, 1f];
				 EXPLAIN SELECT id FROM pts
				 ORDER BY vector::distance::euclidean(point, {vector}) LIMIT 3"
			),
		)
		.await?;
		let Value::String(plan) = plan else {
			anyhow::bail!("unexpected EXPLAIN result: {plan:?}")
		};
		assert_eq!(plan.contains("KnnScan"), pushed, "`{vector}`:\n{plan}");
	}
	Ok(())
}

/// Whether `ORDER BY vector::distance::euclidean(point, [1f, ...]) LIMIT 3`
/// over `table` is answered by a KNN scan.
async fn distance_order_is_pushed(
	ds: &Datastore,
	session: &Session,
	table: &str,
) -> anyhow::Result<bool> {
	let plan = run(
		ds,
		session,
		&format!(
			"EXPLAIN SELECT id FROM {table}
			 ORDER BY vector::distance::euclidean(point, [1f, 1f, 1f, 1f, 1f, 1f]) LIMIT 3"
		),
	)
	.await?;
	let Value::String(plan) = plan else {
		anyhow::bail!("unexpected EXPLAIN result: {plan:?}")
	};
	Ok(plan.contains("KnnScan"))
}

/// A vector index holds no entry for a record whose vector is `NONE` or
/// `NULL`, and ranks an array of arrays as the vector it flattens to, where
/// `ORDER BY vector::distance::*` raises for both. The index records that it
/// stored one, whether a document write or its build met the record, and from
/// then on the ordering keeps the sort.
async fn order_by_distance_needs_every_record_indexed(index: &str) -> anyhow::Result<()> {
	let ds = Datastore::new("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	run(&ds, &session, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await?;
	let rows = |table: &str| -> String {
		let rows: Vec<String> =
			(0..50u64).map(|i| format!("{{ id: {table}:{i}, point: {} }}", vector(i))).collect();
		format!("INSERT INTO {table} [{}];", rows.join(", "))
	};

	// Written through the index: complete until a record without a vector arrives.
	run(&ds, &session, &format!("DEFINE INDEX ix ON written FIELDS point {index};")).await?;
	run(&ds, &session, &rows("written")).await?;
	assert!(distance_order_is_pushed(&ds, &session, "written").await?);
	run(&ds, &session, "CREATE written:none SET other = 1;").await?;
	assert!(!distance_order_is_pushed(&ds, &session, "written").await?);
	run(&ds, &session, &format!("DEFINE INDEX ix ON nested FIELDS point {index};")).await?;
	run(&ds, &session, &rows("nested")).await?;
	assert!(distance_order_is_pushed(&ds, &session, "nested").await?);
	run(&ds, &session, "CREATE nested:pair SET point = [[1f, 1f, 1f], [1f, 1f, 1f]];").await?;
	assert!(!distance_order_is_pushed(&ds, &session, "nested").await?);

	// Built over existing records: complete only when every one holds a vector.
	run(&ds, &session, &rows("built")).await?;
	run(&ds, &session, &format!("DEFINE INDEX ix ON built FIELDS point {index};")).await?;
	assert!(distance_order_is_pushed(&ds, &session, "built").await?);
	for (table, absent) in [
		("built_none", "other: 1"),
		("built_null", "point: NULL"),
		("built_nested", "point: [[1f, 1f, 1f], [1f, 1f, 1f]]"),
	] {
		run(&ds, &session, &rows(table)).await?;
		run(&ds, &session, &format!("CREATE {table}:absent CONTENT {{ {absent} }};")).await?;
		run(&ds, &session, &format!("DEFINE INDEX ix ON {table} FIELDS point {index};")).await?;
		assert!(!distance_order_is_pushed(&ds, &session, table).await?, "`{table}`");
	}
	Ok(())
}

#[tokio::test]
async fn hnsw_order_by_distance_needs_every_record_indexed() -> anyhow::Result<()> {
	order_by_distance_needs_every_record_indexed("HNSW DIMENSION 6 DIST EUCLIDEAN TYPE F32").await
}

#[tokio::test]
async fn diskann_order_by_distance_needs_every_record_indexed() -> anyhow::Result<()> {
	order_by_distance_needs_every_record_indexed("DISKANN DIMENSION 6 DIST EUCLIDEAN TYPE F32")
		.await
}
