#![recursion_limit = "256"]
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
