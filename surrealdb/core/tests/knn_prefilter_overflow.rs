#![recursion_limit = "256"]
//! #548: KNN prefilter overflow fallback.
//!
//! Lives in its own integration-test binary (not the shared `it` binary) so
//! the branch-budget env override below is set process-wide before the cnf
//! `LazyLock` statics are first read — they are read-once, so in-process
//! overrides are racy anywhere tests share a binary.

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};

/// With a one-entry branch budget every real allow-list branch overflows, so
/// a planned prefilter must fall back at execute time to the pre-#548 path:
/// the full residual is evaluated in-traversal (the `fetched:` counter
/// proves it ran), EXPLAIN ANALYZE reports `prefilter_tier: fallback`, and
/// the results are identical to the covered plan's.
#[tokio::test(flavor = "multi_thread")]
async fn knn_prefilter_overflow_falls_back_to_in_traversal_filtering() -> anyhow::Result<()> {
	// SAFETY: set before the first datastore/cnf touch of this process, and
	// no other thread reads the environment at this point.
	unsafe { std::env::set_var("SURREAL_BITMAP_BRANCH_BUDGET", "1") };

	let ds = Datastore::new("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	for sql in [
		"DEFINE FIELD category ON pts TYPE string;
		 DEFINE INDEX idx_category ON pts FIELDS category;
		 DEFINE INDEX hn_pt ON pts FIELDS point HNSW DIMENSION 1;",
		"INSERT INTO pts [
			{ id: pts:1, point: [ 10f ], category: 'a' },
			{ id: pts:2, point: [ 20f ], category: 'b' },
			{ id: pts:3, point: [ 30f ], category: 'a' },
			{ id: pts:4, point: [ 40f ], category: 'a' }
		];",
	] {
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}
	}

	let mut results = ds
		.execute(
			"EXPLAIN ANALYZE SELECT id FROM pts WHERE category = 'a' AND point <|2,40|> [0f]",
			&session,
			None,
		)
		.await?;
	let plan = match results.remove(0).result? {
		Value::String(plan) => plan,
		other => anyhow::bail!("unexpected EXPLAIN result: {other:?}"),
	};
	assert!(plan.contains("prefilter_tier: fallback"), "expected fallback tier:\n{plan}");
	// The fallback pushed the full residual into the traversal, which
	// fetched candidate records to evaluate it.
	assert!(plan.contains("fetched:"), "expected in-traversal fetches:\n{plan}");

	// The results are unaffected by the fallback.
	let mut results = ds
		.execute(
			"SELECT id, vector::distance::knn() AS d FROM pts
			 WHERE category = 'a' AND point <|2,40|> [0f] ORDER BY d",
			&session,
			None,
		)
		.await?;
	let rows = results.remove(0).result?;
	assert_eq!(
		rows.to_sql(),
		"[{ d: 10f, id: pts:1 }, { d: 30f, id: pts:3 }]",
		"fallback results must match the covered plan's"
	);
	Ok(())
}
