#![recursion_limit = "256"]
//! #548: graph-tier sibling verification.
//!
//! Lives in its own integration-test binary so the exact-threshold override
//! below is set process-wide before the cnf `LazyLock` statics are first
//! read — with a zero threshold every prefiltered KNN takes the graph tier
//! regardless of allow-list size, which is otherwise unreachable in a small
//! test.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio_util::sync::CancellationToken;

/// A duplicate-vector element admitted through one qualifying doc must not
/// let a sibling that fails the per-doc filter (here the residual
/// `flag = true`; the same chokepoint serves per-record SELECT permissions)
/// occupy a top-K slot: before the fix the hidden sibling was only dropped by
/// the outer filter after the search, so k=2 returned a single row even
/// though two qualifying rows exist.
#[tokio::test(flavor = "multi_thread")]
async fn graph_tier_verifies_each_sibling_doc() -> anyhow::Result<()> {
	// SAFETY: set before the first datastore/cnf touch of this process, and
	// no other thread reads the environment at this point.
	unsafe { std::env::set_var("SURREAL_KNN_PREFILTER_EXACT_THRESHOLD", "0") };

	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	for sql in [
		"DEFINE FIELD category ON pts TYPE string;
		 DEFINE INDEX idx_category ON pts FIELDS category;
		 DEFINE INDEX hn_pt ON pts FIELDS point HNSW DIMENSION 1;",
		// pts:1 and pts:2 share one vector (one graph element, two docs);
		// both are allow-listed via `category`, but only pts:1 passes the
		// residual. pts:3 qualifies fully and sits a little farther.
		"INSERT INTO pts [
			{ id: pts:1, point: [ 10f ], category: 'a', flag: true },
			{ id: pts:2, point: [ 10f ], category: 'a', flag: false },
			{ id: pts:3, point: [ 20f ], category: 'a', flag: true }
		];",
	] {
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}
	}

	// Compact the pending vector updates into the graph: the sibling leak
	// only exists for compacted multi-doc elements (the pending scan is
	// already per-doc).
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	// Zero exact threshold ⇒ the 3-member allow-list takes the graph tier.
	let mut results = ds
		.execute(
			"EXPLAIN ANALYZE SELECT id FROM pts
			 WHERE category = 'a' AND flag = true AND point <|2,40|> [0f]",
			&session,
			None,
		)
		.await?;
	let plan = match results.remove(0).result? {
		Value::String(plan) => plan,
		other => anyhow::bail!("unexpected EXPLAIN result: {other:?}"),
	};
	assert!(plan.contains("prefilter_tier: graph"), "expected graph tier:\n{plan}");

	// Both qualifying rows must come back: the non-matching sibling of the
	// admitted duplicate-vector element takes no slot.
	let mut results = ds
		.execute(
			"SELECT id, vector::distance::knn() AS d FROM pts
			 WHERE category = 'a' AND flag = true AND point <|2,40|> [0f] ORDER BY d",
			&session,
			None,
		)
		.await?;
	let rows = results.remove(0).result?;
	assert_eq!(
		rows.to_sql(),
		"[{ d: 10f, id: pts:1 }, { d: 20f, id: pts:3 }]",
		"the sibling failing the residual must not consume a top-K slot"
	);
	Ok(())
}
