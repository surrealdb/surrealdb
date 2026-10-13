#![recursion_limit = "256"]
//! #550: bitmap OR union fusion overflow fallback.
//!
//! Lives in its own integration-test binary (not the shared `it` binary) so
//! the branch-budget env override below is set process-wide before the cnf
//! `LazyLock` statics are first read — they are read-once, so in-process
//! overrides are racy anywhere tests share a binary.

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};

/// With a one-entry branch budget every OR branch overflows its drain, so a
/// fused OR plan must abandon the bitmap at execute time and stream the
/// `UnionIndexScan` fallback instead: EXPLAIN ANALYZE reports
/// `union_tier: fallback` on the fused plan, and the results are identical to
/// the bitmap path's — no rows dropped, no error, one emission per record
/// even when both branches match it.
#[tokio::test(flavor = "multi_thread")]
async fn bitmap_or_overflow_falls_back_to_streaming_union() -> anyhow::Result<()> {
	// SAFETY: set before the first datastore/cnf touch of this process, and
	// no other thread reads the environment at this point.
	unsafe { std::env::set_var("SURREAL_BITMAP_BRANCH_BUDGET", "1") };

	let ds = Datastore::new("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	for sql in [
		"DEFINE FIELD category ON docs TYPE string;
		 DEFINE FIELD flag ON docs TYPE bool;
		 DEFINE INDEX idx_category ON docs FIELDS category;
		 DEFINE INDEX idx_flag ON docs FIELDS flag;",
		"INSERT INTO docs [
			{ id: docs:1, category: 'a', flag: false },
			{ id: docs:2, category: 'a', flag: true },
			{ id: docs:3, category: 'b', flag: true },
			{ id: docs:4, category: 'b', flag: false }
		];",
	] {
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}
	}

	let mut results = ds
		.execute(
			"EXPLAIN ANALYZE SELECT id FROM docs WHERE category = 'a' OR flag = true",
			&session,
			None,
		)
		.await?;
	let plan = match results.remove(0).result? {
		Value::String(plan) => plan,
		other => anyhow::bail!("unexpected EXPLAIN result: {other:?}"),
	};
	// The planner still chose the fused plan; the fallback fired at execute
	// time, not plan time.
	assert!(plan.contains("BitmapOr"), "expected a fused OR plan:\n{plan}");
	assert!(plan.contains("union_tier: fallback"), "expected fallback tier:\n{plan}");

	// The results are unaffected by the fallback: nothing dropped, and
	// docs:2 (matching both branches) is emitted exactly once.
	let mut results = ds
		.execute(
			"RETURN array::sort(SELECT VALUE id FROM docs WHERE category = 'a' OR flag = true);",
			&session,
			None,
		)
		.await?;
	let rows = results.remove(0).result?;
	assert_eq!(
		rows.to_sql(),
		"[docs:1, docs:2, docs:3]",
		"fallback results must match the bitmap plan's"
	);
	Ok(())
}
