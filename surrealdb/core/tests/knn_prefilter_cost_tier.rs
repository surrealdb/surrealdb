#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! The cost model of the KNN prefilter, end to end.
//!
//! Lives in its own integration-test binary so the overrides below are set
//! process-wide before the cnf `LazyLock` statics are first read: a zero exact
//! threshold leaves every pre-filtered search to the cost model, an exact
//! cost of 50 puts its crossover where a small table can show it, and an HNSW
//! boost of 4 widens the walk. With `ef` 40, widened to 160, and `M 8` (16
//! neighbours on the base layer), the exact tier is taken while
//! `members² × 50 ≤ 160 × 16 × nodes`: up to 226 members of a 1,000-node
//! graph. The default cost of 5 would take it up to 715.

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

/// The tier `EXPLAIN ANALYZE` reports for the KNN over `bucket < {below}`,
/// with the width of the walk it reports where the tier walks the graph.
async fn tier(ds: &Datastore, session: &Session, below: u64) -> anyhow::Result<String> {
	let plan = run(
		ds,
		session,
		&format!(
			"EXPLAIN ANALYZE SELECT id FROM pts WHERE bucket < {below} AND point <|5,40|> [500f, 3f]"
		),
	)
	.await?;
	let Value::String(plan) = plan else {
		anyhow::bail!("unexpected EXPLAIN result: {plan:?}")
	};
	let attr = |name: &str| {
		plan.split(&format!("{name}: "))
			.nth(1)
			.and_then(|rest| rest.split([']', ',']).next())
			.map(str::to_owned)
	};
	let tier = attr("prefilter_tier").unwrap_or_else(|| panic!("no prefilter tier in:\n{plan}"));
	Ok(match attr("prefilter_ef") {
		Some(ef) => format!("{tier} {ef}"),
		None => tier,
	})
}

/// The KNN over `bucket < {below}` through the index, and by brute force.
async fn both_ways(ds: &Datastore, session: &Session, below: u64) -> anyhow::Result<[String; 2]> {
	let query = |op: &str| {
		format!(
			"SELECT id, vector::distance::knn() AS d FROM pts
			 WHERE bucket < {below} AND point <|5,{op}|> [500f, 3f] ORDER BY d, id"
		)
	};
	Ok([
		run(ds, session, &query("40")).await?.to_sql(),
		run(ds, session, &query("EUCLIDEAN")).await?.to_sql(),
	])
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cost_model_chooses_the_tier() -> anyhow::Result<()> {
	// SAFETY: set before the first datastore/cnf touch of this process, and
	// no other thread reads the environment at this point.
	unsafe {
		std::env::set_var("SURREAL_KNN_PREFILTER_EXACT_THRESHOLD", "0");
		std::env::set_var("SURREAL_KNN_PREFILTER_EXACT_COST", "50");
		std::env::set_var("SURREAL_KNN_PREFILTER_HNSW_EF_BOOST", "4");
	}

	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		"DEFINE FIELD bucket ON pts TYPE int;
		 DEFINE INDEX idx_bucket ON pts FIELDS bucket;
		 DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN M 8;",
	)
	.await?;
	// 1,000 records, 50 in each of 20 buckets: `bucket < 2` names 100 of
	// them, `bucket < 6` 300.
	let inserts: Vec<String> = (0..1_000u64)
		.map(|i| format!("{{ id: pts:{i}, point: [{i}f, {}f], bucket: {} }}", (i * 7) % 13, i % 20))
		.collect();
	run(&ds, &session, &format!("INSERT INTO pts [{}];", inserts.join(", "))).await?;

	// Every vector is pending and the graph holds no node: the share of the
	// graph an allow-list holds is capped at one, so even 100 members weigh
	// the traversal at `ef × degree` each and stay on the graph tier, whose
	// search over the pending vectors is exact.
	assert_eq!(tier(&ds, &session, 2).await?, "graph 160");
	let [index, brute] = both_ways(&ds, &session, 2).await?;
	assert_eq!(index, brute);

	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;

	// Compacted into a 1,000-node graph: 100 members are cheaper to score
	// exactly than the traversal, and score as brute force does.
	assert_eq!(tier(&ds, &session, 2).await?, "exact");
	let [index, brute] = both_ways(&ds, &session, 2).await?;
	assert_eq!(index, brute);
	// 300 members stay on the graph under the configured cost, where the
	// default cost would score them exactly, and the walk is widened by the
	// configured HNSW boost: 40 × 4.
	assert_eq!(tier(&ds, &session, 6).await?, "graph 160");
	let rows = run(
		&ds,
		&session,
		"SELECT VALUE bucket FROM pts WHERE bucket < 6 AND point <|5,40|> [500f, 3f]",
	)
	.await?;
	let Value::Array(rows) = rows else {
		anyhow::bail!("unexpected result: {rows:?}")
	};
	assert_eq!(rows.len(), 5);
	assert!(
		rows.iter().all(|b| matches!(b, Value::Number(n) if n.to_int().is_some_and(|b| b < 6))),
		"{rows:?}"
	);
	Ok(())
}
