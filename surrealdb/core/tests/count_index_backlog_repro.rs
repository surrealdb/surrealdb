//! Reproduction: COUNT-index read latency scales with the un-compacted
//! `!iu` delta backlog, independently of how many records the table holds.
//!
//! A COUNT index stores one signed delta key per counted mutation
//! (`IndexCountKey`, keyed by `Uuid::now_v7()`), and the read path
//! (`IndexCountThingIterator::next_count`) sums *every* outstanding delta on
//! each read. Compaction folds them into a baseline.
//!
//! An embedded `Datastore` does not spawn the background compaction task, so
//! this reproduces the steady state whenever compaction falls behind the write
//! rate.
//!
//! The record count is held constant and only the delta backlog is varied, by
//! churning records (each CREATE writes a `+1` delta and each DELETE a `-1`
//! delta, so a create/delete pair leaves the table size unchanged but adds two
//! deltas). This separates "cost of scanning deltas" from "cost of scanning
//! records".
//!
//! "mutations" counts *counted mutations*, not the `!iu` keys they produce.
//! Those are equal only while a counted mutation writes its own entry; once
//! deltas are aggregated per transaction, one statement writes one entry
//! regardless. The key count itself is asserted separately in
//! `count_index_writes_one_delta_per_transaction`.
//!
//! The reproducible signal is the *pre-compaction* column, which scales
//! linearly with the backlog (~1us per outstanding delta) on every run. The
//! post-compaction column is printed for context only and is noisy across runs
//! (observed anywhere from ~0.1ms to ~22ms for the same 100k-delta case), so do
//! not read a residual cost into it without confirming on a disk-backed
//! backend. Nothing is asserted on the timings; the assertions cover only that
//! the count stays correct as the backlog grows and after compaction.

#![cfg(feature = "kv-mem")]
// Matches the sibling integration tests (`create.rs`, `define.rs`, `update.rs`,
// …): CI runs clippy with `-D warnings`, which promotes the workspace's
// `unwrap_used = "warn"` to an error, and unwrapping is the house style for test
// setup where a failure should just fail the test.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio_util::sync::CancellationToken;

async fn run(ds: &Datastore, session: &Session, sql: &str) {
	for res in ds.execute(sql, session, None).await.unwrap() {
		res.result.unwrap();
	}
}

async fn query_dbg(ds: &Datastore, session: &Session, sql: &str) -> String {
	let mut res = ds.execute(sql, session, None).await.unwrap();
	format!("{:?}", res.remove(0).result.unwrap())
}

/// Time `SELECT count()`, returning (count, micros).
async fn count_latency(ds: &Datastore, session: &Session) -> (usize, u128) {
	let _ = ds.execute("SELECT count() FROM item GROUP ALL", session, None).await.unwrap();
	let start = Instant::now();
	let mut res = ds.execute("SELECT count() FROM item GROUP ALL", session, None).await.unwrap();
	let micros = start.elapsed().as_micros();
	let rendered = format!("{:?}", res.remove(0).result.unwrap());
	let n: usize = rendered
		.split("Int(")
		.nth(1)
		.and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
		.and_then(|s| s.parse().ok())
		.unwrap_or_else(|| panic!("could not parse count from {rendered}"));
	(n, micros)
}

async fn compact(ds: &Arc<Datastore>) -> u128 {
	let start = Instant::now();
	Datastore::index_compaction(Arc::clone(ds), Duration::from_secs(5), CancellationToken::new())
		.await
		.unwrap();
	start.elapsed().as_micros()
}

async fn new_ds() -> (Arc<Datastore>, Session) {
	let ds = Arc::new(Datastore::new("memory").await.unwrap());
	let session = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&session,
		"DEFINE NAMESPACE test; DEFINE DATABASE test;
         DEFINE TABLE item SCHEMALESS; DEFINE INDEX idx_total ON item COUNT;",
	)
	.await;
	(ds, session)
}

/// Confirm the COUNT index actually serves the query being timed.
#[tokio::test(flavor = "multi_thread")]
async fn count_query_uses_the_count_index() {
	let (ds, session) = new_ds().await;
	run(&ds, &session, "CREATE |item:1..=100| RETURN NONE").await;
	let plan = query_dbg(&ds, &session, "SELECT count() FROM item GROUP ALL EXPLAIN").await;
	println!("\n  EXPLAIN: {plan}\n");
	assert!(
		plan.to_lowercase().contains("count"),
		"expected the plan to reference the count index, got: {plan}"
	);
}

/// Records held constant, delta backlog varied.
#[tokio::test(flavor = "multi_thread")]
async fn count_read_cost_tracks_delta_backlog_not_record_count() {
	const BASE: usize = 5_000;

	println!("\n  Table held at {BASE} records; only the delta backlog varies.");
	println!("\n  churn pairs | mutations | records | count() latency | after compaction");
	println!("  ------------+-----------+---------+-----------------+-----------------");

	for pairs in [0usize, 10_000, 25_000, 50_000] {
		let (ds, session) = new_ds().await;
		run(&ds, &session, &format!("CREATE |item:1..={BASE}| RETURN NONE")).await;
		// Fold the base records away so the only backlog is the churn below.
		compact(&ds).await;

		// Churn: each pair adds a +1 and a -1 delta, net zero records.
		if pairs > 0 {
			let from = BASE + 1;
			let to = BASE + pairs;
			run(&ds, &session, &format!("CREATE |item:{from}..={to}| RETURN NONE")).await;
			run(&ds, &session, &format!("DELETE item:{from}..={to} RETURN NONE")).await;
		}

		let (n, micros) = count_latency(&ds, &session).await;
		assert_eq!(n, BASE, "record count must be unchanged by churn");

		compact(&ds).await;
		let (n2, after) = count_latency(&ds, &session).await;
		assert_eq!(n2, BASE, "count must stay correct after compaction");

		println!("  {pairs:>11} | {:>9} | {BASE:>7} | {micros:>12} us | {after:>13} us", pairs * 2);
	}
}

/// Steady small commits: the shape a live workload actually has.
///
/// Per-transaction aggregation alone does not help here — each commit already
/// contributes a single entry — so this isolates whether the delta log is
/// bounded. Without a bound the log grows one entry per commit and every
/// `count()` sums all of them; with one, read cost stops tracking commit count.
/// No background compaction runs, which is the state an instance reaches
/// whenever compaction falls behind the write rate.
#[tokio::test(flavor = "multi_thread")]
async fn count_read_cost_is_flat_across_many_small_transactions() {
	println!("\n  commits | records | count() latency");
	println!("  --------+---------+----------------");

	for commits in [100usize, 500, 2_000] {
		let (ds, session) = new_ds().await;
		// Each statement is its own transaction.
		for i in 1..=commits {
			run(&ds, &session, &format!("CREATE item:{i} RETURN NONE")).await;
		}
		let (n, micros) = count_latency(&ds, &session).await;
		assert_eq!(n, commits, "count must stay exact across many commits");
		println!("  {commits:>7} | {commits:>7} | {micros:>12} us");
	}
}
