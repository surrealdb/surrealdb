//! Blocking `REBUILD INDEX` on RocksDB, whose optimistic transactions validate
//! a commit against the write history the memtables still retain.
//!
//! A blocking build commits far more writes of its own than that history
//! holds. The statement that started the build must therefore not keep its
//! own transaction open across the build: RocksDB would refuse to validate its
//! commit, because it could no longer tell whether anything conflicted. The
//! datastores here retain a few tens of KiB of history, so a build over a few
//! thousand rows outlasts it many times over.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::Write;
use temp_dir::TempDir;

use crate::CommunityComposer;
use crate::catalog::providers::CatalogProvider;
use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::kvs::testing::{RetryableConflictSite, inject_retryable_conflict};
use crate::kvs::{Datastore, QueryRequest};
use crate::types::PublicValue;

/// Rows in the indexed table.
const ROWS: usize = 2_000;

/// Dimension of the indexed vectors.
const DIMENSION: usize = 16;

/// A RocksDB datastore whose memtables retain a few tens of KiB of write
/// history, with the `test`/`test` namespace and database defined.
///
/// The returned [`TempDir`] owns the store's files: hold it for as long as the
/// datastore is used, and shut the datastore down before it drops.
async fn small_history_ds() -> (TempDir, Arc<Datastore>, Session) {
	let dir = TempDir::new().unwrap();
	let ds = open_small_history_ds(&dir, None, None).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	(dir, ds, ses)
}

/// Open the small-history RocksDB datastore stored in `dir`, with statements
/// bounded by `transaction_timeout` and queries by `query_timeout`.
async fn open_small_history_ds(
	dir: &TempDir,
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
) -> Arc<Datastore> {
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	let config = ConfigMap::empty()
		.with_key_value("rocksdb_write_buffer_size", "65536")
		.with_key_value("rocksdb_max_write_buffer_number", "2")
		.with_key_value("rocksdb_min_write_buffer_number_to_merge", "1");
	Datastore::builder()
		.without_maintenance_tasks()
		.with_config(config)
		.with_transaction_timeout(transaction_timeout)
		.with_query_timeout(query_timeout)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap()
}

/// Run `sql`, returning each statement's result.
async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Result<PublicValue, String>> {
	ds.execute(sql, ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|r| r.result.map_err(|e| e.to_string()))
		.collect()
}

/// Run a single statement that must succeed, returning its value.
async fn run_one(ds: &Datastore, ses: &Session, sql: &str) -> PublicValue {
	let mut results = run(ds, ses, sql).await;
	assert_eq!(results.len(), 1, "expected one result for `{sql}`");
	match results.remove(0) {
		Ok(value) => value,
		Err(err) => panic!("`{sql}` failed: {err}"),
	}
}

/// `ROWS` records in `t`, record `t:i` holding an `I8` vector whose first
/// component is `i % 128`, so the nearest neighbour of `[k, 0, ...]` is `t:k`.
async fn seed_rows(ds: &Datastore, ses: &Session) {
	seed_row_count(ds, ses, ROWS).await;
}

/// `rows` records in `t`, shaped as [`seed_rows`] shapes them, inserted
/// `ROWS` at a time: `array::range` refuses a range much larger.
async fn seed_row_count(ds: &Datastore, ses: &Session, rows: usize) {
	for from in (0..rows).step_by(ROWS) {
		let to = rows.min(from + ROWS);
		let sql = format!(
			"INSERT INTO t array::map(array::range({from}, {to}), |$i| {{ \
				id: $i, \
				v: array::concat([$i % 128], array::map(array::range(1, {DIMENSION}), |$j| ($i * $j) % 7)) \
			}}) RETURN NONE;"
		);
		run_one(ds, ses, &sql).await;
	}
}

/// The build report `INFO FOR INDEX` gives for `ix` on `t`, as JSON.
async fn building(ds: &Datastore, ses: &Session, ix: &str) -> serde_json::Value {
	let info = run_one(ds, ses, &format!("INFO FOR INDEX {ix} ON t")).await;
	info.into_json_value().get("building").cloned().expect("INFO FOR INDEX reports the build")
}

/// Assert that `ix` is queryable, holds every row of `t`, and answers a KNN
/// query from the index.
async fn assert_ready_and_answers_knn(ds: &Datastore, ses: &Session, ix: &str) {
	let building = building(ds, ses, ix).await;
	assert_eq!(building["status"], "ready", "{ix} is not ready: {building}");
	assert_eq!(building["initial"], ROWS, "{ix} did not index every row: {building}");
	let query = format!(
		"SELECT id, vector::distance::knn() AS d FROM t \
		 WHERE v <|3,40|> array::concat([5], array::repeat(0, {})) ORDER BY d",
		DIMENSION - 1
	);
	let found = run_one(ds, ses, &query).await.into_json_value();
	assert_eq!(found.as_array().map(Vec::len), Some(3), "KNN over {ix} returned {found}");
	let explain = run_one(ds, ses, &format!("{query} EXPLAIN")).await.into_json_value();
	assert!(explain.to_string().contains(ix), "the KNN query does not use {ix}: {explain}");
}

/// A blocking `REBUILD INDEX` of a DiskANN index larger than the retained
/// write history commits, and returns only once the index is ready again.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_diskann_rebuild_outlasting_the_retained_history_commits() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(
		&ds,
		&ses,
		&format!(
			"DEFINE INDEX knn_ix ON t FIELDS v DISKANN DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE I8"
		),
	)
	.await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "REBUILD INDEX knn_ix ON t").await;
	assert_ready_and_answers_knn(&ds, &ses, "knn_ix").await;
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` of a b-tree index larger than the retained write
/// history commits too: the failure is the statement's transaction, not the
/// index kind.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_btree_rebuild_outlasting_the_retained_history_commits() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "REBUILD INDEX ix ON t").await;
	let building = building(&ds, &ses, "ix").await;
	assert_eq!(building["status"], "ready", "ix is not ready: {building}");
	assert_eq!(building["initial"], ROWS, "ix did not index every row: {building}");
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` inside `BEGIN ... COMMIT` still waits inside its
/// transaction, so the whole transaction commits or fails as one: a build that
/// outlasts the retained history fails the commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_inside_a_transaction_waits_inside_it() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_rows(&ds, &ses).await;
	let results = run(&ds, &ses, "BEGIN; REBUILD INDEX ix ON t; COMMIT;").await;
	let commit = results.last().expect("COMMIT reports a result");
	assert!(
		commit.as_ref().is_err_and(|err| err.contains("could not check for conflicts")),
		"a rebuild awaited inside its transaction cannot outlast the history: {results:?}"
	);
	ds.shutdown().await.unwrap();
}

/// Wait for the build of `ix` on `t` to settle, returning its build report.
async fn settled_build(ds: &Datastore, ses: &Session, ix: &str) -> serde_json::Value {
	for _ in 0..600 {
		let report = building(ds, ses, ix).await;
		if matches!(report["status"].as_str(), Some("ready" | "aborted" | "error")) {
			return report;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	panic!("the build of {ix} did not settle");
}

/// Rows enough that a build takes many times the timeouts below.
const MANY_ROWS: usize = 30_000;

/// Run a blocking `REBUILD INDEX` of a b-tree index over `MANY_ROWS` on a
/// datastore reopened with the given timeouts, asserting that the rebuild
/// reports its commit when the build outlasts them, and that the build it
/// leaves running finishes.
async fn assert_outlasted_rebuild_reports_its_commit(
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
) {
	let (dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_row_count(&ds, &ses, MANY_ROWS).await;
	ds.shutdown().await.unwrap();
	drop(ds);
	let ds = open_small_history_ds(&dir, transaction_timeout, query_timeout).await;
	let results = run(&ds, &ses, "REBUILD INDEX ix ON t").await;
	let [Err(err)] = results.as_slice() else {
		panic!("the rebuild did not outlast its timeout: {results:?}");
	};
	assert!(
		err.starts_with("The index rebuild was committed, but its build did not finish"),
		"the timeout is reported as an uncommitted statement: {err}"
	);
	let report = settled_build(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "ready", "the build left running did not finish: {report}");
	assert_eq!(report["initial"], MANY_ROWS, "ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` whose build outlasts the transaction timeout
/// reports that its rebuild committed and carries on, not that it did not
/// complete.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_outlasting_the_transaction_timeout_reports_its_commit() {
	assert_outlasted_rebuild_reports_its_commit(Some(Duration::from_millis(250)), None).await;
}

/// A blocking `REBUILD INDEX` whose build outlasts the query timeout stops
/// waiting at that timeout, and reports its commit as it does at the
/// transaction timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_outlasting_the_query_timeout_reports_its_commit() {
	assert_outlasted_rebuild_reports_its_commit(None, Some(Duration::from_millis(250))).await;
}

/// A blocking `REBUILD INDEX` whose commit fails aborts the build it started,
/// rather than leaving it to publish a layout the definition it rolled back
/// to does not describe, and the next `REBUILD INDEX` succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_whose_commit_fails_aborts_its_build() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_rows(&ds, &ses).await;
	let lost_commit = inject_retryable_conflict(RetryableConflictSite::AwaitedBuildCommit, ds.id());
	let results = run(&ds, &ses, "REBUILD INDEX ix ON t").await;
	drop(lost_commit);
	assert!(
		matches!(results.as_slice(), [Err(err)] if err.contains("injected conflict")),
		"the rebuild's commit was not lost: {results:?}"
	);
	let report = settled_build(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "aborted", "the uncommitted build was not aborted: {report}");
	run_one(&ds, &ses, "REBUILD INDEX ix ON t").await;
	let report = building(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "ready", "ix is not ready: {report}");
	assert_eq!(report["initial"], ROWS, "ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` whose build fails after the commit fails the
/// statement with the build's error and leaves the index `error`, and the next
/// `REBUILD INDEX` retries it.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_whose_build_fails_reports_it_and_can_be_retried() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "CREATE t:1 SET v = 1").await;
	run_one(&ds, &ses, "CREATE t:2 SET v = 1").await;
	// A UNIQUE index over a duplicate: the concurrent build fails without
	// failing its statement, which leaves a definition to rebuild.
	run_one(&ds, &ses, "DEFINE INDEX u_ix ON t FIELDS v UNIQUE CONCURRENTLY").await;
	let report = settled_build(&ds, &ses, "u_ix").await;
	assert_eq!(report["status"], "error", "the duplicate did not fail the build: {report}");
	let results = run(&ds, &ses, "REBUILD INDEX u_ix ON t").await;
	assert!(
		matches!(results.as_slice(), [Err(err)] if err.contains("u_ix")),
		"the failed build did not fail the rebuild: {results:?}"
	);
	let report = building(&ds, &ses, "u_ix").await;
	assert_eq!(report["status"], "error", "the failed rebuild is not reported: {report}");
	run_one(&ds, &ses, "UPDATE t:2 SET v = 2").await;
	run_one(&ds, &ses, "REBUILD INDEX u_ix ON t").await;
	let report = building(&ds, &ses, "u_ix").await;
	assert_eq!(report["status"], "ready", "the retried rebuild is not ready: {report}");
	assert_eq!(report["initial"], 2, "u_ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// Cancelling a blocking `REBUILD INDEX` while it waits for its build after
/// the commit reports that its rebuild committed, and leaves the build to
/// finish.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_blocking_rebuild_after_its_commit_leaves_its_build_running() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_row_count(&ds, &ses, MANY_ROWS).await;
	let cancel = CancelHandle::new();
	let rebuild = {
		let (ds, ses, cancel) = (Arc::clone(&ds), ses.clone(), cancel.clone());
		tokio::spawn(async move {
			let request = QueryRequest::new("REBUILD INDEX ix ON t", &ses).with_cancel(cancel);
			ds.run(request).await.unwrap().into_iter().map(|r| r.result).collect::<Vec<_>>()
		})
	};
	// The build reports progress from its own transactions once it has
	// started, which the statement commits straight after.
	for _ in 0..600 {
		if building(&ds, &ses, "ix").await["status"] != "ready" {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	tokio::time::sleep(Duration::from_millis(200)).await;
	cancel.trip();
	let results = rebuild.await.unwrap();
	let [Err(err)] = results.as_slice() else {
		panic!("the rebuild was not cancelled while waiting: {results:?}");
	};
	assert!(
		err.to_string().starts_with("The index rebuild was committed, but the query was cancelled"),
		"the cancellation is reported as an uncommitted statement: {err}"
	);
	let report = settled_build(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "ready", "the build left running did not finish: {report}");
	assert_eq!(report["initial"], MANY_ROWS, "ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` nested in a block waits for its build inside the
/// statement, so a build that fails fails the block before anything after it
/// runs: only a statement with nothing after it can leave its build for after
/// the commit and keep the statement atomic.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_nested_in_a_block_keeps_the_block_atomic() {
	let (_dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "CREATE t:1 SET v = 1").await;
	run_one(&ds, &ses, "CREATE t:2 SET v = 1").await;
	run_one(&ds, &ses, "DEFINE INDEX u_ix ON t FIELDS v UNIQUE CONCURRENTLY").await;
	settled_build(&ds, &ses, "u_ix").await;
	run_one(&ds, &ses, "DEFINE TABLE marker").await;
	let results = run(&ds, &ses, "{ REBUILD INDEX u_ix ON t; CREATE marker:1; }").await;
	assert!(
		matches!(results.as_slice(), [Err(err)] if err.contains("u_ix")),
		"the failed build did not fail the block: {results:?}"
	);
	let markers = run_one(&ds, &ses, "SELECT * FROM marker").await.into_json_value();
	assert_eq!(markers, serde_json::json!([]), "the block committed past its failed rebuild");
	ds.shutdown().await.unwrap();
}
