//! Blocking `REBUILD INDEX` on SurrealKV.
//!
//! A blocking build commits far more writes of its own than one transaction
//! should carry. The statement that started the build therefore commits its own
//! transaction before the build, and returns only once the index is ready again.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_kvs::TransactionType::Write;
use temp_dir::TempDir;
use web_time::Instant;

use crate::CommunityComposer;
use crate::catalog::INDEX_FORMAT_VERSION;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::kvs::testing::{RetryableConflictSite, inject_retryable_conflict};
use crate::kvs::{Datastore, QueryRequest};
use crate::types::PublicValue;
use crate::val::TableName;

/// Rows in the indexed table.
const ROWS: usize = 2_000;

/// Dimension of the indexed vectors.
const DIMENSION: usize = 16;

/// A SurrealKV datastore with the `test`/`test` namespace and database
/// defined.
///
/// The returned [`TempDir`] owns the store's files: hold it for as long as the
/// datastore is used, and shut the datastore down before it drops.
async fn surrealkv_ds() -> (TempDir, Arc<Datastore>, Session) {
	let dir = TempDir::new().unwrap();
	let ds = open_surrealkv_ds(&dir, None, None).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	(dir, ds, ses)
}

/// Open the SurrealKV datastore stored in `dir`, with statements
/// bounded by `transaction_timeout` and queries by `query_timeout`.
async fn open_surrealkv_ds(
	dir: &TempDir,
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
) -> Arc<Datastore> {
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	Datastore::builder()
		.without_maintenance_tasks()
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

/// Assert that `ix` is queryable, holds all `rows` rows of `t`, and answers a
/// KNN query from the index.
async fn assert_ready_and_answers_knn(ds: &Datastore, ses: &Session, ix: &str, rows: usize) {
	let building = building(ds, ses, ix).await;
	assert_eq!(building["status"], "ready", "{ix} is not ready: {building}");
	assert_eq!(building["initial"], rows, "{ix} did not index every row: {building}");
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

/// A blocking `REBUILD INDEX` of a DiskANN index commits, and returns only
/// once the index is ready again.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_diskann_rebuild_commits() {
	let (_dir, ds, ses) = surrealkv_ds().await;
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
	assert_ready_and_answers_knn(&ds, &ses, "knn_ix", ROWS).await;
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` of a b-tree index commits too.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_btree_rebuild_commits() {
	let (_dir, ds, ses) = surrealkv_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "REBUILD INDEX ix ON t").await;
	let building = building(&ds, &ses, "ix").await;
	assert_eq!(building["status"], "ready", "ix is not ready: {building}");
	assert_eq!(building["initial"], ROWS, "ix did not index every row: {building}");
	ds.shutdown().await.unwrap();
}

/// How long a build may go without changing its build report before
/// [`settled_build`] gives up on it.
const BUILD_STALL_LIMIT: Duration = Duration::from_secs(30);

/// Wait for the build of `ix` on `t` to settle, returning its build report.
///
/// The wait has no overall deadline, since a build's duration depends on the
/// machine running it. It fails only once the build report has not changed
/// for [`BUILD_STALL_LIMIT`]: a running build advances its report as it
/// commits each batch.
async fn settled_build(ds: &Datastore, ses: &Session, ix: &str) -> serde_json::Value {
	let mut last = serde_json::Value::Null;
	let mut changed = Instant::now();
	loop {
		let report = building(ds, ses, ix).await;
		if matches!(report["status"].as_str(), Some("ready" | "aborted" | "error")) {
			return report;
		}
		if report != last {
			last = report;
			changed = Instant::now();
		} else if changed.elapsed() > BUILD_STALL_LIMIT {
			panic!("the build of {ix} made no progress for {BUILD_STALL_LIMIT:?}: {last}");
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

/// Rows enough that a build takes many times the timeouts below.
const MANY_ROWS: usize = 8_000;

/// Run a blocking `REBUILD INDEX` of a b-tree index over `MANY_ROWS` on a
/// datastore reopened with the given timeouts, asserting that the rebuild
/// reports its commit when the build outlasts them, and that the build it
/// leaves running finishes.
async fn assert_outlasted_rebuild_reports_its_commit(
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
) {
	let (dir, ds, ses) = surrealkv_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_row_count(&ds, &ses, MANY_ROWS).await;
	ds.shutdown().await.unwrap();
	drop(ds);
	let ds = open_surrealkv_ds(&dir, transaction_timeout, query_timeout).await;
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
	let (_dir, ds, ses) = surrealkv_ds().await;
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
	let (_dir, ds, ses) = surrealkv_ds().await;
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
	run_one(&ds, &ses, "DELETE t:2").await;
	run_one(&ds, &ses, "REBUILD INDEX u_ix ON t").await;
	let report = building(&ds, &ses, "u_ix").await;
	assert_eq!(report["status"], "ready", "the retried rebuild is not ready: {report}");
	assert_eq!(report["initial"], 1, "u_ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// Cancelling a blocking `REBUILD INDEX` while it waits for its build after
/// the commit reports that its rebuild committed, and leaves the build to
/// finish.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_blocking_rebuild_after_its_commit_leaves_its_build_running() {
	let (_dir, ds, ses) = surrealkv_ds().await;
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
	let (_dir, ds, ses) = surrealkv_ds().await;
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

/// Stamp the stored definition of `ix` on `t` with `format_version`, as a
/// binary that wrote that format would have left it, and bump the table so
/// cached index lists pick the change up.
///
/// An index stamped at format 0 reads and writes its private, legacy doc-ID
/// layout, which the current format's shared doc-ID layout does not share.
async fn stamp_index_format(ds: &Datastore, ix: &str, format_version: u16) {
	let tx = ds.transaction(Write).await.unwrap();
	let ns = tx.get_ns_by_name("test", None).await.unwrap().expect("namespace exists");
	let db = tx.get_db_by_name("test", "test", None).await.unwrap().expect("database exists");
	let tb: TableName = "t".into();
	let def = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index exists");
	let mut stamped = (*def).clone();
	stamped.format_version = format_version;
	tx.put_tb_index(ns.namespace_id, db.database_id, &tb, &stamped).await.unwrap();
	let table = tx.expect_tb(ns.namespace_id, db.database_id, &tb).await.unwrap();
	tx.put_tb("test", "test", &table).await.unwrap();
	tx.commit().await.unwrap();
}

/// The format version the committed definition of `ix` on `t` names.
async fn index_format(ds: &Datastore, ix: &str) -> u16 {
	let tx = ds.transaction(Write).await.unwrap();
	let ns = tx.get_ns_by_name("test", None).await.unwrap().expect("namespace exists");
	let db = tx.get_db_by_name("test", "test", None).await.unwrap().expect("database exists");
	let tb: TableName = "t".into();
	let def = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index exists");
	tx.cancel().await.unwrap();
	def.format_version
}

/// The rows the KNN query of [`assert_ready_and_answers_knn`] returns, or the
/// error it fails with.
async fn knn_rows(ds: &Datastore, ses: &Session) -> Result<serde_json::Value, String> {
	let query = format!(
		"SELECT id, vector::distance::knn() AS d FROM t \
		 WHERE v <|3,40|> array::concat([5], array::repeat(0, {})) ORDER BY d",
		DIMENSION - 1
	);
	let mut results = run(ds, ses, &query).await;
	assert_eq!(results.len(), 1, "expected one result for the KNN query");
	results.remove(0).map(PublicValue::into_json_value)
}

/// Run `sql`, a blocking `REBUILD INDEX` of a DiskANN index on the legacy
/// doc-ID layout inside a transaction.
///
/// A rebuild that moves the index's format stages its generation in the
/// transaction and builds once the transaction has committed, so the build
/// never holds the transaction open. The commit succeeds and stamps the definition at the current
/// format; once the build settles, the index is `ready` with every row and answers KNN through the
/// index.
async fn assert_legacy_rebuild_commits_and_answers_through_the_index(sql: &str) {
	let (_dir, ds, ses) = surrealkv_ds().await;
	run_one(
		&ds,
		&ses,
		&format!(
			"DEFINE INDEX knn_ix ON t FIELDS v DISKANN DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE I8"
		),
	)
	.await;
	stamp_index_format(&ds, "knn_ix", 0).await;
	seed_rows(&ds, &ses).await;
	let found = knn_rows(&ds, &ses).await;
	assert_eq!(
		found.as_ref().ok().and_then(|rows| rows.as_array()).map(Vec::len),
		Some(3),
		"the legacy index does not answer KNN before the rebuild: {found:?}"
	);
	let results = run(&ds, &ses, sql).await;
	assert!(
		results.iter().all(Result::is_ok),
		"the rebuild's transaction did not commit: {results:?}"
	);
	assert_eq!(
		index_format(&ds, "knn_ix").await,
		INDEX_FORMAT_VERSION,
		"the committed rebuild did not stamp the current format"
	);
	// The commit publishes the staged generation, so the report is that
	// generation's from here on, and settles once its build does.
	let report = settled_build(&ds, &ses, "knn_ix").await;
	assert_eq!(report["status"], "ready", "the committed rebuild did not finish: {report}");
	assert_ready_and_answers_knn(&ds, &ses, "knn_ix", ROWS).await;
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` of a legacy-layout index inside `BEGIN ...
/// COMMIT` commits, and the rebuilt index answers KNN through the index.
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_rebuild_in_a_transaction_commits_and_answers_through_the_index() {
	assert_legacy_rebuild_commits_and_answers_through_the_index(
		"BEGIN; REBUILD INDEX knn_ix ON t; COMMIT;",
	)
	.await;
}

/// A blocking `REBUILD INDEX` of a legacy-layout index nested in a block
/// commits, and the rebuilt index answers KNN through the index.
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_rebuild_nested_in_a_block_commits_and_answers_through_the_index() {
	assert_legacy_rebuild_commits_and_answers_through_the_index("{ REBUILD INDEX knn_ix ON t; };")
		.await;
}
