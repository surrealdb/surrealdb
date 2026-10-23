//! Blocking `REBUILD INDEX` on RocksDB, whose optimistic transactions validate
//! a commit against the write history the memtables still retain.
//!
//! A blocking build commits far more writes of its own than that history
//! holds. The statement that started the build must therefore not keep its
//! own transaction open across the build: RocksDB would refuse to validate its
//! commit, because it could no longer tell whether anything conflicted. The
//! datastores here retain a few tens of KiB of history, so a build over a few
//! thousand rows outlasts it many times over.
//!
//! RocksDB validates only a transaction that wrote or took a locked read. A
//! `REBUILD INDEX` writes in its own transaction when it moves an index
//! persisted in an older format onto the current one, so the tests that pin
//! the history limit rebuild such an index (see [`define_legacy_index`]).

use std::sync::Arc;
use std::time::Duration;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::Write;
use temp_dir::TempDir;
use web_time::Instant;

use crate::CommunityComposer;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::kvs::testing::{
	RetryableConflictSite, inject_retryable_conflict, inject_retryable_conflicts,
};
use crate::kvs::{Datastore, QueryRequest};
use crate::types::PublicValue;
use crate::val::TableName;

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

/// Run `define`, a `DEFINE INDEX` of index `ix` on the empty table `t`, then
/// rewrite the stored definition's format version to `0`, simulating an index
/// persisted by a binary that predates the shared doc-ID space.
///
/// The rows written afterwards are indexed through the layout that version
/// selects, and the next `REBUILD INDEX` stamps the current format version
/// in its own transaction.
async fn define_legacy_index(ds: &Datastore, ses: &Session, define: &str, ix: &str) {
	run_one(ds, ses, define).await;
	let txn = ds.transaction(Write).await.unwrap();
	let ns = txn.get_ns_by_name("test", None).await.unwrap().expect("namespace should exist");
	let db =
		txn.get_db_by_name("test", "test", None).await.unwrap().expect("database should exist");
	let tb: TableName = "t".into();
	let def = txn
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index should exist");
	let mut legacy = (*def).clone();
	legacy.format_version = 0;
	txn.put_tb_index(ns.namespace_id, db.database_id, &tb, &legacy).await.unwrap();
	// Rewrite the table definition so cached index lists are refreshed.
	let tb_def = txn.expect_tb(ns.namespace_id, db.database_id, &tb).await.unwrap();
	txn.put_tb("test", "test", &tb_def).await.unwrap();
	txn.commit().await.unwrap();
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
	define_legacy_index(
		&ds,
		&ses,
		&format!(
			"DEFINE INDEX knn_ix ON t FIELDS v DISKANN DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE I8"
		),
		"knn_ix",
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
	define_legacy_index(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v", "ix").await;
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
	define_legacy_index(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v", "ix").await;
	seed_rows(&ds, &ses).await;
	let results = run(&ds, &ses, "BEGIN; REBUILD INDEX ix ON t; COMMIT;").await;
	let commit = results.last().expect("COMMIT reports a result");
	assert!(
		commit.as_ref().is_err_and(|err| err.contains("could not check for conflicts")),
		"a rebuild awaited inside its transaction cannot outlast the history: {results:?}"
	);
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

/// The timeout the outlasted-timeout tests set. Generous, so everything the
/// rebuild does before its commit fits within it however loaded the machine:
/// the build itself is held for as long as the test needs.
const OUTLASTED_TIMEOUT: Duration = Duration::from_secs(2);

/// Run a blocking `REBUILD INDEX` of a b-tree index on a datastore reopened
/// with the given timeouts, its build held at its start so that it outlasts
/// them, asserting that the rebuild reports its commit, and that the build it
/// leaves running finishes once released.
async fn assert_outlasted_rebuild_reports_its_commit(
	transaction_timeout: Option<Duration>,
	query_timeout: Option<Duration>,
) {
	let (dir, ds, ses) = small_history_ds().await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	seed_rows(&ds, &ses).await;
	ds.shutdown().await.unwrap();
	drop(ds);
	let ds = open_small_history_ds(&dir, transaction_timeout, query_timeout).await;
	let held = hold_builds(&ds);
	let results = run(&ds, &ses, "REBUILD INDEX ix ON t").await;
	drop(held);
	let [Err(err)] = results.as_slice() else {
		panic!("the rebuild did not outlast its timeout: {results:?}");
	};
	assert!(
		err.starts_with("The index rebuild was committed, but its build did not finish"),
		"the timeout is reported as an uncommitted statement: {err}"
	);
	let report = settled_build(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "ready", "the build left running did not finish: {report}");
	assert_eq!(report["initial"], ROWS, "ix did not index every row: {report}");
	ds.shutdown().await.unwrap();
}

/// A blocking `REBUILD INDEX` whose build outlasts the transaction timeout
/// reports that its rebuild committed and carries on, not that it did not
/// complete.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_outlasting_the_transaction_timeout_reports_its_commit() {
	assert_outlasted_rebuild_reports_its_commit(Some(OUTLASTED_TIMEOUT), None).await;
}

/// A blocking `REBUILD INDEX` whose build outlasts the query timeout stops
/// waiting at that timeout, and reports its commit as it does at the
/// transaction timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_outlasting_the_query_timeout_reports_its_commit() {
	assert_outlasted_rebuild_reports_its_commit(None, Some(OUTLASTED_TIMEOUT)).await;
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

/// Injected conflicts enough to hold a build at its start for as long as a
/// test needs: the build retries each one after a pause, and carries on once
/// the guard that armed them drops.
const HELD_BUILD_CONFLICTS: usize = 1_000;

/// Hold every build on `ds` at its start until the returned guard drops.
fn hold_builds(ds: &Datastore) -> crate::kvs::testing::RetryableConflictGuard {
	inject_retryable_conflicts(
		RetryableConflictSite::ConcurrentIndexInitialCleanup,
		ds.id(),
		HELD_BUILD_CONFLICTS,
	)
}

/// The format version the committed definition of `ix` on `t` names.
async fn index_format(ds: &Datastore, ix: &str) -> u16 {
	let txn = ds.transaction(Write).await.unwrap();
	let ns = txn.get_ns_by_name("test", None).await.unwrap().expect("namespace should exist");
	let db =
		txn.get_db_by_name("test", "test", None).await.unwrap().expect("database should exist");
	let tb: TableName = "t".into();
	let def = txn
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index should exist");
	txn.cancel().await.unwrap();
	def.format_version
}

/// Wait until the committed definition of `ix` on `t` is at the current
/// format version: a `REBUILD INDEX` of a legacy-format index restamps it, and
/// the committed definition changes only once the rebuild's transaction has
/// committed.
async fn wait_for_rebuild_commit(ds: &Datastore, ix: &str) {
	let deadline = Instant::now() + BUILD_STALL_LIMIT;
	while index_format(ds, ix).await != crate::catalog::INDEX_FORMAT_VERSION {
		assert!(Instant::now() < deadline, "the rebuild of {ix} did not commit");
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}

/// Cancelling a blocking `REBUILD INDEX` while it waits for its build after
/// the commit reports that its rebuild committed, and leaves the build to
/// finish.
#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_blocking_rebuild_after_its_commit_leaves_its_build_running() {
	let (_dir, ds, ses) = small_history_ds().await;
	define_legacy_index(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v", "ix").await;
	seed_rows(&ds, &ses).await;
	// Held, the build is still running whenever the query is cancelled.
	let held = hold_builds(&ds);
	let cancel = CancelHandle::new();
	let rebuild = {
		let (ds, ses, cancel) = (Arc::clone(&ds), ses.clone(), cancel.clone());
		tokio::spawn(async move {
			let request = QueryRequest::new("REBUILD INDEX ix ON t", &ses).with_cancel(cancel);
			ds.run(request).await.unwrap().into_iter().map(|r| r.result).collect::<Vec<_>>()
		})
	};
	// Once its transaction has committed, the statement does nothing but wait
	// for its build.
	wait_for_rebuild_commit(&ds, "ix").await;
	cancel.trip();
	let results = rebuild.await.unwrap();
	drop(held);
	let [Err(err)] = results.as_slice() else {
		panic!("the rebuild was not cancelled while waiting: {results:?}");
	};
	assert!(
		err.to_string().starts_with("The index rebuild was committed, but the query was cancelled"),
		"the cancellation is reported as an uncommitted statement: {err}"
	);
	let report = settled_build(&ds, &ses, "ix").await;
	assert_eq!(report["status"], "ready", "the build left running did not finish: {report}");
	assert_eq!(report["initial"], ROWS, "ix did not index every row: {report}");
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

/// The slow-query log lines emitted on the thread that installed it.
#[derive(Clone, Default)]
struct SlowQueryLines(Arc<std::sync::Mutex<Vec<String>>>);

impl SlowQueryLines {
	fn lines(&self) -> Vec<String> {
		self.0.lock().unwrap().clone()
	}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SlowQueryLines {
	fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
		struct Message(String);
		impl tracing::field::Visit for Message {
			fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
				if field.name() == "message" {
					self.0 = format!("{value:?}");
				}
			}
		}
		let mut message = Message(String::new());
		event.record(&mut message);
		if message.0.starts_with("Slow query detected") {
			self.0.lock().unwrap().push(message.0);
		}
	}
}

/// A blocking `REBUILD INDEX` whose build outlasts the slow-query threshold is
/// slow-logged once, with the time spent waiting for its build after the
/// commit counted as the statement's.
///
/// Runs on a single-threaded runtime, so the log lines of every task are
/// emitted on the thread that collects them.
#[tokio::test]
async fn a_blocking_rebuild_is_slow_logged_once_its_build_has_settled() {
	use tracing_subscriber::layer::SubscriberExt as _;
	const THRESHOLD: Duration = Duration::from_secs(1);
	let lines = SlowQueryLines::default();
	let _subscriber =
		tracing::subscriber::set_default(tracing_subscriber::registry().with(lines.clone()));
	let dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_slow_log(THRESHOLD, Vec::new(), Vec::new())
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	run_one(&ds, &ses, "CREATE t:1 SET v = 1").await;
	// Held past the threshold, the build makes the statement slow, while
	// everything it does before its commit takes far less.
	let held = hold_builds(&ds);
	let rebuild = {
		let (ds, ses) = (Arc::clone(&ds), ses.clone());
		tokio::spawn(async move { run(&ds, &ses, "REBUILD INDEX ix ON t").await })
	};
	tokio::time::sleep(THRESHOLD + Duration::from_millis(200)).await;
	drop(held);
	let results = rebuild.await.unwrap();
	assert!(matches!(results.as_slice(), [Ok(_)]), "the rebuild failed: {results:?}");
	let lines = lines.lines();
	assert_eq!(lines.len(), 1, "the rebuild was not slow-logged exactly once: {lines:?}");
	assert!(
		lines[0].contains("REBUILD INDEX ix ON t"),
		"the slow log names another statement: {lines:?}"
	);
	ds.shutdown().await.unwrap();
}
