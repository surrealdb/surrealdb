//! Blocking index builds on RocksDB, whose optimistic transactions validate a
//! commit against the write history the memtables still retain.
//!
//! A blocking build commits far more writes of its own than that history
//! holds. The statement that started the build must therefore not keep its
//! own transaction open across the build: RocksDB would refuse to validate
//! its commit, because it could no longer tell whether anything conflicted.
//! The datastores here retain a few tens of KiB of history, so a build over a
//! few thousand rows outlasts it many times over.

use std::sync::Arc;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::{Read, Write};
use temp_dir::TempDir;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
};
use crate::catalog::{
	DOC_IDS_FORMAT_VERSION, INDEX_FORMAT_VERSION, IndexDefinition, TableDefinition,
};
use crate::ctx::CancelHandle;
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest};
use crate::types::PublicValue;
use crate::val::TableName;

/// Rows in the indexed table.
const ROWS: usize = 4_000;

/// A RocksDB datastore whose memtables retain a few tens of KiB of write
/// history, with the `test`/`test` namespace and database defined.
///
/// The returned [`TempDir`] owns the store's files: hold it for as long as the
/// datastore is used, and shut the datastore down before it drops.
async fn small_history_ds() -> (TempDir, Arc<Datastore>, Session) {
	let dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	let config = ConfigMap::empty()
		.with_key_value("rocksdb_write_buffer_size", "65536")
		.with_key_value("rocksdb_max_write_buffer_number", "2")
		.with_key_value("rocksdb_min_write_buffer_number_to_merge", "1");
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(config)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	(dir, ds, ses)
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

/// `ROWS` records in `t`, each with a distinct value of `v`.
async fn seed_rows(ds: &Datastore, ses: &Session) {
	let sql = format!(
		"INSERT INTO t array::map(array::range(0, {ROWS}), |$i| {{ \
			id: $i, v: string::concat('value-', <string>$i, '-', string::repeat('x', 64)) \
		}}) RETURN NONE;"
	);
	run_one(ds, ses, &sql).await;
}

/// The build report `INFO FOR INDEX` gives for `ix` on `t`, as JSON.
async fn building(ds: &Datastore, ses: &Session, ix: &str) -> serde_json::Value {
	let info = run_one(ds, ses, &format!("INFO FOR INDEX {ix} ON t")).await;
	info.into_json_value().get("building").cloned().expect("INFO FOR INDEX reports the build")
}

/// Assert that `ix` is queryable and holds every row of `t`.
async fn assert_ready_and_populated(ds: &Datastore, ses: &Session, ix: &str) {
	let building = building(ds, ses, ix).await;
	assert_eq!(building["status"], "ready", "{ix} is not ready: {building}");
	assert_eq!(building["initial"], ROWS, "{ix} did not index every row: {building}");
	let indexed = run_one(
		ds,
		ses,
		&format!("SELECT count() FROM t WITH INDEX {ix} WHERE v >= 'value-' GROUP ALL"),
	)
	.await;
	assert_eq!(indexed.into_json_value(), serde_json::json!([{ "count": ROWS }]));
	let one = run_one(
		ds,
		ses,
		"SELECT VALUE id FROM t WHERE v = string::concat('value-7-', string::repeat('x', 64))",
	)
	.await;
	assert_eq!(one.into_json_value(), serde_json::json!(["t:7"]));
}

/// Wait for a `CONCURRENTLY` build of `ix` to finish.
async fn wait_until_ready(ds: &Datastore, ses: &Session, ix: &str) {
	for _ in 0..500 {
		let building = building(ds, ses, ix).await;
		match building["status"].as_str() {
			Some("ready") => return,
			Some("error") => panic!("{ix} failed to build: {building}"),
			_ => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
		}
	}
	panic!("{ix} did not become ready");
}

/// The definitions of the indexes on `t`.
async fn table_indexes(ds: &Datastore) -> Vec<IndexDefinition> {
	let tx = ds.transaction(Read).await.unwrap();
	let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
	let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
	let indexes = tx.all_tb_indexes(ns, db, &TableName::from("t"), None).await.unwrap();
	tx.cancel().await.unwrap();
	indexes.to_vec()
}

/// A plain `DEFINE INDEX` builds while its statement's transaction is already
/// committed, and still returns only once the index is ready.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_define_index_outlasting_the_retained_history_commits() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v").await;
	assert_ready_and_populated(&ds, &ses, "ix").await;
	ds.shutdown().await.unwrap();
}

/// A blocking build that fails after its definition has committed leaves no
/// index behind, as one that failed inside its transaction does.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_define_index_whose_build_fails_leaves_no_index() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "UPDATE t:1, t:2 SET dup = 'same' RETURN NONE").await;
	let results = run(&ds, &ses, "DEFINE INDEX uniq ON t FIELDS dup UNIQUE").await;
	let [Err(err)] = results.as_slice() else {
		panic!("a UNIQUE index over duplicates must fail to build: {results:?}");
	};
	assert!(err.contains("uniq"), "the error should name the index: {err}");
	assert!(table_indexes(&ds).await.is_empty(), "the failed index must not stay defined");
	ds.shutdown().await.unwrap();
}

/// A `REBUILD INDEX` that moves an index to the current on-disk format writes
/// the new format into the definition, so its statement transaction has
/// writes to validate, and is held apart from the build the same way.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_rebuild_that_restamps_the_format_commits() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v CONCURRENTLY").await;
	wait_until_ready(&ds, &ses, "ix").await;
	// Stamp the definition with an older format, as one written by an earlier
	// release would be, so the rebuild has to restamp it.
	{
		let tx = ds.transaction(Write).await.unwrap();
		let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap();
		let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let tb_name = TableName::from("t");
		let ix = tx.expect_tb_index(ns.namespace_id, db.database_id, &tb_name, "ix").await.unwrap();
		let older = IndexDefinition {
			format_version: DOC_IDS_FORMAT_VERSION,
			..(*ix).clone()
		};
		tx.put_tb_index(ns.namespace_id, db.database_id, &tb_name, &older).await.unwrap();
		let tb = tx.expect_tb(ns.namespace_id, db.database_id, &tb_name).await.unwrap();
		tx.replace_tb(
			&ns.name,
			&db.name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..(*tb).clone()
			},
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
	}
	assert_eq!(table_indexes(&ds).await[0].format_version, DOC_IDS_FORMAT_VERSION);
	run_one(&ds, &ses, "REBUILD INDEX ix ON t").await;
	assert_eq!(table_indexes(&ds).await[0].format_version, INDEX_FORMAT_VERSION);
	assert_ready_and_populated(&ds, &ses, "ix").await;
	ds.shutdown().await.unwrap();
}

/// A build awaited inside a transaction the statement cannot commit early —
/// one of `BEGIN ... COMMIT`, or the swap of a `DEFINE INDEX OVERWRITE` —
/// still holds that transaction across the build. Outlasting the retained
/// history then fails the commit, and the transaction's atomicity holds: the
/// new index is not left behind, and a replaced index stays as it was.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_awaited_inside_its_transaction_fails_atomically_past_the_history() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v CONCURRENTLY").await;
	wait_until_ready(&ds, &ses, "ix").await;

	let results = run(&ds, &ses, "BEGIN; DEFINE INDEX iy ON t FIELDS v; COMMIT;").await;
	assert!(results.last().is_some_and(Result::is_err), "the commit fails: {results:?}");
	let results = run(&ds, &ses, "DEFINE INDEX OVERWRITE ix ON t FIELDS id, v").await;
	assert!(results.last().is_some_and(Result::is_err), "the commit fails: {results:?}");

	let indexes = table_indexes(&ds).await;
	assert_eq!(indexes.len(), 1, "only the original index is defined: {indexes:?}");
	assert_eq!(indexes[0].name, "ix");
	assert_eq!(indexes[0].cols.len(), 1, "the replaced definition is kept");
	assert_ready_and_populated(&ds, &ses, "ix").await;
	ds.shutdown().await.unwrap();
}

/// `OVERWRITE` and `IF NOT EXISTS` of an index that does not exist yet define a
/// new index, which commits before its build as a plain `DEFINE INDEX` does:
/// only replacing an existing index keeps the build inside the transaction.
/// An `IF NOT EXISTS` that finds its index builds nothing, and commits as any
/// other statement does.
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_or_if_not_exists_of_a_new_index_commits_before_its_build() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "DEFINE INDEX OVERWRITE ix ON t FIELDS v").await;
	assert_ready_and_populated(&ds, &ses, "ix").await;
	run_one(&ds, &ses, "DEFINE INDEX IF NOT EXISTS iy ON t FIELDS v").await;
	assert_ready_and_populated(&ds, &ses, "iy").await;
	run_one(&ds, &ses, "DEFINE INDEX IF NOT EXISTS iy ON t FIELDS id, v").await;
	let indexes = table_indexes(&ds).await;
	assert_eq!(indexes.len(), 2, "both new indexes are defined: {indexes:?}");
	assert!(
		indexes.iter().all(|ix| ix.cols.len() == 1),
		"IF NOT EXISTS left the existing definition alone: {indexes:?}"
	);
	ds.shutdown().await.unwrap();
}

/// A blocking `DEFINE INDEX` whose query is cancelled before its transaction
/// commits is cancelled with it: no definition is published and then taken
/// back, so the index is neither defined nor left with a build state.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_define_index_cancelled_before_its_commit_defines_nothing() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	let cancel = CancelHandle::new();
	cancel.trip();
	let request = QueryRequest::new("DEFINE INDEX ix ON t FIELDS v", &ses).with_cancel(cancel);
	let results = ds.run(request).await.unwrap().into_iter().map(|r| r.result).collect::<Vec<_>>();
	assert!(
		matches!(results.as_slice(), [Err(err)] if err.to_string().contains("cancelled")),
		"the statement was not cancelled: {results:?}"
	);
	let indexes = table_indexes(&ds).await;
	assert!(indexes.is_empty(), "a cancelled statement defined an index: {indexes:?}");
	let state = run_one(&ds, &ses, "INFO FOR TABLE t").await.into_json_value();
	assert_eq!(state["indexes"], serde_json::json!({}), "the table lists an index: {state}");
	ds.shutdown().await.unwrap();
}

/// `CONCURRENTLY` never waited for its build, so it never held its
/// transaction across one: it returns before the build finishes, and the
/// build still completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_concurrent_define_index_is_unchanged() {
	let (_dir, ds, ses) = small_history_ds().await;
	seed_rows(&ds, &ses).await;
	run_one(&ds, &ses, "DEFINE INDEX ix ON t FIELDS v CONCURRENTLY").await;
	wait_until_ready(&ds, &ses, "ix").await;
	assert_ready_and_populated(&ds, &ses, "ix").await;
	ds.shutdown().await.unwrap();
}
