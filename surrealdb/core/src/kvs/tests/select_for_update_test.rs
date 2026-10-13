//! Tests for `SELECT ... FOR UPDATE` conflict registration.
//!
//! A `FOR UPDATE` read registers the fetched record key on the storage
//! engine for commit-time conflict detection: a transaction that locked a
//! record with `SELECT ... FOR UPDATE` commits successfully only if no
//! other transaction commits a write to that record after the locking
//! transaction's snapshot was established. These tests drive the same
//! [`Datastore::run`] entry point the RPC session
//! transaction layer uses, with an await-sequenced interleaving so the
//! outcome is deterministic, and run each scenario under both planner
//! strategies so the legacy compute path and the streaming executor path
//! both uphold the contract.

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use temp_dir::TempDir;

use crate::CommunityComposer;
use crate::catalog::providers::CatalogProvider;
use crate::dbs::{NewPlannerStrategy, Session};
use crate::kvs::{Datastore, QueryRequest, Transaction, is_retryable_transaction_conflict};

/// The planner strategies each scenario runs under: the legacy compute
/// executor and the streaming executor.
const STRATEGIES: [NewPlannerStrategy; 2] =
	[NewPlannerStrategy::ComputeOnly, NewPlannerStrategy::BestEffortReadOnlyStatements];

/// A RocksDB datastore with the `test`/`test` namespace and database
/// created, plus an owner session scoped to them and pinned to the given
/// planner strategy.
///
/// The returned [`TempDir`] owns the store's directory: hold it for as
/// long as the datastore is used, and drop it to delete the store.
async fn rocksdb_ds(strategy: NewPlannerStrategy) -> (TempDir, Arc<Datastore>, Session) {
	let dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	let ds =
		Datastore::builder().build_with_factory_path(&path, CommunityComposer()).await.unwrap();
	let mut ses = Session::owner().with_ns("test").with_db("test");
	ses.new_planner_strategy = strategy;
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	(dir, ds, ses)
}

/// Run a query on the given caller-held transaction, asserting every
/// statement succeeded.
async fn run_in_tx(ds: &Datastore, ses: &Session, tx: &Arc<Transaction>, sql: &str) {
	let results =
		ds.run(QueryRequest::new(sql, ses).with_transaction(Arc::clone(tx))).await.unwrap();
	for result in results {
		result.result.unwrap();
	}
}

/// Run an auto-committed query, asserting every statement succeeded.
async fn run(ds: &Datastore, ses: &Session, sql: &str) {
	for result in ds.execute(sql, ses, None).await.unwrap() {
		result.result.unwrap();
	}
}

/// A record read with `SELECT ... FOR UPDATE` conflicts with a concurrent
/// committed write to that record: the locking transaction fails to commit
/// with a retryable transaction conflict, even though its own writes touch
/// a different record.
#[tokio::test]
async fn rocksdb_select_for_update_conflicts_with_concurrent_write() {
	for strategy in STRATEGIES {
		let (_dir, ds, ses) = rocksdb_ds(strategy).await;
		run(&ds, &ses, "CREATE item:one SET value = 1;").await;

		// tx1 takes a locked read of the record.
		let tx1 = Arc::new(ds.transaction(Write).await.unwrap());
		run_in_tx(&ds, &ses, &tx1, "LET $a = SELECT * FROM ONLY item:one FOR UPDATE;").await;

		// tx2 commits a write to the locked record.
		let tx2 = Arc::new(ds.transaction(Write).await.unwrap());
		run_in_tx(&ds, &ses, &tx2, "UPDATE item:one SET value = 2;").await;
		tx2.commit().await.unwrap();

		// tx1 writes a different record, then fails to commit: the locked
		// read of item:one no longer validates against tx1's snapshot.
		run_in_tx(&ds, &ses, &tx1, "CREATE other:one;").await;
		let err = tx1.commit().await.unwrap_err();
		assert!(
			is_retryable_transaction_conflict(&err),
			"[{strategy}] expected a retryable transaction conflict, got: {err}"
		);
	}
}

/// Without a concurrent writer the locked read is inert: the locking
/// transaction commits normally and its writes are visible afterwards.
#[tokio::test]
async fn rocksdb_select_for_update_commits_without_concurrent_write() {
	for strategy in STRATEGIES {
		let (_dir, ds, ses) = rocksdb_ds(strategy).await;
		run(&ds, &ses, "CREATE item:one SET value = 1;").await;

		let tx1 = Arc::new(ds.transaction(Write).await.unwrap());
		run_in_tx(&ds, &ses, &tx1, "LET $a = SELECT * FROM ONLY item:one FOR UPDATE;").await;
		run_in_tx(&ds, &ses, &tx1, "CREATE other:one;").await;
		tx1.commit().await.unwrap();

		let mut results =
			ds.execute("SELECT count() FROM other GROUP ALL;", &ses, None).await.unwrap();
		let value = results.remove(0).result.unwrap();
		assert_eq!(
			value,
			crate::types::PublicValue::Array(surrealdb_types::array![surrealdb_types::object! {
				count: 1
			}]),
			"[{strategy}]"
		);
	}
}

/// A plain `SELECT` (no `FOR UPDATE`) registers no conflict: the reading
/// transaction commits successfully despite a concurrent committed write
/// to the record it read.
#[tokio::test]
async fn rocksdb_plain_select_does_not_conflict_with_concurrent_write() {
	for strategy in STRATEGIES {
		let (_dir, ds, ses) = rocksdb_ds(strategy).await;
		run(&ds, &ses, "CREATE item:one SET value = 1;").await;

		let tx1 = Arc::new(ds.transaction(Write).await.unwrap());
		run_in_tx(&ds, &ses, &tx1, "LET $a = SELECT * FROM ONLY item:one;").await;

		let tx2 = Arc::new(ds.transaction(Write).await.unwrap());
		run_in_tx(&ds, &ses, &tx2, "UPDATE item:one SET value = 2;").await;
		tx2.commit().await.unwrap();

		run_in_tx(&ds, &ses, &tx1, "CREATE other:one;").await;
		tx1.commit().await.unwrap();
	}
}

/// A pushed-down `LIMIT 0` must not skip the locked read: the lock is a
/// side effect of evaluating the FROM target, independent of how many rows
/// the statement returns. A non-foldable function-call source plans as a
/// DynamicScan with the limit pushed into the scan (no outer Limit
/// operator), while a literal or constant-folded source plans as a
/// RecordIdScan below a Limit operator — both plans must register the key,
/// on both executor paths.
#[tokio::test]
async fn rocksdb_select_for_update_limit_zero_still_locks() {
	for strategy in STRATEGIES {
		for (label, lock_stmt) in [
			("dynamic", "LET $a = SELECT * FROM fn::rid() LIMIT 0 FOR UPDATE;"),
			("literal", "LET $a = SELECT * FROM item:one LIMIT 0 FOR UPDATE;"),
		] {
			let (_dir, ds, ses) = rocksdb_ds(strategy).await;
			run(&ds, &ses, "CREATE item:one SET value = 1;").await;
			run(&ds, &ses, "DEFINE FUNCTION fn::rid() { RETURN item:one };").await;

			// tx1 takes a locked read whose row is discarded by LIMIT 0.
			let tx1 = Arc::new(ds.transaction(Write).await.unwrap());
			run_in_tx(&ds, &ses, &tx1, lock_stmt).await;

			// tx2 commits a write to the locked record.
			let tx2 = Arc::new(ds.transaction(Write).await.unwrap());
			run_in_tx(&ds, &ses, &tx2, "UPDATE item:one SET value = 2;").await;
			tx2.commit().await.unwrap();

			// tx1 writes a different record, then must fail to commit.
			run_in_tx(&ds, &ses, &tx1, "CREATE other:one;").await;
			let err = tx1.commit().await.unwrap_err();
			assert!(
				is_retryable_transaction_conflict(&err),
				"[{strategy} {label}] expected a retryable transaction conflict, got: {err}"
			);
		}
	}
}
