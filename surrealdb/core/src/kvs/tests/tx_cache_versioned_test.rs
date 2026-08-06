//! Versioned-read cache-invalidation tests.
//!
//! These exercise MVCC time-travel reads (`all_tb(.., Some(version))`). The
//! memory backend no longer supports versioning after the surrealmx 0.23
//! upgrade, so they run on the surrealkv backend, which retains native
//! versioning.
#![cfg(feature = "kv-surrealkv")]

use surrealdb_kvs::TransactionType::Write;
use temp_dir::TempDir;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::dbs::{Capabilities, Session};
use crate::kvs::Datastore;
use crate::val::TableName;

/// Build a versioned surrealkv datastore in a fresh temporary directory. The
/// returned `TempDir` guard must be kept alive for the datastore's lifetime.
async fn versioned_datastore() -> (TempDir, Datastore) {
	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path(&path)
		.await
		.unwrap();
	(dir, ds)
}

/// Test that a versioned read of tables does not pollute the current-view cache.
///
/// The LRU cache keys do not incorporate a version, so historical reads must
/// bypass the cache entirely. If they wrote into the cache, a subsequent
/// None (current) read would see stale data.
#[tokio::test]
async fn test_versioned_read_does_not_pollute_table_cache() {
	let (_dir, ds) = versioned_datastore().await;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
	ds.execute("DEFINE TABLE my_table", &ses, None).await.unwrap();

	let tx = ds.transaction(Write).await.unwrap();
	let ns_def = tx.get_ns_by_name("test", None).await.unwrap().unwrap();
	let db_def = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let ns = ns_def.namespace_id;
	let db = db_def.database_id;

	let tables = tx.all_tb(ns, db, None).await.unwrap();
	assert_eq!(tables.len(), 1, "Current view should have 1 table");

	let old_tables = tx.all_tb(ns, db, Some(0)).await.unwrap();
	assert_eq!(old_tables.len(), 0, "Historical read at version 0 should be empty");

	let tables_again = tx.all_tb(ns, db, None).await.unwrap();
	assert_eq!(tables_again.len(), 1, "Current view must still have 1 table after versioned read");

	tx.cancel().await.unwrap();
}

/// Test that a versioned read of field definitions does not pollute the current-view cache.
#[tokio::test]
async fn test_versioned_read_does_not_pollute_field_cache() {
	let (_dir, ds) = versioned_datastore().await;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
	ds.execute("DEFINE TABLE my_table", &ses, None).await.unwrap();
	ds.execute("DEFINE FIELD name ON TABLE my_table TYPE string", &ses, None).await.unwrap();

	let tx = ds.transaction(Write).await.unwrap();
	let ns_def = tx.get_ns_by_name("test", None).await.unwrap().unwrap();
	let db_def = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let ns = ns_def.namespace_id;
	let db = db_def.database_id;
	let tb = TableName::from("my_table");

	let fields = tx.all_tb_fields(ns, db, &tb, None).await.unwrap();
	assert_eq!(fields.len(), 1, "Current view should have 1 field");

	let old_fields = tx.all_tb_fields(ns, db, &tb, Some(0)).await.unwrap();
	assert_eq!(old_fields.len(), 0, "Historical read at version 0 should be empty");

	let fields_again = tx.all_tb_fields(ns, db, &tb, None).await.unwrap();
	assert_eq!(fields_again.len(), 1, "Current view must still have 1 field after versioned read");

	tx.cancel().await.unwrap();
}
