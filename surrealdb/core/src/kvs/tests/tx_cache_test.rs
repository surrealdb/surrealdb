//! Tests for transaction cache invalidation
//!
//! These tests verify that the transaction cache is properly invalidated
//! when entities are added or removed, preventing stale cache data from
//! causing "not found" errors.

// Common test setup helpers
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceDefinition, NamespaceId, TableId};
use crate::dbs::{Capabilities, Session};
use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Write;
use crate::val::TableName;

/// Helper to create a Datastore and write transaction with namespace and database set up
async fn setup_tx_with_ns_db() -> (Datastore, crate::kvs::Transaction, NamespaceId, DatabaseId) {
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
	let tx = ds.transaction(Write, Optimistic).await.unwrap();

	let ns_def = NamespaceDefinition {
		namespace_id: NamespaceId(1),
		name: "test".to_string(),
		comment: None,
	};
	tx.put_ns(ns_def).await.unwrap();

	let db_def = DatabaseDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		name: "test".to_string(),
		strict: false,
		comment: None,
		changefeed: None,
	};
	tx.put_db("test", db_def).await.unwrap();

	(ds, tx, NamespaceId(1), DatabaseId(1))
}

/// Test that verifies index is usable after creation
#[tokio::test]
async fn test_index_usable_after_creation() {
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
	let ses = Session::owner().with_ns("test").with_db("test");

	// Setup
	ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
	ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();

	// Create an index
	let mut res =
		ds.execute("DEFINE INDEX test_idx ON test_table FIELDS name", &ses, None).await.unwrap();
	assert!(res.remove(0).result.is_ok());

	// Insert some data that uses the indexed field
	let mut res = ds.execute("CREATE test_table SET name = 'test'", &ses, None).await.unwrap();
	assert!(res.remove(0).result.is_ok(), "INSERT should succeed with index present");

	// Query using the indexed field
	let mut res =
		ds.execute("SELECT * FROM test_table WHERE name = 'test'", &ses, None).await.unwrap();
	let val = res.remove(0).result.unwrap();
	assert!(val.is_array() && !val.as_array().unwrap().is_empty(), "Query should return results");
}

/// Test that directly verifies cache invalidation within a single transaction
/// when adding an index via put_tb_index.
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_index_put() {
	use crate::catalog::{Index, IndexDefinition, IndexId};

	let (_ds, tx, ns, db) = setup_tx_with_ns_db().await;
	let tb = TableName::from("test_table");

	// Step 1: Populate the cache with an empty index list
	let indexes = tx.all_tb_indexes(ns, db, &tb).await.unwrap();
	assert_eq!(indexes.len(), 0, "Initially there should be no indexes");

	// Step 2: Add an index via put_tb_index
	let ix_def = IndexDefinition {
		index_id: IndexId(1),
		name: "test_idx".to_string(),
		table_name: tb.clone(),
		cols: vec![],
		index: Index::Idx,
		comment: None,
		prepare_remove: false,
	};
	tx.put_tb_index(ns, db, &tb, &ix_def).await.unwrap();

	// Step 3: Query all indexes again — this must see the new index
	let indexes = tx.all_tb_indexes(ns, db, &tb).await.unwrap();
	assert_eq!(
		indexes.len(),
		1,
		"After put_tb_index, all_tb_indexes should return the new index (cache must be invalidated)"
	);
	assert_eq!(indexes[0].name, "test_idx");

	tx.cancel().await.unwrap();
}

/// Test that directly verifies cache invalidation within a single transaction
/// when removing an index via del_tb_index.
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_index_delete() {
	use crate::catalog::{Index, IndexDefinition, IndexId};

	let (_ds, tx, ns, db) = setup_tx_with_ns_db().await;
	let tb = TableName::from("test_table");

	// Add an index
	let ix_def = IndexDefinition {
		index_id: IndexId(1),
		name: "test_idx".to_string(),
		table_name: tb.clone(),
		cols: vec![],
		index: Index::Idx,
		comment: None,
		prepare_remove: false,
	};
	tx.put_tb_index(ns, db, &tb, &ix_def).await.unwrap();

	// Populate the cache with the list containing one index
	let indexes = tx.all_tb_indexes(ns, db, &tb).await.unwrap();
	assert_eq!(indexes.len(), 1, "Should have one index");

	// Remove the index
	tx.del_tb_index(ns, db, &tb, "test_idx").await.unwrap();

	// Query again — must see empty list
	let indexes = tx.all_tb_indexes(ns, db, &tb).await.unwrap();
	assert_eq!(
		indexes.len(),
		0,
		"After del_tb_index, all_tb_indexes should return empty list (cache must be invalidated)"
	);

	// Also verify individual cache entry is invalidated
	let ix = tx.get_tb_index(ns, db, &tb, "test_idx").await.unwrap();
	assert!(ix.is_none(), "After del_tb_index, get_tb_index should return None");

	tx.cancel().await.unwrap();
}

/// Test that directly verifies cache invalidation within a single transaction
/// when adding a field via put_tb_field.
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_field_put() {
	use std::str::FromStr;

	use crate::catalog::FieldDefinition;
	use crate::expr::Idiom;

	let (_ds, tx, ns, db) = setup_tx_with_ns_db().await;
	let tb = TableName::from("test_table");

	// Step 1: Populate the cache with an empty field list
	let fields = tx.all_tb_fields(ns, db, &tb, None).await.unwrap();
	assert_eq!(fields.len(), 0, "Initially there should be no fields");

	// Step 2: Add a field via put_tb_field
	let fd_def = FieldDefinition {
		name: Idiom::from_str("name").unwrap(),
		table: tb.clone(),
		..Default::default()
	};
	tx.put_tb_field(ns, db, &tb, &fd_def).await.unwrap();

	// Step 3: Query all fields again — this must see the new field
	let fields = tx.all_tb_fields(ns, db, &tb, None).await.unwrap();
	assert_eq!(
		fields.len(),
		1,
		"After put_tb_field, all_tb_fields should return the new field (cache must be invalidated)"
	);

	tx.cancel().await.unwrap();
}

/// Test that verifies multiple sequential index operations work correctly
#[tokio::test]
async fn test_multiple_index_operations_cache_consistency() {
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
	let ses = Session::owner().with_ns("test").with_db("test");

	// Setup
	ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
	ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();

	// Add first index
	let mut res =
		ds.execute("DEFINE INDEX idx1 ON test_table FIELDS field1", &ses, None).await.unwrap();
	assert!(res.remove(0).result.is_ok());

	// Add second index
	let mut res =
		ds.execute("DEFINE INDEX idx2 ON test_table FIELDS field2", &ses, None).await.unwrap();
	assert!(res.remove(0).result.is_ok());

	// Verify both indexes exist
	let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
	let val = res.remove(0).result.unwrap();
	let info = format!("{:?}", val);
	assert!(info.contains("idx1") && info.contains("idx2"), "Both indexes should be visible");

	// Remove first index
	let mut res = ds.execute("REMOVE INDEX idx1 ON test_table", &ses, None).await.unwrap();
	assert!(res.remove(0).result.is_ok());

	// Verify only second index remains
	let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
	let val = res.remove(0).result.unwrap();
	let info = format!("{:?}", val);
	assert!(
		!info.contains("idx1") && info.contains("idx2"),
		"Only idx2 should remain after removing idx1"
	);
}

/// Test cache invalidation for put_ns (namespace list cache).
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_ns_put() {
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
	let tx = ds.transaction(Write, Optimistic).await.unwrap();

	// Populate the cache with an empty namespace list
	let nss = tx.all_ns().await.unwrap();
	assert_eq!(nss.len(), 0, "Initially there should be no namespaces");

	// Add a namespace
	let ns_def = NamespaceDefinition {
		namespace_id: NamespaceId(1),
		name: "test".to_string(),
		comment: None,
	};
	tx.put_ns(ns_def).await.unwrap();

	// Query again — must see the new namespace
	let nss = tx.all_ns().await.unwrap();
	assert_eq!(
		nss.len(),
		1,
		"After put_ns, all_ns should return the new namespace (cache must be invalidated)"
	);

	tx.cancel().await.unwrap();
}

/// Test cache invalidation for put_db and del_db (database list cache).
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_db_put_and_del() {
	let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
	let tx = ds.transaction(Write, Optimistic).await.unwrap();

	let ns_def = NamespaceDefinition {
		namespace_id: NamespaceId(1),
		name: "test".to_string(),
		comment: None,
	};
	tx.put_ns(ns_def).await.unwrap();

	// Populate the cache with an empty database list
	let dbs = tx.all_db(NamespaceId(1)).await.unwrap();
	assert_eq!(dbs.len(), 0, "Initially there should be no databases");

	// Add a database
	let db_def = DatabaseDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		name: "testdb".to_string(),
		strict: false,
		comment: None,
		changefeed: None,
	};
	tx.put_db("test", db_def).await.unwrap();

	// Query again — must see the new database
	let dbs = tx.all_db(NamespaceId(1)).await.unwrap();
	assert_eq!(
		dbs.len(),
		1,
		"After put_db, all_db should return the new database (cache must be invalidated)"
	);

	// Delete the database
	tx.del_db("test", "testdb", false).await.unwrap();

	// Query again — must see empty list
	let dbs = tx.all_db(NamespaceId(1)).await.unwrap();
	assert_eq!(
		dbs.len(),
		0,
		"After del_db, all_db should return empty list (cache must be invalidated)"
	);

	tx.cancel().await.unwrap();
}

/// Test cache invalidation for put_tb and del_tb (table list cache).
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_tb_put_and_del() {
	use crate::catalog::TableDefinition;

	let (_ds, tx, ns, db) = setup_tx_with_ns_db().await;

	// Populate the cache with an empty table list
	let tbs = tx.all_tb(ns, db, None).await.unwrap();
	assert_eq!(tbs.len(), 0, "Initially there should be no tables");

	// Add a table
	let tb_def = TableDefinition::new(ns, db, TableId(1), TableName::from("test_table"));
	tx.put_tb("test", "test", &tb_def).await.unwrap();

	// Query again — must see the new table
	let tbs = tx.all_tb(ns, db, None).await.unwrap();
	assert_eq!(
		tbs.len(),
		1,
		"After put_tb, all_tb should return the new table (cache must be invalidated)"
	);

	// Delete the table
	tx.del_tb("test", "test", &TableName::from("test_table")).await.unwrap();

	// Query again — must see empty list
	let tbs = tx.all_tb(ns, db, None).await.unwrap();
	assert_eq!(
		tbs.len(),
		0,
		"After del_tb, all_tb should return empty list (cache must be invalidated)"
	);

	tx.cancel().await.unwrap();
}

/// Test cache invalidation for put_db_param (param list cache).
/// This also validates the pattern used for put_db_function, put_db_module, and put_db_api.
#[tokio::test]
async fn test_single_tx_cache_invalidation_on_param_put() {
	use crate::catalog::ParamDefinition;

	let (_ds, tx, ns, db) = setup_tx_with_ns_db().await;

	// Populate the cache with an empty param list
	let pas = tx.all_db_params(ns, db).await.unwrap();
	assert_eq!(pas.len(), 0, "Initially there should be no params");

	// Add a param
	let pa_def = ParamDefinition {
		name: "test_param".to_string(),
		value: crate::val::Value::Bool(true),
		..Default::default()
	};
	tx.put_db_param(ns, db, &pa_def).await.unwrap();

	// Query again — must see the new param
	let pas = tx.all_db_params(ns, db).await.unwrap();
	assert_eq!(
		pas.len(),
		1,
		"After put_db_param, all_db_params should return the new param (cache must be invalidated)"
	);

	tx.cancel().await.unwrap();
}
