//! The compatibility level a database is created at: the store's default,
//! which a store this release creates leaves unrecorded and the data migration that brings an
//! earlier release's store up to this one records as level 3, and the level an implicitly
//! created database takes.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseCompatibility, DatabaseId, FromStored as _, NamespaceId, StoredTableDefinition,
	TableDefinition, TableId,
};
use crate::dbs::Session;
use crate::key::schema::{MigrationKey, StorageVersionKey, StoreDefaultsKey, VersionKey};
use crate::kvs::Datastore;
use crate::kvs::migration::STORE_DEFAULTS;
use crate::kvs::version::{MajorVersion, StorageVersion, StoreDefaults};
use crate::val::TableName;

const LEVEL_3: DatabaseCompatibility = DatabaseCompatibility::LEVEL_3;
const LEVEL_4: DatabaseCompatibility = DatabaseCompatibility::LEVEL_4;

/// A datastore that has not been bootstrapped: nothing has written to it.
async fn unsettled() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

/// An [`unsettled`] datastore that opts in to upgrading a store a 3.x release
/// wrote, for the tests that make it one.
async fn unsettled_upgradable() -> Arc<Datastore> {
	Datastore::builder()
		.without_maintenance_tasks()
		.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("experimental_upgrade_from_3x", "true"),
		)
		.build_with_path("memory")
		.await
		.unwrap()
}

/// The defaults the datastore records, if any.
async fn recorded(ds: &Datastore) -> Option<StoreDefaults> {
	let txn = ds.transaction(Read).await.unwrap();
	let defaults = txn.get_key(&StoreDefaultsKey {}, None).await.unwrap();
	txn.cancel().await.unwrap();
	defaults
}

/// The level a database created now takes by default.
async fn default_level(ds: &Datastore) -> DatabaseCompatibility {
	let txn = ds.transaction(Read).await.unwrap();
	let level = txn.default_compatibility().await.unwrap();
	txn.cancel().await.unwrap();
	level
}

/// Whether the ledger records migration `id` as applied.
async fn applied(ds: &Datastore, id: u32) -> bool {
	let txn = ds.transaction(Read).await.unwrap();
	let applied = txn.exists_key(&MigrationKey::new(id), None).await.unwrap();
	txn.cancel().await.unwrap();
	applied
}

/// Stands in for a datastore an earlier release created and last wrote:
/// its version keys, a database, and no recorded defaults.
async fn written_by_an_earlier_release(ds: &Datastore) {
	let txn = ds.transaction(Write).await.unwrap();
	txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	txn.set_key(&StorageVersionKey {}, &StorageVersion::parse("3.3.0").unwrap()).await.unwrap();
	txn.ensure_ns_db(None, "test", "test").await.unwrap();
	txn.commit().await.unwrap();
	// What `ensure_ns_db` took for the database is not what the earlier
	// release would have stored; only the absent record matters here.
	let txn = ds.transaction(Write).await.unwrap();
	txn.del_key(&StoreDefaultsKey {}).await.unwrap();
	txn.commit().await.unwrap();
}

/// The compatibility level of database `db` of namespace `ns`.
async fn level_of(ds: &Datastore, ns: &str, db: &str) -> DatabaseCompatibility {
	let txn = ds.transaction(Read).await.unwrap();
	let level = txn.get_db_by_name(ns, db, None).await.unwrap().unwrap().compatibility.0;
	txn.cancel().await.unwrap();
	level
}

/// Run `sql` as the owner in `ns`/`db`, failing on the first error.
async fn run(ds: &Datastore, ns: &str, db: &str, sql: &str) {
	let session = Session::owner().with_ns(ns).with_db(db);
	for response in ds.execute(sql, &session, None).await.unwrap() {
		response.result.unwrap();
	}
}

/// A store this release creates records no defaults, and its databases take level 4. The
/// migration that records an earlier release's defaults is applied without running.
#[tokio::test]
async fn a_new_store_defaults_to_level_4() {
	let ds = unsettled().await;
	let (_, created) = ds.check_version().await.unwrap();
	assert!(created);
	assert_eq!(recorded(&ds).await, None);
	assert_eq!(default_level(&ds).await, LEVEL_4);
	assert!(applied(&ds, STORE_DEFAULTS.raw()).await);
	run(&ds, "test", "test", "DEFINE NAMESPACE test; DEFINE DATABASE fresh;").await;
	assert_eq!(level_of(&ds, "test", "fresh").await, LEVEL_4);
}

/// A store whose first start stamped `!v` and stopped before `check_version`
/// recorded the migration baseline still baselines `STORE_DEFAULTS` on the next
/// start rather than running it and stamping level 3.
#[tokio::test]
async fn a_new_store_reopened_after_version_bootstrap_still_defaults_to_level_4() {
	let first = unsettled().await;
	let (_, created) = first.settle_storage_version().await.unwrap();
	assert!(created);
	assert_eq!(first.storage_version().await.unwrap(), None);

	let reopened = first.fork_for_test_with_node_id(uuid::Uuid::new_v4());
	let (_, created_on_reopen) = reopened.check_version().await.unwrap();
	assert!(!created_on_reopen);
	assert_eq!(recorded(&reopened).await, None);
	assert_eq!(default_level(&reopened).await, LEVEL_4);
	assert!(applied(&reopened, STORE_DEFAULTS.raw()).await);
}

/// A store an earlier release created defaults to level 3 once this release
/// has started on it: a database created after the upgrade stands at the
/// level of the databases created before it. Its existing databases, which
/// store no level, read at level 3 too.
#[tokio::test]
async fn an_upgraded_store_defaults_to_level_3() {
	let ds = unsettled_upgradable().await;
	written_by_an_earlier_release(&ds).await;
	let (_, created) = ds.check_version().await.unwrap();
	assert!(!created);
	assert_eq!(recorded(&ds).await, Some(StoreDefaults::LEGACY));
	assert_eq!(default_level(&ds).await, LEVEL_3);
	assert!(applied(&ds, STORE_DEFAULTS.raw()).await);
	run(&ds, "test", "test", "DEFINE DATABASE later;").await;
	assert_eq!(level_of(&ds, "test", "later").await, LEVEL_3);
	run(&ds, "test", "test", "DEFINE DATABASE raised COMPATIBILITY 4;").await;
	assert_eq!(level_of(&ds, "test", "raised").await, LEVEL_4);
}

/// The migration records an earlier release's defaults only where none
/// stands: a default an operator set is kept.
#[tokio::test]
async fn a_recorded_default_is_kept() {
	let ds = unsettled_upgradable().await;
	written_by_an_earlier_release(&ds).await;
	let txn = ds.transaction(Write).await.unwrap();
	txn.set_default_compatibility(LEVEL_4).await.unwrap();
	txn.commit().await.unwrap();
	ds.check_version().await.unwrap();
	assert_eq!(default_level(&ds).await, LEVEL_4);
}

/// `ALTER SYSTEM COMPATIBILITY` sets the level databases created later take,
/// and leaves the existing ones where they stand.
#[tokio::test]
async fn altering_the_system_default_changes_only_new_databases() {
	let ds = unsettled().await;
	ds.check_version().await.unwrap();
	run(&ds, "test", "test", "DEFINE NAMESPACE test; DEFINE DATABASE before;").await;
	run(&ds, "test", "test", "ALTER SYSTEM COMPATIBILITY 3; DEFINE DATABASE after;").await;
	assert_eq!(default_level(&ds).await, LEVEL_3);
	assert_eq!(level_of(&ds, "test", "before").await, LEVEL_4);
	assert_eq!(level_of(&ds, "test", "after").await, LEVEL_3);
}

/// A database a statement creates implicitly takes the store's default.
#[tokio::test]
async fn an_implicitly_created_database_takes_the_store_default() {
	let ds = unsettled().await;
	ds.check_version().await.unwrap();
	run(&ds, "test", "implicit_4", "DEFINE PARAM $p VALUE 1;").await;
	assert_eq!(level_of(&ds, "test", "implicit_4").await, LEVEL_4);
	run(&ds, "test", "test", "ALTER SYSTEM COMPATIBILITY 3;").await;
	run(&ds, "test", "implicit_3", "DEFINE PARAM $p VALUE 1;").await;
	assert_eq!(level_of(&ds, "test", "implicit_3").await, LEVEL_3);
	let txn = ds.transaction(Write).await.unwrap();
	txn.ensure_ns_db(None, "test", "ensured").await.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(level_of(&ds, "test", "ensured").await, LEVEL_3);
}

/// A database whose ids already hold table definitions was defined by an
/// earlier release: a row created for it implicitly takes level 3, whatever
/// the store's default. Without them it takes the default.
#[tokio::test]
async fn implicit_creation_over_existing_tables_takes_level_3() {
	let ds = unsettled().await;
	ds.check_version().await.unwrap();
	let (ns, db) = (NamespaceId(41), DatabaseId(42));
	let txn = ds.transaction(Write).await.unwrap();
	assert_eq!(txn.implicit_compatibility(ns, db).await.unwrap(), LEVEL_4);
	let table = TableDefinition::from_stored(&StoredTableDefinition::new(
		ns,
		db,
		TableId(7),
		TableName::from("t"),
	))
	.unwrap();
	txn.put_tb("orphan_ns", "orphan_db", &table).await.unwrap();
	assert_eq!(txn.implicit_compatibility(ns, db).await.unwrap(), LEVEL_3);
	txn.cancel().await.unwrap();
}

/// No statement leaves a table in a database with no definition row: one
/// run in a database that does not exist either fails or creates the row.
#[tokio::test]
async fn no_statement_defines_a_table_in_a_database_with_no_row() {
	let ds = unsettled().await;
	ds.check_version().await.unwrap();
	run(&ds, "test", "test", "DEFINE NAMESPACE test;").await;
	for (db, sql) in [
		("by_define", "DEFINE TABLE t"),
		("by_create", "CREATE t:1"),
		("by_insert", "INSERT INTO t { id: 1 }"),
		("by_relate", "RELATE a:1->e->b:1"),
		("by_field", "DEFINE FIELD f ON t"),
		("by_index", "DEFINE INDEX i ON t FIELDS f"),
	] {
		let session = Session::owner().with_ns("test").with_db(db);
		let responses = ds.execute(sql, &session, None).await.unwrap();
		let succeeded = responses.iter().all(|response| response.result.is_ok());
		let txn = ds.transaction(Read).await.unwrap();
		let row = txn.get_db_by_name("test", db, None).await.unwrap();
		let tables = match &row {
			Some(row) => txn.all_tb(row.namespace_id, row.database_id, None).await.unwrap().len(),
			None => 0,
		};
		txn.cancel().await.unwrap();
		assert!(!succeeded || row.is_some(), "`{sql}` succeeded with no database row");
		assert!(row.is_some() || tables == 0, "`{sql}` left tables with no database row");
	}
}
