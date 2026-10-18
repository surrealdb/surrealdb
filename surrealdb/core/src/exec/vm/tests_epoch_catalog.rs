//! The catalog definitions shared by the read transactions of one catalog
//! epoch. Every committed change of a namespace, database, user or table
//! definition moves the epoch, and a read transaction of the new epoch reads
//! the new definition.

use std::sync::Arc;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::{Datastore, TransactionType};
use crate::val::TableName;

/// Whether both reads answered the same allocation.
fn shared<T>(a: &Option<Arc<T>>, b: &Option<Arc<T>>) -> bool {
	match (a, b) {
		(Some(a), Some(b)) => Arc::ptr_eq(a, b),
		_ => false,
	}
}

const SETUP: &str = "
	DEFINE TABLE person SCHEMALESS;
	CREATE person:1 SET name = 'ann';
";

/// The catalog epoch a read transaction opened now names.
async fn epoch(ds: &Datastore) -> u64 {
	let txn = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let epoch = txn.catalog_epoch().expect("a memory store is written by its datastore");
	txn.cancel().await.expect("cancelled");
	epoch
}

async fn run(ds: &Datastore, session: &Session, sql: &str) {
	for response in ds.execute(sql, session, None).await.expect("the request runs") {
		response.result.unwrap_or_else(|e| panic!("{sql}: {e}"));
	}
}

#[tokio::test]
async fn each_change_of_a_definition_moves_the_catalog_epoch() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let live = owner.clone().with_rt(true);
	for sql in [
		"DEFINE NAMESPACE OVERWRITE test COMMENT 'one'",
		"DEFINE DATABASE OVERWRITE test COMMENT 'one'",
		"DEFINE NAMESPACE other",
		"REMOVE NAMESPACE other",
		"DEFINE DATABASE other",
		"REMOVE DATABASE other",
		"DEFINE USER u ON ROOT PASSWORD 'p'",
		"REMOVE USER u ON ROOT",
		"DEFINE USER u ON NAMESPACE PASSWORD 'p'",
		"REMOVE USER u ON NAMESPACE",
		"DEFINE USER u ON DATABASE PASSWORD 'p'",
		"REMOVE USER u ON DATABASE",
	] {
		let before = epoch(ds).await;
		run(ds, &owner, sql).await;
		assert!(epoch(ds).await > before, "{sql} moves the catalog epoch");
	}
	let before = epoch(ds).await;
	let id = ds
		.execute("LIVE SELECT * FROM person", &live, None)
		.await
		.expect("the request runs")
		.remove(0)
		.result
		.expect("the live query starts");
	assert!(epoch(ds).await > before, "a live query's registration moves the catalog epoch");
	let before = epoch(ds).await;
	let mut vars = surrealdb_types::Variables::new();
	vars.insert("id", id);
	for response in ds.execute("KILL $id", &live, Some(vars)).await.expect("the request runs") {
		response.result.expect("the live query ends");
	}
	assert!(epoch(ds).await > before, "a live query's end moves the catalog epoch");

	let before = epoch(ds).await;
	run(ds, &owner, "CREATE person:2; UPDATE person:1 SET name = 'bea'; SELECT * FROM person")
		.await;
	assert_eq!(epoch(ds).await, before, "a write of records moves no epoch");
}

#[tokio::test]
async fn a_definition_read_under_an_epoch_is_served_until_it_changes() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let person = TableName::from("person");
	struct Read {
		ns: Option<Arc<crate::catalog::NamespaceDefinition>>,
		db: Option<Arc<crate::catalog::DatabaseDefinition>>,
		tb: Option<Arc<crate::catalog::TableDefinition>>,
	}
	let read = |kind: TransactionType| {
		let person = person.clone();
		async move {
			let txn = ds.transaction(kind).await.expect("a transaction");
			let read = Read {
				ns: txn.get_ns_by_name("test", None).await.expect("read"),
				db: txn.get_db_by_name("test", "test", None).await.expect("read"),
				tb: txn.get_tb_by_name("test", "test", &person, None).await.expect("read"),
			};
			txn.cancel().await.expect("cancelled");
			read
		}
	};
	let comment = |read: &Read| {
		(
			read.ns.as_ref().and_then(|d| d.comment.clone()),
			read.db.as_ref().and_then(|d| d.comment.clone()),
			read.tb.as_ref().and_then(|d| d.comment.clone()),
		)
	};
	let first = read(TransactionType::Read).await;
	let again = read(TransactionType::Read).await;
	assert!(shared(&first.ns, &again.ns), "the namespace is served to the epoch's reads");
	assert!(shared(&first.db, &again.db), "the database is served to the epoch's reads");
	assert!(shared(&first.tb, &again.tb), "the table is served to the epoch's reads");
	let written = read(TransactionType::Write).await;
	assert!(!shared(&first.ns, &written.ns), "a writer reads the store");

	run(ds, &owner, "DEFINE NAMESPACE OVERWRITE test COMMENT 'ns'").await;
	assert_eq!(comment(&read(TransactionType::Read).await).0.as_deref(), Some("ns"));
	run(ds, &owner, "DEFINE DATABASE OVERWRITE test COMMENT 'db'").await;
	assert_eq!(comment(&read(TransactionType::Read).await).1.as_deref(), Some("db"));
	run(ds, &owner, "DEFINE TABLE OVERWRITE person SCHEMALESS COMMENT 'tb'").await;
	assert_eq!(comment(&read(TransactionType::Read).await).2.as_deref(), Some("tb"));

	run(ds, &owner, "REMOVE TABLE person").await;
	assert!(read(TransactionType::Read).await.tb.is_none(), "a removed table is not read");
	run(ds, &owner, "REMOVE DATABASE test").await;
	assert!(read(TransactionType::Read).await.db.is_none(), "a removed database is not read");
	run(ds, &owner, "REMOVE NAMESPACE test").await;
	assert!(read(TransactionType::Read).await.ns.is_none(), "a removed namespace is not read");
}

#[tokio::test]
async fn a_live_query_registered_after_reads_of_its_table_is_notified() {
	let (send, recv) = crate::channel::bounded(100);
	let ds = Datastore::builder()
		.with_capabilities(crate::dbs::Capabilities::all())
		.with_auth(false)
		.with_notify(send)
		.build_with_path("memory")
		.await
		.expect("a datastore");
	let owner = Session::owner().with_ns("test").with_db("test");
	run(&ds, &owner, "DEFINE NAMESPACE test; DEFINE DATABASE test").await;
	run(&ds, &owner, SETUP).await;
	// Reads that keep the table's definition for the epoch.
	for _ in 0..3 {
		run(&ds, &owner, "SELECT * FROM person:1").await;
	}
	let live = owner.clone().with_rt(true);
	let id = ds
		.execute("LIVE SELECT * FROM person", &live, None)
		.await
		.expect("the request runs")
		.remove(0)
		.result
		.expect("the live query starts");
	for _ in 0..3 {
		run(&ds, &owner, "SELECT * FROM person:1").await;
	}
	run(&ds, &owner, "CREATE person:7").await;
	let notification = tokio::time::timeout(std::time::Duration::from_secs(3), recv.recv())
		.await
		.expect("a notification arrives")
		.expect("the channel is open");
	assert_eq!(notification.action, crate::types::PublicAction::Create);

	let mut vars = surrealdb_types::Variables::new();
	vars.insert("id", id);
	for response in ds.execute("KILL $id", &live, Some(vars)).await.expect("the request runs") {
		response.result.expect("the live query ends");
	}
	for _ in 0..3 {
		run(&ds, &owner, "SELECT * FROM person:1").await;
	}
	run(&ds, &owner, "CREATE person:8").await;
	// The live query's end is announced; a record created after it is not.
	while let Ok(Ok(after)) =
		tokio::time::timeout(std::time::Duration::from_millis(300), recv.recv()).await
	{
		assert_eq!(after.action, crate::types::PublicAction::Killed, "{after:?}");
	}
}
