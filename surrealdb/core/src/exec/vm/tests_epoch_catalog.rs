//! The catalog definitions shared by the transactions of one catalog epoch.
//! Every committed change of a namespace, database, user or table definition
//! moves the epoch, and a transaction of the new epoch reads the new
//! definition. A writer is served from them too, until it changes a
//! definition, unless its backend tracks plain reads.

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
	assert!(shared(&first.ns, &written.ns), "a writer that changed nothing is served the epoch's");

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

/// A table's layouts are shared by the read transactions of an epoch, never
/// kept from a writer, and read again once a definition moves the epoch.
#[tokio::test]
async fn a_tables_layouts_are_served_to_the_epochs_reads() {
	let db = TestDb::new(
		"DEFINE TABLE pk SCHEMAFULL;
		 DEFINE FIELD name ON pk TYPE string;
		 CREATE pk:1 SET name = 'ann';",
	)
	.await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let pk = TableName::from("pk");
	let read = |kind: TransactionType| {
		let pk = pk.clone();
		async move {
			let txn = ds.transaction(kind).await.expect("a transaction");
			let def = txn.get_db_by_name("test", "test", None).await.expect("read").expect("db");
			let layouts = txn
				.table_layouts(def.namespace_id, def.database_id, &pk, None)
				.await
				.expect("the layouts are read");
			txn.cancel().await.expect("cancelled");
			layouts
		}
	};
	let written = read(TransactionType::Write).await;
	let first = read(TransactionType::Read).await;
	assert!(!Arc::ptr_eq(&written, &first), "a writer's read is not kept for the epoch");
	let again = read(TransactionType::Read).await;
	assert!(Arc::ptr_eq(&first, &again), "the layouts are served to the epoch's reads");
	assert!(!Arc::ptr_eq(&first, &read(TransactionType::Write).await), "a writer reads the store");

	run(ds, &owner, "DEFINE FIELD age ON pk TYPE option<int>").await;
	let changed = read(TransactionType::Read).await;
	assert!(
		!Arc::ptr_eq(&first, &changed),
		"a definition moves the epoch the layouts were kept in"
	);
	let stored = read(TransactionType::Write).await;
	assert_ne!(first.head(), changed.head(), "the field's definition minted a generation");
	assert_eq!(changed.head(), stored.head(), "a read of the new epoch reads the new layouts");
	assert_eq!(changed.ring(), stored.ring(), "a read of the new epoch reads the new ring");

	// A migration that only mints, with no definition written, moves the epoch too.
	let cached = read(TransactionType::Read).await;
	assert!(Arc::ptr_eq(&changed, &cached), "the new layouts are served to the epoch's reads");
	run(ds, &owner, "ALTER TABLE pk MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS").await;
	let migrated = read(TransactionType::Read).await;
	let stored = read(TransactionType::Write).await;
	assert_ne!(cached.head(), migrated.head(), "the migration minted a generation");
	assert_eq!(migrated.head(), stored.head(), "a read after the migration reads its layouts");
	assert_eq!(migrated.ring(), stored.ring(), "a read after the migration reads its ring");
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

/// What a transaction answers of the listings and definitions an epoch keeps.
struct Listed {
	tb: Arc<crate::catalog::TableDefinition>,
	tb_by_name: Option<Arc<crate::catalog::TableDefinition>>,
	tables: Arc<[crate::catalog::TableDefinition]>,
	fields: Arc<[crate::catalog::FieldDefinition]>,
	events: Arc<[crate::catalog::EventDefinition]>,
	views: Arc<[crate::catalog::TableDefinition]>,
}

async fn listed(txn: &crate::kvs::Transaction) -> Listed {
	let person = TableName::from("person");
	let ns = txn.get_ns_by_name("test", None).await.expect("read").expect("defined").namespace_id;
	let db =
		txn.get_db_by_name("test", "test", None).await.expect("read").expect("defined").database_id;
	Listed {
		tb: txn.get_tb(ns, db, &person, None).await.expect("read").expect("defined"),
		tb_by_name: txn.get_tb_by_name("test", "test", &person, None).await.expect("read"),
		tables: txn.all_tb(ns, db, None).await.expect("read"),
		fields: txn.all_tb_fields(ns, db, &person, None).await.expect("read"),
		events: txn.all_tb_events(ns, db, &person, None).await.expect("read"),
		views: txn.all_tb_views(ns, db, &person, None).await.expect("read"),
	}
}

fn same_listings(a: &Listed, b: &Listed) -> bool {
	Arc::ptr_eq(&a.tb, &b.tb)
		&& shared(&a.tb_by_name, &b.tb_by_name)
		&& Arc::ptr_eq(&a.tables, &b.tables)
		&& Arc::ptr_eq(&a.fields, &b.fields)
		&& Arc::ptr_eq(&a.events, &b.events)
		&& Arc::ptr_eq(&a.views, &b.views)
}

const LISTED: &str = "
	DEFINE FIELD OVERWRITE name ON person TYPE string;
	DEFINE EVENT OVERWRITE seen ON person WHEN true THEN (RETURN 1);
	DEFINE TABLE OVERWRITE people AS SELECT name FROM person;
";

#[tokio::test]
async fn a_writer_that_changed_nothing_is_served_the_listings_of_its_epoch() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	run(ds, &owner, LISTED).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let first = listed(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert_eq!(first.fields.len(), 1);
	assert_eq!(first.events.len(), 1);
	assert_eq!(first.views.len(), 1);

	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	assert!(same_listings(&first, &listed(&writer).await), "a writer is served the epoch's");
	writer.cancel().await.expect("cancelled");

	// A backend that tracks plain reads keeps the writer's catalog reads real.
	let tracked = ds
		.transaction_at(TransactionType::Write, surrealdb_kvs::Isolation::Serializable)
		.await
		.expect("a transaction");
	let read = listed(&tracked).await;
	assert!(!Arc::ptr_eq(&first.fields, &read.fields), "a tracking writer reads the store");
	assert!(!Arc::ptr_eq(&first.tables, &read.tables), "a tracking writer reads the store");
	tracked.cancel().await.expect("cancelled");
}

/// Copy the stored `name` field of `person` and its `seen` event under the keys `extra`, in
/// `writer`, with no catalog version stamped, and answer the ids.
async fn write_extra_definitions(
	writer: &crate::kvs::Transaction,
	stamp: bool,
) -> (crate::catalog::NamespaceId, crate::catalog::DatabaseId) {
	use std::borrow::Cow;

	use crate::key::schema::{EventKey, FieldKey};
	let person = TableName::from("person");
	let ns = writer.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
	let db = writer.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
	let field = |fd: &'static str| FieldKey {
		ns,
		db,
		tb: Cow::Borrowed(&person),
		fd: Cow::Borrowed(fd),
	};
	let event = |ev: &'static str| EventKey {
		ns,
		db,
		tb: Cow::Borrowed(&person),
		ev: Cow::Borrowed(ev),
	};
	let stored = writer.get_key(&field("name"), None).await.unwrap().expect("a field");
	writer.set_key(&field("extra"), &stored).await.unwrap();
	let stored = writer.get_key(&event("seen"), None).await.unwrap().expect("an event");
	writer.set_key(&event("extra"), &stored).await.unwrap();
	if stamp {
		writer.bump_catalog_version(ns, db, &person).await.unwrap();
	}
	(ns, db)
}

#[tokio::test]
async fn a_writer_reads_the_definitions_it_wrote_and_publishes_none_of_them() {
	let person = TableName::from("person");
	for (stamp, warm) in [(false, true), (false, false), (true, true), (true, false)] {
		let db = TestDb::new(SETUP).await;
		let (ds, owner) = (db.datastore(), TestDb::owner());
		run(ds, &owner, LISTED).await;
		// A new epoch, whose listings are warm or are first read by the writer below.
		run(ds, &owner, "DEFINE FIELD OVERWRITE name ON person TYPE string COMMENT 'y'").await;
		if warm {
			let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
			listed(&reader).await;
			reader.cancel().await.expect("cancelled");
		}

		let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
		let (ns, dbid) = write_extra_definitions(&writer, stamp).await;
		assert!(writer.catalog_epoch().is_none(), "the writer wrote definitions (stamp={stamp})");
		let fields = writer.all_tb_fields(ns, dbid, &person, None).await.unwrap();
		let events = writer.all_tb_events(ns, dbid, &person, None).await.unwrap();
		assert_eq!(fields.len(), 2, "the writer reads its own field (stamp={stamp}, warm={warm})");
		assert_eq!(events.len(), 2, "the writer reads its own event (stamp={stamp}, warm={warm})");
		writer.cancel().await.expect("cancelled");

		let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
		let after = listed(&reader).await;
		reader.cancel().await.expect("cancelled");
		assert_eq!(after.fields.len(), 1, "nothing the writer wrote is served (stamp={stamp})");
		assert_eq!(after.events.len(), 1, "nothing the writer wrote is served (stamp={stamp})");
	}
}

#[tokio::test]
async fn an_epoch_hit_notes_the_catalog_reads_a_store_read_notes() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	run(ds, &owner, LISTED).await;
	let mut recorded = Vec::new();
	for _ in 0..2 {
		let txn = ds.transaction(TransactionType::Write).await.expect("a transaction");
		let recording = txn.record_catalog_reads();
		let read = listed(&txn).await;
		recorded.push((recording.finish(), read.views.len()));
		txn.cancel().await.expect("cancelled");
	}
	assert_eq!(recorded[0], recorded[1], "the second read is served by the epoch");
}

#[tokio::test]
async fn a_writer_does_not_name_the_epoch_after_a_concurrent_definition_commits() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	run(ds, &owner, LISTED).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let warm = listed(&reader).await;
	reader.cancel().await.expect("cancelled");

	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	run(ds, &owner, "DEFINE FIELD late ON person TYPE int").await;
	assert!(writer.catalog_epoch().is_none(), "the settled count moved under the writer");
	let (ns, dbid) = (warm.tb.namespace_id, warm.tb.database_id);
	let person = TableName::from("person");
	let seen = writer.all_tb_fields(ns, dbid, &person, None).await.expect("read");
	assert_eq!(seen.len(), 1, "the writer reads its own snapshot, not the new epoch's");
	writer.cancel().await.expect("cancelled");
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	assert_eq!(listed(&reader).await.fields.len(), 2);
	reader.cancel().await.expect("cancelled");
}

#[tokio::test]
async fn what_a_writer_reads_is_kept_for_its_epoch_unless_it_changed_the_catalog() {
	let db = TestDb::new(SETUP).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	run(ds, &owner, LISTED).await;

	// A new epoch, whose first reader is a writer.
	run(ds, &owner, "DEFINE FIELD OVERWRITE name ON person TYPE string COMMENT 'a'").await;
	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let kept = listed(&writer).await;
	writer.cancel().await.expect("cancelled");
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	assert!(same_listings(&kept, &listed(&reader).await), "a writer's read serves the epoch");
	reader.cancel().await.expect("cancelled");

	// Another, whose first reader stamped a version before it read.
	run(ds, &owner, "DEFINE FIELD OVERWRITE name ON person TYPE string COMMENT 'b'").await;
	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let person = TableName::from("person");
	writer.bump_catalog_version(kept.tb.namespace_id, kept.tb.database_id, &person).await.unwrap();
	let read = listed(&writer).await;
	writer.cancel().await.expect("cancelled");
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let later = listed(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(!Arc::ptr_eq(&read.fields, &later.fields), "a writer that stamped keeps nothing");
	assert!(!Arc::ptr_eq(&read.tables, &later.tables), "a writer that stamped keeps nothing");
}

/// A writer that read a table's fields commits a key it did not write, after
/// a concurrent `DEFINE FIELD ... ASSERT` committed.
async fn commit_after_concurrent_define(
	ds: &Datastore,
	isolation: surrealdb_kvs::Isolation,
) -> anyhow::Result<()> {
	let owner = TestDb::owner();
	run(ds, &owner, SETUP).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let warm = listed(&reader).await;
	reader.cancel().await.expect("cancelled");

	let writer = ds.transaction_at(TransactionType::Write, isolation).await.expect("a writer");
	let person = TableName::from("person");
	let (ns, db) = (warm.tb.namespace_id, warm.tb.database_id);
	writer.all_tb_fields(ns, db, &person, None).await.expect("read");
	run(ds, &owner, "DEFINE FIELD guarded ON person TYPE int ASSERT $value > 0").await;
	writer.set(b"/unrelated".to_vec().into(), b"1".to_vec()).await.expect("a write");
	writer.commit().await
}

#[tokio::test]
async fn a_serializable_writer_conflicts_with_a_concurrent_define_field_it_read_around() {
	let db = TestDb::new("").await;
	let outcome =
		commit_after_concurrent_define(db.datastore(), surrealdb_kvs::Isolation::Serializable)
			.await;
	assert!(outcome.is_err(), "the writer's read of the fields conflicts: {outcome:?}");
}

#[tokio::test]
async fn a_snapshot_writer_commits_after_a_concurrent_define_field_it_read_around() {
	let db = TestDb::new("").await;
	let outcome =
		commit_after_concurrent_define(db.datastore(), surrealdb_kvs::Isolation::Snapshot).await;
	assert!(outcome.is_ok(), "plain reads join no conflict set: {outcome:?}");
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn a_surrealkv_writer_commits_after_a_concurrent_define_field_it_read_around() {
	let dir = tempfile::tempdir().expect("a directory");
	let db = TestDb::new_on_surrealkv("", dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
	let outcome =
		commit_after_concurrent_define(db.datastore(), surrealdb_kvs::Isolation::Snapshot).await;
	assert!(outcome.is_ok(), "plain reads join no conflict set: {outcome:?}");
}
