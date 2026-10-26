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

/// A table's layouts are shared by the transactions of an epoch, writers that
/// have written no layouts among them, and read again once a definition moves
/// the epoch.
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
	let first = read(TransactionType::Write).await;
	let again = read(TransactionType::Read).await;
	assert!(Arc::ptr_eq(&first, &again), "a writer's read is kept for the epoch");
	assert!(Arc::ptr_eq(&first, &read(TransactionType::Write).await), "a writer is served them");

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

/// A writer that mints a generation names no epoch from then on: it reads the
/// layouts it wrote, and the epoch's are untouched by them.
#[tokio::test]
async fn a_writer_reads_the_layouts_it_mints() {
	let db = TestDb::new(
		"DEFINE TABLE pk SCHEMAFULL;
		 DEFINE FIELD name ON pk TYPE string;
		 CREATE pk:1 SET name = 'ann';",
	)
	.await;
	let ds = db.datastore();
	let pk = TableName::from("pk");
	let txn = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let def = txn.get_db_by_name("test", "test", None).await.expect("read").expect("db");
	let (ns, dbid) = (def.namespace_id, def.database_id);
	let shared = txn.table_layouts(ns, dbid, &pk, None).await.expect("the layouts are read");
	assert!(txn.catalog_epoch().is_some(), "a writer that wrote nothing names its epoch");
	txn.mint_next(ns, dbid, &pk, None, &shared).await.expect("a generation is minted");
	assert!(txn.catalog_epoch().is_none(), "a writer that minted names no epoch");
	let own = txn.table_layouts(ns, dbid, &pk, None).await.expect("the layouts are read");
	assert_ne!(own.head(), shared.head(), "the writer reads the generation it minted");
	txn.cancel().await.expect("cancelled");

	let txn = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let after = txn.table_layouts(ns, dbid, &pk, None).await.expect("the layouts are read");
	txn.cancel().await.expect("cancelled");
	assert!(Arc::ptr_eq(&shared, &after), "a cancelled mint leaves the epoch's layouts");
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

const INDEXED: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE INDEX age_idx ON person FIELDS age;
	DEFINE INDEX email_uniq ON person FIELDS email UNIQUE;
	CREATE person:1 SET age = 1, email = 'a@x.io';
";

/// An indexed table that nothing has written or planned over since the index was defined.
const UNSETTLED: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE INDEX age_idx ON person FIELDS age;
";

/// The shapes a transaction answers for the `age_idx` index of `person`, and the index.
async fn age_shapes(
	txn: &crate::kvs::Transaction,
) -> (crate::catalog::IndexId, Arc<surrealdb_datastore::index_shapes::IndexShapes>) {
	let person = TableName::from("person");
	let ns = txn.get_ns_by_name("test", None).await.expect("read").expect("defined").namespace_id;
	let db =
		txn.get_db_by_name("test", "test", None).await.expect("read").expect("defined").database_id;
	let indexes = txn.all_tb_indexes(ns, db, &person, None).await.expect("read");
	let age = indexes.iter().find(|ix| ix.name == "age_idx").expect("defined").index_id;
	let shapes = txn.table_index_shapes(ns, db, &person, &indexes).await.expect("read");
	(age, Arc::clone(shapes.of(age).expect("read for the index")))
}

/// Plan a read over the index, which reads it as settled online and keeps that for the epochs.
async fn settle_online(ds: &Datastore, session: &Session) {
	run(ds, session, "SELECT * FROM person WHERE age = 1").await;
}

#[tokio::test]
async fn the_shapes_of_a_settled_index_are_served_from_its_epochs() {
	let db = TestDb::new(INDEXED).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	settle_online(ds, &owner).await;

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (_, first) = age_shapes(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(first.is_tracked(), "the index records the shapes it stores");

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (_, again) = age_shapes(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(Arc::ptr_eq(&first, &again), "a reader of the epoch is served the shapes");

	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let (_, served) = age_shapes(&writer).await;
	writer.cancel().await.expect("cancelled");
	assert!(Arc::ptr_eq(&first, &served), "a writer that changed nothing is served the shapes");

	// A backend that tracks plain reads keeps the writer's reads real.
	let tracked = ds
		.transaction_at(TransactionType::Write, surrealdb_kvs::Isolation::Serializable)
		.await
		.expect("a transaction");
	let (_, read) = age_shapes(&tracked).await;
	tracked.cancel().await.expect("cancelled");
	assert!(!Arc::ptr_eq(&first, &read), "a tracking writer reads the store");
}

#[tokio::test]
async fn the_shapes_of_an_index_not_read_as_settled_are_read_from_the_store() {
	let db = TestDb::new(UNSETTLED).await;
	let ds = db.datastore();
	let mut reads = Vec::new();
	for _ in 0..2 {
		let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
		reads.push(age_shapes(&reader).await.1);
		reader.cancel().await.expect("cancelled");
	}
	assert!(!Arc::ptr_eq(&reads[0], &reads[1]), "nothing is kept for an index not known online");
}

#[tokio::test]
async fn a_document_write_that_records_a_shape_moves_the_epoch_the_shapes_are_kept_under() {
	let db = TestDb::new(INDEXED).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	settle_online(ds, &owner).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (_, before) = age_shapes(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(!before.records_all([(0, surrealdb_datastore::index_shapes::ValueShape::Array)]));
	let epoch_before = epoch(ds).await;

	run(ds, &owner, "CREATE person:2 SET age = [1, 2], email = 'b@x.io'").await;
	assert!(epoch(ds).await > epoch_before, "the recording stamps the table's version");
	settle_online(ds, &owner).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (_, after) = age_shapes(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(
		after.records_all([(0, surrealdb_datastore::index_shapes::ValueShape::Array)]),
		"the new shape is read, not the old epoch's"
	);
}

#[tokio::test]
async fn a_writer_that_wrote_a_shape_reads_its_own_after_a_rollback_and_keeps_none() {
	use surrealdb_datastore::index_shapes::{ShapeRecorder, ValueShape};
	let db = TestDb::new(INDEXED).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	settle_online(ds, &owner).await;
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (age, first) = age_shapes(&reader).await;
	let (ns, dbid) = (
		reader.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id,
		reader.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id,
	);
	reader.cancel().await.expect("cancelled");

	// A build's recording stamps no catalog version, so the writer still names its epochs and
	// only what it wrote itself keeps the epoch's shapes from answering for it.
	let person = TableName::from("person");
	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let build = |shape| [(0u16, shape)];
	writer
		.record_index_shapes(
			ns,
			dbid,
			&person,
			age,
			&build(ValueShape::Array),
			ShapeRecorder::Build,
		)
		.await
		.unwrap();
	assert!(writer.catalog_epoch().is_some(), "a build's recording stamps no version");
	writer.new_save_point().await.unwrap();
	writer
		.record_index_shapes(
			ns,
			dbid,
			&person,
			age,
			&[(1, ValueShape::Whole)],
			ShapeRecorder::Build,
		)
		.await
		.unwrap();
	writer.rollback_to_save_point().await.unwrap();
	let (_, read) = age_shapes(&writer).await;
	assert!(
		read.records_all(build(ValueShape::Array)),
		"the writer reads the shape it wrote before the save point"
	);
	assert!(
		!read.records_all([(1, ValueShape::Whole)]),
		"the writer does not read the shape its rollback undid"
	);
	writer.cancel().await.expect("cancelled");

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let (_, after) = age_shapes(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(Arc::ptr_eq(&first, &after), "a writer that wrote shapes keeps none for the epoch");
}

const REFERENCED: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE refd SCHEMALESS;
	DEFINE FIELD target ON refd TYPE record<person>;
";

/// Whether a transaction's reference purge gate says `person` may have incoming references.
async fn purge_gate(txn: &crate::kvs::Transaction) -> bool {
	let person = TableName::from("person");
	let ns = txn.get_ns_by_name("test", None).await.expect("read").expect("defined").namespace_id;
	let db =
		txn.get_db_by_name("test", "test", None).await.expect("read").expect("defined").database_id;
	txn.table_may_have_incoming_references(ns, db, &person).await.expect("read")
}

async fn catalog_state(
	txn: &crate::kvs::Transaction,
) -> Arc<surrealdb_datastore::tx::DatabaseCatalogState> {
	let ns = txn.get_ns_by_name("test", None).await.expect("read").expect("defined").namespace_id;
	let db =
		txn.get_db_by_name("test", "test", None).await.expect("read").expect("defined").database_id;
	txn.database_catalog_state(ns, db).await.expect("read")
}

#[tokio::test]
async fn a_deletes_catalog_state_is_served_from_the_epoch_until_a_definition_changes() {
	let db = TestDb::new(REFERENCED).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	let may = purge_gate(&reader).await;
	let state = catalog_state(&reader).await;
	reader.cancel().await.expect("cancelled");
	assert!(!may, "no reference field targets `person`");

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	assert!(Arc::ptr_eq(&state, &catalog_state(&reader).await), "a reader of the epoch is served");
	reader.cancel().await.expect("cancelled");
	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	assert!(Arc::ptr_eq(&state, &catalog_state(&writer).await), "a writer of the epoch is served");
	writer.cancel().await.expect("cancelled");

	// A backend that tracks plain reads keeps the writer's catalog reads real.
	let tracked = ds
		.transaction_at(TransactionType::Write, surrealdb_kvs::Isolation::Serializable)
		.await
		.expect("a transaction");
	let read = catalog_state(&tracked).await;
	tracked.cancel().await.expect("cancelled");
	assert!(!Arc::ptr_eq(&state, &read), "a tracking writer reads the store");

	// A reference field moves the epoch, and the next delete's gate sees it.
	run(ds, &owner, "DEFINE FIELD OVERWRITE target ON refd TYPE record<person> REFERENCE").await;
	let writer = ds.transaction(TransactionType::Write).await.expect("a transaction");
	let may = purge_gate(&writer).await;
	let after = catalog_state(&writer).await;
	writer.cancel().await.expect("cancelled");
	assert!(may, "a table a reference field targets may have incoming references");
	assert_ne!(*state, *after, "the stamp of the changed table is in the state");
}

#[tokio::test]
async fn a_writer_that_changed_a_field_reads_its_own_catalog_for_its_gate_and_publishes_none() {
	let db = TestDb::new(REFERENCED).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	assert!(!purge_gate(&reader).await);
	reader.cancel().await.expect("cancelled");

	let writer = Arc::new(ds.transaction(TransactionType::Write).await.expect("a transaction"));
	let sql = "DEFINE FIELD OVERWRITE target ON refd TYPE record<person> REFERENCE";
	let request = crate::kvs::QueryRequest::new(sql, &owner).with_transaction(Arc::clone(&writer));
	for response in ds.run(request).await.expect("the request runs") {
		response.result.expect("the field is defined");
	}
	assert!(
		purge_gate(&writer).await,
		"a writer reads the field it defined, not the epoch's catalog"
	);
	writer.cancel().await.expect("cancelled");

	let reader = ds.transaction(TransactionType::Read).await.expect("a transaction");
	assert!(!purge_gate(&reader).await, "nothing the writer wrote is served");
	reader.cancel().await.expect("cancelled");
}
