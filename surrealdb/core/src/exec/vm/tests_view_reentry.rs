//! A view's maintenance runs each row it writes through the view table's own
//! order, which maintains the views over that table in turn. A catalog an
//! earlier release wrote can hold a view that reads itself, directly or through
//! other views, which no `DEFINE TABLE` here produces, so the catalog is written
//! directly: a write that reaches such a view fails, naming the views it reads
//! itself through, and the datastore goes on serving.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;

use crate::catalog::ViewDefinition;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::exec::operators::test_util::TestDb;
use crate::key::schema::ForeignTableKey;
use crate::kvs::TransactionType::Write;
use crate::val::TableName;

/// Make the materialized view `view` read `source` besides the tables it reads,
/// as a catalog an earlier release wrote can hold it: the definition reads
/// `source`, it is registered on every table it reads, and each of those takes
/// a fresh views stamp, so a write to any of them maintains the definition.
async fn also_reads(db: &TestDb, view: &str, source: &str) {
	let txn = db.datastore().transaction(Write).await.unwrap();
	let database = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let (ns, dbid) = (database.namespace_id, database.database_id);
	let view = TableName::from(view);
	let mut definition = (*txn.get_tb(ns, dbid, &view, None).await.unwrap().unwrap()).clone();
	let Some(ViewDefinition::Materialized {
		tables,
		..
	}) = &mut definition.view
	else {
		panic!("`{view}` must be a materialized view");
	};
	tables.push(TableName::from(source));
	let tables = tables.clone();
	txn.replace_tb("test", "test", &definition).await.unwrap();
	let stored = definition.to_stored();
	for table in &tables {
		let registration = ForeignTableKey {
			ns,
			db: dbid,
			tb: Cow::Borrowed(table),
			ft: Cow::Borrowed(&view),
		};
		txn.set_key(&registration, &stored).await.unwrap();
		let mut read = (*txn.get_tb(ns, dbid, table, None).await.unwrap().unwrap()).clone();
		read.cache_tables_ts = uuid::Uuid::now_v7();
		txn.replace_tb("test", "test", &read).await.unwrap();
	}
	txn.commit().await.unwrap();
}

/// Run `sql` as the owner, answering its one statement's failure as text.
async fn failure(db: &TestDb, sql: &str) -> String {
	let mut responses = db.datastore().execute(sql, &TestDb::owner(), None).await.unwrap();
	assert_eq!(responses.len(), 1, "`{sql}` is one statement");
	match responses.remove(0).result {
		Ok(value) => panic!("`{sql}` must fail, and answered {value:?}"),
		Err(error) => error.to_string(),
	}
}

/// Run `sql` as the owner, answering its one statement's value as SurrealQL.
async fn value(db: &TestDb, sql: &str) -> String {
	use surrealdb_types::ToSql as _;
	let mut responses = db.datastore().execute(sql, &TestDb::owner(), None).await.unwrap();
	assert_eq!(responses.len(), 1, "`{sql}` is one statement");
	responses.remove(0).result.unwrap().to_sql()
}

#[tokio::test]
async fn a_write_reaching_a_view_that_reads_itself_fails() {
	let db = TestDb::new("DEFINE TABLE src SCHEMALESS; DEFINE TABLE s AS SELECT n FROM src;").await;
	also_reads(&db, "s", "s").await;
	assert_eq!(
		failure(&db, "CREATE src:1 SET n = 1 RETURN NONE").await,
		"Cannot maintain the view `s`: it reads itself"
	);
	assert_eq!(value(&db, "SELECT * FROM src").await, "[]");
	assert_eq!(value(&db, "SELECT * FROM s").await, "[]");
}

#[tokio::test]
async fn a_write_reaching_a_view_through_the_views_over_it_fails_naming_them() {
	let db = TestDb::new(
		"DEFINE TABLE src SCHEMALESS;
		 DEFINE TABLE v1 AS SELECT n FROM src;
		 DEFINE TABLE v2 AS SELECT n FROM v1;
		 DEFINE TABLE v3 AS SELECT n FROM v2;",
	)
	.await;
	also_reads(&db, "v1", "v3").await;
	assert_eq!(
		failure(&db, "CREATE src:1 SET n = 1 RETURN NONE").await,
		"Cannot maintain the view `v1`: it reads itself through `v3`, which reads `v2`"
	);
	for table in ["src", "v1", "v2", "v3"] {
		assert_eq!(value(&db, &format!("SELECT * FROM {table}")).await, "[]", "`{table}`");
	}
}

/// Two views over one source and a third over both reach the third twice, down
/// different paths, and neither while the other is in flight: no view reads
/// itself, and the write maintains them all.
#[tokio::test]
async fn a_view_reached_down_two_paths_is_maintained_by_both() {
	let db = TestDb::new(
		"DEFINE TABLE src SCHEMALESS;
		 DEFINE TABLE a AS SELECT n FROM src;
		 DEFINE TABLE b AS SELECT n FROM src;
		 DEFINE TABLE both AS SELECT count() AS c FROM a, b GROUP ALL;",
	)
	.await;
	db.run("CREATE src:1 SET n = 1 RETURN NONE").await;
	assert_eq!(value(&db, "SELECT c FROM both").await, "[{ c: 2 }]");
}
