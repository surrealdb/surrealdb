//! What the SQL front ends are told of the columns a table declares, and to whom.

use std::sync::Arc;

use surrealdb_sqlplan::{BinderCatalog, MemoryCatalog, SqlType};

use crate::dbs::Session;
use crate::iam::{Level, Role};
use crate::kvs::TransactionType::Write;
use crate::kvs::{Datastore, QueryRequest, QuerySource};

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// A datastore whose `test`/`test` database has the schemafull table `emp` with the columns `name`
/// and `note`, and the schemaless table `loose`.
async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::builder()
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("the datastore opens");
	let setup = "DEFINE NAMESPACE test; DEFINE DATABASE test;
		DEFINE TABLE emp SCHEMAFULL;
		DEFINE FIELD name ON emp TYPE string;
		DEFINE FIELD note ON emp TYPE option<string>;
		DEFINE TABLE loose SCHEMALESS;
		CREATE emp:1 SET name = 'Ann' RETURN NONE;";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("a setup statement");
	}
	ds
}

/// The error a request for `source` as `session` answers, rendered.
async fn refusal(ds: &Datastore, session: &Session, source: QuerySource<'_>) -> String {
	match ds.run(QueryRequest::new(source, session)).await {
		Err(error) => error.to_string(),
		Ok(mut results) => match results.remove(0).result {
			Err(error) => error.to_string(),
			Ok(value) => panic!("expected the request to fail, got {value:?}"),
		},
	}
}

/// The catalog a session's `SELECT * FROM emp` is described by.
#[cfg(feature = "postgres")]
async fn pgsql_catalog(ds: &Datastore, session: &Session) -> MemoryCatalog {
	use surrealdb_pgsql::ast::stmt::Statement;
	let stmts = surrealdb_pgsql::parse_statements("SELECT * FROM emp").expect("the text parses");
	let Statement::Select(select) = &stmts[0] else {
		panic!("expected a SELECT");
	};
	ds.pgsql_catalog(session, None, select).await.expect("the catalog is read")
}

#[cfg(feature = "mysql")]
async fn mysql_catalog(ds: &Datastore, session: &Session) -> MemoryCatalog {
	use surrealdb_mysql::ast::stmt::Statement;
	let stmts = surrealdb_mysql::parse_statements("SELECT * FROM emp").expect("the text parses");
	let Statement::Select(select) = &stmts[0] else {
		panic!("expected a SELECT");
	};
	ds.mysql_catalog(session, None, select).await.expect("the catalog is read")
}

/// The catalogs of both dialects, which are described alike.
async fn catalogs(ds: &Datastore, session: &Session) -> Vec<(&'static str, MemoryCatalog)> {
	let mut catalogs = Vec::new();
	#[cfg(feature = "postgres")]
	catalogs.push(("pgsql", pgsql_catalog(ds, session).await));
	#[cfg(feature = "mysql")]
	catalogs.push(("mysql", mysql_catalog(ds, session).await));
	catalogs
}

/// The columns the catalog declares `emp` to have, or `None` when it does not describe the table
/// as one that declares all its columns.
fn declared_columns(catalog: &MemoryCatalog) -> Option<Vec<String>> {
	catalog.get_table(None, "emp").filter(|table| table.closed).map(|table| table.column_names())
}

fn record_session() -> Session {
	Session::for_record(
		"test",
		"test",
		"access",
		surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("users", "alice")),
	)
}

/// The columns of a table are described to the sessions that may view the definitions of tables,
/// and to no other: a name that is an error or a column that is read as NONE would tell a
/// session what the table declares.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn only_a_session_that_may_view_tables_is_told_their_columns() {
	let ds = datastore().await;
	let declared = Some(["id", "name", "note"].map(str::to_owned).to_vec());
	let viewer = Session::viewer().with_ns("test").with_db("test");
	let mut database_viewer =
		Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Viewer);
	database_viewer.db = Some("test".to_owned());
	let mut namespace_editor =
		Session::for_level(Level::Namespace("test".to_owned()), Role::Editor);
	namespace_editor.db = Some("test".to_owned());
	for (who, session, expected) in [
		("owner", session(), declared.clone()),
		("viewer", viewer, declared.clone()),
		("database viewer", database_viewer, declared.clone()),
		("namespace editor", namespace_editor, declared),
		("record user", record_session(), None),
	] {
		for (dialect, catalog) in catalogs(&ds, &session).await {
			assert_eq!(declared_columns(&catalog), expected, "{dialect}, {who}");
		}
	}
}

/// The type of a column a client reads is told to every session that is served, as the column's
/// name is: it is the column the statement named.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn every_session_that_is_served_is_told_the_types_of_the_columns_it_reads() {
	let ds = datastore().await;
	for session in [session(), record_session()] {
		for (dialect, catalog) in catalogs(&ds, &session).await {
			assert_eq!(catalog.column_type(None, "emp", "name"), SqlType::Text, "{dialect}");
		}
	}
}

/// A table that is not `SCHEMAFULL`, or is a view, does not declare all its columns.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_table_that_does_not_declare_all_its_columns_is_not_described_as_closed() {
	let ds = datastore().await;
	let setup = "DEFINE TABLE vw AS SELECT name FROM emp;
		DEFINE FIELD name ON loose TYPE string;";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("a setup statement");
	}
	for table in ["loose", "vw"] {
		use surrealdb_pgsql::ast::stmt::Statement;
		let sql = format!("SELECT * FROM {table}");
		let stmts = surrealdb_pgsql::parse_statements(&sql).expect("the text parses");
		let Statement::Select(select) = &stmts[0] else {
			panic!("expected a SELECT");
		};
		let catalog =
			ds.pgsql_catalog(&session(), None, select).await.expect("the catalog is read");
		assert!(catalog.get_table(None, table).is_none_or(|table| !table.closed), "{table}");
	}
}

/// The catalog the tables of a request are read into names the database they are read from as its
/// current database, which is what the name resolution of both dialects holds a qualified table
/// name against: a catalog without one accepts any qualifier.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn the_catalog_of_a_request_names_the_database_it_was_read_from() {
	use super::sql_links::Purpose;
	let ds = datastore().await;
	let tables = ["emp".to_owned()];
	for (database, expected) in [(None, "test"), (Some("other"), "other")] {
		for purpose in [Purpose::Lowering, Purpose::Describing] {
			let described = ds
				.read_tables(&session(), database, None, &tables, purpose, true)
				.await
				.expect("the tables are read")
				.expect("the session is served");
			assert_eq!(described.catalog.current_database(), Some(expected), "{database:?}");
		}
	}
}

/// A table that a database other than the session's names is refused, where it would otherwise be
/// read as the table of the session's own database that has its name. A request that reads the
/// columns a table declares is lowered against a catalog that names the session's database.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn a_table_of_another_database_is_refused() {
	let ds = datastore().await;
	for (dialect, other, own) in [
		#[cfg(feature = "postgres")]
		(
			"pgsql",
			QuerySource::pgsql("SELECT name FROM otherdb.public.emp"),
			QuerySource::pgsql("SELECT name FROM test.public.emp"),
		),
		#[cfg(feature = "mysql")]
		(
			"mysql",
			QuerySource::mysql("SELECT name FROM otherdb.emp"),
			QuerySource::mysql("SELECT name FROM test.emp"),
		),
	] {
		let refused = refusal(&ds, &session(), other).await;
		assert!(refused.contains("cross-database queries not supported"), "{dialect}: {refused}");
		let results = ds
			.run(QueryRequest::new(own, &session()))
			.await
			.unwrap_or_else(|error| panic!("{dialect}: the request is refused: {error}"));
		for result in results {
			result.result.unwrap_or_else(|error| panic!("{dialect}: {error}"));
		}
	}
}

/// A session whose level cannot reach the database it selected is refused as the executor refuses
/// it, whatever column the statement names: the lookup of the table reads nothing for it.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn a_session_outside_its_scope_is_refused_rather_than_told_a_column() {
	let ds = datastore().await;
	let mut elsewhere =
		Session::for_level(Level::Database("other".to_owned(), "other".to_owned()), Role::Owner);
	elsewhere.ns = Some("test".to_owned());
	elsewhere.db = Some("test".to_owned());
	let surql = refusal(&ds, &elsewhere, QuerySource::text("SELECT nosuch FROM emp")).await;
	assert!(surql.contains("permission to change to the test namespace"), "{surql}");
	#[cfg(feature = "postgres")]
	assert_eq!(refusal(&ds, &elsewhere, QuerySource::pgsql("SELECT nosuch FROM emp")).await, surql);
	#[cfg(feature = "mysql")]
	assert_eq!(refusal(&ds, &elsewhere, QuerySource::mysql("SELECT nosuch FROM emp")).await, surql);
}

/// A column the caller's own transaction has defined is a column of the statements it runs.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn a_column_the_callers_transaction_defines_is_seen() {
	let ds = datastore().await;
	let session = session();
	let txn = Arc::new(ds.transaction(Write).await.expect("a write transaction"));
	let define = QueryRequest::new(
		QuerySource::text("DEFINE FIELD extra ON emp TYPE option<int>"),
		&session,
	)
	.with_transaction(Arc::clone(&txn));
	for result in ds.run(define).await.expect("the definition runs") {
		result.result.expect("the definition");
	}
	for source in [
		#[cfg(feature = "postgres")]
		QuerySource::pgsql("SELECT name, extra FROM emp"),
		#[cfg(feature = "mysql")]
		QuerySource::mysql("SELECT name, extra FROM emp"),
	] {
		let request = QueryRequest::new(source, &session).with_transaction(Arc::clone(&txn));
		for result in ds.run(request).await.expect("the request runs") {
			result.result.expect("the statement reads the column");
		}
	}
	txn.cancel().await.expect("the transaction cancels");
}

/// The statements of a request are all lowered before any of them runs, so a table the request
/// drops and creates again is read by the columns the request gives it, not by the columns it had.
/// However many answers its definitions give, the select answers last.
#[cfg(any(feature = "postgres", feature = "mysql"))]
#[tokio::test]
async fn a_table_created_again_in_the_request_is_read_by_its_new_columns() {
	use surrealdb_types::ToSql;
	let ds = datastore().await;
	for (dialect, source) in [
		#[cfg(feature = "postgres")]
		(
			"pgsql",
			QuerySource::pgsql(
				"DROP TABLE emp; CREATE TABLE emp (w INT); \
				 INSERT INTO emp (id, w) VALUES (1, 7); SELECT w FROM emp",
			),
		),
		#[cfg(feature = "mysql")]
		(
			"mysql",
			QuerySource::mysql(
				"DROP TABLE emp; CREATE TABLE emp (w INT); \
				 INSERT INTO emp (id, w) VALUES (1, 7); SELECT w FROM emp",
			),
		),
	] {
		let results = ds
			.run(QueryRequest::new(source, &session()))
			.await
			.unwrap_or_else(|error| panic!("{dialect}: the request is refused: {error}"));
		let mut answers = Vec::new();
		for result in results {
			answers.push(result.result.unwrap_or_else(|error| panic!("{dialect}: {error}")));
		}
		let last = answers.pop().expect("the select answers");
		assert_eq!(last.to_sql(), "[{ w: 7i32 }]", "{dialect}");
	}
}
