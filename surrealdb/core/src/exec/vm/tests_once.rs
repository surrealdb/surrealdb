//! A statement in value position that the plan runs once ([`NestedStrategy::Once`]) is run at
//! the first evaluation that reaches it, and its value answers every later one, while a statement
//! the plan runs for every row is run for every row. Each test runs statements through the
//! datastore, as a session would, in a database of its own, and counts the statements in scalar
//! position the run evaluated rather than answered from a kept value
//! ([`super::nested::STATEMENTS_RUN`]), which the process keeps by database: a count is the test's
//! whichever thread a run took, and whatever tests run beside it.
//!
//! [`NestedStrategy::Once`]: crate::exec::ir::expr::ops::NestedStrategy::Once

use surrealdb_types::ToSql;

use super::nested::STATEMENTS_RUN;
use crate::exec::operators::test_util::TestDb;

const RECORDS: &str = "
	DEFINE TABLE record SCHEMALESS;
	DEFINE FIELD number ON record TYPE int;
	DEFINE INDEX record_number ON record FIELDS number;
	CREATE record:1 SET number = 4;
	CREATE record:2 SET number = 9;
	CREATE record:3 SET number = 2;
	CREATE record:4 SET number = 7;
";

/// The session of the database `db`, which only one test uses.
fn session(db: &str) -> crate::dbs::Session {
	crate::dbs::Session::owner().with_ns("test").with_db(db)
}

/// The runs of a statement in scalar position counted against the database `db`.
fn counted(db: &str) -> usize {
	STATEMENTS_RUN
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.get(db)
		.copied()
		.unwrap_or(0)
}

/// The records of the database `db`.
async fn records(db: &str) -> TestDb {
	let test = TestDb::new("").await;
	let define = format!("DEFINE DATABASE {db};");
	let namespace = crate::dbs::Session::owner().with_ns("test");
	for response in test.datastore().execute(&define, &namespace, None).await.expect("runs") {
		response.result.expect("the database is defined");
	}
	for response in test.datastore().execute(RECORDS, &session(db), None).await.expect("runs") {
		response.result.expect("the records are written");
	}
	test
}

/// The value `sql` answers in the database `db`, as SurrealQL, and how many statements in scalar
/// position its run evaluated.
async fn runs(test: &TestDb, db: &str, sql: &str) -> (String, usize) {
	let before = counted(db);
	let mut responses = test.datastore().execute(sql, &session(db), None).await.expect("runs");
	let value = responses.remove(0).result.expect("answers");
	(value.to_sql(), counted(db) - before)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uncorrelated_statement_in_value_position_is_run_once_per_statement() {
	const DB: &str = "once_uncorrelated";
	let test = records(DB).await;
	for (sql, expected) in [
		(
			"SELECT id, (SELECT VALUE math::max(number) FROM record GROUP ALL)[0] AS m FROM record \
			 LIMIT 100",
			"[{ id: record:1, m: 9 }, { id: record:2, m: 9 }, { id: record:3, m: 9 }, { id: \
			 record:4, m: 9 }]",
		),
		(
			"SELECT VALUE (SELECT VALUE math::max(number) FROM record GROUP ALL)[0] FROM record",
			"[9, 9, 9, 9]",
		),
		(
			"SELECT VALUE (SELECT VALUE math::max(number) FROM record GROUP ALL) FROM record LIMIT 2",
			"[[9], [9]]",
		),
		(
			"SELECT VALUE (SELECT count() FROM record GROUP ALL)[0].count FROM record ORDER BY id",
			"[4, 4, 4, 4]",
		),
		(
			"SELECT VALUE id FROM record WHERE number = (SELECT VALUE math::max(number) FROM record GROUP ALL)[0]",
			"[record:2]",
		),
	] {
		let (value, run) = runs(&test, DB, sql).await;
		assert_eq!(value, expected, "{sql}");
		assert_eq!(run, 1, "{sql}");
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_correlated_statement_in_value_position_is_run_for_every_row() {
	const DB: &str = "once_correlated";
	let test = records(DB).await;
	let sql = "SELECT VALUE (SELECT VALUE number FROM record WHERE number = $parent.number)[0] \
		FROM record ORDER BY id";
	let (value, run) = runs(&test, DB, sql).await;
	assert_eq!(value, "[4, 9, 2, 7]");
	assert_eq!(run, 4);
}
