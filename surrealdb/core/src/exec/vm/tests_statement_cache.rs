//! The cache of compiled top-level statements. Each test runs requests through a datastore of its
//! own, whose [`Datastore::statement_cache_stats`] count only that test's requests.
//!
//! A read-only statement of a request is compiled once per statement text and frame; the
//! compilation reads the request's variables when it runs, so requests that bind other values to
//! them are served the one kept. A catalog change the compilation depends on makes the kept one
//! stale, and the next request compiles again.

use std::sync::Arc;

use surrealdb_cnf::ConfigMap;
use surrealdb_types::{RecordId, ToSql, Variables};

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::{Datastore, StatementCacheStats};

const PEOPLE: &str = "
	DEFINE TABLE person SCHEMALESS;
	CREATE person:1 SET name = 'ann', age = 31;
	CREATE person:2 SET name = 'bob', age = 25;
	CREATE person:3 SET name = 'cat', age = 40;
	CREATE person:4 SET name = 'dan', age = 25;
";

/// `person:n` as the variable `id`.
fn person(n: i64) -> Variables {
	let mut vars = Variables::new();
	vars.insert("id", RecordId::new("person", n));
	vars
}

/// `n` as the variable `n`.
fn number(n: i64) -> Variables {
	let mut vars = Variables::new();
	vars.insert("n", n);
	vars
}

/// The value the first statement of `sql` answers, as SurrealQL; a failing statement fails the
/// test.
async fn answer(ds: &Datastore, session: &Session, sql: &str, vars: Option<Variables>) -> String {
	let mut responses = ds.execute(sql, session, vars).await.expect("the request runs");
	responses.remove(0).result.expect("the statement answers").to_sql()
}

/// The value the last statement of `sql` answers, as SurrealQL.
async fn last_answer(ds: &Datastore, session: &Session, sql: &str) -> String {
	let responses = ds.execute(sql, session, None).await.expect("the request runs");
	responses.into_iter().last().expect("a statement").result.expect("answers").to_sql()
}

/// `after` less `before`.
fn since(before: StatementCacheStats, after: StatementCacheStats) -> StatementCacheStats {
	StatementCacheStats {
		hits: after.hits - before.hits,
		misses: after.misses - before.misses,
		invalidations: after.invalidations - before.invalidations,
	}
}

fn stats(hits: u64, misses: u64, invalidations: u64) -> StatementCacheStats {
	StatementCacheStats {
		hits,
		misses,
		invalidations,
	}
}

#[tokio::test]
async fn a_statement_is_served_the_compilation_kept_for_it_under_other_values() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	assert_eq!(now(), stats(0, 0, 0));
	for (n, name) in [(1, "ann"), (2, "bob"), (3, "cat"), (4, "dan"), (2, "bob")] {
		let got =
			answer(ds, &owner, "SELECT name FROM type::record('person', $n)", Some(number(n)))
				.await;
		assert_eq!(got, format!("[{{ name: '{name}' }}]"), "`$n` = {n}");
	}
	assert_eq!(now(), stats(4, 1, 0));
	for (n, name) in [(3, "cat"), (1, "ann"), (3, "cat")] {
		let sql = "SELECT VALUE name FROM type::record('person', $n)";
		let got = answer(ds, &owner, sql, Some(number(n))).await;
		assert_eq!(got, format!("['{name}']"), "`$n` = {n}");
	}
	assert_eq!(now(), stats(6, 2, 0));
}

#[tokio::test]
async fn a_source_named_by_a_variable_is_compiled_with_its_request() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let sql = "SELECT VALUE name FROM $src ORDER BY name";
	let bind = |value: surrealdb_types::Value| {
		let mut vars = Variables::new();
		vars.insert("src", value);
		Some(vars)
	};
	let record = |n: i64| surrealdb_types::Value::RecordId(RecordId::new("person", n));
	let table = surrealdb_types::Value::Table(surrealdb_types::Table::new("person"));
	let array =
		surrealdb_types::Value::Array(surrealdb_types::Array::from(vec![record(4), record(1)]));
	assert_eq!(answer(ds, &owner, sql, bind(record(2))).await, "['bob']");
	assert_eq!(answer(ds, &owner, sql, bind(table)).await, "['ann', 'bob', 'cat', 'dan']");
	assert_eq!(answer(ds, &owner, sql, bind(array)).await, "['ann', 'dan']");
	assert_eq!(answer(ds, &owner, sql, bind(record(3))).await, "['cat']");
	// Main plans `FROM $src` for the table or record the value names.
	assert_eq!(now(), stats(0, 0, 0));
}

#[tokio::test]
async fn a_value_a_let_binds_is_the_value_the_statement_reads() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	for (min, expected) in [
		(30, "['ann', 'cat']"),
		(26, "['ann', 'cat']"),
		(40, "['cat']"),
		(0, "['ann', 'bob', 'cat', 'dan']"),
		(30, "['ann', 'cat']"),
	] {
		let sql = format!(
			"LET $min = {min}; SELECT VALUE name FROM person WHERE age >= $min ORDER BY name"
		);
		assert_eq!(last_answer(ds, &owner, &sql).await, expected, "`$min` = {min}");
	}
}

#[tokio::test]
async fn a_write_is_compiled_with_its_request() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	for n in 10..14 {
		let sql = "CREATE type::record('person', $n) SET name = 'x'";
		answer(ds, &owner, sql, Some(number(n))).await;
		answer(ds, &owner, "UPDATE type::record('person', $n) SET age = 1", Some(number(n))).await;
		answer(ds, &owner, "DELETE type::record('person', $n)", Some(number(n))).await;
	}
	assert_eq!(now(), stats(0, 0, 0));
}

#[tokio::test]
async fn a_field_defined_or_removed_changes_what_a_kept_statement_reads() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let read =
		|n: i64| answer(ds, &owner, "SELECT * FROM type::record('person', $n)", Some(number(n)));
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	assert_eq!(read(2).await, "[{ age: 25, id: person:2, name: 'bob' }]");
	let before = now();
	assert_eq!(before, stats(1, 1, 0));

	db.run("DEFINE FIELD twice ON person COMPUTED age * 2").await;
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann', twice: 62 }]");
	assert_eq!(read(2).await, "[{ age: 25, id: person:2, name: 'bob', twice: 50 }]");
	let after = since(before, now());
	assert_eq!(after.hits + after.misses, 2, "{after:?}");

	db.run("REMOVE FIELD twice ON person").await;
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	assert_eq!(read(3).await, "[{ age: 40, id: person:3, name: 'cat' }]");
}

const ACCESS: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
	CREATE doc:1 SET owner = user:alice;
	CREATE doc:2 SET owner = user:bob;
";

fn record_user(user: &str) -> Session {
	let rid = surrealdb_types::Value::RecordId(RecordId::new("user", user));
	Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn a_table_redefined_or_removed_changes_what_a_kept_statement_is_allowed() {
	let db = TestDb::new_with_auth(ACCESS).await;
	let ds = db.datastore();
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let alice = record_user("alice");
	let sql = "SELECT VALUE id FROM type::record('doc', $n)";
	let doc = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("n", n);
		vars
	};
	let read = |n: i64| answer(ds, &alice, sql, Some(doc(n)));
	assert_eq!(read(1).await, "[doc:1]");
	assert_eq!(read(2).await, "[doc:2]");
	assert_eq!(now(), stats(1, 1, 0));

	db.run("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth")
		.await;
	assert_eq!(read(1).await, "[doc:1]");
	assert_eq!(read(2).await, "[]");

	db.run("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS NONE").await;
	assert_eq!(read(1).await, "[]");

	db.run("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS FULL").await;
	assert_eq!(read(2).await, "[doc:2]");

	db.run("REMOVE TABLE doc").await;
	let removed = ds.execute(sql, &alice, Some(doc(1))).await.expect("the request runs").remove(0);
	let error = removed.result.expect_err("a table that is not defined is not read");
	assert!(error.message().contains("does not exist"), "{error}");
}

#[tokio::test]
async fn a_table_defined_after_a_statement_that_failed_on_it_is_read_by_it() {
	let db = TestDb::new("").await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let sql = "SELECT * FROM type::record('late', $n)";
	let missing = |what: &str| format!("{what}: the table does not exist");
	let read = || async {
		match ds
			.execute(sql, &owner, Some(number(1)))
			.await
			.expect("the request runs")
			.remove(0)
			.result
		{
			Ok(value) => value.to_sql(),
			Err(e) => {
				assert!(e.message().contains("does not exist"), "{e}");
				missing("error")
			}
		}
	};
	assert_eq!(read().await, missing("error"));
	db.run(
		"DEFINE TABLE late SCHEMAFULL; DEFINE FIELD v ON late TYPE int DEFAULT 7; CREATE late:1",
	)
	.await;
	assert_eq!(read().await, "[{ id: late:1, v: 7 }]");
	db.run("REMOVE TABLE late").await;
	assert_eq!(read().await, missing("error"));
}

#[tokio::test]
async fn a_database_with_another_name_is_served_its_own_compilation() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	db.run("DEFINE DATABASE other; USE DB other; DEFINE TABLE person SCHEMALESS; CREATE person:1 SET name = 'zed'")
		.await;
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let other = Session::owner().with_ns("test").with_db("other");
	let sql = "SELECT VALUE name FROM type::record('person', $n)";
	assert_eq!(answer(ds, &owner, sql, Some(number(1))).await, "['ann']");
	assert_eq!(answer(ds, &other, sql, Some(number(1))).await, "['zed']");
	assert_eq!(answer(ds, &owner, sql, Some(number(1))).await, "['ann']");
	assert_eq!(now(), stats(1, 2, 0));
}

/// Statements run with their variables, as `(statement, variable name, values)`.
const READS: &[(&str, &str, &[i64])] = &[
	("SELECT * FROM $id", "id", &[1, 2, 3, 4, 9]),
	("SELECT VALUE name FROM $id", "id", &[4, 1]),
	("SELECT * FROM ONLY $id", "id", &[3, 9, 1]),
	("SELECT name FROM $id LIMIT 1 FETCH friends", "id", &[2, 9]),
	("SELECT name, age FROM type::record('person', $n)", "n", &[1, 2, 3, 4, 9]),
	("SELECT VALUE id FROM person WHERE age = $n ORDER BY id", "n", &[25, 31, 99]),
	("SELECT * FROM person WHERE age > $n ORDER BY age, id LIMIT 2", "n", &[0, 26, 40]),
	("SELECT VALUE array::len(<array> $n) FROM ONLY $id", "id", &[1]),
	("SELECT count() FROM person WHERE age = $n GROUP ALL", "n", &[25, 7]),
	("RETURN $n + 1", "n", &[1, 2]),
	("SELECT VALUE (SELECT VALUE name FROM $id) FROM person:1", "id", &[2, 3]),
	("SELECT VALUE name FROM person ORDER BY name LIMIT $n", "n", &[1, 3]),
];

/// The answers of every statement of [`READS`] against a datastore built with `config`.
async fn run_reads(config: ConfigMap) -> (Vec<String>, StatementCacheStats) {
	let db = TestDb::new_with_config(PEOPLE, config).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let mut answers = Vec::new();
	for round in 0..2 {
		for (sql, name, values) in READS {
			for value in *values {
				let mut vars = Variables::new();
				match *name {
					"id" => vars.insert("id", RecordId::new("person", *value)),
					_ => vars.insert("n", *value),
				}
				let mut responses = ds.execute(sql, &owner, Some(vars)).await.expect("runs");
				let result = responses.remove(0).result;
				let shown = match result {
					Ok(value) => value.to_sql(),
					Err(e) => format!("error: {e}"),
				};
				answers.push(format!("round {round}: {sql} [{value}] => {shown}"));
			}
		}
	}
	(answers, now())
}

#[tokio::test]
async fn the_answers_are_the_same_with_the_cache_on_and_off() {
	let (on, kept) = run_reads(ConfigMap::empty()).await;
	let (off, none) =
		run_reads(ConfigMap::empty().with_key_value("statement_cache_size", "0")).await;
	assert_eq!(on.len(), off.len());
	for (on, off) in on.iter().zip(&off) {
		assert_eq!(on, off);
	}
	assert!(kept.hits > kept.misses, "{kept:?}");
	assert_eq!(none.hits, 0, "{none:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_and_catalog_changes_each_read_a_consistent_catalog() {
	const READERS: usize = 6;
	const ROUNDS: i64 = 60;
	let db = Arc::new(TestDb::new(PEOPLE).await);
	let mut tasks = tokio::task::JoinSet::new();
	for reader in 0..READERS {
		let db = Arc::clone(&db);
		tasks.spawn(async move {
			let (ds, owner) = (db.datastore(), TestDb::owner());
			for round in 0..ROUNDS {
				let n = (round + reader as i64) % 4 + 1;
				let rows = ds
					.execute("SELECT * FROM type::record('person', $n)", &owner, Some(number(n)))
					.await
					.expect("the request runs")
					.remove(0)
					.result
					.expect("the statement answers")
					.to_sql();
				let age = [31, 25, 40, 25][(n - 1) as usize];
				let plain = format!("id: person:{n}");
				assert!(rows.contains(&plain), "person:{n} is read: {rows}");
				let twice = format!("twice: {}", age * 2);
				assert!(
					rows.contains("twice") == rows.contains(&twice),
					"a computed field is read whole or not at all: {rows}"
				);
			}
		});
	}
	let definer = Arc::clone(&db);
	tasks.spawn(async move {
		for _ in 0..ROUNDS / 3 {
			definer.run("DEFINE FIELD twice ON person COMPUTED age * 2").await;
			tokio::task::yield_now().await;
			definer.run("REMOVE FIELD twice ON person").await;
		}
	});
	while let Some(done) = tasks.join_next().await {
		done.expect("no task panics");
	}
	let kept = db.datastore().statement_cache_stats();
	assert!(kept.hits > 0, "{kept:?}");
	// What follows the last change reads what the catalog holds.
	let rows = answer(
		db.datastore(),
		&TestDb::owner(),
		"SELECT * FROM type::record('person', $n)",
		Some(number(1)),
	)
	.await;
	assert_eq!(rows, "[{ age: 31, id: person:1, name: 'ann' }]");
}

#[tokio::test]
async fn a_table_or_field_altered_changes_what_a_kept_statement_reads() {
	let db = TestDb::new_with_auth(ACCESS).await;
	let ds = db.datastore();
	let alice = record_user("alice");
	let sql = "SELECT * FROM $id";
	let doc = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("id", RecordId::new("doc", n));
		Some(vars)
	};
	db.run(
		"DEFINE FIELD secret ON doc TYPE string PERMISSIONS FULL; UPDATE doc:1 SET secret = 's1'",
	)
	.await;
	let shown = "[{ id: doc:1, owner: user:alice, secret: 's1' }]";
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, shown);
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, shown);
	db.run("ALTER FIELD secret ON doc PERMISSIONS NONE").await;
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, "[{ id: doc:1, owner: user:alice }]");
	db.run("ALTER TABLE doc PERMISSIONS NONE").await;
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, "[]");
	db.run("ALTER TABLE doc PERMISSIONS FULL").await;
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, "[{ id: doc:1, owner: user:alice }]");
}

#[tokio::test]
async fn a_table_defined_after_a_kept_statement_read_it_absent_is_read() {
	let db = TestDb::new("").await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let sql = "SELECT VALUE v FROM $id";
	let row = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("id", RecordId::new("fresh", n));
		Some(vars)
	};
	let owner = &owner;
	let read = |n: i64| async move {
		match ds.execute(sql, owner, row(n)).await.expect("the request runs").remove(0).result {
			Ok(value) => value.to_sql(),
			Err(e) => format!("error: {}", e.message()),
		}
	};
	let before = read(1).await;
	assert_eq!(before, read(2).await);
	db.run("DEFINE TABLE fresh SCHEMALESS; CREATE fresh:1 SET v = 1; CREATE fresh:2 SET v = 2")
		.await;
	assert_eq!(read(1).await, "[1]");
	assert_eq!(read(2).await, "[2]");
}

#[tokio::test]
async fn a_field_permission_changes_what_a_kept_statement_shows() {
	let db = TestDb::new_with_auth(ACCESS).await;
	let ds = db.datastore();
	let alice = record_user("alice");
	let sql = "SELECT * FROM $id";
	let doc = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("id", RecordId::new("doc", n));
		Some(vars)
	};
	db.run("UPDATE doc:1 SET secret = 's1'; UPDATE doc:2 SET secret = 's2'").await;
	assert_eq!(
		answer(ds, &alice, sql, doc(1)).await,
		"[{ id: doc:1, owner: user:alice, secret: 's1' }]"
	);
	db.run("DEFINE FIELD secret ON doc PERMISSIONS FOR select NONE").await;
	assert_eq!(answer(ds, &alice, sql, doc(1)).await, "[{ id: doc:1, owner: user:alice }]");
	assert_eq!(answer(ds, &alice, sql, doc(2)).await, "[{ id: doc:2, owner: user:bob }]");
	// The owner is not bound by the permission.
	assert_eq!(
		answer(ds, &TestDb::owner(), sql, doc(2)).await,
		"[{ id: doc:2, owner: user:bob, secret: 's2' }]"
	);
	db.run("DEFINE FIELD OVERWRITE secret ON doc PERMISSIONS FULL").await;
	assert_eq!(
		answer(ds, &alice, sql, doc(2)).await,
		"[{ id: doc:2, owner: user:bob, secret: 's2' }]"
	);
}

#[tokio::test]
async fn record_users_sharing_a_kept_statement_each_see_only_what_they_may() {
	let db = TestDb::new_with_auth(ACCESS).await;
	let ds = db.datastore();
	db.run("DEFINE TABLE OVERWRITE doc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth")
		.await;
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let (alice, bob) = (record_user("alice"), record_user("bob"));
	let sql = "SELECT VALUE id FROM type::record('doc', $n)";
	let doc = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("n", n);
		Some(vars)
	};
	for _ in 0..2 {
		assert_eq!(answer(ds, &alice, sql, doc(1)).await, "[doc:1]");
		assert_eq!(answer(ds, &alice, sql, doc(2)).await, "[]");
		assert_eq!(answer(ds, &bob, sql, doc(1)).await, "[]");
		assert_eq!(answer(ds, &bob, sql, doc(2)).await, "[doc:2]");
	}
	let kept = now();
	assert!(kept.hits > 0, "{kept:?}");
}

#[tokio::test]
async fn a_field_defined_in_a_cancelled_transaction_is_never_read_by_a_kept_statement() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let read = |n: i64| answer(ds, &owner, "SELECT * FROM $id", Some(person(n)));
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	let mut vars = Variables::new();
	vars.insert("id", RecordId::new("person", 2));
	let inside = ds
		.execute(
			"BEGIN; DEFINE FIELD twice ON person COMPUTED age * 2; SELECT * FROM $id; CANCEL;",
			&owner,
			Some(vars),
		)
		.await
		.expect("the request runs");
	drop(inside);
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	assert_eq!(read(2).await, "[{ age: 25, id: person:2, name: 'bob' }]");
}

#[tokio::test]
async fn a_database_removed_and_defined_again_is_read_afresh() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let read = |n: i64| answer(ds, &owner, "SELECT VALUE name FROM $id", Some(person(n)));
	assert_eq!(read(1).await, "['ann']");
	assert_eq!(read(1).await, "['ann']");
	db.run("REMOVE DATABASE test; DEFINE DATABASE test; USE DB test; DEFINE TABLE person SCHEMALESS; CREATE person:1 SET name = 'new'")
		.await;
	assert_eq!(read(1).await, "['new']");
	assert_eq!(read(2).await, "[]");
}

#[tokio::test]
async fn a_param_or_function_redefined_changes_what_a_kept_statement_reads() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	db.run(
		"DEFINE PARAM $greeting VALUE 'hi'; DEFINE FUNCTION fn::tag($n: string) { RETURN 'a-' + $n }",
	)
	.await;
	let sql = "SELECT VALUE [$greeting, fn::tag(name)] FROM $id";
	let read = |n: i64| answer(ds, &owner, sql, Some(person(n)));
	assert_eq!(read(1).await, "[['hi', 'a-ann']]");
	assert_eq!(read(2).await, "[['hi', 'a-bob']]");
	db.run("DEFINE PARAM OVERWRITE $greeting VALUE 'yo'").await;
	assert_eq!(read(1).await, "[['yo', 'a-ann']]");
	db.run("DEFINE FUNCTION OVERWRITE fn::tag($n: string) { RETURN 'b-' + $n }").await;
	assert_eq!(read(2).await, "[['yo', 'b-bob']]");
}

#[tokio::test]
async fn a_kept_statement_is_served_when_its_request_arrives_and_binds_nothing_again() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	// In one transaction, so the second statement is compiled with the request.
	let sql = "BEGIN; SELECT VALUE name FROM person:1; SELECT VALUE name FROM type::record('person', $n); COMMIT";
	let served = crate::exec::compiles::statements_after_arrival();
	let responses = ds.execute(sql, &owner, Some(number(2))).await.expect("the request runs");
	let read = responses.into_iter().nth(2).expect("the second select");
	assert_eq!(read.result.expect("answers").to_sql(), "['bob']");
	// Each select is missed and kept: the first for the table its record names.
	assert_eq!(now(), stats(0, 2, 0), "the statements are missed and kept");
	assert_eq!(
		crate::exec::compiles::statements_after_arrival(),
		served,
		"a statement the cache keeps is served as its request arrives, not when it is reached"
	);

	let read = |n: i64| {
		answer(ds, &owner, "SELECT VALUE name FROM type::record('person', $n)", Some(number(n)))
	};
	let binds = crate::exec::planner::COMPILES.with(std::cell::Cell::get);
	assert_eq!(read(3).await, "['cat']");
	assert_eq!(now(), stats(1, 2, 0));
	assert_eq!(
		crate::exec::planner::COMPILES.with(std::cell::Cell::get),
		binds,
		"a statement served the kept compilation is neither bound nor planned again"
	);
	assert_eq!(crate::exec::compiles::statements_after_arrival(), served);
}

#[tokio::test]
async fn a_field_defined_on_a_table_a_kept_statement_names_changes_what_it_reads() {
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let sql = "SELECT * FROM type::record('person', $n)";
	let read = |n: i64| answer(ds, &owner, sql, Some(number(n)));
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	assert_eq!(read(2).await, "[{ age: 25, id: person:2, name: 'bob' }]");
	db.run("DEFINE FIELD twice ON person COMPUTED age * 2").await;
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann', twice: 62 }]");
	db.run("DEFINE FIELD OVERWRITE twice ON person COMPUTED age * 3").await;
	assert_eq!(read(2).await, "[{ age: 25, id: person:2, name: 'bob', twice: 75 }]");
	db.run("REMOVE FIELD twice ON person").await;
	assert_eq!(read(1).await, "[{ age: 31, id: person:1, name: 'ann' }]");
	db.run("REMOVE TABLE person").await;
	let removed = ds.execute(sql, &owner, Some(number(1))).await.expect("runs").remove(0).result;
	assert!(removed.as_ref().map_or(true, |v| v.to_sql() == "[]"), "{removed:?}");
}

const SOURCES: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE INDEX person_age ON person FIELDS age;
	CREATE person:1 SET name = 'ann', age = 31, friends = [person:2, person:3];
	CREATE person:2 SET name = 'bob', age = 25;
	CREATE person:3 SET name = 'cat', age = 40;
	DEFINE TABLE pts SCHEMALESS;
	DEFINE INDEX hnsw ON pts FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN;
	CREATE pts:1 SET v = [1, 1]; CREATE pts:2 SET v = [3, 3]; CREATE pts:3 SET v = [9, 9];
	DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
	RELATE person:1->follows->person:2; RELATE person:2->follows->person:3;
	DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
	DEFINE TABLE doc SCHEMALESS;
	DEFINE INDEX doc_title ON doc FIELDS title FULLTEXT ANALYZER simple BM25;
	CREATE doc:1 SET title = 'hello world'; CREATE doc:2 SET title = 'goodbye';
";

/// The answer of `sql` with `vars` bound, as SurrealQL, or its error.
async fn either(ds: &Datastore, sql: &str, vars: Option<Variables>) -> String {
	match ds.execute(sql, &TestDb::owner(), vars).await.expect("the request runs").remove(0).result
	{
		Ok(value) => value.to_sql(),
		Err(e) => format!("error: {}", e.message()),
	}
}

fn bound(name: &str, value: surrealdb_types::Value) -> Option<Variables> {
	let mut vars = Variables::new();
	vars.insert(name, value);
	Some(vars)
}

fn table(name: &str) -> surrealdb_types::Value {
	surrealdb_types::Value::Table(surrealdb_types::Table::new(name))
}

fn record(table: &str, key: i64) -> surrealdb_types::Value {
	surrealdb_types::Value::RecordId(RecordId::new(table, key))
}

/// A statement whose `FROM $param` is bound to a table or a record answers as the statement with
/// the value written in place does, every time, and is planned with its request as that one is:
/// the plan depends on the table the value names.
#[tokio::test]
async fn a_source_bound_to_a_table_or_a_record_is_planned_as_the_value_written_in_place() {
	let db = TestDb::new(SOURCES).await;
	let ds = db.datastore();
	let base = ds.statement_cache_stats();
	let cases: Vec<(&str, &str, Option<Variables>)> = vec![
		(
			"SELECT VALUE id FROM $t WHERE age = 25",
			"SELECT VALUE id FROM person WHERE age = 25",
			bound("t", table("person")),
		),
		(
			"SELECT VALUE id FROM $t WHERE v <|2,40|> [0, 0]",
			"SELECT VALUE id FROM pts WHERE v <|2,40|> [0, 0]",
			bound("t", table("pts")),
		),
		(
			"SELECT VALUE id FROM $t ORDER BY id",
			"SELECT VALUE id FROM follows ORDER BY id",
			bound("t", table("follows")),
		),
		(
			"SELECT VALUE id FROM $t WITH INDEX nope WHERE age = 25",
			"SELECT VALUE id FROM person WITH INDEX nope WHERE age = 25",
			bound("t", table("person")),
		),
		(
			"SELECT id, search::score(1) AS s FROM $t WHERE title @1@ 'hello'",
			"SELECT id, search::score(1) AS s FROM doc WHERE title @1@ 'hello'",
			bound("t", table("doc")),
		),
		(
			"SELECT VALUE id FROM $id WHERE title @@ 'hello'",
			"SELECT VALUE id FROM doc:1 WHERE title @@ 'hello'",
			bound("id", record("doc", 1)),
		),
	];
	for (param, literal, vars) in cases {
		let expected = either(ds, literal, None).await;
		for round in 0..2 {
			assert_eq!(either(ds, param, vars.clone()).await, expected, "{param}, round {round}");
		}
	}
	assert_eq!(
		since(base, ds.statement_cache_stats()),
		stats(0, 0, 0),
		"a source bound to a table or a record is compiled with its request"
	);
}

/// A name a source's expression reads in a filter or a subquery is a value its plan may fold, not
/// a source: the statement is compiled with its request.
#[tokio::test]
async fn a_name_a_source_reads_in_a_filter_or_a_subquery_is_not_read_as_a_source() {
	let db = TestDb::new(SOURCES).await;
	let ds = db.datastore();
	let base = ds.statement_cache_stats();
	let mut vars = Variables::new();
	vars.insert("id", RecordId::new("person", 1));
	vars.insert("min", 30);
	assert_eq!(
		either(ds, "SELECT VALUE name FROM $id.friends[WHERE age > $min]", Some(vars)).await,
		"['cat']"
	);
	let got = either(
		ds,
		"SELECT VALUE name FROM type::record('person', (SELECT VALUE record::id(id) FROM person WHERE age = $x)[0])",
		{ let mut vars = Variables::new(); vars.insert("x", 25); Some(vars) },
	)
	.await;
	assert_eq!(got, "['bob']");
	assert_eq!(since(base, ds.statement_cache_stats()), stats(0, 0, 0));
}

/// A kept statement's plan reads neither the table its source names nor a parameter's definition:
/// main plans `FROM type::record(<table>, $n)` as a value the statement computes, and a stored
/// parameter is read when the statement runs. Defining or removing an index of the table, or
/// redefining the parameter, leaves the kept plan standing, and each read after is a hit that
/// answers the catalog as it stands. A change of the database is a miss.
#[tokio::test]
async fn a_kept_plan_is_served_across_changes_it_did_not_read_and_compiled_again_after_one_it_did()
{
	let db = TestDb::new(PEOPLE).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	db.run("DEFINE PARAM $tag VALUE 'a-'").await;
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let sql = "SELECT VALUE $tag + name FROM type::record('person', $n)";
	let read = |n: i64| answer(ds, &owner, sql, Some(number(n)));
	assert_eq!(read(2).await, "['a-bob']");
	assert_eq!(read(4).await, "['a-dan']");
	assert_eq!(now(), stats(1, 1, 0));
	db.run("DEFINE PARAM OVERWRITE $tag VALUE 'b-'").await;
	assert_eq!(read(1).await, "['b-ann']");
	assert_eq!(now(), stats(2, 1, 0), "the parameter is read when the statement runs");
	db.run("DEFINE INDEX person_age ON person FIELDS age").await;
	assert_eq!(read(3).await, "['b-cat']");
	assert_eq!(now(), stats(3, 1, 0), "the plan read no index of the table");
	db.run("REMOVE INDEX person_age ON person").await;
	assert_eq!(read(2).await, "['b-bob']");
	assert_eq!(now(), stats(4, 1, 0));
	db.run(
		"REMOVE DATABASE test; DEFINE DATABASE test; USE DB test; DEFINE PARAM $tag VALUE 'c-'; \
		 DEFINE TABLE person SCHEMALESS; CREATE person:1 SET name = 'new'",
	)
	.await;
	assert_eq!(read(1).await, "['c-new']");
	assert_eq!(now(), stats(4, 2, 0), "a database defined again is compiled for afresh");
}

const CLASSES: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE pk SCHEMAFULL PERMISSIONS FULL;
	DEFINE FIELD name ON pk TYPE string;
	DEFINE FIELD secret ON pk TYPE string PERMISSIONS FOR select NONE;
	DEFINE FIELD owner ON pk TYPE record<user>;
	DEFINE TABLE ls SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	CREATE pk:1 SET name = 'p1', secret = 's1', owner = user:alice;
	CREATE pk:2 SET name = 'p2', secret = 's2', owner = user:bob;
	CREATE ls:1 SET name = 'l1', owner = user:alice, extra = true;
	CREATE ls:2 SET name = 'l2', owner = user:bob;
";

/// The answer of `sql` with `vars` bound, as SurrealQL, or its error, for `session`.
async fn either_as(
	ds: &Datastore,
	session: &Session,
	sql: &str,
	vars: Option<Variables>,
) -> String {
	match ds.execute(sql, session, vars).await.expect("the request runs").remove(0).result {
		Ok(value) => value.to_sql(),
		Err(e) => format!("error: {}", e.message()),
	}
}

/// `value` written in place of `$id` in `sql`.
fn written(sql: &str, value: &surrealdb_types::Value) -> String {
	sql.replace("$id", &value.to_sql())
}

/// A statement that reads `FROM $id` only as its source is kept for the class of the value: two
/// records of one table share one compilation, each answered with its own row, and a record of
/// another table is compiled afresh and kept beside it.
#[tokio::test]
async fn a_source_variable_is_kept_for_the_table_its_record_names() {
	let db = TestDb::new(CLASSES).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	for sql in ["SELECT * FROM $id", "SELECT name FROM ONLY $id"] {
		let mut seen = Vec::new();
		for (value, served) in [
			(record("pk", 1), stats(0, 1, 0)),
			(record("pk", 2), stats(1, 0, 0)),
			(record("ls", 1), stats(0, 1, 0)),
			(record("pk", 1), stats(1, 0, 0)),
			(record("ls", 2), stats(1, 0, 0)),
		] {
			// The statement written with the value in place is kept apart from this one.
			let want = either_as(ds, &owner, &written(sql, &value), None).await;
			let at = now();
			let got = either_as(ds, &owner, sql, bound("id", value.clone())).await;
			assert_eq!(got, want, "{sql}");
			assert_eq!(since(at, now()), served, "{sql} with {}", value.to_sql());
			seen.push(got);
		}
		assert_ne!(seen[0], seen[1], "each record answers its own row");
		assert_eq!(seen[0], seen[3]);
	}
}

/// A record user is shown what the table the record names allows, cached or not: the field a
/// permission hides on one table, the rows another table's permission refuses.
#[tokio::test]
async fn a_kept_source_variable_enforces_the_permissions_of_the_table_its_record_names() {
	for size in [None, Some("0")] {
		let config = match size {
			Some(size) => ConfigMap::empty().with_key_value("statement_cache_size", size),
			None => ConfigMap::empty(),
		};
		let db = TestDb::new_with_auth_and_config(CLASSES, config).await;
		let ds = db.datastore();
		let alice = record_user("alice");
		for sql in ["SELECT * FROM $id", "SELECT * FROM ONLY $id", "SELECT VALUE secret FROM $id"] {
			for _ in 0..2 {
				for value in [record("pk", 1), record("ls", 1), record("ls", 2), record("pk", 2)] {
					assert_eq!(
						either_as(ds, &alice, sql, bound("id", value.clone())).await,
						either_as(ds, &alice, &written(sql, &value), None).await,
						"{sql} with {} (cache size {size:?})",
						value.to_sql(),
					);
				}
			}
		}
	}
}

/// A value that is not a record id naming one record, or an array of them, is never served a kept
/// compilation: the statement is compiled with its request and answers as main does.
#[tokio::test]
async fn a_source_variable_bound_to_another_value_is_compiled_with_its_request() {
	let range = surrealdb_types::Value::RecordId(RecordId::new(
		"pk",
		surrealdb_types::RecordIdKey::Range(Box::new(surrealdb_types::RecordIdKeyRange {
			start: std::ops::Bound::Included(surrealdb_types::RecordIdKey::Number(1)),
			end: std::ops::Bound::Included(surrealdb_types::RecordIdKey::Number(2)),
		})),
	));
	let mixed = surrealdb_types::Value::Array(surrealdb_types::Array::from(vec![
		record("pk", 1),
		surrealdb_types::Value::String("pk".into()),
	]));
	let object = {
		let mut object = surrealdb_types::Object::new();
		object.insert("a".to_owned(), surrealdb_types::Value::Number(1.into()));
		surrealdb_types::Value::Object(object)
	};
	let values = [
		table("pk"),
		surrealdb_types::Value::String("pk".into()),
		range,
		mixed,
		surrealdb_types::Value::None,
		object,
	];
	let cached = TestDb::new(CLASSES).await;
	let plain = TestDb::new_with_config(
		CLASSES,
		ConfigMap::empty().with_key_value("statement_cache_size", "0"),
	)
	.await;
	let owner = TestDb::owner();
	let base = cached.datastore().statement_cache_stats();
	for sql in ["SELECT * FROM $id", "SELECT * FROM ONLY $id"] {
		for value in &values {
			for _ in 0..2 {
				assert_eq!(
					either_as(cached.datastore(), &owner, sql, bound("id", value.clone())).await,
					either_as(plain.datastore(), &owner, sql, bound("id", value.clone())).await,
					"{sql} with {}",
					value.to_sql(),
				);
			}
		}
	}
	assert_eq!(since(base, cached.datastore().statement_cache_stats()), stats(0, 0, 0));
}

/// An array of record ids is kept for the tables it names, whatever records of them it holds; an
/// array naming other tables, or none, is compiled afresh. Missing records, a table that does not
/// exist and `ONLY` over an array answer as the value written in place does.
#[tokio::test]
async fn a_source_variable_bound_to_records_is_kept_for_the_tables_they_name() {
	let db = TestDb::new(CLASSES).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	let now = || since(base, ds.statement_cache_stats());
	let array = |items: Vec<surrealdb_types::Value>| {
		surrealdb_types::Value::Array(surrealdb_types::Array::from(items))
	};
	let sql = "SELECT VALUE name FROM $id";
	for (value, served) in [
		(array(vec![record("pk", 1), record("ls", 1)]), stats(0, 1, 0)),
		(array(vec![record("ls", 2), record("pk", 2), record("pk", 1)]), stats(1, 0, 0)),
		(array(vec![record("pk", 9)]), stats(0, 1, 0)),
		(array(vec![record("pk", 2)]), stats(1, 0, 0)),
		(array(vec![]), stats(0, 1, 0)),
	] {
		let at = now();
		assert_eq!(
			either_as(ds, &owner, sql, bound("id", value.clone())).await,
			either_as(ds, &owner, &written(sql, &value), None).await,
			"{}",
			value.to_sql(),
		);
		assert_eq!(since(at, now()), served, "{}", value.to_sql());
	}
	for (sql, value) in [
		("SELECT * FROM ONLY $id", array(vec![record("pk", 1), record("pk", 2)])),
		("SELECT * FROM ONLY $id", record("pk", 9)),
		("SELECT * FROM ONLY $id", record("nosuch", 1)),
		("SELECT * FROM $id", record("nosuch", 1)),
		("SELECT * FROM $id", array(vec![record("nosuch", 1), record("pk", 1)])),
	] {
		for _ in 0..2 {
			assert_eq!(
				either_as(ds, &owner, sql, bound("id", value.clone())).await,
				either_as(ds, &owner, &written(sql, &value), None).await,
				"{sql} with {}",
				value.to_sql(),
			);
		}
	}
}

/// A kept compilation reads the table its source's record names as the catalog holds it when the
/// statement runs: a field defined on it afterwards is read, and one removed is not.
#[tokio::test]
async fn a_field_defined_on_the_table_a_kept_source_variable_names_is_read() {
	let db = TestDb::new(CLASSES).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let sql = "SELECT VALUE twice FROM ONLY $id";
	let read = |n: i64| either_as(ds, &owner, sql, bound("id", record("ls", n)));
	assert_eq!(read(1).await, "NONE");
	assert_eq!(read(2).await, "NONE");
	db.run("DEFINE FIELD twice ON ls COMPUTED string::concat(name, name)").await;
	assert_eq!(read(1).await, "'l1l1'");
	assert_eq!(read(2).await, "'l2l2'");
	db.run("REMOVE FIELD twice ON ls").await;
	assert_eq!(read(1).await, "NONE");
}

/// A statement that filters or orders the rows of `FROM $id`, or reads it with an index hint, is
/// compiled with its request: main resolves the source to the record the value names, and its plan
/// may choose by that record's table.
#[tokio::test]
async fn a_source_variable_a_statement_filters_or_orders_is_compiled_with_its_request() {
	let db = TestDb::new(CLASSES).await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let base = ds.statement_cache_stats();
	for sql in [
		"SELECT * FROM $id WHERE name = 'p1'",
		"SELECT * FROM $id ORDER BY name",
		"SELECT * FROM $id WITH NOINDEX",
		"SELECT name, (SELECT VALUE name FROM pk:2) AS other FROM $id",
		"SELECT * FROM $id GROUP ALL",
	] {
		for n in [1, 2] {
			let value = record("pk", n);
			assert_eq!(
				either_as(ds, &owner, sql, bound("id", value.clone())).await,
				either_as(ds, &owner, &written(sql, &value), None).await,
				"{sql}",
			);
		}
	}
	assert_eq!(since(base, ds.statement_cache_stats()), stats(0, 0, 0));
}
