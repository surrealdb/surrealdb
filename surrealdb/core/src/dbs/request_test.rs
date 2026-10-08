//! A request that arrives whole is compiled once, when its first planned
//! statement is reached, and nothing is compiled after that while it runs.
//!
//! Each test runs the same request twice on fresh datastores: compiled whole,
//! and with every statement planned as it is reached, which is how a streamed
//! request runs. The results must be the same, statement for statement.

use std::sync::Arc;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::exec::compiles;
use crate::kvs::{Datastore, TransactionType};

/// A datastore with the test namespace and database in place.
async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");
	ds
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Each statement's result, rendered, with its error message where it failed.
type Rendered = Vec<Result<String, String>>;

async fn run(ds: &Datastore, sql: &str) -> Rendered {
	ds.execute(sql, &session(), None)
		.await
		.expect("the request runs")
		.into_iter()
		.map(|result| {
			result.result.map(|value| format!("{value:?}")).map_err(|error| error.to_string())
		})
		.collect()
}

/// What running `sql` after `setup` on a fresh datastore answers, with how
/// many request compilations it started and how many compilations it made
/// after it arrived.
async fn measured(setup: &str, sql: &str, per_statement: bool) -> (Rendered, usize, usize) {
	let ds = datastore().await;
	if !setup.is_empty() {
		for result in run(&ds, setup).await {
			result.expect("every setup statement succeeds");
		}
	}
	compiles::set_per_statement(per_statement);
	let requests = compiles::request_compiles();
	let after = compiles::after_arrival();
	let rendered = run(&ds, sql).await;
	let counts = (compiles::request_compiles() - requests, compiles::after_arrival() - after);
	compiles::set_per_statement(false);
	(rendered, counts.0, counts.1)
}

/// Run `sql` both ways and check they answer the same and the whole request
/// compiled once, answering how many compilations it made after it arrived.
fn compare(setup: &str, sql: &str) -> usize {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let (expected, _, _) = measured(setup, sql, true).await;
		let (whole, requests, after) = measured(setup, sql, false).await;
		assert_eq!(whole, expected, "`{sql}` answered differently when compiled whole");
		assert!(requests <= 1, "`{sql}` started {requests} request compilations");
		after
	})
}

/// [`compare`], where nothing the request runs changes the catalog a later
/// statement was planned against, so nothing is compiled after it arrived.
fn check(setup: &str, sql: &str) {
	let after = compare(setup, sql);
	assert_eq!(after, 0, "`{sql}` compiled {after} times after it arrived");
}

#[test]
fn a_request_of_auto_commit_statements_compiles_once() {
	check(
		"DEFINE TABLE t;",
		"CREATE t:1 SET a = 1; SELECT * FROM t; UPDATE t:1 SET a = 2; SELECT a FROM t; \
		 DELETE t:1; SELECT * FROM t;",
	);
}

#[test]
fn a_schema_statement_is_seen_by_the_statements_after_it() {
	for sql in [
		"DEFINE TABLE x SCHEMAFULL; DEFINE FIELD a ON x TYPE int; CREATE x:1 SET a = 1, b = 2; \
		 SELECT * FROM x;",
		"DEFINE FIELD c ON t COMPUTED a + 1; CREATE t:1 SET a = 1; SELECT * FROM t;",
		"CREATE t:1 SET a = 1; DEFINE INDEX ia ON t FIELDS a; SELECT * FROM t WHERE a = 1;",
		"DEFINE FUNCTION fn::f($x: int) { RETURN $x * 2 }; RETURN fn::f(2);",
		"DEFINE PARAM $p VALUE 5; RETURN $p; REMOVE PARAM $p; RETURN $p;",
		"DEFINE TABLE y; REMOVE TABLE y; SELECT * FROM y;",
		"DEFINE FIELD a ON t TYPE int; ALTER FIELD a ON t TYPE string; CREATE t:1 SET a = 1;",
		"DEFINE TABLE z; DEFINE TABLE IF NOT EXISTS z SCHEMAFULL; CREATE z:1 SET a = 1; \
		 SELECT * FROM z;",
		"DEFINE TABLE w SCHEMAFULL; DEFINE TABLE OVERWRITE w SCHEMALESS; CREATE w:1 SET a = 1; \
		 SELECT * FROM w;",
		"DEFINE TABLE v; DEFINE TABLE v; CREATE v:1 SET a = 1; SELECT * FROM v;",
		"CREATE u:1 SET a = 1; SELECT * FROM u;",
	] {
		compare("", sql);
	}
}

/// A schema statement reached through a body the statement does not show —
/// a stored function that redefines another — is seen by the statements after
/// it: none of them runs a copy of the definition it replaced.
#[test]
fn a_schema_statement_inside_a_called_body_is_seen() {
	check(
		"DEFINE FUNCTION fn::f() { RETURN 1 }; \
		 DEFINE FUNCTION fn::redefine() { DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }; RETURN 0 };",
		"RETURN fn::f(); RETURN fn::redefine(); RETURN fn::f();",
	);
}

/// The same, where the redefinition is made by embedded JavaScript.
#[cfg(feature = "scripting")]
#[test]
fn a_schema_statement_made_by_a_script_is_seen() {
	check(
		"DEFINE FUNCTION fn::f() { RETURN 1 };",
		"RETURN fn::f(); \
		 RETURN function() { return surrealdb.query('DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }'); }; \
		 RETURN fn::f();",
	);
}

/// An `ACCESS` statement's `WHERE` clause is served from the compile cache
/// when the statement runs, as its subject is: the request compiles nothing
/// after it arrived outside a cache fill.
#[test]
fn an_access_statements_where_clause_is_served() {
	check(
		"DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER; \
		 DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR; \
		 ACCESS api ON DATABASE GRANT FOR USER tobie;",
		"ACCESS api ON DATABASE SHOW WHERE revocation != NONE; \
		 ACCESS api ON DATABASE REVOKE WHERE revocation != NONE;",
	);
}

#[test]
fn a_block_and_its_failures_answer_as_they_do_statement_by_statement() {
	for sql in [
		"BEGIN; CREATE t:1 SET a = 1; SELECT * FROM t; COMMIT; SELECT * FROM t;",
		"BEGIN; CREATE t:1 SET a = 1; THROW 'no'; CREATE t:2; COMMIT; SELECT * FROM t;",
		"BEGIN; CREATE t:1 SET a = 1; CANCEL; SELECT * FROM t;",
		"BEGIN; CREATE t:1; RETURN 'early'; CREATE t:2; COMMIT; SELECT * FROM t;",
		"BEGIN; CREATE t:1; BEGIN; COMMIT; SELECT * FROM t;",
		"COMMIT; CANCEL; RETURN 1;",
		"BEGIN; CREATE t:1;",
		"CREATE t:1; THROW 'stop'; SELECT * FROM t;",
		"BREAK; CONTINUE; RETURN 1;",
		"BEGIN; DEFINE TABLE x SCHEMAFULL; DEFINE FIELD a ON x TYPE int; CREATE x:1 SET a = 1; \
		 COMMIT; SELECT * FROM x;",
	] {
		compare("DEFINE TABLE t;", sql);
	}
}

#[test]
fn session_statements_shape_the_statements_after_them() {
	for sql in [
		"LET $a = 1; LET $b = $a + 1; RETURN [$a, $b]; CREATE t:1 SET v = $b; SELECT * FROM t;",
		"LET $t = 't'; CREATE type::record($t, 1); SELECT * FROM type::table($t);",
		"USE NS test DB other; CREATE t:1; SELECT * FROM t; USE DB test; SELECT * FROM t;",
		"OPTION IMPORT; CREATE t:1; SELECT * FROM t;",
	] {
		check("DEFINE DATABASE other; DEFINE TABLE t;", sql);
	}
}

/// Another client's schema statement committed between two auto-commit
/// statements of a request is seen by the later one, as it is when each
/// statement is planned as it is reached.
#[test]
fn another_clients_schema_change_between_statements_is_seen() {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		for per_statement in [true, false] {
			let ds = datastore().await;
			for result in run(&ds, "DEFINE TABLE t;").await {
				result.expect("the table is defined");
			}
			compiles::set_per_statement(per_statement);
			let other = Arc::clone(&ds);
			let change = tokio::spawn(async move {
				tokio::time::sleep(std::time::Duration::from_millis(50)).await;
				for result in run(&other, "DEFINE FIELD c ON t COMPUTED a + 1;").await {
					result.expect("the other client's schema statement succeeds");
				}
			});
			let after = compiles::after_arrival();
			let faults = compiles::faults();
			let results = run(&ds, "CREATE t:1 SET a = 1; SLEEP 300ms; SELECT * FROM t;").await;
			let compiled = compiles::after_arrival() - after;
			let served = compiles::faults() - faults;
			change.await.expect("the other client ran");
			compiles::set_per_statement(false);
			let last = results.last().expect("three results").clone().expect("the select runs");
			assert!(
				last.contains("c:") || last.contains("\"c\""),
				"the computed field defined between statements is missing: {last}"
			);
			// The statement after the first transaction is served for the
			// catalog it meets, from the compile cache: never planned again by
			// the driver.
			if !per_statement {
				assert_eq!(compiled, 0, "the request compiled {compiled} times after it arrived");
				assert!(served > 0, "the statement after the change was not served");
			}
		}
	});
}

/// A request is one program: a step for every statement, and the program of
/// every statement compiled when it arrived, all lowered into the units of
/// that program. The statements of the first transaction before any that may
/// change the catalog are compiled there; the others are served when reached.
#[test]
fn a_request_is_one_program_holding_every_statement() {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let ds = datastore().await;
		for result in run(
			&ds,
			"DEFINE TABLE t; DEFINE FUNCTION fn::f($x: int) { RETURN $x + 1 }; CREATE t:1 SET a = 1;",
		)
		.await
		{
			result.expect("the setup runs");
		}
		crate::exec::vm::request::LAST.with(|last| *last.borrow_mut() = None);
		let sql = "BEGIN; SELECT a, a + 1 AS b FROM t; SELECT VALUE a FROM t; LET $x = 1; \
		           RETURN $x; COMMIT; CREATE t:2 SET a = fn::f(1);";
		for result in run(&ds, sql).await {
			result.expect("every statement runs");
		}
		let shape = crate::exec::vm::request::LAST.with(|last| *last.borrow());
		let (steps, statements, _units) = shape.expect("the request was compiled as one program");
		assert_eq!(steps, 7, "one step per statement");
		assert_eq!(statements, 4, "every statement of the first transaction is a program of it");
	});
}

/// A `BREAK` or `CONTINUE` that escapes every loop inside `BEGIN … COMMIT`
/// fails the block as any other error there does: nothing commits, and every
/// statement of the block reports the failed transaction.
#[test]
fn an_escaping_break_inside_a_block_cancels_it() {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		for per_statement in [true, false] {
			let ds = datastore().await;
			for result in run(&ds, "DEFINE TABLE t;").await {
				result.expect("the table is defined");
			}
			compiles::set_per_statement(per_statement);
			let results = run(
				&ds,
				"BEGIN; CREATE t:1; IF true { CONTINUE }; CREATE t:2; COMMIT; \
				 SELECT VALUE id FROM t;",
			)
			.await;
			compiles::set_per_statement(false);
			assert_eq!(results.len(), 6, "{results:?}");
			let failed = |at: usize| results[at].as_ref().err().cloned().unwrap_or_default();
			assert!(failed(1).contains("failed transaction"), "{results:?}");
			assert!(failed(2).contains("Invalid control flow"), "{results:?}");
			assert!(failed(3).contains("cancelled transaction"), "{results:?}");
			assert!(failed(4).contains("Cannot COMMIT"), "{results:?}");
			assert_eq!(results[5], Ok("Array(Array([]))".to_owned()), "nothing committed");
		}
	});
}

/// A statement served when it is reached folds the values of the session it
/// runs in, as one compiled with the request does, and is kept for those
/// values: two principals running the same request text each get the rows
/// their own `$auth` selects, through the index on the field it is compared
/// with, however the cache was filled before them.
#[test]
fn a_served_statement_never_serves_one_sessions_folded_values_to_another() {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let ds = datastore().await;
		let setup = "DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL; \
			DEFINE TABLE doc SCHEMALESS PERMISSIONS \
				FOR select WHERE $auth.admin = true OR owner = $auth \
				FOR create, update, delete NONE; \
			DEFINE INDEX by_owner ON doc FIELDS owner; \
			CREATE user:alice SET admin = false; CREATE user:bob SET admin = false; \
			CREATE user:root SET admin = true; \
			CREATE doc:a SET owner = user:alice; CREATE doc:b SET owner = user:bob;";
		for result in run(&ds, setup).await {
			result.expect("every setup statement succeeds");
		}
		let as_user = |id: &str| {
			Session::for_record(
				"test",
				"test",
				"user",
				surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", id)),
			)
		};
		let request = "RETURN 0; \
			SELECT VALUE id FROM doc WHERE owner = $auth; \
			SELECT VALUE id FROM doc; \
			SELECT VALUE id FROM doc WHERE owner = $auth EXPLAIN;";
		let answer = |id: &'static str| {
			let ds = Arc::clone(&ds);
			let session = as_user(id);
			async move {
				ds.execute(request, &session, None)
					.await
					.expect("the request runs")
					.into_iter()
					.map(|result| {
						result
							.result
							.map(|value| format!("{value:?}"))
							.map_err(|error| error.to_string())
					})
					.collect::<Rendered>()
			}
		};
		let rows = |id: &str| {
			format!(
				"Array(Array([RecordId(RecordId {{ table: Table(\"doc\"), key: String(\"{id}\") }})]))"
			)
		};
		for (principal, own) in [("alice", "a"), ("bob", "b"), ("alice", "a"), ("bob", "b")] {
			let results = answer(principal).await;
			assert_eq!(results[1], Ok(rows(own)), "{principal}: {results:?}");
			assert_eq!(results[2], Ok(rows(own)), "{principal}: {results:?}");
			let explained = results[3].clone().expect("EXPLAIN answers");
			assert!(explained.contains(principal), "{principal}: {explained}");
		}
		let results = answer("root").await;
		assert!(
			results[2].as_ref().is_ok_and(|rows| rows.contains("\"a\"") && rows.contains("\"b\"")),
			"root: {results:?}"
		);
	});
}

/// A statement served when it is reached reads the clock of its own run: the
/// compile cache never answers a later request with the time an earlier one
/// was planned at.
#[test]
fn a_served_statement_reads_the_clock_of_its_run() {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let ds = datastore().await;
		for result in run(&ds, "DEFINE TABLE t; CREATE t:1;").await {
			result.expect("every setup statement succeeds");
		}
		let request = "RETURN 0; \
			SELECT VALUE time::now() FROM t WHERE time::now() > d'2000-01-01T00:00:00Z'; \
			RETURN time::now();";
		let first = run(&ds, request).await;
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		let second = run(&ds, request).await;
		assert_ne!(first[1], second[1], "{first:?} {second:?}");
		assert_ne!(first[2], second[2], "{first:?} {second:?}");
	});
}
