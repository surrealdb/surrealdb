//! A request that arrives whole is compiled once, when its first planned
//! statement is reached, and nothing is compiled after that while it runs.
//!
//! Each test runs the same request twice on fresh datastores: compiled whole,
//! and with every statement planned as it is reached, which is how a streamed
//! request runs. The results must be the same, statement for statement.

use std::sync::Arc;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{Capabilities, Session};
use crate::exec::compiles;
use crate::kvs::{Datastore, TransactionType};

/// A datastore with the test namespace and database in place.
async fn datastore() -> Arc<Datastore> {
	datastore_with(Capabilities::default()).await
}

/// [`datastore`], running with `capabilities`.
async fn datastore_with(capabilities: Capabilities) -> Arc<Datastore> {
	let ds = Datastore::builder()
		.with_capabilities(capabilities)
		.build_with_path("memory")
		.await
		.expect("a memory datastore");
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
	measured_with(Capabilities::default(), setup, sql, per_statement).await
}

/// [`measured`] on a datastore running with `capabilities`.
async fn measured_with(
	capabilities: Capabilities,
	setup: &str,
	sql: &str,
	per_statement: bool,
) -> (Rendered, usize, usize) {
	let ds = datastore_with(capabilities).await;
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
/// when the statement runs. The request compiles nothing after it arrived,
/// except to fill the cache.
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
/// runs in, as one compiled with the request does, and the cached plan is
/// keyed on those values. Two principals running the same request text each
/// get the rows their own `$auth` selects, through the index on the compared
/// field, whatever order the cache was filled in.
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
		for (principal, own, other) in [
			("alice", "a", "bob"),
			("bob", "b", "alice"),
			("alice", "a", "bob"),
			("bob", "b", "alice"),
		] {
			let results = answer(principal).await;
			assert_eq!(results[1], Ok(rows(own)), "{principal}: {results:?}");
			assert_eq!(results[2], Ok(rows(own)), "{principal}: {results:?}");
			let explained = results[3].clone().expect("EXPLAIN answers");
			assert!(!explained.contains(other), "{principal}: {explained}");
		}
		let results = answer("root").await;
		assert!(
			results[2].as_ref().is_ok_and(|rows| rows.contains("\"a\"") && rows.contains("\"b\"")),
			"root: {results:?}"
		);
	});
}

/// A statement reads the clock of its own run, whether it is compiled when the
/// request arrives or served when it is reached: a plan holds no instant, so
/// the compile cache never answers a later request with the time an earlier one
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

/// Two top-level statements of one request read two clocks: each installs the
/// instant it starts at, so a statement after a sleep reads a later one.
#[test]
fn two_statements_read_two_clocks() {
	let results = answers("", "LET $a = time::now(); SLEEP 20ms; RETURN $a < time::now();");
	assert_eq!(results.len(), 3, "{results:?}");
	assert!(results[2].as_ref().is_ok_and(|v| v.contains("true")), "{results:?}");
}

/// Each statement of a block starts at an instant of its own: a statement after
/// a `SLEEP` of the block reads a later clock, and the reads of one statement
/// agree.
#[test]
fn the_statements_of_a_block_read_a_clock_each() {
	let results = answers(
		"",
		"{ LET $a = time::now(); SLEEP 20ms; LET $b = [time::now(), time::now()]; \
		   RETURN [$a < $b[0], $b[0] = $b[1]] };",
	);
	assert_eq!(results.len(), 1, "{results:?}");
	let answer = results[0].as_ref().expect("the block answers");
	assert!(answer.contains("true") && !answer.contains("false"), "{results:?}");
}

/// An embedded script reads the clock of the statement running it, through
/// `surrealdb.functions.time` and through a nested `surrealdb.query()` alike. A
/// script returns a datetime to the millisecond, so the reads are compared by
/// their milliseconds, after a sleep that would separate two wall-clock reads.
#[cfg(feature = "scripting")]
#[test]
fn a_script_reads_the_statement_clock() {
	let results = answers_with(
		Capabilities::default().with_scripting(true),
		"",
		"{ SLEEP 30ms; \
		   LET $b = [function() { return surrealdb.functions.time.now(); }, time::now()]; \
		   LET $c = [function() { return surrealdb.query('RETURN time::now()'); }, time::now()]; \
		   RETURN [time::millis($b[0]) = time::millis($b[1]), \
		           time::millis($c[0]) = time::millis($c[1])] };",
	);
	assert_eq!(results.len(), 1, "{results:?}");
	let answer = results[0].as_ref().expect("the block answers");
	assert!(answer.contains("true") && !answer.contains("false"), "{results:?}");
}

/// What `sql` answers after `setup`, once compiled whole and once planned a
/// statement at a time, which must agree.
fn answers(setup: &str, sql: &str) -> Rendered {
	answers_with(Capabilities::default(), setup, sql)
}

/// [`answers`] on datastores running with `capabilities`.
fn answers_with(capabilities: Capabilities, setup: &str, sql: &str) -> Rendered {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let (expected, _, _) = measured_with(capabilities.clone(), setup, sql, true).await;
		let (whole, _, _) = measured_with(capabilities, setup, sql, false).await;
		assert_eq!(whole, expected, "`{sql}` answered differently when compiled whole");
		whole
	})
}

/// The error text of slot `at`, empty where the slot succeeded.
fn failure(results: &Rendered, at: usize) -> String {
	results[at].as_ref().err().cloned().unwrap_or_default()
}

/// A top-level `RETURN` inside `BEGIN … COMMIT` is a value statement: every
/// statement after it runs, and the block commits what they wrote.
#[test]
fn a_return_in_a_block_is_a_value_statement() {
	let results = answers(
		"DEFINE TABLE t;",
		"BEGIN; CREATE t:1 SET a = 'one'; RETURN 'ret'; CREATE t:2 SET a = 'two'; COMMIT; \
		 SELECT VALUE a FROM t ORDER BY a;",
	);
	assert_eq!(results.len(), 6, "{results:?}");
	assert!(results.iter().all(Result::is_ok), "{results:?}");
	assert!(results[1].as_ref().is_ok_and(|v| v.contains("one")), "{results:?}");
	assert!(results[2].as_ref().is_ok_and(|v| v.contains("ret")), "{results:?}");
	assert!(results[3].as_ref().is_ok_and(|v| v.contains("two")), "{results:?}");
	let rows = results[5].as_ref().expect("the select answers");
	assert!(rows.contains("one") && rows.contains("two"), "both rows committed: {results:?}");
}

/// The block's end rewrites a `RETURN`'s slot like any other recorded slot: a
/// `CANCEL` after it cancels every slot, the `RETURN`'s included.
#[test]
fn a_return_then_cancel_cancels_every_slot() {
	let results = answers(
		"DEFINE TABLE t;",
		"BEGIN; CREATE t:1; RETURN 'ret'; CREATE t:2; CANCEL; SELECT VALUE id FROM t;",
	);
	assert_eq!(results.len(), 6, "{results:?}");
	for at in 1..=3 {
		assert!(failure(&results, at).contains("cancelled transaction"), "{at}: {results:?}");
	}
	assert!(results[4].is_ok(), "CANCEL answers NONE: {results:?}");
	assert_eq!(results[5], Ok("Array(Array([]))".to_owned()), "nothing committed");
}

/// A failure after a `RETURN` fails the `RETURN`'s slot with the rest of the
/// block's earlier slots, and the statements after the failure are cancelled.
#[test]
fn a_return_then_failure_fails_the_return_slot() {
	let results = answers(
		"DEFINE TABLE t;",
		"BEGIN; CREATE t:1; RETURN 'ret'; THROW 'x'; CREATE t:2; COMMIT;",
	);
	assert_eq!(results.len(), 6, "{results:?}");
	assert!(failure(&results, 1).contains("failed transaction"), "{results:?}");
	assert!(failure(&results, 2).contains("failed transaction"), "{results:?}");
	assert!(failure(&results, 3).contains('x'), "{results:?}");
	assert!(failure(&results, 4).contains("cancelled transaction"), "{results:?}");
	assert!(failure(&results, 5).contains("Cannot COMMIT"), "{results:?}");
}

/// A `CANCEL` after a failure is answered NONE, as a `CANCEL` of a healthy
/// block is; every statement of the request has a slot.
#[test]
fn a_cancel_after_a_failure_answers_none() {
	let results = answers("DEFINE TABLE t;", "BEGIN; THROW 'x'; CREATE t:1; CANCEL; RETURN 2;");
	assert_eq!(results.len(), 5, "{results:?}");
	assert!(failure(&results, 1).contains('x'), "{results:?}");
	assert!(failure(&results, 2).contains("cancelled transaction"), "{results:?}");
	assert!(results[3].is_ok(), "CANCEL answers NONE: {results:?}");
	assert!(results[4].as_ref().is_ok_and(|v| v.contains('2')), "{results:?}");
}

/// Whether slot `at` answered the integer `n`.
fn is_int(results: &Rendered, at: usize, n: i64) -> bool {
	results[at].as_ref().is_ok_and(|v| v.contains(&format!("Int({n})")))
}

/// A `LET` in a block that commits stays bound for the statements after the
/// block; one in a block that is cancelled is unbound again.
#[test]
fn a_let_survives_a_commit_and_is_undone_by_a_cancel() {
	let results =
		answers("", "BEGIN; LET $t = 1; COMMIT; RETURN $t; BEGIN; LET $u = 2; CANCEL; RETURN $u;");
	assert_eq!(results.len(), 8, "{results:?}");
	assert!(is_int(&results, 3, 1), "the committed binding stands: {results:?}");
	assert_eq!(results[7], Ok("None".to_owned()), "the cancelled binding is gone: {results:?}");
}

/// A block that fails puts back the value a `LET` inside it overwrote.
#[test]
fn a_let_is_undone_by_a_failed_block() {
	let results = answers("", "LET $x = 0; BEGIN; LET $x = 1; THROW 'e'; COMMIT; RETURN $x;");
	assert_eq!(results.len(), 6, "{results:?}");
	assert!(failure(&results, 3).contains('e'), "{results:?}");
	assert!(is_int(&results, 5, 0), "the binding is the one held at BEGIN: {results:?}");
}

/// A statement planned in the same compile as the block, after its `CANCEL`,
/// reads the reverted binding, not the value the block bound.
#[test]
fn a_statement_planned_after_a_cancel_reads_the_reverted_binding() {
	let results = answers(
		"DEFINE TABLE t;",
		"LET $x = 0; BEGIN; LET $x = 1; CANCEL; RETURN $x; CREATE t:1 SET v = $x; \
		 SELECT VALUE v FROM t;",
	);
	assert_eq!(results.len(), 7, "{results:?}");
	assert!(is_int(&results, 4, 0), "{results:?}");
	assert!(results[6].as_ref().is_ok_and(|v| v.contains("Int(0)")), "{results:?}");
}

/// A cancelled block restores a `LET` that shadowed a defined parameter, so
/// the parameter answers its `DEFINE PARAM` value again rather than `NONE`.
#[test]
fn a_cancelled_let_restores_a_defined_param() {
	let results = answers("", "DEFINE PARAM $p VALUE 3; BEGIN; LET $p = 5; CANCEL; RETURN $p;");
	assert_eq!(results.len(), 5, "{results:?}");
	assert!(is_int(&results, 4, 3), "{results:?}");
}

/// `USE` inside a block that is cancelled keeps its effect: the statements
/// after the block run in the database it selected.
#[tokio::test]
async fn a_use_survives_a_cancelled_block() {
	let ds = datastore().await;
	for result in run(&ds, "DEFINE DATABASE other;").await {
		result.expect("the setup runs");
	}
	let results = run(&ds, "BEGIN; USE DB other; CANCEL; CREATE marker:1;").await;
	assert_eq!(results.len(), 4, "{results:?}");
	assert!(results[3].is_ok(), "the statement after the block runs: {results:?}");
	let there = run(&ds, "USE DB other; SELECT VALUE id FROM marker;").await;
	assert!(
		there[1].as_ref().is_ok_and(|v| v.contains("marker")),
		"the record landed in the database `USE` selected: {there:?}"
	);
}

/// How a request is interrupted at the statement it pulls.
#[derive(Clone, Copy)]
enum Interrupt {
	Cancel,
	Timeout,
}

/// The timeout a request first runs under.
const FIRST_WINDOW: std::time::Duration = std::time::Duration::from_millis(500);

/// The longest timeout a request is given: the statements ahead of the pull that outlast it have
/// stalled.
const LONGEST_WINDOW: std::time::Duration = std::time::Duration::from_secs(150);

/// Each statement's answer, as text, when `sql` is interrupted as its
/// `pull`-th statement is taken from the input (counting from one).
///
/// A timeout is observed by the first statement taken after its deadline has passed. The run
/// waits for the deadline when it takes the `pull`-th statement, so the deadline has passed by
/// that statement, but it has passed earlier when the statements before it took longer than the
/// timeout, as on a loaded machine, and then an earlier statement observed it and the run shows
/// nothing of the `pull`-th. Such a run is repeated with a timeout four times as long, up to
/// [`LONGEST_WINDOW`], until the statements before the pull end within it.
async fn interrupted(sql: &str, pull: usize, interrupt: Interrupt) -> Vec<String> {
	let mut window = FIRST_WINDOW;
	loop {
		if let Some(answers) = interrupted_within(sql, pull, interrupt, window).await {
			return answers;
		}
		let Some(longer) = longer_window(window) else {
			panic!("the statements before the pull of `{sql}` outlasted a timeout of {window:?}");
		};
		window = longer;
	}
}

/// The timeout to retry under once the statements before the pull have outlasted `window`: four
/// times as long, and never longer than [`LONGEST_WINDOW`], which is the last timeout tried.
/// `None` when `window` is already the longest.
fn longer_window(window: std::time::Duration) -> Option<std::time::Duration> {
	(window < LONGEST_WINDOW).then(|| (window * 4).min(LONGEST_WINDOW))
}

/// The timeouts a request is retried under rise to [`LONGEST_WINDOW`] and stop there, so the
/// bound is tried and not stepped over.
#[test]
fn the_retried_timeouts_end_at_the_longest() {
	let mut windows = vec![FIRST_WINDOW];
	while let Some(longer) = longer_window(*windows.last().expect("a first window")) {
		windows.push(longer);
	}
	assert_eq!(windows.last(), Some(&LONGEST_WINDOW), "{windows:?}");
	assert!(windows.windows(2).all(|pair| pair[0] < pair[1]), "{windows:?}");
}

/// [`interrupted`] under a timeout of `window`: `None` when the deadline had passed before the
/// `pull`-th statement was taken. A cancellation has no deadline, so it always answers.
async fn interrupted_within(
	sql: &str,
	pull: usize,
	interrupt: Interrupt,
	window: std::time::Duration,
) -> Option<Vec<String>> {
	use std::sync::atomic::{AtomicBool, Ordering};

	use futures::StreamExt;
	use web_time::Instant;

	let ds = datastore().await;
	let ses = session();
	let opt = ds.setup_options(&ses);
	let mut ctx = ds.setup_ctx().expect("a context");
	let canceller = ctx.add_cancel();
	let deadline = Instant::now() + window;
	if let Interrupt::Timeout = interrupt {
		ctx.add_deadline(deadline, window);
	}
	ctx.attach_session(&ses).expect("the session attaches");

	let ast = crate::syn::parse(sql).expect("the request parses");
	let outlasted = Arc::new(AtomicBool::new(false));
	let passed = Arc::clone(&outlasted);
	let mut pulled = 0;
	// The `pull`-th statement is handed to the executor only once the deadline has passed, and
	// the executor reads the deadline when it takes a statement.
	let stream = futures::stream::iter(ast.expressions).then(move |expr| {
		pulled += 1;
		let mut wait = None;
		if pulled == pull {
			match interrupt {
				Interrupt::Cancel => canceller.cancel(),
				Interrupt::Timeout => match deadline.checked_duration_since(Instant::now()) {
					Some(left) => wait = Some(left + std::time::Duration::from_millis(50)),
					None => passed.store(true, Ordering::Relaxed),
				},
			}
		}
		async move {
			if let Some(wait) = wait {
				common::time::sleep(wait).await;
			}
			Ok(expr)
		}
	});
	let results = super::executor::Executor::execute_stream(&ds, ctx.freeze(), opt, false, stream)
		.await
		.expect("the request runs");
	if outlasted.load(Ordering::Relaxed) {
		return None;
	}
	Some(
		results
			.iter()
			.map(|res| match &res.result {
				Ok(value) => format!("{value:?}"),
				Err(e) => e.to_string(),
			})
			.collect(),
	)
}

/// The statement observed at a cancellation inside `BEGIN` has a slot of its
/// own, answered as the statements after it are, and so has the `COMMIT` that
/// ends the block.
#[tokio::test]
async fn the_statement_observed_at_a_cancellation_has_a_slot() {
	let messages =
		interrupted("BEGIN; CREATE a:1; CREATE a:2; CREATE a:3; COMMIT;", 3, Interrupt::Cancel)
			.await;
	assert_eq!(messages.len(), 5, "one result per statement: {messages:?}");
	for at in 1..=3 {
		assert!(messages[at].contains("cancelled transaction"), "{at}: {messages:?}");
	}
	assert!(messages[4].contains("Cannot COMMIT: the transaction was cancelled"), "{messages:?}");
}

/// The statement observed at a timeout inside `BEGIN` answers the timeout.
#[tokio::test]
async fn the_statement_observed_at_a_timeout_has_a_slot() {
	let messages =
		interrupted("BEGIN; CREATE a:1; CREATE a:2; CREATE a:3; COMMIT;", 3, Interrupt::Timeout)
			.await;
	assert_eq!(messages.len(), 5, "one result per statement: {messages:?}");
	assert!(messages[1].contains("cancelled transaction"), "{messages:?}");
	assert!(messages[2].contains("Timed out"), "the observed statement: {messages:?}");
	assert!(messages[3].contains("Timed out"), "{messages:?}");
	assert!(messages[4].contains("Cannot COMMIT: timed out"), "{messages:?}");
}

/// A `COMMIT` or `CANCEL` observed at the interrupt takes the terminator's
/// slot, and the block ends there.
#[tokio::test]
async fn a_terminator_observed_at_an_interrupt_takes_its_terminator_slot() {
	let messages = interrupted("BEGIN; CREATE a:1; COMMIT;", 3, Interrupt::Cancel).await;
	assert_eq!(messages.len(), 3, "{messages:?}");
	assert!(messages[1].contains("cancelled transaction"), "{messages:?}");
	assert!(messages[2].contains("Cannot COMMIT: the transaction was cancelled"), "{messages:?}");

	let messages = interrupted("BEGIN; CREATE a:1; CANCEL;", 3, Interrupt::Timeout).await;
	assert_eq!(messages.len(), 3, "{messages:?}");
	assert!(messages[2].contains("Timed out"), "{messages:?}");
}

/// A `BREAK` or `CONTINUE` outside every loop of a statement run in a
/// caller-owned transaction answers as it does at the top of a request, and
/// the statements after it run.
#[tokio::test]
async fn a_break_in_a_session_transaction_answers_invalid_control_flow() {
	use crate::kvs::QueryRequest;

	let ds = datastore().await;
	let ses = session();
	let autocommit = ds
		.execute("BREAK;", &ses, None)
		.await
		.expect("the request runs")
		.remove(0)
		.result
		.expect_err("a top-level BREAK fails");
	assert!(autocommit.to_string().contains("outside of loop"), "{autocommit}");

	let txn = Arc::new(ds.transaction(TransactionType::Write).await.expect("a transaction"));
	let results = ds
		.run(
			QueryRequest::new("BREAK; CONTINUE; RETURN 1;", &ses)
				.with_transaction(Arc::clone(&txn)),
		)
		.await
		.expect("the request runs");
	txn.cancel().await.expect("the transaction is cancelled");
	assert_eq!(results.len(), 3);
	for (at, slot) in results.iter().enumerate().take(2) {
		let error = slot.result.as_ref().expect_err("the slot is an error");
		assert_eq!(format!("{error:?}"), format!("{autocommit:?}"), "slot {at}");
	}
	assert!(results[2].result.is_ok(), "the statement after it runs");
}

/// A backend that behaves exactly like the one it wraps until `refusing` is
/// set, and then refuses to open a write transaction.
struct RefusingBuilder {
	inner: Arc<dyn surrealdb_kvs::TransactionBuilder>,
	refusing: Arc<std::sync::atomic::AtomicBool>,
}

impl surrealdb_kvs::TransactionBuilder for RefusingBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> common::future::BoxFut<
		'_,
		surrealdb_kvs::Result<(Box<dyn surrealdb_kvs::Transactable>, bool)>,
	> {
		if matches!(write, TransactionType::Write)
			&& self.refusing.load(std::sync::atomic::Ordering::SeqCst)
		{
			return Box::pin(async {
				Err(surrealdb_kvs::Error::Datastore("no write transaction".to_string()))
			});
		}
		self.inner.new_transaction(write)
	}

	fn shutdown(&self) -> common::future::BoxFut<'_, surrealdb_kvs::Result<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<surrealdb_kvs::Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}
}

/// Builds the datastore of the wrapped path with a switch that makes it refuse
/// write transactions.
struct RefusingComposer(Arc<std::sync::atomic::AtomicBool>);

impl crate::kvs::TransactionBuilderFactory for RefusingComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: tokio_util::sync::CancellationToken,
		config: surrealdb_cnf::ConfigMap,
	) -> anyhow::Result<crate::kvs::TransactionBuilderParts<Self::RouterState>> {
		let parts =
			crate::CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		Ok(crate::kvs::TransactionBuilderParts::without_router_state(Box::new(RefusingBuilder {
			inner: Arc::from(parts.builder),
			refusing: Arc::clone(&self.0),
		})))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		crate::CommunityComposer().path_valid(v)
	}
}

/// A block whose transaction cannot be opened answers every statement it holds,
/// its terminating `COMMIT` or `CANCEL` included, and the statements after the
/// block run as they would have.
#[tokio::test]
async fn a_block_that_cannot_begin_gives_its_terminator_a_slot() {
	let refusing = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_factory_path("memory", RefusingComposer(Arc::clone(&refusing)))
		.await
		.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");
	refusing.store(true, std::sync::atomic::Ordering::SeqCst);

	let render = |results: Vec<crate::dbs::QueryResult>| -> Rendered {
		results
			.into_iter()
			.map(|res| res.result.map(|value| format!("{value:?}")).map_err(|e| e.to_string()))
			.collect()
	};

	let results =
		ds.execute("BEGIN; CREATE a:1; COMMIT;", &session(), None).await.expect("the request runs");
	let answered = render(results);
	assert_eq!(answered.len(), 3, "one result per statement: {answered:?}");
	assert!(failure(&answered, 1).contains("Tried to start a transaction"), "{answered:?}");
	assert!(
		failure(&answered, 2).contains("Cannot COMMIT: the transaction could not be started"),
		"{answered:?}"
	);

	let results =
		ds.execute("BEGIN; CREATE a:1; CANCEL;", &session(), None).await.expect("the request runs");
	let answered = render(results);
	assert_eq!(answered.len(), 3, "one result per statement: {answered:?}");
	assert!(failure(&answered, 1).contains("Tried to start a transaction"), "{answered:?}");
	assert_eq!(answered[2], Ok("None".to_string()), "CANCEL answers NONE: {answered:?}");
}

#[tokio::test]
async fn split_on_set_and_streaming_cartesian_split() {
	let ds = datastore().await;
	let ses = session();
	let results = ds
		.execute(
			"SELECT s[*] FROM [{ s: {1, 2, 3} }] SPLIT s[*]; \
			 SELECT a, b, c FROM [{ a: <array> (0..500), b: <array> (0..500), c: <array> (0..500) }] SPLIT a, b, c LIMIT 2;",
			&ses,
			None,
		)
		.await
		.expect("queries should succeed");
	let set_split = results[0].result.as_ref().expect("set split should succeed");
	assert_eq!(
		format!("{set_split:?}"),
		"Array(Array([Object(Object({\"s\": Set(Set({Number(Int(1))}))})), Object(Object({\"s\": Set(Set({Number(Int(2))}))})), Object(Object({\"s\": Set(Set({Number(Int(3))}))}))]))"
	);
	let cartesian = results[1].result.as_ref().expect("cartesian split with limit should succeed");
	assert!(format!("{cartesian:?}").contains("\"c\": Number(Int(1))"), "{cartesian:?}");
}

#[tokio::test]
async fn order_by_alias_of_call_field_evaluates_once_and_scalar_order_by_succeeds() {
	let ds = datastore().await;
	let ses = session();
	let results = ds
		.execute(
			"CREATE counter:1 SET n = 0; \
			 DEFINE FUNCTION fn::bump() { RETURN (UPDATE ONLY counter:1 SET n += 1); }; \
			 SELECT (fn::bump()).n AS x FROM [1, 2] ORDER BY x DESC; \
			 SELECT VALUE n FROM ONLY counter:1; \
			 SELECT a FROM [1, 2, 3] ORDER BY b;",
			&ses,
			None,
		)
		.await
		.expect("queries should execute");
	let ordered = results[2].result.as_ref().expect("SELECT with ORDER BY alias should succeed");
	assert_eq!(
		format!("{ordered:?}"),
		"Array(Array([Object(Object({\"x\": Number(Int(2))})), Object(Object({\"x\": Number(Int(1))}))]))"
	);
	let count = results[3].result.as_ref().expect("counter query should succeed");
	assert_eq!(format!("{count:?}"), "Number(Int(2))");
	let scalar_order =
		results[4].result.as_ref().expect("SELECT a FROM [1, 2, 3] ORDER BY b should succeed");
	assert_eq!(
		format!("{scalar_order:?}"),
		"Array(Array([Number(Int(1)), Number(Int(2)), Number(Int(3))]))"
	);
}

#[tokio::test]
async fn remove_user_and_access_if_exists_on_missing_namespace_do_not_create_it() {
	let ds = datastore().await;
	let ses = Session::owner().with_ns("ghost_ns");
	let results = ds
		.execute(
			"REMOVE USER IF EXISTS ghost_user ON NAMESPACE; \
			 REMOVE ACCESS IF EXISTS ghost_access ON NAMESPACE; \
			 INFO FOR ROOT;",
			&ses,
			None,
		)
		.await
		.expect("queries should execute");
	results[0].result.as_ref().expect("REMOVE USER IF EXISTS should succeed");
	results[1].result.as_ref().expect("REMOVE ACCESS IF EXISTS should succeed");
	let info = results[2].result.as_ref().expect("INFO FOR ROOT should succeed");
	assert!(!format!("{info:?}").contains("ghost_ns"), "ghost_ns should not be created: {info:?}");
}

#[tokio::test]
async fn transaction_timeout_in_begin_block_runs_statements_after_commit() {
	let ds = Datastore::builder()
		.with_transaction_timeout(Some(std::time::Duration::from_millis(50)))
		.build_with_path("memory")
		.await
		.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");
	let ses = session();
	let results = ds
		.execute("BEGIN; CREATE a:1; SLEEP 5s; CREATE a:2; COMMIT; RETURN 42;", &ses, None)
		.await
		.expect("batch should execute");
	assert_eq!(results.len(), 6, "every statement should have a result slot: {results:?}");
	assert!(results[1].result.is_err(), "recorded statement should fail: {results:?}");
	assert!(results[2].result.is_err(), "timed-out statement should fail: {results:?}");
	assert!(results[3].result.is_err(), "following statement should fail: {results:?}");
	assert!(results[4].result.is_err(), "COMMIT should fail: {results:?}");
	assert_eq!(
		format!("{:?}", results[5].result.as_ref().expect("post-COMMIT statement should run")),
		"Number(Int(42))"
	);
}

#[tokio::test]
async fn define_api_specificity_definer_invoke_permissions_and_charset_content_type() {
	let ds = datastore().await;
	let ses = session();
	let results = ds
		.execute(
			"DEFINE API '/:a/b' FOR get THEN { RETURN { status: 200, body: 'dynamic_first' }; }; \
			 DEFINE API '/a/:b' FOR get THEN { RETURN { status: 200, body: 'fixed_first' }; }; \
			 DEFINE API '/users/:id' FOR get THEN { RETURN { status: 200, body: 'untyped' }; }; \
			 DEFINE API '/users/:id<int>' FOR get THEN { RETURN { status: 200, body: 'typed' }; }; \
			 RETURN api::invoke('/a/b').body; \
			 RETURN api::invoke('/users/42').body; \
			 DEFINE API '/denied' FOR get PERMISSIONS NONE THEN { RETURN { status: 200, body: 'secret' }; }; \
			 DEFINE FUNCTION fn::call_denied() { RETURN api::invoke('/denied').status; } PERMISSIONS FULL; \
			 DEFINE API '/json_charset' FOR post MIDDLEWARE api::req::body('json') THEN { RETURN { status: 200, body: $request.body }; }; \
			 DEFINE API '/auto_charset' FOR post MIDDLEWARE api::req::body('auto') THEN { RETURN { status: 200, body: $request.body }; }; \
			 RETURN api::invoke('/json_charset', { method: 'post', headers: { 'content-type': 'application/json; charset=utf-8' }, body: <bytes> '{\"ok\":true}' }).body; \
			 RETURN api::invoke('/auto_charset', { method: 'post', headers: { 'content-type': 'application/json; charset=utf-8' }, body: <bytes> '{\"ok\":true}' }).body;",
			&ses,
			None,
		)
		.await
		.expect("setup and owner queries should succeed");
	assert_eq!(format!("{:?}", results[4].result.as_ref().unwrap()), "String(\"fixed_first\")");
	assert_eq!(format!("{:?}", results[5].result.as_ref().unwrap()), "String(\"typed\")");
	assert_eq!(
		format!("{:?}", results[10].result.as_ref().unwrap()),
		"Object(Object({\"ok\": Bool(true)}))"
	);
	assert_eq!(
		format!("{:?}", results[11].result.as_ref().unwrap()),
		"Object(Object({\"ok\": Bool(true)}))"
	);

	// A non-admin caller invoking a definer function (`fn::call_denied`) that calls
	// `api::invoke('/denied')` must still have `/denied`'s `PERMISSIONS NONE` enforced.
	let unauth = Session::for_record(
		"test",
		"test",
		"user",
		surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
	);
	let denied = ds
		.execute("RETURN fn::call_denied();", &unauth, None)
		.await
		.expect("fn::call_denied should run");
	assert_eq!(format!("{:?}", denied[0].result.as_ref().unwrap()), "Number(Int(403))");
}
