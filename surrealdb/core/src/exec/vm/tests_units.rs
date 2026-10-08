//! A program compiles, when it is planned, every function body and closure
//! body it can reach, so running it compiles nothing: no binding, mapping,
//! lowering or scalar compilation happens once the program starts
//! ([`crate::exec::compiles`]). Each statement answers its value, for the owner
//! and for a record user whose reads are checked.

use surrealdb_types::ToSql;

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE item PERMISSIONS FOR select WHERE hidden != true, FOR create, update FULL;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD gated ON item PERMISSIONS FOR select FULL, FOR create, update WHERE fn::is_open();
	DEFINE TABLE doc PERMISSIONS FOR select WHERE fn::is_open();
	DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE calc PERMISSIONS FULL;
	DEFINE FIELD doubled ON calc COMPUTED fn::twice(n);
	DEFINE FUNCTION fn::v() { RETURN 1; } PERMISSIONS FULL;
	DEFINE FUNCTION fn::twice($x: int) { RETURN $x * 2; } PERMISSIONS FULL;
	DEFINE FUNCTION fn::nested($x: int) { RETURN fn::twice($x) + fn::v(); } PERMISSIONS FULL;
	DEFINE FUNCTION fn::count_items() { RETURN count(SELECT * FROM item); } PERMISSIONS FULL;
	DEFINE FUNCTION fn::fact($n: int) {
		RETURN IF $n <= 1 { 1 } ELSE { $n * fn::fact($n - 1) };
	} PERMISSIONS FULL;
	DEFINE FUNCTION fn::even($n: int) { RETURN IF $n = 0 { true } ELSE { fn::odd($n - 1) }; }
		PERMISSIONS FULL;
	DEFINE FUNCTION fn::odd($n: int) { RETURN IF $n = 0 { false } ELSE { fn::even($n - 1) }; }
		PERMISSIONS FULL;
	DEFINE FUNCTION fn::is_open() { RETURN count(SELECT * FROM item) > 0; } PERMISSIONS FULL;
	DEFINE FUNCTION fn::guarded() { RETURN 'ok'; } PERMISSIONS WHERE $auth.id = user:reader;
	DEFINE FUNCTION fn::refused() { RETURN 'no'; } PERMISSIONS WHERE $auth.id = user:x;
	DEFINE FUNCTION fn::apply($f: any, $x: any) { RETURN $f($x); } PERMISSIONS FULL;
	DEFINE FUNCTION fn::sees($x: int) { LET $c = count(SELECT * FROM item); RETURN $c + $x; }
		PERMISSIONS WHERE $auth.id = user:reader OR count(SELECT * FROM item) > 1;
	DEFINE PARAM $shown VALUE 'shown' PERMISSIONS FULL;
	DEFINE PARAM $gated VALUE 'gated' PERMISSIONS WHERE $auth.id = user:reader;
	DEFINE PARAM $barred VALUE 'barred' PERMISSIONS NONE;
	DEFINE PARAM $counted VALUE 'counted' PERMISSIONS WHERE count(SELECT * FROM item) > 2;
	CREATE item:1 SET n = 1, secret = 's1', tags = [1, 2];
	CREATE item:2 SET n = 2, secret = 's2', tags = [3];
	CREATE item:3 SET n = 3, hidden = true;
	CREATE doc:1 SET title = 'd';
	CREATE t:1 SET a = 1;
	CREATE calc:1 SET n = 4;
";

/// Statements calling functions and closures, which read.
const READS: &[&str] = &[
	"fn::count_items()",
	"SELECT VALUE id FROM doc",
	"$shown",
	"$gated",
	"$barred",
	"$counted",
	"[$shown, $gated, $counted]",
	"SELECT VALUE [n, $gated] FROM item ORDER BY n",
	"fn::sees(2)",
	"[fn::sees(1), fn::sees(2), $gated]",
	"fn::v()",
	"fn::twice(21)",
	"fn::nested(3)",
	"fn::count_items()",
	"fn::fact(5)",
	"fn::even(6)",
	"SELECT n, fn::twice(n) AS t FROM item ORDER BY n",
	"SELECT VALUE doubled FROM calc",
	"fn::guarded()",
	"fn::refused()",
	"SELECT VALUE id FROM doc",
	"array::map([1, 2], |$x| fn::twice($x))",
	"fn::apply(|$x| $x + 1, 1)",
	"{ LET $f = |$x| fn::twice($x); RETURN $f(2) }",
	"{ LET $f = |$x| $x * 2; RETURN $f(21) }",
	"(|$x: int| $x + 1)(1)",
	"{ LET $f = || (SELECT VALUE n FROM item ORDER BY n); RETURN $f() }",
	"{ LET $f = |$id| (SELECT secret FROM ONLY $id); RETURN [$f(item:1), $f(item:3)] }",
	"array::map([4, 5], |$v| (SELECT VALUE doubled FROM calc WHERE n = $v))",
	"{ LET $y = 10; LET $f = |$x| $x + $y; RETURN array::map([1, 2], $f) }",
	"SELECT n, (|$m| $m + n)(1) AS k FROM item ORDER BY n",
	"{ LET $f = |$s, $n| IF $n = 0 { 0 } ELSE { $s($s, $n - 1) }; RETURN $f($f, 3) }",
	"{ LET $f = || fn::v(); RETURN $f() }",
	"{ LET $f = || $auth.id; RETURN $f() }",
	"array::fold([1, 2, 3], 0, |$acc, $v| $acc + (SELECT VALUE n FROM ONLY item:1))",
];

/// Queries run through the executor on a fresh database: blocks that define
/// schema and then read it, redefine a function and call it, and writes whose
/// permission predicates call a function, each in one transaction.
const WRITES: &[&str] = &[
	"{ RETURN $gated; ALTER PARAM $gated PERMISSIONS NONE; RETURN $gated }",
	"{ RETURN $gated; ALTER PARAM $gated PERMISSIONS FULL; RETURN $gated }",
	"{ RETURN $gated; REMOVE PARAM $gated; RETURN $gated }",
	"{ RETURN $shown; DEFINE PARAM OVERWRITE $shown VALUE 's' PERMISSIONS WHERE false; RETURN $shown }",
	"{ DEFINE PARAM $fresh VALUE 'f' PERMISSIONS WHERE $auth.id = user:reader; RETURN $fresh }",
	"{ RETURN fn::guarded(); DEFINE FUNCTION OVERWRITE fn::guarded() { RETURN 'again' } PERMISSIONS NONE; RETURN fn::guarded() }",
	"{ DEFINE FUNCTION fn::made($x: int) { RETURN $x + count(SELECT * FROM item) } PERMISSIONS WHERE $auth.id = user:reader; RETURN fn::made(1) }",
	"{ DEFINE FUNCTION fn::w() { RETURN 5 }; RETURN fn::w() }",
	"{ DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2 }; RETURN fn::v() }",
	"{ LET $f = || (SELECT * FROM t); DEFINE FIELD b ON t COMPUTED a + 1; RETURN $f() }",
	"UPDATE item:1 SET gated = 1 RETURN gated",
	"BEGIN; DEFINE FUNCTION OVERWRITE fn::v() { RETURN 3 }; RETURN fn::v(); RETURN fn::nested(1); COMMIT;",
];

/// Run `sql` on the VM over `ctx`, answering its rows and how many
/// compilations running the plan made; `None` where the VM declines the
/// statement.
async fn run(ctx: &ExecutionContext, sql: &str) -> Option<(Result<Vec<Value>, String>, usize)> {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound).await?;
	let before = crate::exec::compiles::count();
	let rows = outcome(&op, ctx).await;
	Some((rows, crate::exec::compiles::count() - before))
}

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn units_a_running_program_compiles_nothing_and_answers_its_value() {
	let mut golden = Golden::new("units_a_running_program_compiles_nothing_and_answers_its_value");
	let db = TestDb::new_with_auth(SETUP).await;
	let mut compiled = Vec::new();
	let mut declined = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in READS {
			let Some((vm, compiles)) = run(&ctx, sql).await else {
				declined.push(format!("`{sql}` for the {who}"));
				continue;
			};
			golden.rows(format!("{who}: {sql}"), &vm);
			if compiles > 0 {
				compiled.push(format!("`{sql}` for the {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	golden.check();
}

/// A statement's answer as a client receives it, on one line.
fn render<E: std::fmt::Display>(result: Result<crate::types::PublicValue, E>) -> String {
	match result {
		Ok(value) => value.to_sql().replace('\n', "\\n"),
		Err(e) => format!("error: {}", e.to_string().replace('\n', "\\n")),
	}
}

/// The answers of a run and the state it leaves, as a golden body.
fn render_executed(answers: &[String], state: &[String]) -> String {
	format!("{}\nstate:\n{}", answers.join("\n"), state.join("\n"))
}

/// What `sql` answers run through the executor by `session` on a fresh
/// database, what `item` and `t` hold after, and how many compilations the
/// programs it ran made while they ran.
async fn executed(sql: &str, session: &crate::dbs::Session) -> (Vec<String>, Vec<String>, usize) {
	let db = TestDb::new_with_auth(SETUP).await;
	let before = crate::exec::compiles::count();
	let responses = db.datastore().execute(sql, session, None).await.expect("runs");
	let compiles = crate::exec::compiles::count() - before;
	let answers = responses.into_iter().map(|r| render(r.result)).collect();
	let mut state = Vec::new();
	for table in ["item", "t"] {
		let read = db
			.datastore()
			.execute(&format!("SELECT * FROM {table} ORDER BY id"), &TestDb::owner(), None)
			.await
			.expect("reads");
		state.extend(read.into_iter().map(|r| render(r.result)));
	}
	(answers, state, compiles)
}

#[tokio::test]
async fn units_a_block_that_defines_and_redefines_compiles_nothing_when_it_runs() {
	let mut golden =
		Golden::new("units_a_block_that_defines_and_redefines_compiles_nothing_when_it_runs");
	let mut compiled = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for sql in WRITES {
			let (answers, state, compiles) = executed(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_executed(&answers, &state));
			if compiles > 0 {
				compiled.push(format!("`{sql}` for the {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	golden.check();
}

/// What `sql` answers run through the executor by `session`, and how many
/// function bodies planning it compiled and how many it linked from the
/// node's units.
async fn requested(
	db: &TestDb,
	sql: &str,
	session: &crate::dbs::Session,
) -> (Vec<String>, (usize, usize)) {
	let before = super::units::BODIES.with(std::cell::Cell::get);
	let responses = db.datastore().execute(sql, session, None).await.expect("runs");
	let after = super::units::BODIES.with(std::cell::Cell::get);
	let answers = responses.into_iter().map(|r| render(r.result)).collect();
	(answers, (after.0 - before.0, after.1 - before.1))
}

/// A second request for what an earlier one compiled compiles no body: it
/// links the units the first left, for the owner and for a record user alike,
/// and the two get units of their own.
#[tokio::test]
async fn units_a_warm_request_compiles_no_body() {
	let mut golden = Golden::new("units_a_warm_request_compiles_no_body");
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "RETURN [fn::nested(3), fn::fact(4), fn::count_items()]";
	let (cold, (compiled, _)) = requested(&db, sql, &TestDb::owner()).await;
	assert!(compiled > 0, "the first request compiled no body");
	let (warm, (compiled, linked)) = requested(&db, sql, &TestDb::owner()).await;
	assert_eq!(warm, cold);
	assert_eq!(compiled, 0, "the warm request compiled {compiled} bodies");
	assert!(linked > 0);
	// A record user's plan is bound for another principal: its own units.
	let (reader_cold, (compiled, _)) = requested(&db, sql, &reader()).await;
	assert!(compiled > 0, "the record user linked the owner's units");
	let (reader_warm, (compiled, _)) = requested(&db, sql, &reader()).await;
	assert_eq!(reader_warm, reader_cold);
	assert_eq!(compiled, 0);
	let (owner_again, (compiled, _)) = requested(&db, sql, &TestDb::owner()).await;
	assert_eq!(owner_again, cold);
	assert_eq!(compiled, 0, "the record user's units displaced the owner's");
	golden.record("owner", cold.join("\n"));
	golden.record("record user", reader_cold.join("\n"));
	golden.check();
}

/// Redefining a function, or changing a table a body reads, compiles again
/// exactly the units that read it. (A body that only returns an expression is
/// inlined where it is called and compiled as no unit, so these bodies hold
/// statements or recursion.)
#[tokio::test]
async fn units_a_schema_change_invalidates_the_units_that_read_it() {
	let db = TestDb::new_with_auth(SETUP).await;
	let owner = TestDb::owner();
	for sql in ["RETURN fn::fact(3)", "RETURN fn::count_items()"] {
		requested(&db, sql, &owner).await;
	}
	db.datastore()
		.execute(
			"DEFINE FUNCTION OVERWRITE fn::count_items() { RETURN count(SELECT * FROM item WHERE n > 1); } PERMISSIONS FULL",
			&owner,
			None,
		)
		.await
		.expect("redefines");
	let (answer, (compiled, _)) = requested(&db, "RETURN fn::count_items()", &owner).await;
	assert!(answer[0].contains('2'), "{answer:?}");
	assert_eq!(compiled, 1, "fn::count_items compiled {compiled} bodies");
	let (_, (compiled, _)) = requested(&db, "RETURN fn::fact(3)", &owner).await;
	assert_eq!(compiled, 0, "redefining fn::count_items invalidated fn::fact");
	db.datastore()
		.execute("CREATE item:9 SET n = 9; DEFINE FIELD extra ON item VALUE 1", &owner, None)
		.await
		.expect("changes item");
	let (answer, (compiled, _)) = requested(&db, "RETURN fn::count_items()", &owner).await;
	assert!(answer[0].contains('3'), "{answer:?}");
	assert_eq!(compiled, 1, "a field on item left fn::count_items standing");
	let (_, (compiled, _)) = requested(&db, "RETURN fn::fact(3)", &owner).await;
	assert_eq!(compiled, 0, "a field on item invalidated fn::fact");
}

/// A unit compiled against a transaction's uncommitted redefinition is served
/// to no other transaction, while it is open or after it rolls back.
#[tokio::test]
async fn units_an_uncommitted_redefinition_is_its_transactions_alone() {
	let db = TestDb::new_with_auth(SETUP).await;
	let owner = TestDb::owner();
	let call = "fn::count_items()";
	let read = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let (before, _) = run(&read, call).await.expect("the VM runs it");
	read.txn().cancel().await.expect("cancel");
	// A transaction redefines the function and calls it, leaving the unit it
	// compiles for the node, and stays open.
	let open = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Write).await;
	let (defined, _) = run(
		&open,
		"DEFINE FUNCTION OVERWRITE fn::count_items() { RETURN 100 + count(SELECT * FROM item); } PERMISSIONS FULL",
	)
	.await
	.expect("the VM runs it");
	defined.expect("redefines");
	let compiled = super::units::BODIES.with(std::cell::Cell::get).0;
	let (inside, _) = run(&open, call).await.expect("the VM runs it");
	assert!(super::units::BODIES.with(std::cell::Cell::get).0 > compiled);
	assert_eq!(inside, Ok(vec![Value::from(103)]));
	// Another transaction, while it is open, and one after it rolls back.
	let beside = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let (outside, _) = run(&beside, call).await.expect("the VM runs it");
	assert_eq!(outside, before, "another transaction saw the uncommitted body");
	beside.txn().cancel().await.expect("cancel");
	open.txn().cancel().await.expect("cancel");
	let after = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let (after, _) = run(&after, call).await.expect("the VM runs it");
	assert_eq!(after, before, "the rolled-back body was served");
}

/// Sessions planning the same calls at once, as different principals, each
/// answer what they answer alone.
#[tokio::test]
async fn units_concurrent_sessions_answer_what_they_answer_alone() {
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "RETURN [fn::nested(3), fn::count_items(), fn::guarded(), fn::refused()]";
	let (owner_alone, _) = requested(&db, sql, &TestDb::owner()).await;
	let (reader_alone, _) = requested(&db, sql, &reader()).await;
	let owner = TestDb::owner();
	let reader = reader();
	let runs = (0..16).map(|i| {
		let session = if i % 2 == 0 {
			owner.clone()
		} else {
			reader.clone()
		};
		let db = &db;
		async move { (i % 2 == 0, requested(db, sql, &session).await.0) }
	});
	for (is_owner, answer) in futures::future::join_all(runs).await {
		assert_eq!(
			answer,
			if is_owner {
				owner_alone.clone()
			} else {
				reader_alone.clone()
			}
		);
	}
}

/// A function-heavy request, as an API handler makes one, repeated: planned
/// and run through the executor with the node's units, and compiling every
/// body with its plan. Run with
/// `cargo test --release -p surrealdb-core --lib units_request_throughput --
/// --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn units_request_throughput() {
	let db = TestDb::new_with_auth(SETUP).await;
	let session = TestDb::owner();
	let sql = "RETURN [fn::nested(3), fn::fact(6), fn::even(8), fn::count_items(), \
	           fn::apply(|$x| $x * 2, 4), array::map([1, 2, 3], |$v| fn::nested($v))]";
	let requests = 1000;
	for (label, units) in [("units", true), ("no units", false)] {
		super::units::OFF.with(|off| off.set(!units));
		let mut best = std::time::Duration::MAX;
		for _ in 0..5 {
			let start = web_time::Instant::now();
			for _ in 0..requests {
				let responses = db.datastore().execute(sql, &session, None).await.expect("runs");
				assert!(responses.iter().all(|r| r.result.is_ok()));
			}
			best = best.min(start.elapsed());
		}
		super::units::OFF.with(|off| off.set(false));
		eprintln!("{label}: {requests} requests in {best:?} ({:?} each)", best / requests);
	}
}

/// A function runs under the auth its definer capped it with: for each
/// definer of the same function and each caller, the VM answers a call whose
/// body reads, whose predicate judges the caller, and whose arguments call
/// another.
#[tokio::test]
async fn units_a_body_runs_under_the_auth_limit_its_definer_gave_it() {
	let mut golden = Golden::new("units_a_body_runs_under_the_auth_limit_its_definer_gave_it");
	const DEFINE: &str = "
		DEFINE FUNCTION fn::limited() { LET $n = count(SELECT * FROM item); RETURN $n }
			PERMISSIONS FULL;
		DEFINE FUNCTION fn::judged() { RETURN count(SELECT * FROM item) }
			PERMISSIONS WHERE $auth.id = user:reader OR $auth.id = NONE OR true;
		DEFINE FUNCTION fn::through($x: int) { RETURN $x + fn::limited() } PERMISSIONS FULL;
	";
	const CALL: &str = "[fn::limited(), fn::judged(), fn::through(fn::limited())]";
	let definers = [
		("owner", TestDb::owner()),
		("editor", crate::dbs::Session::editor().with_ns("test").with_db("test")),
		("viewer", crate::dbs::Session::viewer().with_ns("test").with_db("test")),
	];
	let callers = [
		("owner", TestDb::owner()),
		("editor", crate::dbs::Session::editor().with_ns("test").with_db("test")),
		("record user", reader()),
	];
	for (definer, definer_session) in &definers {
		let db = TestDb::new_with_auth(SETUP).await;
		db.datastore().execute(DEFINE, definer_session, None).await.expect("defines");
		for (caller_name, caller) in &callers {
			let responses = db.datastore().execute(CALL, caller, None).await.expect("runs");
			let called: Vec<String> = responses.into_iter().map(|r| render(r.result)).collect();
			golden.record(
				format!("defined by the {definer}, called by the {caller_name}"),
				called.join("\n"),
			);
		}
	}
	golden.check();
}

/// The root program `sql` lowers to, as `ctx` plans it.
async fn lowered(ctx: &ExecutionContext, sql: &str) -> super::program::Program {
	let bound = bind(ctx, sql).await;
	let planner = planner(ctx);
	let physical =
		super::optimize::optimize(super::map::map(&planner, &bound).await.expect("the plan maps"));
	super::lower::lower(&planner, &physical).await.expect("the plan lowers")
}

/// The body unit the call `function` of `program` runs, and the unit table its
/// ids index: a bundle's, for a call lowered at the top of a call path.
fn body_of(
	program: &super::program::Program,
	function: usize,
) -> (&super::program::Program, &[std::sync::Arc<super::program::Program>]) {
	let spec = &program.functions[function];
	let holder = &program.units[spec.body.expect("a call that is not inlined").0 as usize];
	let entry = holder.unit.as_ref().and_then(|head| head.entry).expect("a bundle");
	(&holder.units[entry.0 as usize], &holder.units)
}

/// What `sql` answers on the VM in `ctx`.
async fn answers(ctx: &ExecutionContext, sql: &str) -> Vec<Value> {
	let (rows, _) = run(ctx, sql).await.expect("the VM covers it");
	rows.expect("it runs")
}

/// A recursion, and a recursion through other functions, is a cycle of unit
/// ids in the arena the call's bundle holds: a body that calls a function on
/// the path being lowered holds the id of that function's unit.
#[tokio::test]
async fn units_a_recursion_is_a_cycle_of_ids() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let program = lowered(&ctx, "fn::fact(3)").await;
	let (fact, table) = body_of(&program, 0);
	assert_eq!(table.len(), 1, "one body, calling itself");
	assert_eq!(fact.functions.len(), 1);
	assert_eq!(fact.functions[0].body, Some(super::program::UnitId(0)));
	assert!(fact.units.is_empty(), "a body holds no arena of its own");

	let program = lowered(&ctx, "fn::even(4)").await;
	let (even, table) = body_of(&program, 0);
	assert_eq!(table.len(), 2, "two bodies, calling one another");
	let odd_id = even.functions[0].body.expect("even calls odd");
	assert_eq!(odd_id, super::program::UnitId(1));
	let odd = &table[odd_id.0 as usize];
	assert_eq!(odd.functions[0].body, Some(super::program::UnitId(0)));
	assert_eq!(answers(&ctx, "fn::even(4)").await, vec![Value::Bool(true)]);
}

/// A call whose function is redefined after the program was lowered fails
/// closed: it reads the definition, finds one that hashes otherwise, and runs
/// neither.
#[tokio::test]
async fn units_a_redefined_function_is_an_internal_error_not_a_compilation() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, "fn::fact(3)").await;
	let plan = super::plan(&planner(&ctx), &bound).await.expect("the VM covers it");
	let redefine =
		bind(&ctx, "DEFINE FUNCTION OVERWRITE fn::fact($n: int) { RETURN 100 } PERMISSIONS FULL")
			.await;
	let redefine = super::plan(&planner(&ctx), &redefine).await.expect("the VM covers it");
	outcome(&redefine, &ctx).await.expect("redefined");
	let before = crate::exec::compiles::count();
	let error = outcome(&plan, &ctx).await.expect_err("a definition it was not lowered from");
	assert!(error.contains("redefined after the program"), "{error}");
	assert_eq!(crate::exec::compiles::count(), before, "the call compiled while it ran");
}

/// A call of a function that is not defined fails, with no body to run.
#[tokio::test]
async fn units_a_call_of_an_undefined_function_fails() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let (vm, _) = run(&ctx, "fn::missing(1)").await.expect("the VM covers it");
	assert!(vm.is_err());
	let mut golden = Golden::new("units_a_call_of_an_undefined_function_fails");
	golden.rows("fn::missing(1)", &vm);
	golden.check();
}

/// Writes that name their tables only as their records arrive, over [`SETUP`]'s
/// tables, a table the statement defines first, a table the statement
/// creates on demand and a table a loop defines and removes: a record is
/// written by the unit lowered for its table when the statement arrived.
const DISPATCHED: &[&str] = &[
	"CREATE type::record('t', 5) SET a = 5 RETURN AFTER",
	"CREATE type::record('absent', 1) SET a = 1 RETURN AFTER",
	"CREATE type::record('absent', 1), type::record('absent', 2) SET a = 1 RETURN AFTER",
	"CREATE t:7, item:9 SET n = 9 RETURN AFTER",
	"UPSERT type::record('t', 1) SET a = 2 RETURN AFTER",
	"UPDATE type::table('item') SET n += 1 RETURN AFTER",
	"UPDATE (SELECT VALUE id FROM item) SET touched = true RETURN AFTER",
	"DELETE type::table('t') RETURN BEFORE",
	"{ LET $tb = 't'; INSERT INTO $tb { id: 4, a: 4 } }",
	"CREATE |t:1..3| SET a = 3 RETURN AFTER",
	"{ LET $x = t:9; CREATE $x SET a = 1 RETURN AFTER }",
	"{ DEFINE TABLE made PERMISSIONS FULL; CREATE type::record('made', 1) SET a = 1; RETURN (SELECT * FROM made) }",
	"{ DEFINE TABLE sch SCHEMAFULL PERMISSIONS FULL; DEFINE FIELD a ON sch TYPE int; CREATE type::record('sch', 1) SET a = 1 RETURN AFTER }",
	"{ DEFINE TABLE sch SCHEMAFULL PERMISSIONS FULL; DEFINE FIELD a ON sch TYPE int; CREATE type::record('sch', 1) SET b = 1 }",
	"FOR $i IN 0..3 { CREATE type::record('t', $i) SET a = $i; }",
	"UPDATE type::record('calc', 1) SET n = 5 RETURN doubled",
];

/// Blocks in which a table is created on demand, defined, changed or removed
/// between the writes that name it: the VM runs those whose changes it models
/// as one program lowered when the query arrives, and declines the others
/// whole, so either way nothing compiles while a program runs.
const CHANGING: &[&str] = &[
	"{ CREATE type::record('fresh', 1); CREATE type::record('fresh', 2); RETURN (SELECT * FROM fresh ORDER BY id) }",
	"FOR $i IN 0..3 { CREATE type::record('absent', $i) SET a = $i; }",
	"FOR $i IN 0..2 { DEFINE TABLE tmp PERMISSIONS FULL; CREATE type::record('tmp', $i); REMOVE TABLE tmp; }",
	"FOR $i IN 0..2 { CREATE type::record('t', $i) SET a = $i; DEFINE FIELD b ON t COMPUTED a + $i; }",
];

/// What `sql` answers run through the executor by `session` on a fresh
/// [`SETUP`] database, what the tables the statements of [`DISPATCHED`] write
/// hold after, and how many compilations the programs it ran made while they
/// ran.
async fn dispatched(sql: &str, session: &crate::dbs::Session) -> (Vec<String>, Vec<String>, usize) {
	let db = TestDb::new_with_auth(SETUP).await;
	let before = crate::exec::compiles::count();
	let responses = db.datastore().execute(sql, session, None).await.expect("runs");
	let compiles = crate::exec::compiles::count() - before;
	let answers = responses.into_iter().map(|r| render(r.result)).collect();
	let mut state = Vec::new();
	for table in ["item", "t", "calc", "absent", "fresh", "made", "sch", "tmp"] {
		let read = db
			.datastore()
			.execute(&format!("SELECT * FROM {table} ORDER BY id"), &TestDb::owner(), None)
			.await
			.expect("reads");
		state.extend(read.into_iter().map(|r| render(r.result)));
	}
	(answers, state, compiles)
}

/// A write that names its tables only as its records arrive answers and
/// leaves its records, for the owner and for a record user, and plans and
/// lowers nothing once its program starts.
#[tokio::test]
async fn units_a_dispatching_write_compiles_nothing_when_it_runs() {
	let mut golden = Golden::new("units_a_dispatching_write_compiles_nothing_when_it_runs");
	let mut compiled = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for sql in DISPATCHED.iter().chain(CHANGING) {
			let (answers, state, compiles) = dispatched(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_executed(&answers, &state));
			if compiles > 0 {
				compiled.push(format!("`{sql}` for the {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	golden.check();
}

/// Reads of tables only the data names, of tables the query defines and of
/// tables a function body reads, for the owner and for a record user: each
/// answers its rows, and running it compiles nothing.
#[tokio::test]
async fn units_a_read_of_a_table_the_data_names_compiles_nothing_when_it_runs() {
	let mut golden =
		Golden::new("units_a_read_of_a_table_the_data_names_compiles_nothing_when_it_runs");
	let mut compiled = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for sql in [
			"DEFINE FUNCTION fn::rd($t: string) { RETURN SELECT VALUE id FROM type::table($t) } PERMISSIONS FULL; RETURN fn::rd('item');",
			"DEFINE FUNCTION fn::rd($t: string) { RETURN SELECT VALUE id FROM type::table($t) } PERMISSIONS FULL; DEFINE TABLE pr PERMISSIONS FOR select WHERE fn::rd('item') != []; CREATE pr:1; RETURN SELECT * FROM pr;",
			"{ LET $t = 'item'; RETURN SELECT VALUE id FROM type::table(rand::enum($t)) }",
			"RETURN (SELECT VALUE (SELECT VALUE id FROM type::table('it' + rand::enum('em'))) FROM t);",
			"{ CREATE t:9 SET a = 1; RETURN SELECT VALUE id FROM type::table(rand::enum('t')) }",
			"{ CREATE fresh:9 SET a = 1; RETURN SELECT VALUE id FROM type::table(rand::enum('fresh')) }",
			"{ DEFINE FIELD c ON t COMPUTED 7; RETURN SELECT c FROM type::table(rand::enum('t')) }",
		] {
			let (answers, state, compiles) = executed(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_executed(&answers, &state));
			if compiles > 0 {
				compiled.push(format!("`{sql}` for the {who}: {compiles} compilations"));
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	golden.check();
}

/// What `sql` answers for `session` on a fresh database with `SETUP`,
/// compiling every body with its plan, as a node keeping no units does.
async fn alone(sql: &str, session: &crate::dbs::Session) -> Vec<String> {
	let db = TestDb::new_with_auth(SETUP).await;
	super::units::OFF.with(|off| off.set(true));
	let (answers, _) = requested(&db, sql, session).await;
	super::units::OFF.with(|off| off.set(false));
	answers
}

/// A function unit is compiled once per class of principal and served to
/// every principal of that class: system users of one level kind whatever
/// their role, and record users whatever their record. Each answers what it
/// answers compiling alone, its own `$auth` included, and a principal of
/// another class never runs a unit compiled for the first.
#[tokio::test]
async fn units_are_shared_by_every_principal_of_a_class() {
	let sql = "RETURN [fn::nested(3), fn::fact(4), fn::count_items(), fn::sees(1), \
	           fn::guarded(), fn::refused(), fn::apply(|$x| $x + 1, 1)]";
	let db_user = |role| {
		crate::dbs::Session::for_level(
			crate::iam::Level::Database("test".into(), "test".into()),
			role,
		)
	};
	let record = |id: &str| {
		let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", id));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	};
	// Each class, its first principal compiling and the rest linking.
	let classes: Vec<(&str, Vec<crate::dbs::Session>)> = vec![
		(
			"root",
			vec![
				TestDb::owner(),
				crate::dbs::Session::editor().with_ns("test").with_db("test"),
				crate::dbs::Session::viewer().with_ns("test").with_db("test"),
			],
		),
		(
			"database",
			vec![
				db_user(crate::iam::Role::Owner),
				db_user(crate::iam::Role::Editor),
				db_user(crate::iam::Role::Viewer),
			],
		),
		("record", vec![record("reader"), record("x"), record("other")]),
	];
	let db = TestDb::new_with_auth(SETUP).await;
	for (class, sessions) in &classes {
		for (i, session) in sessions.iter().enumerate() {
			let (answers, (compiled, linked)) = requested(&db, sql, session).await;
			assert_eq!(answers, alone(sql, session).await, "{class} principal {i}");
			if i == 0 {
				assert!(compiled > 0, "the first {class} principal linked another class's units");
			} else {
				assert_eq!(compiled, 0, "{class} principal {i} compiled {compiled} units");
				assert!(linked > 0);
			}
		}
	}
}

/// A unit kept for one class is linked into a plan for a principal of
/// another class nowhere: the frame it was lowered for admits the class it
/// was compiled for alone, and a run under a principal of another class is
/// refused.
#[tokio::test]
async fn a_unit_frame_admits_its_class_alone() {
	use super::program::{PrincipalClass, UnitFrame};
	let owner = crate::iam::Auth::for_root(crate::iam::Role::Owner);
	let viewer = crate::iam::Auth::for_root(crate::iam::Role::Viewer);
	let db_owner = crate::iam::Auth::for_db(crate::iam::Role::Owner, "test", "test");
	let other_db = crate::iam::Auth::for_db(crate::iam::Role::Owner, "test", "other");
	let reader = crate::iam::Auth::for_record("user:reader".into(), "test", "test", "user");
	let other = crate::iam::Auth::for_record("user:other".into(), "test", "test", "user");
	let anon = crate::iam::Auth::default();
	let class = |auth: &crate::iam::Auth, enabled| {
		PrincipalClass::of(enabled, auth, Some("test"), Some("test"))
	};
	assert_eq!(class(&owner, true), class(&viewer, true));
	assert_eq!(class(&reader, true), class(&other, true));
	assert_ne!(class(&owner, true), class(&db_owner, true));
	assert_ne!(class(&db_owner, true), class(&other_db, true));
	// An anonymous session is checked where authentication is enforced, and
	// a record user's class is that of a checked principal left alone by
	// every cap, which an anonymous one on an open datastore is not.
	assert_eq!(class(&anon, true), class(&reader, true));
	assert_ne!(class(&anon, false), class(&reader, true));
	let digest = |auth| UnitFrame::digest(true, Some(auth), Some("test"), Some("test"));
	assert_ne!(digest(&owner), digest(&reader));
	assert_eq!(UnitFrame::digest(true, None, Some("test"), Some("test")), 0);
}

/// How long the first request for a function call and for a `FETCH` takes on
/// a fresh node, which compiles their units, against a warm request, which
/// loads them from the compile cache: the median of 9 fresh datastores for
/// the first, and of 200 requests for the warm. Run with
/// `cargo test --release -p surrealdb-core --lib units_first_and_warm_latency
/// -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn units_first_and_warm_latency() {
	const LATENCY_SETUP: &str = "
		DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select WHERE $auth.id != NONE OR true;
		DEFINE FIELD label ON person COMPUTED 'p' + <string> n;
		DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD author ON post TYPE option<record>;
		DEFINE FUNCTION fn::score($x: int) {
			LET $n = count(SELECT * FROM person WHERE n < $x);
			RETURN $n * 2 + $x;
		} PERMISSIONS FULL;
		FOR $i IN 0..50 { CREATE type::record('person', $i) SET n = $i };
		FOR $i IN 0..50 { CREATE type::record('post', $i) SET author = type::record('person', $i) };
	";
	fn median(mut xs: Vec<std::time::Duration>) -> std::time::Duration {
		xs.sort();
		xs[xs.len() / 2]
	}
	for (label, sql) in [
		("function call", "RETURN fn::score(10)"),
		("fetch", "SELECT * FROM post ORDER BY id LIMIT 10 FETCH author"),
	] {
		let mut first = Vec::new();
		let mut warm = Vec::new();
		for _ in 0..9 {
			let db = TestDb::new_with_auth(LATENCY_SETUP).await;
			let session = TestDb::owner();
			let started = web_time::Instant::now();
			let responses = db.datastore().execute(sql, &session, None).await.expect("runs");
			first.push(started.elapsed());
			assert!(responses.iter().all(|r| r.result.is_ok()));
			for _ in 0..200 / 9 + 1 {
				let started = web_time::Instant::now();
				let responses = db.datastore().execute(sql, &session, None).await.expect("runs");
				warm.push(started.elapsed());
				assert!(responses.iter().all(|r| r.result.is_ok()));
			}
		}
		eprintln!("{label}: first {:?}, warm {:?}", median(first), median(warm));
	}
}

/// A unit's effect summary is derived from its definition: the tables its
/// reads and writes name, any table where a target is evaluated, and the
/// functions it calls named, not folded in. A table's read unit reads its own
/// table and the tables its permissions and computed fields read.
#[tokio::test]
async fn a_unit_carries_its_derived_footprint() {
	use super::dispatch::Reach;
	use crate::val::TableName;
	let db = TestDb::new_with_auth(&format!(
		"{SETUP}
		 DEFINE FUNCTION fn::writes($x: int) {{ CREATE t SET n = $x; RETURN fn::count_items() }}
			PERMISSIONS FULL;
		 DEFINE FUNCTION fn::dynamic($t: string) {{ RETURN SELECT * FROM type::table($t) }}
			PERMISSIONS FULL;"
	))
	.await;
	let owner = TestDb::owner();
	db.datastore()
		.execute(
			"RETURN [fn::count_items(), fn::writes(1), fn::dynamic('t'), SELECT * FROM type::table(rand::enum('doc'))]",
			&owner,
			None,
		)
		.await
		.expect("runs");
	let ctx = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let planner = super::tests::planner(&ctx);
	let units = std::sync::Arc::clone(ctx.ctx().compiled_units().expect("the node keeps units"));
	let txn = ctx.txn();
	let frame = super::lower::statement_frame(&planner);
	let tables = |names: &[&str]| {
		Reach::Tables(names.iter().map(|name| TableName::from(name.to_string())).collect())
	};
	let function = |name: &'static str| {
		let (planner, units, txn, ctx, frame) = (&planner, &units, &txn, &ctx, frame.clone());
		async move {
			let key = planner.function_unit_key(name).await.expect("keyed");
			units
				.functions()
				.lookup(&key, &frame, txn, ctx.ctx())
				.await
				.flatten()
				.unwrap_or_else(|| panic!("fn::{name} kept"))
				.footprint
		}
	};
	let counted = function("count_items").await;
	assert_eq!(counted.reads, tables(&["item"]));
	assert_eq!(counted.writes, Reach::none());
	let writes = function("writes").await;
	assert!(writes.may_write(&TableName::from("t".to_owned())));
	assert!(!writes.may_write(&TableName::from("item".to_owned())));
	assert_eq!(writes.calls, ["count_items".to_owned()].into_iter().collect());
	assert!(writes.effects.contains(crate::expr::logical::EffectSet::WRITES_DATA));
	assert_eq!(function("dynamic").await.reads, Reach::Any);
	let key = planner.table_unit_key(&TableName::from("doc".to_owned())).await.expect("keyed");
	let read = units.table_reads().lookup(&key, &frame, &txn, ctx.ctx()).await.expect("kept");
	// The owner's read applies no permission.
	assert_eq!(read.footprint.reads, tables(&["doc"]));
	assert!(read.footprint.calls.is_empty());
	txn.cancel().await.expect("cancel");
	// A record user's read judges `doc`'s permission, which calls
	// `fn::is_open`: named, not folded in.
	let sql = "SELECT * FROM type::table(rand::enum('doc'))";
	db.datastore().execute(sql, &reader(), None).await.expect("runs");
	let ctx = db.exec_ctx_as(&reader(), crate::kvs::TransactionType::Read).await;
	let planner = super::tests::planner(&ctx);
	let txn = ctx.txn();
	let frame = super::lower::statement_frame(&planner);
	let read = units.table_reads().lookup(&key, &frame, &txn, ctx.ctx()).await.expect("kept");
	assert_eq!(read.footprint.reads, tables(&["doc"]));
	assert!(read.footprint.calls.contains("is_open"), "{:?}", read.footprint);
	txn.cancel().await.expect("cancel");
}

/// A function called where reads are made at a `VERSION` stamp runs the
/// definition as of the stamp, in the statement and in a subquery of it,
/// after the current definition's unit was compiled and kept; an unversioned
/// call still runs the current one. On every versioned backend this build
/// has here (surrealkv; the language suite runs it on RocksDB too), and on memory
/// for the unversioned calls, which it cannot stamp.
#[tokio::test]
async fn a_versioned_read_calls_the_function_as_of_its_stamp() {
	let setup =
		"DEFINE FUNCTION fn::v() { RETURN (SELECT VALUE 'old' FROM ONLY t:1) } PERMISSIONS FULL;
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL; CREATE t:1;";
	let redefine = "DEFINE FUNCTION OVERWRITE fn::v() { RETURN (SELECT VALUE 'new' FROM ONLY t:1) } PERMISSIONS FULL;";
	let session = TestDb::owner();
	let mem = TestDb::new(setup).await;
	mem.datastore().execute(redefine, &session, None).await.expect("redefines");
	let current: Vec<String> = mem
		.datastore()
		.execute("RETURN fn::v()", &session, None)
		.await
		.unwrap()
		.into_iter()
		.map(|r| render(r.result))
		.collect();
	assert_eq!(current, vec!["'new'".to_owned()]);
	#[allow(unused_mut)]
	let mut paths: Vec<&str> = Vec::new();
	#[cfg(feature = "kv-surrealkv")]
	paths.push("surrealkv");
	for scheme in paths {
		let dir = temp_dir::TempDir::new().expect("a temporary directory");
		let path =
			format!("{scheme}://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
		let ds = crate::kvs::Datastore::builder()
			.with_capabilities(crate::dbs::Capabilities::all())
			.build_with_path(&path)
			.await
			.expect("a versioned datastore");
		ds.execute("DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test", &session, None)
			.await
			.expect("defines the database");
		ds.execute(setup, &session, None).await.expect("sets up");
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		let stamp = ds.execute("RETURN time::now()", &session, None).await.unwrap();
		let stamp = render(stamp.into_iter().next().expect("a response").result);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		ds.execute(redefine, &session, None).await.expect("redefines");
		let sql = format!(
			"RETURN fn::v();
			 SELECT VALUE fn::v() FROM t VERSION {stamp};
			 SELECT VALUE (SELECT VALUE fn::v() FROM ONLY t:1) FROM t VERSION {stamp};
			 RETURN fn::v();"
		);
		let answers: Vec<String> = ds
			.execute(&sql, &session, None)
			.await
			.expect("runs")
			.into_iter()
			.map(|r| render(r.result))
			.collect();
		assert_eq!(answers, vec!["'new'", "['old']", "['old']", "'new'"], "{scheme} at {stamp}");
	}
}

/// A body that reads an argument where only a value answers is compiled for
/// the values each call brings. The node keeps at most
/// `specialised_body_cache_size` such bodies, and once a function's body has
/// been compiled for `specialised_body_values` distinct values its calls run
/// the body compiled to read them when they run, where that compiles; where it
/// does not, the bodies compiled for values are evicted least recently used
/// first. Many distinct values answer what each answers alone either way.
#[tokio::test]
async fn bodies_compiled_for_argument_values_stay_bounded() {
	let setup = "
		DEFINE TABLE t;
		FOR $i IN 0..60 { CREATE type::record('t', $i) SET n = $i };
		DEFINE FUNCTION fn::by_n($n: int) { RETURN (SELECT VALUE id FROM t WHERE n = $n) };
		DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE doc;
		DEFINE INDEX ft ON doc FIELDS text FULLTEXT ANALYZER words BM25;
		FOR $i IN 0..30 { CREATE type::record('doc', $i) SET text = 'w' + <string> $i };
		DEFINE FUNCTION fn::search($q: string) {
			RETURN (SELECT VALUE search::score(1) > 0 FROM doc WHERE text @1@ $q)
		};
	";
	let config = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("specialised_body_cache_size", "4")
		.with_key_value("specialised_body_values", "8");
	let db = TestDb::new_with_config(setup, config).await;
	let owner = TestDb::owner();
	for sql in [
		// A body that compiles to read its argument: after eight values the
		// calls run it.
		"LET $all = SELECT VALUE fn::by_n(n) = [id] FROM t; RETURN [array::all($all), array::len($all)]",
		// A body whose MATCHES needs its argument's value: every call runs a
		// body compiled for its value.
		"LET $all = SELECT VALUE fn::search(text) = [true] FROM doc; RETURN [array::all($all), array::len($all)]",
	] {
		let responses = db.datastore().execute(sql, &owner, None).await.expect("runs");
		let answer = responses.into_iter().last().expect("answers").result.expect("succeeds");
		let expected = if sql.contains("by_n") {
			"[true, 60]"
		} else {
			"[true, 30]"
		};
		assert_eq!(answer.to_sql(), expected, "`{sql}`");
	}
	let ctx = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let units = std::sync::Arc::clone(ctx.ctx().compiled_units().expect("the node keeps units"));
	assert!(
		units.specialised_bodies().len() <= 4,
		"the node keeps {} bodies compiled for values",
		units.specialised_bodies().len()
	);
	assert!(units.generic_bodies().len() >= 1, "no body compiled to read its arguments");
}

/// A body compiled for a call's argument values is served only to calls whose
/// values are the same values, not merely equal ones: `1`, `1.0f` and `1dec`
/// compare equal, as do `0.0f` and `-0.0f`, and as do arrays of them, but a
/// body that folded one answers as that one. Each call answers on a node
/// that compiled bodies for all the values before it as it answers on a node
/// that compiled none.
#[tokio::test]
async fn bodies_compiled_for_argument_values_key_them_type_exact() {
	let setup = "
		DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE t;
		DEFINE INDEX ft ON t FIELDS txt FULLTEXT ANALYZER words BM25;
		CREATE t:1 SET txt = '1';
		DEFINE FUNCTION fn::f($q: any) {
			RETURN [type::of($q), <string> $q, (SELECT VALUE id FROM t WHERE txt @@ $q)]
		};
	";
	let owner = TestDb::owner();
	let answer = |db: &TestDb, arg: &str| {
		let sql = format!("RETURN fn::f({arg})");
		let ds = std::sync::Arc::clone(db.datastore());
		let owner = owner.clone();
		async move {
			let responses = ds.execute(&sql, &owner, None).await.expect("runs");
			responses.into_iter().last().expect("answers").result.expect("succeeds").to_sql()
		}
	};
	let warm = TestDb::new(setup).await;
	let args =
		["1", "1.0f", "1dec", "1.0dec", "0.0f", "-0.0f", "[1]", "[1.0f]", "{ a: 1 }", "{ a: 1f }"];
	for arg in args.iter().chain(args.iter()) {
		let cold = TestDb::new(setup).await;
		assert_eq!(answer(&warm, arg).await, answer(&cold, arg).await, "fn::f({arg})");
	}
	assert_eq!(answer(&warm, "1.0f").await, "['float', '1f', []]");
	assert_eq!(answer(&warm, "1").await, "['int', '1', [t:1]]");
}

/// A body compiled for argument values larger than
/// `specialised_body_max_arg_bytes` holds them, so it is compiled for its call
/// alone and not kept; one compiled for smaller values is kept.
#[tokio::test]
async fn bodies_compiled_for_large_argument_values_are_not_kept() {
	let setup = "
		DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE t;
		DEFINE INDEX ft ON t FIELDS txt FULLTEXT ANALYZER words BM25;
		CREATE t:1 SET txt = 'small';
		DEFINE FUNCTION fn::f($q: string) { RETURN (SELECT VALUE id FROM t WHERE txt @@ $q) };
	";
	let config =
		surrealdb_cnf::ConfigMap::empty().with_key_value("specialised_body_max_arg_bytes", "64");
	let db = TestDb::new_with_config(setup, config).await;
	let owner = TestDb::owner();
	let large = "x".repeat(256);
	for (sql, expected) in [
		(format!("RETURN fn::f('{large}')"), "[]"),
		(format!("RETURN fn::f('{large}')"), "[]"),
		("RETURN fn::f('small')".to_owned(), "[t:1]"),
	] {
		let responses = db.datastore().execute(&sql, &owner, None).await.expect("runs");
		let answer = responses.into_iter().last().expect("answers").result.expect("succeeds");
		assert_eq!(answer.to_sql(), expected, "`{sql}`");
	}
	let ctx = db.exec_ctx_as(&owner, crate::kvs::TransactionType::Read).await;
	let units = std::sync::Arc::clone(ctx.ctx().compiled_units().expect("the node keeps units"));
	assert_eq!(units.specialised_bodies().len(), 1, "only the small value's body is kept");
}

/// An index function reads the `MATCHES` query the name a body binds holds
/// — a function's argument, a `FOR` loop's variable — never a parameter of
/// the same name the caller binds around it: a statement answers alike with
/// and without the caller's binding.
#[tokio::test]
async fn index_functions_never_read_the_callers_param_for_a_bound_query() {
	let setup = "
		DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE doc;
		DEFINE INDEX ft ON doc FIELDS txt FULLTEXT ANALYZER words BM25 HIGHLIGHTS;
		CREATE doc:1 SET txt = 'one two';
		DEFINE FUNCTION fn::hl($q: string) {
			RETURN SELECT VALUE search::highlight('<', '>', 1) FROM doc WHERE txt @1@ $q
		};
	";
	let db = TestDb::new(setup).await;
	let owner = TestDb::owner();
	let answer = |sql: String| {
		let ds = std::sync::Arc::clone(db.datastore());
		let owner = owner.clone();
		async move {
			let responses = ds.execute(&sql, &owner, None).await.expect("runs");
			match responses.into_iter().last().expect("answers").result {
				Ok(value) => value.to_sql(),
				Err(e) => e.to_string(),
			}
		}
	};
	let looped = "(FOR $q IN ['one'] {
		RETURN SELECT VALUE search::highlight('<', '>', 1) FROM doc WHERE txt @1@ $q
	})";
	for body in ["fn::hl('one')", looped] {
		let alone = answer(format!("RETURN {body}")).await;
		let bound = answer(format!("LET $q = 'two'; RETURN {body}")).await;
		assert_eq!(bound, alone, "`{body}` with the caller's `$q` bound");
		assert_ne!(bound, "['one <two>']", "`{body}` highlighted the caller's `$q`");
	}
	assert_eq!(answer("RETURN fn::hl('one')".to_owned()).await, "['<one> two']");
}
