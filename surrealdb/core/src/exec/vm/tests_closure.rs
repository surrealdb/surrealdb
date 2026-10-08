//! A closure invoked on the VM runs its body as a program lowered for the
//! invoking frame, and answers the recorded rows, for the owner and for a
//! record user whose reads are checked.

use surrealdb_types::ToSql;

use super::closure::BODIES_RUN;
use super::golden::Golden;
use super::tests::{bind, outcome, planner, render_written, without_generated_ids};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE item PERMISSIONS FOR select WHERE hidden != true;
	DEFINE FIELD secret ON item PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD twice ON item COMPUTED n * 2;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
	DEFINE FUNCTION fn::v() { RETURN 1; } PERMISSIONS FULL;
	DEFINE FUNCTION fn::mk() { RETURN |$x| $x * 3 } PERMISSIONS FULL;
	DEFINE PARAM $p VALUE 1 PERMISSIONS FULL;
	CREATE item:1 SET n = 1, secret = 's1', tags = [1, 2];
	CREATE item:2 SET n = 2, secret = 's2', tags = [3];
	CREATE item:3 SET n = 3, hidden = true;
	CREATE t:1 SET a = 1;
";

/// Statements invoking a closure, each with whether its invocations run the
/// body as a program.
const READS: &[(&str, bool)] = &[
	("{ LET $f = |$x| $x * 2; RETURN $f(21) }", true),
	("(|$x: int| $x + 1)(1)", true),
	("{ LET $f = || (SELECT VALUE n FROM item ORDER BY n); RETURN $f() }", true),
	(
		"{ LET $f = |$id| (SELECT secret, twice FROM ONLY $id); RETURN [$f(item:1), $f(item:3)] }",
		true,
	),
	("array::map([1, 2, 3], |$v| (SELECT VALUE twice FROM item WHERE n = $v))", true),
	("{ LET $y = 10; LET $f = |$x| $x + $y; RETURN array::map([1, 2], $f) }", true),
	("{ LET $y = 10; LET $f = |$x| $x + $y; LET $y = 20; RETURN $f(1) }", true),
	("SELECT n, (|$m| $m + n)(1) AS k FROM item ORDER BY n", true),
	("SELECT VALUE tags.map(|$x| $x + n) FROM item ORDER BY n", false),
	("SELECT VALUE ['n'].map(|$x| type::field($x)) FROM item ORDER BY n", true),
	("{ LET $f = |$x| { IF $x > 1 { RETURN 'big' }; 'small' }; RETURN [$f(1), $f(2)] }", true),
	("(|| { BREAK })()", true),
	("(|$x: int| $x)('a')", false),
	("(|$x| -> int { $x })('nope')", true),
	("(|$x| -> int { $x })(7)", true),
	("(|$x: option<int>| $x)()", true),
	("{ LET $f = |$s, $n| IF $n = 0 { 0 } ELSE { $s($s, $n - 1) }; RETURN $f($f, 3) }", true),
	("{ LET $f = |$s| $s($s); RETURN $f($f) }", true),
	("{ LET $f = |$s| array::map([0], |$i| $s($s)); RETURN $f($f) }", true),
	(
		"{ LET $f = || (SELECT VALUE n FROM item WHERE time::now() > d'2000-01-01'); RETURN $f() }",
		true,
	),
	("{ LET $f = || fn::v(); RETURN $f() }", true),
	("(|$r| $r->nothing)(item:1)", true),
	("{ LET $f = || $this; RETURN $f() }", true),
	("{ LET $f = || $auth.id; RETURN $f() }", true),
	("SELECT VALUE (|| $parent.n)() FROM item ORDER BY n", true),
	("array::fold([1, 2, 3], 0, |$acc, $v| $acc + (SELECT VALUE n FROM ONLY item:1))", true),
];

/// Run `sql` on the VM, answering its rows and how many closure bodies ran as
/// programs.
async fn run(ctx: &ExecutionContext, sql: &str) -> (Result<Vec<Value>, String>, usize) {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let before = BODIES_RUN.with(std::cell::Cell::get);
	let rows = outcome(&op, ctx).await;
	(rows, BODIES_RUN.with(std::cell::Cell::get) - before)
}

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn expr2_a_closure_body_runs_as_a_program() {
	let db = TestDb::new_with_auth(SETUP).await;
	let mut golden = Golden::new("expr2_a_closure_body_runs_as_a_program");
	let mut shapes = Vec::new();
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for (sql, on_vm) in READS {
			let (rows, ran) = run(&ctx, sql).await;
			golden.rows(format!("{who}: {sql}"), &rows);
			if (ran > 0) != *on_vm {
				shapes.push(format!("`{sql}` for the {who}: {ran} bodies ran as programs"));
			}
		}
	}
	assert!(shapes.is_empty(), "{shapes:#?}");
	golden.check();
}

/// Run `sql` on a fresh database as the owner in a write transaction, and
/// answer its rows and every record of `log` and `t`.
async fn written(sql: &str) -> (Result<Vec<Value>, String>, Vec<String>, usize) {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let (rows, ran) = run(&ctx, sql).await;
	let rows = rows.map(|rows| rows.into_iter().map(without_generated_ids).collect());
	let txn = ctx.txn();
	if rows.is_ok() {
		txn.commit().await.expect("commit");
	} else {
		txn.cancel().await.expect("cancel");
	}
	let read = db.exec_ctx().await;
	let mut state = Vec::new();
	for table in ["log", "t"] {
		let op = crate::exec::operators::test_util::plan_statement(
			&format!("SELECT * FROM {table}"),
			&read,
		)
		.await;
		let mut rows: Vec<String> = outcome(&op, &read)
			.await
			.expect("the state reads")
			.into_iter()
			.map(|row| format!("{table}: {}", without_generated_ids(row).to_sql()))
			.collect();
		rows.sort();
		state.extend(rows);
	}
	(rows, state, ran)
}

/// A body that writes, or reads what an earlier invocation or the statement
/// wrote, or the catalog the statement changed, sees each of them: nothing one
/// invocation computes is seen by the next. Each statement is listed with
/// whether an invocation runs a unit: one invoked only after a schema statement
/// changed what the body was lowered against faults for a unit lowered against
/// the changed catalog.
#[tokio::test]
async fn expr2_a_closure_body_reads_what_was_written_before_it_ran() {
	let mut golden = Golden::new("expr2_a_closure_body_reads_what_was_written_before_it_ran");
	for (sql, runs_a_program) in [
		(
			"{ LET $f = |$v| (CREATE log SET v = $v RETURN VALUE v); RETURN array::map([1, 2], $f) }",
			true,
		),
		(
			"{ LET $f = || (SELECT VALUE count() FROM log GROUP ALL); CREATE log; LET $a = $f(); CREATE log; RETURN [$a, $f()] }",
			true,
		),
		(
			"{ LET $f = || (SELECT * FROM t); DEFINE FIELD b ON t COMPUTED a + 1; RETURN $f() }",
			true,
		),
		(
			"{ LET $f = || fn::v(); LET $a = $f(); DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2; }; RETURN [$a, $f()] }",
			true,
		),
		(
			"{ LET $f = || fn::v() + $p; LET $a = $f(); DEFINE PARAM OVERWRITE $p VALUE 5; RETURN [$a, $f()] }",
			true,
		),
		(
			"array::map([1, 2, 3], |$v| (CREATE log SET v = $v, c = (SELECT VALUE count() FROM log GROUP ALL) RETURN VALUE c))",
			true,
		),
	] {
		let (rows, state, ran) = written(sql).await;
		golden.record(sql, render_written(&rows, &state));
		assert_eq!(ran > 0, runs_a_program, "`{sql}`: {ran} bodies ran as programs");
	}
	golden.check();
}

/// A closure value is never stored: a write that stores one in a record fails
/// and leaves the record as it was, so no reader finds a closure in stored data
/// and no write has a closure body to compile for its readers.
#[tokio::test]
async fn expr2_a_write_storing_a_closure_fails() {
	let mut golden = Golden::new("expr2_a_write_storing_a_closure_fails");
	for sql in ["CREATE t:9 SET f = |$x| $x + 1", "UPDATE t:1 SET f = |$x| $x RETURN NONE"] {
		let (rows, state, _) = written(sql).await;
		assert!(rows.is_err(), "`{sql}` stored a closure: {rows:?}");
		golden.record(sql, render_written(&rows, &state));
	}
	golden.check();
}

/// The closure values `sql` makes on the VM, as the object they are fields of.
async fn made(ctx: &ExecutionContext, sql: &str) -> Value {
	let (rows, _) = run(ctx, sql).await;
	rows.expect("the closures are made").into_iter().next().expect("one row")
}

/// Statements invoking closures the way a literal, a method, a function and
/// `eval` hand them over, each with whether a unit runs the body: a literal a
/// builtin invokes per element runs in place, a closure a function returns
/// runs the unit the run faults for, and the closure `eval` builds is the
/// value of a statement `eval` runs on its own, which no unit stands for.
const HANDED_OVER: &[(&str, bool)] = &[
	("array::map([1, 2, 3], |$x| $x + 1)", false),
	("array::filter([1, 2, 3, 4], |$x| $x % 2 = 0)", false),
	("array::fold([1, 2, 3], 0, |$acc, $x| $acc + $x)", true),
	("{ LET $f = |$x| $x + 1; RETURN [1, 2].map($f) }", true),
	("{ LET $n = 10; LET $f = |$x| |$y| $x + $y + $n; RETURN $f(1)(2) }", true),
	("{ LET $f = fn::mk(); RETURN $f(4) }", true),
	("{ LET $f = eval::surql('|$x| $x * 5'); RETURN $f(3) }", false),
];

#[tokio::test]
async fn expr2_a_closure_handed_over_is_invoked() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let mut golden = Golden::new("expr2_a_closure_handed_over_is_invoked");
	let mut shapes = Vec::new();
	for (sql, on_unit) in HANDED_OVER {
		let (rows, ran) = run(&ctx, sql).await;
		golden.rows(*sql, &rows);
		if *on_unit && ran == 0 {
			shapes.push(format!("`{sql}`: no body ran as a unit"));
		}
	}
	assert!(shapes.is_empty(), "{shapes:#?}");
	golden.check();
}

/// The closure `sql` makes, a field `f` of the object it answers on the VM.
async fn made_closure(ctx: &ExecutionContext, sql: &str) -> Value {
	let Value::Object(object) = made(ctx, sql).await else {
		panic!("`{sql}` answers no object");
	};
	object.get("f").expect("the closure").clone()
}

/// What a run of [`run_handed`] did.
struct Handed {
	rows: Result<Vec<Value>, String>,
	/// The faults the run served.
	faults: usize,
	/// The bodies it ran as units.
	bodies: usize,
	/// The compilations the program made while it ran, faults aside.
	compiles: usize,
	/// The bodies compiled to the operator tree's form for an invocation.
	trees: usize,
}

/// `sql`, planned on the VM in `ctx` with no `$f` bound and run with `$f`
/// bound to `closure`, so no program the run plans holds a unit for it.
async fn run_handed(ctx: &ExecutionContext, sql: &str, closure: &Value) -> Handed {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound).await.expect("the VM plans it");
	let later = ctx.with_param("f", closure.clone());
	let bodies = BODIES_RUN.with(std::cell::Cell::get);
	let faults = crate::exec::compiles::faults();
	let compiles = crate::exec::compiles::count();
	let trees = crate::exec::compiles::tree_bodies();
	let rows = outcome(&op, &later).await;
	Handed {
		rows,
		faults: crate::exec::compiles::faults() - faults,
		bodies: BODIES_RUN.with(std::cell::Cell::get) - bodies,
		compiles: crate::exec::compiles::count() - compiles,
		trees: crate::exec::compiles::tree_bodies() - trees,
	}
}

/// A closure the program was not planned with — held by a parameter bound
/// after the program was planned — has no unit of the program: its first
/// invocation faults, the host compiles its body once, and every invocation
/// runs the unit. A later run links the unit the node kept and faults for
/// none; the program compiles nothing itself.
#[tokio::test]
async fn expr2_a_closure_that_exists_only_at_run_time_faults_once_for_its_unit() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let closure = made_closure(&ctx, "{ f: |$x| $x + 1 }").await;
	let run = run_handed(&ctx, "[$f(1), $f(2), $f(3)]", &closure).await;
	assert_eq!(
		run.rows,
		Ok(vec![Value::from(vec![Value::from(2), Value::from(3), Value::from(4)])])
	);
	assert_eq!(run.faults, 1, "the first invocation faults, and no other");
	assert_eq!(run.bodies, 3, "every invocation runs the unit");
	assert_eq!(run.compiles, 0, "the program compiled something");
	assert_eq!(run.trees, 0, "an invocation took the operator tree's body");
	// A later run, in a transaction of its own, faults for none.
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let run = run_handed(&ctx, "[$f(4), $f(5)]", &closure).await;
	assert_eq!(run.rows, Ok(vec![Value::from(vec![Value::from(5), Value::from(6)])]));
	assert_eq!(run.faults, 0, "the node kept the unit");
	assert_eq!(run.bodies, 2);
	assert_eq!(run.compiles, 0, "the program compiled something");
	// So does a closure-taking builtin invoking it per element.
	let run = run_handed(&ctx, "array::map([1, 2, 3, 4], $f)", &closure).await;
	assert_eq!(run.faults, 0);
	assert_eq!(run.bodies, 4);
}

/// A unit the node kept is linked only while the catalog its lowering read
/// holds: a schema change to a definition the body reads makes the next run
/// fault for a unit lowered against the change, and that run answers what
/// the changed definition answers.
#[tokio::test]
async fn expr2_a_schema_change_a_body_reads_faults_for_a_new_unit() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let closure = made_closure(&ctx, "{ f: |$x| $x + fn::v() }").await;
	let sql = "[$f(10), $f(20)]";
	let run = run_handed(&ctx, sql, &closure).await;
	assert_eq!(run.rows, Ok(vec![Value::from(vec![Value::from(11), Value::from(21)])]));
	assert_eq!(run.faults, 1);
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	assert_eq!(run_handed(&ctx, sql, &closure).await.faults, 0, "nothing the body reads changed");
	db.run("DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2; } PERMISSIONS FULL;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let run = run_handed(&ctx, sql, &closure).await;
	assert_eq!(run.rows, Ok(vec![Value::from(vec![Value::from(12), Value::from(22)])]));
	assert_eq!(run.faults, 1, "the function the body calls changed");
	assert_eq!(run.bodies, 2);
	// A schema change the body does not read keeps the unit.
	db.run("DEFINE TABLE other SCHEMALESS;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	assert_eq!(run_handed(&ctx, sql, &closure).await.faults, 0, "nothing the body reads changed");
}

/// A unit faulted for by one principal is not linked for another: the record
/// user's run faults for a unit lowered for its own frame, and each answers
/// what its own principal may read.
#[tokio::test]
async fn expr2_a_faulted_unit_is_linked_for_its_own_principal_alone() {
	let db = TestDb::new_with_auth(SETUP).await;
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let closure = made_closure(&owner, "{ f: |$id| (SELECT VALUE secret FROM ONLY $id) }").await;
	let sql = "[$f(item:1), $f(item:2)]";
	// Each principal's first run faults for the closure's unit, for the
	// descriptors its frame compiles of the table its body reads a record of,
	// and for the units of the stored clauses those descriptors evaluate, of
	// which a record user, whose reads are checked, compiles more.
	let run = run_handed(&owner, sql, &closure).await;
	assert_eq!(run.faults, 3);
	let own = run.rows;
	let record_user = db.exec_ctx_as(&reader(), crate::kvs::TransactionType::Read).await;
	let run = run_handed(&record_user, sql, &closure).await;
	assert_eq!(run.faults, 7, "the owner's unit was linked for a record user");
	// The closure's two invocations, and the units of the stored clauses the
	// record user's reads evaluate.
	assert_eq!(run.bodies, 10);
	let theirs = run.rows;
	assert_ne!(own, theirs, "the record user reads no secret");
	let mut golden = Golden::new("expr2_a_faulted_unit_is_linked_for_its_own_principal_alone");
	golden.rows("owner", &own);
	golden.rows("record user", &theirs);
	golden.check();
}

/// The fault is the instruction's: a builtin, a method and an `Invoke`
/// handed a closure no table holds each yield for its unit before the call,
/// so every invocation inside the call runs the unit and none compiles the
/// operator tree's body. Each distinct closure faults once.
#[tokio::test]
async fn expr2_an_instruction_handing_over_a_closure_faults_for_it_before_the_call() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let mut golden =
		Golden::new("expr2_an_instruction_handing_over_a_closure_faults_for_it_before_the_call");
	for (sql, made, bodies) in [
		("array::map([1, 2, 3], $f)", "{ f: |$x| $x + 7 }", 3),
		("array::fold([1, 2, 3], 0, $f)", "{ f: |$acc, $x| $acc + $x * 11 }", 3),
		("[1, 2].map($f)", "{ f: |$x| $x * 13 }", 2),
		("[$f(1), array::map([2, 3], $f)]", "{ f: |$x| $x - 17 }", 3),
		("{ g: $f }.g(5)", "{ f: |$x| $x + 19 }", 1),
		("{ abs: $f }.abs(5)", "{ f: |$x| $x + 23 }", 1),
	] {
		let closure = made_closure(&ctx, made).await;
		let run = run_handed(&ctx, sql, &closure).await;
		golden.rows(format!("{sql} with {made}"), &run.rows);
		assert_eq!(run.faults, 1, "`{sql}`: the closure faults once");
		assert_eq!(run.bodies, bodies, "`{sql}`: every invocation runs the unit");
		assert_eq!(run.trees, 0, "`{sql}`: an invocation took the operator tree's body");
		assert_eq!(run.compiles, 0, "`{sql}`: the program compiled something");
	}
	golden.check();
}

/// The counters of a run agree exactly with the faults it took: a closure
/// bound after planning faults once at its first use; a builtin invoking it
/// for many elements faults no more; and a unit that stops standing in the
/// middle of the builtin's call — the body's first write defines its table,
/// which the unit was lowered without — is linked again by one more counted
/// fault, never compiled inside the call. Every invocation runs a unit and the
/// program compiles nothing outside a fault, whether the closure reaches its
/// invocations through a builtin or through one call per element.
#[tokio::test]
async fn expr2_a_running_program_compiles_a_closure_only_in_a_counted_fault() {
	let db = TestDb::new_with_auth(SETUP).await;
	let mut golden =
		Golden::new("expr2_a_running_program_compiles_a_closure_only_in_a_counted_fault");
	let elements = |n: usize| (1..=n).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
	let calls = |n: usize| (1..=n).map(|i| format!("$f({i})")).collect::<Vec<_>>().join(", ");
	for (made, sql, faults, bodies) in [
		// First use.
		(
			"{ f: |$x| (CREATE log SET v = $x RETURN VALUE v) }",
			"array::map([1], $f)".to_owned(),
			1,
			1,
		),
		// Repeated use in one builtin call.
		("{ f: |$x| $x * 29 }", format!("array::map([{}], $f)", elements(50)), 1, 50),
		("{ f: |$x| $x * 31 }", format!("[{}]", calls(50)), 1, 50),
		// A unit invalidated in the middle of the call.
		(
			"{ f: |$x| { CREATE autoa SET v = $x; $x } }",
			format!("array::map([{}], $f)", elements(10)),
			2,
			10,
		),
		("{ f: |$x| { CREATE autob SET v = $x; $x } }", format!("[{}]", calls(10)), 2, 10),
	] {
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let closure = made_closure(&ctx, made).await;
		let run = run_handed(&ctx, &sql, &closure).await;
		ctx.txn().cancel().await.expect("cancel");
		golden.rows(made, &run.rows);
		assert_eq!(
			(run.faults, run.bodies, run.compiles, run.trees),
			(faults, bodies, 0, 0),
			"`{sql}` with `{made}`: (faults, bodies, compiles outside a fault, tree bodies)"
		);
	}
	golden.check();
}

/// A body that fails to compile is lowered once, on the compile cache's miss,
/// which keeps the failure compiling it reports — the body compiled to the
/// operator tree's form for that error alone, never evaluated — and every
/// invocation, in that run and later ones, reports it without compiling it
/// again. A call that invokes nothing reports nothing.
#[tokio::test]
async fn expr2_a_body_that_fails_to_compile_reports_the_faults_failure() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let crate::expr::logical::LogicalExpr::Closure(closure) =
		crate::exec::operators::test_util::parse_expr("|$x| (LET $a = $x)")
	else {
		panic!("a closure");
	};
	let closure = std::sync::Arc::new(*closure);
	let digest = super::program::ClosureDigest::of(&closure);
	// A value holding no compiled body, as one the VM made does.
	let value = || {
		Value::Closure(Box::new(crate::val::Closure::Builtin(std::sync::Arc::new(
			crate::exec::physical_expr::function::CompiledClosure::planned(
				&closure,
				digest,
				crate::dbs::Variables::new(),
			),
		))))
	};
	let mut golden = Golden::new("expr2_a_body_that_fails_to_compile_reports_the_faults_failure");
	for (i, sql) in ["array::map([1, 2, 3], $f)", "[$f(1), $f(2)]", "array::map([], $f)"]
		.into_iter()
		.enumerate()
	{
		let value = value();
		let run = run_handed(&ctx, sql, &value).await;
		golden.rows(sql, &run.rows);
		let expected = if i == 0 {
			(1, 0, 0, 1)
		} else {
			(0, 0, 0, 0)
		};
		assert_eq!(
			(run.faults, run.bodies, run.compiles, run.trees),
			expected,
			"`{sql}`: (faults, bodies, compiles outside a fault, tree bodies)"
		);
	}
	golden.check();
}

/// A body reading the clock runs as a unit that reads it when it runs: the
/// unit the host lowers on the compile cache's miss folds no clock, so one
/// unit serves every invocation, and the counters say exactly what was
/// compiled for it and where — the miss once per closure and frame, and
/// nothing in any later run, row or invocation.
#[tokio::test]
async fn expr2_a_body_reading_the_clock_runs_a_unit_reading_it_when_it_runs() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let body = "(SELECT VALUE n FROM item WHERE time::now() > d'2000-01-01' ORDER BY n)";
	let closure = made_closure(&ctx, &format!("{{ f: || {body} }}")).await;
	let single = made_closure(&ctx, &format!("{{ f: || {body} }}")).await;
	let first = run_handed(&ctx, "[$f()]", &single).await;
	assert_eq!(
		(first.faults, first.bodies, first.trees, first.compiles),
		(1, 1, 0, 0),
		"the first run faults once for the unit, and runs it"
	);
	let sql = "[$f(), $f()]";
	let run = run_handed(&ctx, sql, &closure).await;
	let mut golden =
		Golden::new("expr2_a_body_reading_the_clock_runs_a_unit_reading_it_when_it_runs");
	golden.rows(sql, &run.rows);
	assert_eq!(
		(run.faults, run.bodies, run.trees, run.compiles),
		(0, 2, 0, 0),
		"a later run of the same closure compiled its body again"
	);
	// A literal made per row, and one made per invocation of an enclosing
	// body, run the unit the program or the cache holds.
	for (sql, rows) in [
		(format!("SELECT VALUE (|| {body})() FROM item"), 3),
		(format!("array::map([1, 2, 3, 4], |$v| (|| {body})())"), 1),
	] {
		let bound = bind(&ctx, &sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
		let trees = crate::exec::compiles::tree_bodies();
		let compiles = crate::exec::compiles::count();
		let bodies = BODIES_RUN.with(std::cell::Cell::get);
		let ran = outcome(&op, &ctx).await;
		assert_eq!(ran.as_ref().map(Vec::len), Ok(rows), "`{sql}`: {ran:?}");
		assert_eq!(crate::exec::compiles::tree_bodies() - trees, 0, "`{sql}`: compiled a tree");
		assert_eq!(crate::exec::compiles::count() - compiles, 0, "`{sql}`: planned while running");
		assert!(BODIES_RUN.with(std::cell::Cell::get) > bodies, "`{sql}`: ran no unit");
		golden.rows(&sql, &ran);
	}
	golden.check();
}

/// An invocation from a computed field in the middle of computing it, and one
/// read at a `VERSION` stamp, run the body as a unit: one the program lowered
/// for the frame and stamp, or one the host lowers for them on the
/// instruction's fault. Nothing compiles outside a fault, and no body takes
/// the operator tree's form.
#[tokio::test]
async fn expr2_an_invocation_mid_computation_or_at_a_version_runs_a_unit() {
	let db = TestDb::new_with_auth(
		// The rows are written before the fields that hold a closure are
		// defined: a write of a table whose field clause holds a closure is
		// not covered.
		"DEFINE TABLE thing SCHEMALESS PERMISSIONS FULL;
		 DEFINE FUNCTION fn::mk() { RETURN |$x| $x * 3 } PERMISSIONS FULL;
		 DEFINE TABLE other SCHEMALESS PERMISSIONS FULL;
		 CREATE other:1 SET n = 1;
		 CREATE other:2 SET n = 2;
		 CREATE other:3 SET n = 3;
		 CREATE thing:1 SET n = 1;
		 CREATE thing:2 SET n = 2;
		 CREATE thing:3 SET n = 3;
		 DEFINE FIELD c ON thing COMPUTED (|$x| $x * 2)(n);
		 DEFINE FIELD d ON other COMPUTED array::map([n], fn::mk());",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let mut golden = Golden::new("expr2_an_invocation_mid_computation_or_at_a_version_runs_a_unit");
	for sql in [
		"SELECT VALUE c FROM thing ORDER BY id",
		"SELECT VALUE (|$x| $x + 1)(n) FROM [{ n: 1 }, { n: 2 }] VERSION d'2100-01-01T00:00:00Z'",
		// A value made per row by a function's body.
		"SELECT VALUE d FROM other ORDER BY id",
	] {
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
		let (trees, compiles) =
			(crate::exec::compiles::tree_bodies(), crate::exec::compiles::count());
		let bodies = BODIES_RUN.with(std::cell::Cell::get);
		let ran = outcome(&op, &ctx).await;
		assert_eq!(crate::exec::compiles::tree_bodies() - trees, 0, "`{sql}`: compiled a tree");
		assert_eq!(crate::exec::compiles::count() - compiles, 0, "`{sql}`: compiled outside it");
		assert!(BODIES_RUN.with(std::cell::Cell::get) > bodies, "`{sql}`: ran no unit");
		golden.rows(sql, &ran);
	}
	golden.check();
	// A closure the program does not hold, invoked at a stamp, faults once
	// for a unit lowered at the stamp, which serves every invocation at it.
	let closure = made_closure(&ctx, "{ f: |$x| $x + 1 }").await;
	let run = run_handed(
		&ctx,
		"SELECT VALUE $f(n) FROM [{ n: 1 }, { n: 2 }, { n: 3 }] VERSION d'2100-01-01T00:00:00Z'",
		&closure,
	)
	.await;
	assert_eq!(run.rows, Ok(vec![Value::from(2), Value::from(3), Value::from(4)]));
	assert_eq!(
		(run.faults, run.bodies, run.trees, run.compiles),
		(1, 3, 0, 0),
		"(faults, bodies, tree bodies, compiles outside a fault)"
	);
}

/// What a unit admits a body to do is judged where the body does it: a write a
/// body makes in a read-only transaction fails, and a write the body does not
/// reach refuses nothing.
#[tokio::test]
async fn expr2_a_unit_is_refused_an_effect_where_the_frame_refuses_it() {
	let mut golden = Golden::new("expr2_a_unit_is_refused_an_effect_where_the_frame_refuses_it");
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let closure = made_closure(
		&ctx,
		"{ f: |$go| IF $go { (CREATE log SET v = 1 RETURN VALUE v) } ELSE { 'skipped' } }",
	)
	.await;
	for sql in ["$f(false)", "$f(true)"] {
		let run = run_handed(&ctx, sql, &closure).await;
		golden.rows(format!("read-only: {sql}"), &run.rows);
		assert_eq!(run.trees, 0, "`{sql}`: an invocation took the operator tree's body");
	}
	assert_eq!(
		run_handed(&ctx, "$f(false)", &closure).await.rows,
		Ok(vec![Value::from("skipped")])
	);
	assert!(run_handed(&ctx, "$f(true)", &closure).await.rows.is_err());
	// So in every frame that refuses writes: the unit is linked and runs, and
	// the write it reaches fails with the frame's error.
	use crate::dbs::NoWriteFrame;
	for frame in [
		NoWriteFrame::PermissionPredicate,
		NoWriteFrame::ComputedField,
		NoWriteFrame::SurrealismReadOnly,
	] {
		let writable = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let refusing = writable.refusing_writes(frame);
		let skipped = run_handed(&refusing, "$f(false)", &closure).await;
		assert_eq!(skipped.rows, Ok(vec![Value::from("skipped")]), "{frame:?}");
		assert_eq!(skipped.bodies, 1, "{frame:?}: the unit did not run");
		let written = run_handed(&refusing, "$f(true)", &closure).await;
		assert!(written.rows.is_err(), "{frame:?}: the write was not refused");
		golden.rows(format!("{frame:?}: $f(true)"), &written.rows);
		assert_eq!(written.bodies, 1, "{frame:?}: the unit did not run");
	}
	golden.check();
}

/// A unit's head declares what its body may do beyond computing its value:
/// a write, a read of rows, a body the head cannot see; a schema statement is
/// declared as a write.
#[tokio::test]
async fn expr2_a_closure_unit_declares_its_effects_on_its_head() {
	use crate::expr::logical::EffectSet;
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (sql, declared, unit) in [
		("|$x| $x + 1", EffectSet::PURE, true),
		("|| rand::uuid()", EffectSet::ENTROPY, true),
		("|| { CREATE log SET v = 1 }", EffectSet::WRITES_DATA, true),
		("|$g| $g(1)", EffectSet::OPAQUE_BODY, true),
		("|| (SELECT VALUE n FROM item)", EffectSet::READS_DATA, true),
		("|$r| $r->edge", EffectSet::READS_DATA, true),
		("|| { DEFINE TABLE made SCHEMALESS }", EffectSet::WRITES_DATA, true),
	] {
		let crate::expr::logical::LogicalExpr::Closure(closure) =
			crate::exec::operators::test_util::parse_expr(sql)
		else {
			panic!("`{sql}` is no closure");
		};
		let closure = std::sync::Arc::new(*closure);
		let lowered = super::lower::lower_closure_body(&planner(&ctx), &closure, Vec::new()).await;
		assert_eq!(lowered.is_ok(), unit, "`{sql}`: whether the VM lowers a unit: {lowered:?}");
		let lowered = lowered.ok();
		let effects = match &lowered {
			Some(lowered) => {
				lowered
					.unit
					.unit
					.as_ref()
					.and_then(|head| head.closure.as_ref())
					.expect("a closure unit")
					.effects
			}
			None => crate::exec::logical::effects_of_body(&closure.body, ctx.function_registry()),
		};
		assert!(effects.contains(declared), "`{sql}` declares {effects:?}");
		if declared.is_pure() {
			assert!(effects.is_pure(), "`{sql}` declares {effects:?}");
		}
	}
}

/// A speculative run of a schema statement grants only what its real run
/// answers alike and what acts on nothing outside the transaction. The
/// instruction handing over a closure whose unit declares more is refused
/// before the call, so the body never runs speculatively; outside a
/// speculation the same invocations run.
#[tokio::test]
async fn expr2_a_speculative_run_links_no_unit_whose_effects_it_does_not_grant() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (made, granted) in [
		("{ f: |$x| $x + 1 }", true),
		("{ f: |$x| rand::uuid() }", false),
		("{ f: |$x| (CREATE log SET v = $x RETURN VALUE v) }", false),
		// A body reading the clock, and one holding a schema statement.
		("{ f: |$x| (SELECT VALUE n FROM item WHERE time::now() > d'2000-01-01') }", false),
		("{ f: |$x| { DEFINE TABLE IF NOT EXISTS made SCHEMALESS } }", false),
	] {
		let closure = made_closure(&ctx, made).await;
		let sql = "array::map([1, 2], $f)";
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM plans it");
		let speculation = ctx
			.txn()
			.speculate(crate::kvs::speculation::Overlay::default())
			.expect("the transaction speculates");
		let mut view = crate::ctx::Context::new_child(ctx.ctx());
		view.set_transaction(std::sync::Arc::clone(speculation.txn()));
		let speculating = ctx.clone().with_new_ctx(view.freeze()).with_param("f", closure.clone());
		let bodies = BODIES_RUN.with(std::cell::Cell::get);
		let ran = outcome(&op, &speculating).await;
		match granted {
			true => assert!(ran.is_ok(), "`{made}`: {ran:?}"),
			false => {
				let error = ran.expect_err(&format!("`{made}` ran speculatively"));
				assert!(error.contains("only its run"), "`{made}`: {error}");
				assert_eq!(BODIES_RUN.with(std::cell::Cell::get), bodies, "`{made}` ran a unit");
			}
		}
		drop(speculating);
		drop(speculation);
		let ran = outcome(&op, &ctx.with_param("f", closure)).await;
		assert!(ran.is_ok(), "`{made}`: {ran:?}");
	}
}

/// A run links no closure unit lowered to an instruction set other than its
/// own.
#[tokio::test]
async fn expr2_a_unit_of_another_instruction_set_is_not_linked() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let crate::expr::logical::LogicalExpr::Closure(closure) =
		crate::exec::operators::test_util::parse_expr("|$x| $x + 1")
	else {
		panic!("a closure");
	};
	let closure = std::sync::Arc::new(*closure);
	let lowered = super::lower::lower_closure_body(&planner(&ctx), &closure, Vec::new())
		.await
		.expect("the VM lowers the body");
	assert!(super::closure::takes(&lowered.unit, &lowered.table));
	let mut other = (*lowered.unit).clone();
	if let Some(head) = other.unit.as_mut().and_then(|head| head.closure.as_mut()) {
		head.isa = super::program::ISA_VERSION + 1;
	}
	assert!(!super::closure::takes(&std::sync::Arc::new(other), &lowered.table));
}

/// The code reference a closure value carries is SHA-256 over the encoding
/// `ClosureDigest::of` states, pinned here: the parameters with their kinds,
/// the return kind and the body as written, and none of what it captured.
#[tokio::test]
async fn expr2_a_closure_digest_is_stable_and_names_the_closure_alone() {
	use super::program::ClosureDigest;
	let digest = |sql: &str| {
		let crate::expr::logical::LogicalExpr::Closure(closure) =
			crate::exec::operators::test_util::parse_expr(sql)
		else {
			panic!("`{sql}` is no closure");
		};
		ClosureDigest::of(&closure)
	};
	let pinned = digest("|$x: int| $x + 1");
	// The encoding, as `ClosureDigest::of` documents it.
	let mut encoded = Vec::new();
	encoded.extend_from_slice(b"surrealdb.closure");
	encoded.extend_from_slice(&ClosureDigest::ENCODING.to_le_bytes());
	encoded.extend_from_slice(&1u32.to_le_bytes());
	let text = |encoded: &mut Vec<u8>, text: &str| {
		encoded.extend_from_slice(&(text.len() as u64).to_le_bytes());
		encoded.extend_from_slice(text.as_bytes());
	};
	text(&mut encoded, "x");
	text(&mut encoded, "int");
	encoded.push(0);
	text(&mut encoded, "$x + 1");
	let expected: [u8; 32] = sha2::Digest::finalize(sha2::Digest::chain_update(
		<sha2::Sha256 as sha2::Digest>::new(),
		&encoded,
	))
	.into();
	assert_eq!(pinned.to_string(), PINNED_DIGEST, "the encoding of a known closure moved");
	assert_eq!(pinned.to_string().len(), 64, "a 256-bit digest");
	assert_eq!(
		pinned.to_string(),
		expected.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
	);
	// Written alike, the same closure; layout of the source does not matter.
	assert_eq!(digest("|$x:int|   $x+1"), pinned);
	assert_eq!(digest("|$x: int| -> int { $x + 1 }"), digest("|$x: int|->int{$x+1}"));
	// A renamed parameter, another kind, a return kind, another body.
	for other in [
		"|$y: int| $y + 1",
		"|$x: int| $x + 2",
		"|$x: float| $x + 1",
		"|$x: int| -> int { $x + 1 }",
		"|$x: int| { $x + 1 }",
		"|$x: int, $z: any| $x + 1",
	] {
		assert_ne!(digest(other), pinned, "`{other}`");
	}
	// The values a closure captured are not part of its digest.
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let one = made_closure(&ctx, "{ LET $n = 1; ({ f: |$x| $x + $n }) }").await;
	let two = made_closure(&ctx, "{ LET $n = 2; ({ f: |$x| $x + $n }) }").await;
	assert_ne!(one, two, "different captures make different values");
	let code = |value: &Value| {
		let Value::Closure(closure) = value else {
			panic!("a closure");
		};
		let crate::val::Closure::Builtin(obj) = closure.as_ref() else {
			panic!("a compiled closure");
		};
		obj.as_any()
			.downcast_ref::<crate::exec::physical_expr::function::CompiledClosure>()
			.expect("a compiled closure")
			.digest()
	};
	assert_eq!(code(&one), code(&two));
	assert_eq!(code(&one), digest("|$x| $x + $n"));
}

/// Bodies that differ only in a literal's type, an escape, a quote or a key's
/// spelling. Equal closures have one digest. Closures that differ have
/// different digests unless their canonical text is one, which happens for
/// [`COINCIDING`] alone; every other body's text reads back to the body it was
/// printed from. A run never takes a coinciding closure's unit for the other
/// ([`expr2_closures_that_share_a_digest_never_share_a_unit`]).
#[test]
fn expr2_a_closure_digest_tells_apart_bodies_that_print_alike() {
	use super::program::ClosureDigest;
	const BODIES: &[&str] = &[
		"1",
		"1f",
		"1dec",
		"1.0",
		"1.0f",
		"'1'",
		"\"1\"",
		"r'a:1'",
		"a:1",
		"a:⟨1⟩",
		"'a:1'",
		"\"a'b\"",
		"'a\\'b'",
		"'a\"b'",
		"'$x'",
		"$x",
		"1s",
		"'1s'",
		"d'2020-01-01T00:00:00Z'",
		"'2020-01-01T00:00:00Z'",
		"[1, 2]",
		"[1, '2']",
		"{ a: 1 }",
		"{ 'a': 1 }",
		"{ 'a b': 1 }",
		"{ a: '1' }",
		"<int> '1'",
		"<string> 1",
		"NONE",
		"NULL",
		"'NONE'",
		"true",
		"'true'",
		"-1",
		"- 1",
		"1 - -1",
		"$x.a",
		"$x['a']",
		"$x[0]",
		"$x['0']",
	];
	let closures: Vec<crate::expr::logical::LogicalClosure> = BODIES
		.iter()
		.map(|body| {
			let crate::expr::logical::LogicalExpr::Closure(closure) =
				crate::exec::operators::test_util::parse_expr(&format!("|$x| {body}"))
			else {
				panic!("`{body}` is no closure body");
			};
			*closure
		})
		.collect();
	let mut coinciding = Vec::new();
	for (i, a) in closures.iter().enumerate() {
		let crate::expr::logical::LogicalExpr::Closure(again) =
			crate::exec::operators::test_util::parse_expr(&format!("|$x| {}", a.body_sql))
		else {
			panic!("`{}` reads back as no closure", a.body_sql);
		};
		assert_eq!(again.body_sql, a.body_sql, "`{}` prints otherwise when read back", BODIES[i]);
		assert_eq!(ClosureDigest::of(&again), ClosureDigest::of(a), "`{}`", BODIES[i]);
		let coincides = COINCIDING.iter().any(|pair| pair.0 == BODIES[i] || pair.1 == BODIES[i]);
		if !coincides {
			assert_eq!(again.body, a.body, "`{}` reads back as another body", BODIES[i]);
		}
		for (j, b) in closures.iter().enumerate().skip(i + 1) {
			let same = ClosureDigest::of(a) == ClosureDigest::of(b);
			if a == b {
				assert!(same, "`{}` and `{}` are one closure", BODIES[i], BODIES[j]);
			} else if same {
				coinciding.push((BODIES[i], BODIES[j]));
			}
		}
	}
	assert_eq!(coinciding, COINCIDING, "the closures whose canonical text coincides");
}

/// Closures that differ but print as one canonical text, and so share a
/// digest: a negation of a literal prints as the negative literal.
const COINCIDING: &[(&str, &str)] = &[("-1", "- 1")];

/// Two closures that share a digest are told apart wherever a unit is found
/// for a value: the run faults for each, and the node links neither's unit
/// for the other.
#[tokio::test]
async fn expr2_closures_that_share_a_digest_never_share_a_unit() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let literal = made_closure(&ctx, "{ f: || -1 }").await;
	let negated = made_closure(&ctx, "{ f: || - 1 }").await;
	let code = |value: &Value| {
		let Value::Closure(closure) = value else {
			panic!("a closure");
		};
		let crate::val::Closure::Builtin(obj) = closure.as_ref() else {
			panic!("a compiled closure");
		};
		let compiled = obj
			.as_any()
			.downcast_ref::<crate::exec::physical_expr::function::CompiledClosure>()
			.expect("a compiled closure");
		(compiled.digest(), compiled.logical().body.clone())
	};
	let (literal_code, literal_body) = code(&literal);
	let (negated_code, negated_body) = code(&negated);
	assert_eq!(literal_code, negated_code, "the closures no longer share a digest");
	assert_ne!(literal_body, negated_body);
	for closure in [&literal, &negated] {
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
		let run = run_handed(&ctx, "$f()", closure).await;
		assert_eq!(run.rows, Ok(vec![Value::from(-1)]));
		assert_eq!(run.faults, 1, "a closure linked the unit of another that shares its digest");
		assert_eq!(run.bodies, 1);
	}
	for closure in [&literal, &negated] {
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
		assert_eq!(run_handed(&ctx, "$f()", closure).await.faults, 0, "the node kept each");
	}
}

/// The canonical text the digest's encoding reads, pinned: a change to how a
/// closure's kinds or body print changes every digest, and must come with a
/// new `ClosureDigest::ENCODING`.
#[test]
fn expr2_the_printing_a_closure_digest_reads_is_pinned() {
	use surrealdb_types::ToSql;
	assert_eq!(
		super::program::ClosureDigest::ENCODING,
		1,
		"update the pins below with the version"
	);
	for (closure, kinds, body) in [
		("|$x: int| $x+1", vec!["int"], "$x + 1"),
		(
			"|$x: option<int>, $y: array<string, 3>| [$x,$y]",
			vec!["none | int", "array<string, 3>"],
			"[$x, $y]",
		),
		("|$x: any| -> int { RETURN $x }", vec!["any"], "{ RETURN $x }"),
		("|| { a: 1, 'b c': \"d'e\" }", vec![], "{ a: 1, \"b c\": \"d'e\" }"),
		("|$r| $r->edge->(? WHERE x > 1.5f)", vec!["any"], "$r->edge->(? WHERE x > 1.5f)"),
		("|| d'2020-01-01T00:00:00Z' + 1d", vec![], "d'2020-01-01T00:00:00Z' + 1d"),
	] {
		let crate::expr::logical::LogicalExpr::Closure(parsed) =
			crate::exec::operators::test_util::parse_expr(closure)
		else {
			panic!("`{closure}` is no closure");
		};
		let printed: Vec<String> = parsed.args.iter().map(|(_, kind)| kind.to_sql()).collect();
		assert_eq!(printed, kinds, "`{closure}`");
		assert_eq!(&*parsed.body_sql, body, "`{closure}`");
	}
}

/// `ClosureDigest::of` `|$x: int| $x + 1`, at encoding version 1.
const PINNED_DIGEST: &str = "535136ab825927f4ad871917d59f799bddec895ba14514d86cf7c70470131040";

/// How many faults this thread's runs took for a closure, as against those
/// for the read of a table ([`super::fault::READ_FAULTS`]).
fn closure_faults() -> usize {
	crate::exec::compiles::faults() - super::fault::READ_FAULTS.with(std::cell::Cell::get)
}

/// A unit lowered for one principal is not run for another: the closure is
/// invoked in the invoking principal's own frame, the run faulting for a unit
/// lowered for it, and answers what that principal may read.
#[tokio::test]
async fn expr2_a_closure_unit_lowered_for_another_principal_is_not_run() {
	let db = TestDb::new_with_auth(SETUP).await;
	let sql = "{ LET $f = |$id| (SELECT VALUE secret FROM ONLY $id); RETURN $f(item:1) }";
	let owner = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let record_user = db.exec_ctx_as(&reader(), crate::kvs::TransactionType::Read).await;
	let bound = bind(&owner, sql).await;
	let op = super::plan(&planner(&owner), &bound).await.expect("the VM plans it");
	let before = BODIES_RUN.with(std::cell::Cell::get);
	let faults = closure_faults();
	let own = outcome(&op, &owner).await;
	assert!(BODIES_RUN.with(std::cell::Cell::get) > before, "the owner's unit did not run");
	assert_eq!(closure_faults(), faults, "the owner's run faulted for the closure");
	let faults = closure_faults();
	let elsewhere = outcome(&op, &record_user).await;
	assert_eq!(closure_faults() - faults, 1, "a unit lowered for the owner ran for a record user");
	let mut golden = Golden::new("expr2_a_closure_unit_lowered_for_another_principal_is_not_run");
	golden.rows("owner", &own);
	golden.rows("record user", &elsewhere);
	golden.check();
}

/// Per-element invocation of a closure value, as `array::map` and `$f(…)` make
/// it, timed on the VM. Run with `cargo test --release -p surrealdb-core --lib
/// expr2_closure_throughput -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn expr2_closure_throughput() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE item SCHEMALESS; CREATE |item:1..200| SET n = rand::int(1, 100);",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for (label, sql) in [
		("map literal", "array::len(array::map(array::range(0, 4000), |$v| $v * 2))"),
		(
			"map value",
			"{ LET $f = |$v| $v * 2; RETURN array::len(array::map(array::range(0, 4000), $f)) }",
		),
		(
			"fold value",
			"{ LET $f = |$acc, $v| $acc + $v; RETURN array::fold(array::range(0, 4000), 0, $f) }",
		),
		(
			"invoke per row",
			"{ LET $f = |$x| $x + 1; RETURN math::sum(SELECT VALUE $f(n) FROM item) }",
		),
		(
			"map statement body",
			"array::len(array::map(array::range(0, 200), |$v| (SELECT VALUE n FROM ONLY type::record('item', $v + 1))))",
		),
	] {
		let mut answers = Vec::new();
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
		let mut best = std::time::Duration::MAX;
		for _ in 0..9 {
			let started = web_time::Instant::now();
			answers.push(outcome(&op, &ctx).await);
			best = best.min(started.elapsed());
		}
		eprintln!("{label}: {best:?}");
		assert!(answers.windows(2).all(|w| w[0] == w[1]), "{label}");
	}
}
