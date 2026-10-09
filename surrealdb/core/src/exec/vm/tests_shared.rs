//! A pure, non-leaf subexpression an evaluation region repeats is evaluated
//! once per row and read from a register at its later occurrences, with the
//! rows and the errors the operator tree gives, and a draw of a generator is
//! never shared. A region is one pipeline's row code, one data clause, one
//! `IF` condition chain or one `FOR` range; sharing never crosses one.

use super::program::{ArithOp, Instr, NavOp, Program, UnaryOp};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::TransactionType;
use crate::val::Value;

const SETUP: &str = "
	CREATE t:1 SET a = 3, b = 'x';
	CREATE t:2 SET a = -1, b = 'y';
	CREATE t:3 SET a = 5, b = 'z';
";

async fn lowered(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// The rows of `sql` on the VM, which must plan it, and on the operator tree.
async fn both(
	ctx: &ExecutionContext,
	sql: &str,
) -> (Result<Vec<Value>, String>, Result<Vec<Value>, String>) {
	let bound = bind(ctx, sql).await;
	let vm = super::plan(&planner(ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let tree = planner(ctx).plan_logical(bound).await.expect("the operator tree plans");
	(outcome(&vm, ctx).await, outcome(&tree, ctx).await)
}

/// Every instruction of every pipeline of the program: the scans, the host
/// pipelines a data clause or a block holds, and the block code.
fn instructions(program: &Program) -> impl Iterator<Item = &Instr> {
	program.pipelines.iter().chain(&program.host_code).flat_map(|pipeline| &pipeline.code)
}

fn count(program: &Program, wanted: impl Fn(&Instr) -> bool) -> usize {
	instructions(program).filter(|instr| wanted(instr)).count()
}

fn multiplies(program: &Program) -> usize {
	count(program, |instr| {
		matches!(
			instr,
			Instr::Arith {
				op: ArithOp::Multiply,
				..
			}
		)
	})
}

fn accesses(program: &Program) -> usize {
	count(program, |instr| matches!(instr, Instr::Access { .. }))
}

fn casts(program: &Program) -> usize {
	count(program, |instr| {
		matches!(
			instr,
			Instr::Unary {
				op: UnaryOp::Cast { .. },
				..
			}
		)
	})
}

fn negations(program: &Program) -> usize {
	count(program, |instr| {
		matches!(
			instr,
			Instr::Unary {
				op: UnaryOp::Negate,
				..
			}
		)
	})
}

fn indexes(program: &Program) -> usize {
	count(program, |instr| {
		matches!(
			instr,
			Instr::Nav {
				op: NavOp::Index,
				..
			}
		)
	})
}

fn calls(program: &Program) -> usize {
	count(program, |instr| matches!(instr, Instr::Call { .. }))
}

/// `sql` on the VM alone, over a database set up with `setup`, for a block the
/// operator tree does not plan.
async fn vm_only(setup: &str, sql: &str) -> Result<Vec<Value>, String> {
	vm_only_as(setup, sql, &TestDb::owner()).await
}

/// [`vm_only`] as `session`.
async fn vm_only_as(
	setup: &str,
	sql: &str,
	session: &crate::dbs::Session,
) -> Result<Vec<Value>, String> {
	let db = TestDb::new(setup).await;
	let ctx = db.exec_ctx_as(session, TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	outcome(&op, &ctx).await
}

/// `sql` on the VM and on the operator tree, each over a database of its own
/// set up with `setup`, for a statement that writes.
async fn both_writing(
	setup: &str,
	sql: &str,
) -> (Result<Vec<Value>, String>, Result<Vec<Value>, String>) {
	let vm = TestDb::new(setup).await;
	let ctx = vm.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let vm = outcome(&op, &ctx).await;
	let tree = TestDb::new(setup).await;
	let ctx = tree.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = planner(&ctx).plan_logical(bound).await.expect("the operator tree plans");
	(vm, outcome(&op, &ctx).await)
}

#[tokio::test]
async fn a_repeated_subexpression_is_evaluated_once() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT a * 2 AS x, a * 2 + 1 AS y FROM t";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
	// Beside a value that is not repeated, the product is still computed once.
	let sql = "SELECT VALUE (a * 2) + (a * 2) FROM t";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert_eq!(vm, tree, "`{sql}`");
}

#[tokio::test]
async fn a_value_is_read_where_the_operator_tree_reads_it_again() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT a * 2 > 1 AND a * 2 < 9 AS inside FROM t",
		"SELECT (IF a > 0 THEN a * 2 ELSE 0 END) AS x, a * 2 AS y FROM t",
		"SELECT a * 2 AS x, (IF a > 0 THEN a * 2 ELSE 0 END) AS y FROM t",
		"SELECT a * 2 AS x, b = 'x' OR a * 2 > 5 AS y FROM t",
		"SELECT b = 'x' AND a * 2 > 1 AS x, a * 2 AS y FROM t",
		"SELECT a * 2 AS x, (SELECT VALUE a * 2 FROM ONLY t:1) AS y FROM t",
		"SELECT a * 2 AS x, a * 2 + 1 AS y, a * 2 + 1 AS z, (a * 2 + 1) * 3 AS w FROM t",
		"SELECT a * 2 AS x, a * 2 AS x FROM t",
	] {
		let (vm, tree) = both(&ctx, sql).await;
		assert!(vm.is_ok(), "`{sql}`: {vm:?}");
		assert_eq!(vm, tree, "`{sql}`");
	}
}

#[tokio::test]
async fn a_fallible_shared_value_raises_once_where_main_raises() {
	let db = TestDb::new("CREATE t:1 SET v = 'x';").await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT <int> v AS x, <int> v + 1 AS y FROM t";
	let (vm, tree) = both(&ctx, sql).await;
	assert!(tree.is_err(), "`{sql}`: {tree:?}");
	assert_eq!(vm, tree, "`{sql}`");
}

#[tokio::test]
async fn draws_are_not_shared() {
	let db = TestDb::new("CREATE t:1;").await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT rand::uuid() AS a, rand::uuid() AS b FROM ONLY t:1";
	let bound = bind(&ctx, sql).await;
	let vm = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&vm, &ctx).await.expect("the VM runs");
	let [Value::Object(row)] = rows.as_slice() else {
		panic!("one row, got {rows:?}");
	};
	assert_ne!(row.get("a"), row.get("b"), "`{sql}`");
}

#[tokio::test]
async fn a_read_of_a_proven_object_is_evaluated_once() {
	let db = TestDb::new("CREATE t:1;").await;
	let mut session = TestDb::owner();
	session.variables.insert("v", crate::syn::value("{ f: 1 }").expect("parses"));
	let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
	let sql = "SELECT (<object> $v).f AS x, (<object> $v).f AS y FROM ONLY t:1";
	assert_eq!(accesses(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
}

const ROWS: &str = "
	CREATE t:1 SET a = 3, b = 'x', v = 'x', arr = [1, 2, 3], o = { k: 1 };
	CREATE t:2 SET a = -1, b = 'y', v = 'y', arr = [4, 5], o = { k: 2 };
	CREATE t:3 SET a = 5, b = 'z', v = '7', arr = [6], o = { k: 3 };
";

/// The program and both outcomes of `sql` over `ROWS`, the program holding
/// `shares` multiplications less than the unshared baseline says.
async fn counted(sql: &str, count: fn(&Program) -> usize, expect: usize, parity: bool) {
	let db = TestDb::new(ROWS).await;
	let ctx = db.exec_ctx().await;
	assert_eq!(count(&lowered(&ctx, sql).await), expect, "`{sql}`");
	if parity {
		let (vm, tree) = both(&ctx, sql).await;
		assert_eq!(vm, tree, "`{sql}`");
	}
}

#[tokio::test]
async fn reach_matches_the_vm_control_flow() {
	// The first condition of an `IF` holds the first occurrence and the branch
	// reads it; an occurrence in a branch that comes first is lowered as
	// written and does not block later unconditional ones; the index of an
	// index step is evaluated against the document root and is not shared with
	// the row's reads.
	for (sql, products) in [
		("SELECT (IF a * 2 > 1 THEN a * 2 ELSE 0 END) AS x FROM t", 1),
		("SELECT (IF b = 'x' THEN 0 ELSE a * 2 END) AS x, a * 2 AS y FROM t", 2),
		("SELECT (IF b = 'x' THEN a * 2 ELSE 0 END) AS x, a * 2 AS y, a * 2 + 1 AS z FROM t", 2),
		("SELECT a * 2 > 1 AND a * 2 < 9 FROM t", 1),
		("SELECT b = 'x' AND a * 2 > 1 AS x, a * 2 AS y FROM t", 2),
		("SELECT arr[a * 2 % 2] AS x, a * 2 AS y FROM t", 2),
	] {
		counted(sql, multiplies, products, true).await;
	}
	// No SQL statement reaches a projection-root `SetField` with a value that
	// is not a leaf, so the `SetField` classification is pinned by the DAG's
	// own tests (`dag::tests::a_set_field_*`).
}

#[tokio::test]
async fn a_predicate_shares_with_the_projection() {
	counted("SELECT a * 2 AS x FROM t WHERE a * 2 > 1", multiplies, 1, true).await;
	// A cast that fails raises where the predicate raises, with its message.
	let db = TestDb::new(ROWS).await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT <int> v AS x FROM t WHERE <int> v > 0";
	assert_eq!(casts(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(tree.is_err(), "`{sql}`: {tree:?}");
	assert_eq!(vm, tree, "`{sql}`");
	// A layer between the predicate and the projection starts a new frame: the
	// filter runs beneath a `COMPUTED` field's layer and the projection above
	// it, so the product is computed on each side (and once more by the field's
	// own body).
	let db = TestDb::new(
		"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD a ON t TYPE int;
		 DEFINE FIELD c ON t COMPUTED a * 10;
		 CREATE t:1 SET a = 3; CREATE t:2 SET a = 5;",
	)
	.await;
	let ctx = db.exec_ctx().await;
	let sql = "SELECT a * 2 AS x, c FROM t WHERE a * 2 > 1";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 3, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
	// Above a field-permission layer the filter and the projection read the
	// same row, so they share.
	let db = TestDb::new(
		"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD a ON t PERMISSIONS FOR select WHERE a > 0;
		 CREATE t:1 SET a = 3; CREATE t:2 SET a = 5;",
	)
	.await;
	let ctx = db.exec_ctx_as(&record("bob"), TransactionType::Read).await;
	let sql = "SELECT a * 2 AS x FROM t WHERE a * 2 > 1";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
}

#[tokio::test]
async fn a_data_clause_shares_its_values() {
	let db = TestDb::new(ROWS).await;
	let ctx = db.exec_ctx().await;
	let sql = "UPDATE t SET x = a * 2, y = a * 2 + 1";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 1, "`{sql}`");
	// The `WHERE` and the data clause are separate regions.
	let sql = "UPDATE t SET x = a * 2 WHERE a * 2 > 1";
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 2, "`{sql}`");
	for sql in [
		"UPDATE t SET x = a * 2, y = a * 2 + 1",
		"UPDATE t SET x = <int> v, y = <int> v + 1",
		"UPDATE t SET x = a * 2 WHERE a * 2 > 1",
		"UPDATE t CONTENT { x: a * 2, y: a * 2 + 1 }",
		"UPDATE t MERGE { x: a * 2, y: a * 2 + 1 }",
	] {
		let (vm, tree) = both_writing(ROWS, sql).await;
		assert_eq!(vm, tree, "`{sql}`");
	}
	let (vm, tree) = both_writing(ROWS, "UPDATE t SET x = <int> v, y = <int> v + 1").await;
	assert!(tree.is_err(), "{tree:?}");
	assert_eq!(vm, tree);
}

#[tokio::test]
async fn an_if_statement_shares_its_conditions() {
	let db = TestDb::new("").await;
	let ctx = db.exec_ctx().await;
	// A block is run by the VM alone, so the answers are those of the
	// statement's own semantics, and the same statement with its conditions
	// written so that they are not shared.
	for (p, expected) in [(1, 2), (3, 2), (9, 1)] {
		let shared = format!(
			"{{ LET $p = {p}; IF $p * 2 > 10 {{ RETURN 1 }} ELSE IF $p * 2 > 1 {{ RETURN 2 }} ELSE {{ RETURN 3 }} }}"
		);
		let unshared = format!(
			"{{ LET $p = {p}; IF $p * 2 > 10 {{ RETURN 1 }} ELSE IF 2 * $p > 1 {{ RETURN 2 }} ELSE {{ RETURN 3 }} }}"
		);
		assert_eq!(multiplies(&lowered(&ctx, &shared).await), 1, "`{shared}`");
		assert_eq!(multiplies(&lowered(&ctx, &unshared).await), 2, "`{unshared}`");
		let answer = vm_only("", &shared).await;
		assert_eq!(answer, Ok(vec![Value::from(expected)]), "`{shared}`");
		assert_eq!(answer, vm_only("", &unshared).await, "`{shared}`");
	}
	// A failing cast in a condition raises where it is first evaluated.
	let sql = "{ LET $p = 'x'; IF <int> $p > 10 { RETURN 1 } ELSE IF <int> $p > 1 { RETURN 2 } }";
	assert_eq!(casts(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let answer = vm_only("", sql).await;
	assert!(answer.is_err(), "`{sql}`: {answer:?}");
	assert_eq!(
		answer,
		vm_only(
			"",
			"{ LET $p = 'x'; IF <int> $p > 10 { RETURN 1 } ELSE IF (<int> $p) + 0 > 1 { RETURN 2 } }"
		)
		.await
	);
}

#[tokio::test]
async fn a_for_range_shares_within_itself() {
	let sql = "{ FOR $i IN [$p * 2, $p * 2 + 1] { CREATE r SET v = $i }; RETURN SELECT VALUE v FROM r ORDER BY v }";
	let unshared = "{ FOR $i IN [$p * 2, 2 * $p + 1] { CREATE r SET v = $i }; RETURN SELECT VALUE v FROM r ORDER BY v }";
	let mut session = TestDb::owner();
	session.variables.insert("p", crate::syn::value("2").expect("parses"));
	let db = TestDb::new("").await;
	let ctx = db.exec_ctx_as(&session, TransactionType::Write).await;
	assert_eq!(multiplies(&lowered(&ctx, sql).await), 1, "`{sql}`");
	assert_eq!(multiplies(&lowered(&ctx, unshared).await), 2, "`{unshared}`");
	let answer = vm_only_as("", sql, &session).await;
	assert_eq!(answer, Ok(vec![Value::Array(vec![Value::from(4), Value::from(5)].into())]));
	assert_eq!(answer, vm_only_as("", unshared, &session).await);
}

#[tokio::test]
async fn aggregate_arguments_are_not_shared_outside_their_region() {
	let db = TestDb::new(ROWS).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT a * 2 AS k, math::sum(a * 2) AS s, count() FROM t GROUP BY k",
		"SELECT a * 2 AS k, array::len(array::group(a * 2)) AS n FROM t GROUP BY k",
	] {
		let (vm, tree) = both(&ctx, sql).await;
		assert!(vm.is_ok(), "`{sql}`: {vm:?}");
		assert_eq!(vm, tree, "`{sql}`");
	}
	// A row whose argument cannot be cast raises where the operator tree does.
	let sql = "SELECT math::sum(<int> v) AS s FROM t GROUP ALL";
	let (vm, tree) = both(&ctx, sql).await;
	assert_eq!(vm, tree, "`{sql}`");
}

fn record(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn a_permission_checked_caller_sees_main_results() {
	let db = TestDb::new(
		"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		 DEFINE FIELD a ON t PERMISSIONS FOR select WHERE a > 0;
		 DEFINE FIELD c ON t COMPUTED rand::int(1, 1000000000);
		 DEFINE PARAM $q VALUE 3 PERMISSIONS NONE;
		 CREATE t:1 SET a = 3; CREATE t:2 SET a = -1; CREATE t:3 SET a = 5;",
	)
	.await;
	let ctx = db.exec_ctx_as(&record("bob"), TransactionType::Read).await;
	for sql in [
		"SELECT a * 2 AS x, a * 2 + 1 AS y FROM t",
		"SELECT a * 2 AS x FROM t WHERE a * 2 > 1",
		"SELECT $q * 2 AS x, $q * 2 AS y FROM ONLY t:1",
	] {
		let (vm, tree) = both(&ctx, sql).await;
		assert_eq!(vm, tree, "`{sql}`");
	}
	// Whether the two reads of a computed field agree per row is what the
	// operator tree says, and the VM says the same.
	let sql = "SELECT c + 0 AS x, c + 0 AS y FROM t";
	let (vm, tree) = both(&ctx, sql).await;
	let agree = |rows: &Result<Vec<Value>, String>| {
		rows.as_ref().map_err(String::clone).map(|rows| {
			rows.iter()
				.map(|row| match row {
					Value::Object(row) => row.get("x") == row.get("y"),
					other => panic!("a row object, got {other:?}"),
				})
				.collect::<Vec<_>>()
		})
	};
	assert_eq!(agree(&vm), agree(&tree), "`{sql}`: {vm:?} against {tree:?}");
}

#[tokio::test]
async fn each_fallible_kind_shares_only_under_dominance() {
	let db = TestDb::new(
		"DEFINE FUNCTION fn::f($x: int) { RETURN $x + 1 };
		 CREATE t:1 SET a = 3, b = 'x', v = 'x', arr = [1, 2];
		 CREATE t:2 SET a = 4, b = 'y', v = 'y', arr = [3];",
	)
	.await;
	let ctx = db.exec_ctx().await;
	// (kind, count, unconditional pair, conditional-first pair, whether the
	// pair raises on the operator tree)
	type Counter = fn(&Program) -> usize;
	let kinds: [(&str, Counter, &str, &str, bool); 4] = [
		// A range that is not of integers fails to slice an array.
		(
			"index",
			indexes,
			"SELECT arr['a'..'b'] AS x, (arr['a'..'b']) + 0 AS y FROM t",
			"SELECT b = 'x' AND arr['a'..'b'] = [1] AS x, (arr['a'..'b']) + 0 AS y FROM t",
			true,
		),
		(
			"negate",
			negations,
			"SELECT -v AS x, -v + 0 AS y FROM t",
			"SELECT b = 'x' AND -v = 1 AS x, -v + 0 AS y FROM t",
			true,
		),
		(
			"builtin",
			calls,
			"SELECT math::sqrt(v) AS x, math::sqrt(v) + 0 AS y FROM t",
			"SELECT b = 'x' AND math::sqrt(v) = 1 AS x, math::sqrt(v) + 0 AS y FROM t",
			true,
		),
		(
			"cast",
			casts,
			"SELECT <int> v AS x, <int> v + 0 AS y FROM t",
			"SELECT b = 'x' AND <int> v = 1 AS x, <int> v + 0 AS y FROM t",
			true,
		),
	];
	for (kind, counter, together, conditional, raises) in kinds {
		let program = lowered(&ctx, together).await;
		assert_eq!(counter(&program), 1, "{kind}: `{together}`");
		let (vm, tree) = both(&ctx, together).await;
		assert_eq!(tree.is_err(), raises, "{kind}: `{together}`: {tree:?}");
		assert_eq!(vm, tree, "{kind}: `{together}`");
		let program = lowered(&ctx, conditional).await;
		assert_eq!(counter(&program), 2, "{kind}: `{conditional}`");
		let (vm, tree) = both(&ctx, conditional).await;
		assert_eq!(vm, tree, "{kind}: `{conditional}`");
	}
	// A draw and a call whose body is not in the expression are never shared.
	for (sql, counter, expect) in [
		(
			"SELECT rand::int(1, 9) + 0 AS x, rand::int(1, 9) + 0 AS y FROM ONLY t:1",
			calls as Counter,
			2,
		),
		(
			"SELECT fn::f(a) + 0 AS x, fn::f(a) + 0 AS y FROM t",
			|program: &Program| {
				count(program, |instr| matches!(instr, Instr::FunctionEnter { .. }))
			},
			2,
		),
	] {
		let program = lowered(&ctx, sql).await;
		assert_eq!(counter(&program), expect, "`{sql}`");
	}
	let sql = "SELECT fn::f(a) + 0 AS x, fn::f(a) + 0 AS y FROM t";
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
}

#[tokio::test]
async fn a_read_of_a_proven_object_stays_in_the_vm() {
	let db = TestDb::new(ROWS).await;
	let in_place = |program: &Program| program.pipelines.iter().all(|pipeline| pipeline.in_place);
	let ctx = db.exec_ctx().await;
	for sql in ["SELECT { x: a }.x AS p FROM t", "SELECT (<string> b).x AS p FROM t"] {
		let program = lowered(&ctx, sql).await;
		assert_eq!(accesses(&program), 1, "`{sql}`");
		assert!(in_place(&program), "`{sql}` stays on the stored bytes");
		let (vm, tree) = both(&ctx, sql).await;
		assert!(vm.is_ok(), "`{sql}`: {vm:?}");
		assert_eq!(vm, tree, "`{sql}`");
	}
	// A parameter read puts the pipeline on decoded rows whatever its kind, so
	// the read of a proven object through one is pinned by the answer alone.
	let mut session = TestDb::owner();
	session.variables.insert("v", crate::syn::value("{ f: 1 }").expect("parses"));
	let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
	let sql = "SELECT (<object> $v).f AS p FROM t";
	assert_eq!(accesses(&lowered(&ctx, sql).await), 1, "`{sql}`");
	let (vm, tree) = both(&ctx, sql).await;
	assert!(vm.is_ok(), "`{sql}`: {vm:?}");
	assert_eq!(vm, tree, "`{sql}`");
	// A base that may be a record id, an array or a geometry hands the read to
	// the host.
	let ctx = db.exec_ctx().await;
	for sql in ["SELECT a.x AS p FROM t", "SELECT (<array> [a]).f AS p FROM t"] {
		let program = lowered(&ctx, sql).await;
		assert_eq!(accesses(&program), 1, "`{sql}`");
		assert!(!in_place(&program), "`{sql}` is read by the host");
		let (vm, tree) = both(&ctx, sql).await;
		assert_eq!(vm, tree, "`{sql}`");
	}
}
