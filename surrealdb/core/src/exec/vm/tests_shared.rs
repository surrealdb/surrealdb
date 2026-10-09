//! A pure, non-leaf subexpression an evaluation region repeats is evaluated
//! once per row and read from a register at its later occurrences, with the
//! rows and the errors an unshared evaluation gives, and a draw of a generator is
//! never shared. A region is one pipeline's row code, one data clause, one
//! `IF` condition chain or one `FOR` range; sharing never crosses one.

use super::program::{ArithOp, Instr, Program, UnaryOp};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::TransactionType;
use crate::val::Value;

async fn lowered(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
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

/// `sql` on the VM alone, over a database set up with `setup`, for a block.
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
