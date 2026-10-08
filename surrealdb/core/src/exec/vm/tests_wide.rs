//! Bodies that hold more registers than the stack file, and calls of builtins
//! that run statements, lowered as instructions and answering their values.

use super::golden::Golden;
use super::program::STACK_REGISTERS;
use super::tests::{SCHEMA, bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

/// A call with `terms` arguments, each held in a register of its own.
fn wide_call(terms: usize) -> String {
	let args = (1..=terms).map(|i| format!("age + {i}")).collect::<Vec<_>>().join(", ");
	format!("SELECT VALUE string::concat({args}) FROM person ORDER BY id")
}

#[tokio::test]
async fn a_body_wider_than_the_stack_file_runs() {
	let mut golden = Golden::new("a_body_wider_than_the_stack_file_runs");
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for terms in [8, 40, 100, 200] {
		let sql = wide_call(terms);
		let bound = bind(&ctx, &sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM covers it");
		golden.rows(format!("{terms} terms"), &outcome(&vm, &ctx).await);
		let physical = super::optimize::optimize(
			super::map::map(&planner(&ctx), &bound).await.expect("statement maps"),
		);
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let widest = program
			.pipelines
			.iter()
			.chain(program.host_code.iter())
			.map(|pipeline| pipeline.registers)
			.max()
			.unwrap_or(0);
		assert_eq!(widest > STACK_REGISTERS, terms >= 100, "{terms} terms: {widest}");
	}
	golden.check();
}

#[tokio::test]
async fn a_builtin_that_runs_statements_is_a_call_by_name() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for sql in [
		"SELECT VALUE eval::surql('RETURN 1 + 1') FROM person:1",
		"SELECT VALUE eval::surql('RETURN $a', { a: age }) FROM person:1",
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::optimize::optimize(
			super::map::map(&planner(&ctx), &bound).await.expect("statement maps"),
		);
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		assert!(!program.calls.is_empty(), "`{sql}` calls nothing by name");
		assert_ne!(program.access_mode, crate::exec::AccessMode::ReadOnly, "`{sql}` may write");
	}
}
