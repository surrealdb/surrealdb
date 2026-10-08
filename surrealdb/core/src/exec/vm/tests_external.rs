//! Calls of code the host runs — embedded scripts and models here, module
//! exports alike — are instructions of the program, entered before their
//! arguments and made with them, and answer what the build gives them.

use super::golden::Golden;
use super::tests::{SCHEMA, bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

/// Calls of code the host runs, in a build that runs no script, model or
/// module.
const CALLS: &[&str] = &[
	"function() { return 1; }",
	"function($limit) { return arguments[0]; }",
	"SELECT VALUE function() { return this.name; } FROM person",
	"ml::foo<1.0.0>(1)",
	"SELECT VALUE ml::foo<1.0.0>({ age: age }) FROM person",
	// The call is entered before its arguments are evaluated: the refusal
	// to run it is raised ahead of the argument's error.
	"function(<int> 'x') { return 1; }",
	"ml::foo<1.0.0>(<int> 'x')",
];

#[tokio::test]
async fn external_calls_are_instructions_and_answer_what_the_build_gives_them() {
	let mut golden =
		Golden::new("external_calls_are_instructions_and_answer_what_the_build_gives_them");
	let db = TestDb::new(SCHEMA).await;
	for sql in CALLS {
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let physical = super::optimize::optimize(
			super::map::map(&planner(&ctx), &bound).await.expect("the statement maps"),
		);
		let program =
			super::lower::lower(&planner(&ctx), &physical).await.expect("the program lowers");
		assert!(!program.externals.is_empty(), "`{sql}` makes no external call");
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM covers the call");
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	golden.check();
}
