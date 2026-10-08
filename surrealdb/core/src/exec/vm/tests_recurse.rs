//! Recursions whose body re-enters through `.@`, and shortest paths to a
//! target computed when the statement runs, are instructions of the program
//! and answer their values.

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	CREATE n:1 SET name = 'one', next = [n:2, n:3];
	CREATE n:2 SET name = 'two', next = [n:4];
	CREATE n:3 SET name = 'three', next = [];
	CREATE n:4 SET name = 'four';
	DEFINE PARAM $target VALUE n:4;
";

const RECURSIONS: &[&str] = &[
	"n:1.{..}.{ id, next: next.@ }",
	"n:1.{1..2}.{ name, kids: next.@ }",
	"n:1.{2}.{ id, kids: next.@ }",
	"n:1.{..3}.{ id, deep: { inner: next.@ } }",
	"n:1.{..}.next.@",
	// A closure reading only its own parameters runs in the body.
	"n:1.{..}.{ id, kids: next.@.chain(|$v| $v ?? []) }",
	"n:1.{..+shortest=$target}.next",
	"n:1.{..+shortest=$target+inclusive}.next",
	// `.@` outside a recursion is the marker's own error.
	"n:1.@",
];

#[tokio::test]
async fn repeat_recursions_are_instructions_and_answer_their_values() {
	let mut golden = Golden::new("repeat_recursions_are_instructions_and_answer_their_values");
	let db = TestDb::new(SETUP).await;
	for sql in RECURSIONS {
		let ctx = db.exec_ctx().await;
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM covers the recursion");
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	golden.check();
}
