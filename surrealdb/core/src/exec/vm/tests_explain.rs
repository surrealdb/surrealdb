//! `EXPLAIN` and `EXPLAIN ANALYZE` render the plan the VM runs a statement by:
//! the access path of each source, a statement tested for membership in a
//! predicate beneath the strategy it runs by, a locked read, a `LET` source and
//! the index functions of a statement, none of them declined to another
//! engine.

use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

const SETUP: &str = "
	DEFINE TABLE team SCHEMALESS;
	DEFINE TABLE person SCHEMALESS;
	DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
	DEFINE INDEX person_name ON person FIELDS name FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
	CREATE team:1 SET active = true;
	CREATE team:2 SET active = false;
	CREATE person:1 SET team = team:1, name = 'ann smith';
	CREATE person:2 SET team = team:2, name = 'bob jones';
	CREATE person:3 SET team = team:1, name = 'cat smith';
";

/// The rendering `EXPLAIN` or `EXPLAIN ANALYZE` answers for `sql`.
async fn rendered(ctx: &ExecutionContext, sql: &str) -> String {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, ctx).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
	match rows.as_slice() {
		[Value::String(plan)] => plan.to_string(),
		other => panic!("`{sql}` answers {other:?}"),
	}
}

#[tokio::test]
async fn explain_renders_a_membership_join_as_its_predicate_and_the_statement_beneath_it() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"EXPLAIN SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true)",
		"EXPLAIN ANALYZE SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true)",
		"EXPLAIN SELECT VALUE id FROM person WHERE team NOT IN (SELECT VALUE id FROM team WHERE active = true)",
	] {
		let plan = rendered(&ctx, sql).await;
		assert!(plan.contains("Filter"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("table: person"), "`{sql}` renders\n{plan}");
		// The statement the predicate tests membership in, with the way it runs.
		assert!(plan.contains("Subquery [ctx: Db] [strategy: once]"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("table: team"), "`{sql}` renders\n{plan}");
	}
}

#[tokio::test]
async fn explain_analyze_inside_a_statement_runs_the_statement_it_explains() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	let bound = bind(&ctx, "SELECT VALUE (EXPLAIN ANALYZE SELECT * FROM person) FROM team:1").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM covers it");
	let rows = outcome(&op, &ctx).await.expect("runs");
	let [Value::String(plan)] = rows.as_slice() else {
		panic!("answers {rows:?}");
	};
	assert!(plan.contains("table: person"), "{plan}");
	assert!(plan.contains("Total rows: 3"), "{plan}");
}

#[tokio::test]
async fn explain_renders_a_locked_read() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	let plan = rendered(&ctx, "EXPLAIN SELECT * FROM person:1 FOR UPDATE").await;
	assert!(plan.contains("RecordIdScan"), "{plan}");
	assert!(plan.contains("for_update: true"), "{plan}");
}

#[tokio::test]
async fn explain_renders_a_let_source() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"EXPLAIN LET $x = (SELECT * FROM person)",
		"EXPLAIN ANALYZE LET $x = (SELECT * FROM person)",
	] {
		let plan = rendered(&ctx, sql).await;
		assert!(plan.starts_with("Let [ctx: Db] [name: $x]"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("TableScan"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("table: person"), "`{sql}` renders\n{plan}");
	}
}

#[tokio::test]
async fn explain_names_the_access_path_of_a_statement_with_index_functions() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"EXPLAIN SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM person WHERE name @1@ 'smith'",
		"EXPLAIN ANALYZE SELECT id, search::score(1) AS s FROM person WHERE name @1@ 'smith'",
	] {
		let plan = rendered(&ctx, sql).await;
		assert!(plan.contains("FullTextScan"), "`{sql}` renders\n{plan}");
		assert!(plan.contains("index: person_name"), "`{sql}` renders\n{plan}");
	}
	let analyzed = rendered(
		&ctx,
		"EXPLAIN ANALYZE SELECT id, search::score(1) AS s FROM person WHERE name @1@ 'smith'",
	)
	.await;
	assert!(analyzed.contains("Total rows: 2"), "{analyzed}");
}
