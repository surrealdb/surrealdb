//! The rule-based physical planner names what it chose in the plan: the
//! variant each breaker holds its state by.

use super::plan::{Plan, Spill};
use super::tests::{bind, planner};
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE t SCHEMALESS;
	CREATE t:1 SET n = 1, g = 'a';
	CREATE t:2 SET n = 2, g = 'b';
	CREATE t:3 SET n = 3, g = 'a';
";

/// The spill variant of every sort and aggregate on `plan`'s chain of single
/// inputs, outermost first.
fn breakers(plan: &Plan) -> Vec<(&'static str, Spill)> {
	let mut out = Vec::new();
	let mut node = Some(plan);
	while let Some(at) = node {
		match at {
			Plan::Sort {
				spill,
				..
			} => out.push(("sort", *spill)),
			Plan::Aggregate {
				spill,
				..
			} => out.push(("aggregate", *spill)),
			Plan::Distinct {
				spill,
				..
			} => out.push(("distinct", *spill)),
			_ => {}
		}
		node = at.input();
	}
	out
}

/// An unbounded breaker is planned as the variant that can spill, and one a
/// limit or `GROUP ALL` bounds as bounded.
#[tokio::test]
async fn a_breaker_names_the_variant_that_holds_its_state() {
	let db = TestDb::new(SETUP).await;
	let ctx = db.exec_ctx().await;
	for (sql, expected) in [
		("SELECT * FROM t ORDER BY n", vec![("sort", Spill::Spillable)]),
		("SELECT * FROM t ORDER BY n LIMIT 2", vec![("sort", Spill::Bounded)]),
		("SELECT math::sum(n) AS s FROM t GROUP ALL", vec![("aggregate", Spill::Bounded)]),
		("SELECT g, count() FROM t GROUP BY g", vec![("aggregate", Spill::Spillable)]),
		(
			"SELECT g, count() AS c FROM t GROUP BY g ORDER BY c DESC",
			vec![("sort", Spill::Spillable), ("aggregate", Spill::Spillable)],
		),
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::optimize::optimize(
			super::map::map(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
		);
		assert_eq!(breakers(&physical.root), expected, "`{sql}`: {:#?}", physical.root);
	}
}
