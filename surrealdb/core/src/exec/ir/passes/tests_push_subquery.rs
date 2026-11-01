//! Tests of the pass that moves a `WHERE` filter into the subquery its rows come from.

use super::driver::{LoPass, NoObserver, PassConfig, PassReport, run_passes};
use super::push_subquery::PushIntoSubquery;
use super::tests_support::{Env, bind, ops_of_tag, verify_logical};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpTag;

/// Runs the pass over `ir` and answers its report.
async fn push(ir: &mut Ir) -> PassReport {
	let env = Env::new();
	let cx = env.cx();
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(PushIntoSubquery)];
	let reports = run_passes(
		&passes,
		ir,
		&mut FactCache::new(),
		&NotYetFolder,
		&cx,
		&PassConfig::RELEASE,
		&mut NoObserver,
	)
	.await
	.expect("the pass runs");
	reports.into_iter().next().expect("one pass ran").1
}

/// How many filters `ir` holds.
fn filters(ir: &Ir) -> usize {
	ops_of_tag(ir, OpTag::Logical(LogTag::Filter)).len()
}

/// How many `AND` operators `ir` holds.
fn conjunctions(ir: &Ir) -> usize {
	ops_of_tag(ir, OpTag::Expr(ExprTag::ShortCircuit)).len()
}

/// The filter over a subquery that returns its rows unchanged merges into the subquery's own
/// filter, the subquery's conjuncts first.
#[tokio::test]
async fn a_filter_merges_into_the_filter_of_the_subquery() {
	let mut ir = bind("SELECT * FROM (SELECT * FROM t WHERE a) WHERE b = 2").await;
	assert_eq!(filters(&ir), 2);
	assert_eq!(conjunctions(&ir), 0);
	let report = push(&mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_eq!(filters(&ir), 1);
	assert_eq!(conjunctions(&ir), 1);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A subquery with no filter of its own gets one directly beneath its projection.
#[tokio::test]
async fn a_filter_becomes_the_filter_of_a_subquery_that_has_none() {
	let mut ir = bind("SELECT * FROM (SELECT * FROM t) WHERE b = 2").await;
	assert_eq!(filters(&ir), 1);
	let report = push(&mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_eq!(filters(&ir), 1);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A chain of subqueries ends with the one predicate over the innermost source.
#[tokio::test]
async fn a_filter_moves_through_every_subquery_of_a_chain() {
	let mut ir =
		bind("SELECT * FROM (SELECT * FROM (SELECT * FROM t WHERE a) WHERE b) WHERE c").await;
	assert_eq!(filters(&ir), 3);
	push(&mut ir).await;
	assert_eq!(filters(&ir), 1);
	assert_eq!(conjunctions(&ir), 2);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A filter over a subquery that returns its rows unchanged moves whole, a conjunct that can raise
/// included, and one that names a row-scoped parameter keeps the whole filter above.
#[tokio::test]
async fn a_filter_over_an_identity_projection_moves_whole() {
	for sql in [
		"SELECT * FROM (SELECT * FROM t) WHERE <int> b = 2",
		"SELECT * FROM (SELECT * FROM t) WHERE a = 1 OR a = 2",
		"SELECT * FROM (SELECT * FROM t) WHERE string::len(b) > 1",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 1, "`{sql}`");
		let env = Env::new();
		verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
	}
	for sql in [
		"SELECT * FROM (SELECT * FROM t) WHERE b = $parent.b",
		"SELECT * FROM (SELECT * FROM t) WHERE a = 1 AND b = $this.b",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
	}
}

/// A conjunct moves through a projection that names its fields, with each name that lands under
/// another name replaced by the expression it is projected as.
#[tokio::test]
async fn a_filter_moves_through_a_named_projection() {
	for sql in [
		"SELECT * FROM (SELECT a, b FROM t) WHERE a = 1",
		"SELECT * FROM (SELECT a AS z, b FROM t) WHERE z = 1",
		"SELECT * FROM (SELECT *, a AS z FROM t) WHERE z = 1 AND c = 2",
		"SELECT * FROM (SELECT a > 1 AS z FROM t) WHERE z = true",
		"SELECT * FROM (SELECT * OMIT b FROM t) WHERE a = 1",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 1, "`{sql}`");
		assert_eq!(filters(&ir), 1, "`{sql}`");
		let env = Env::new();
		verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
	}
}

/// A filter stays where it is when the subquery reshapes its rows, a conjunct reads a name the
/// projection computes, omits or does not supply, a field of the projection can raise, a literal
/// stands beneath, or the filter reads the row of an enclosing statement.
#[tokio::test]
async fn a_filter_that_cannot_move_stays() {
	for sql in [
		"SELECT * FROM (SELECT a + 1 AS z FROM t) WHERE z = 1",
		"SELECT * FROM (SELECT a FROM t) WHERE b = 1",
		"SELECT * FROM (SELECT * OMIT b FROM t) WHERE b = 1",
		"SELECT * FROM (SELECT a AS z, a.b AS y FROM t) WHERE z = 1",
		"SELECT * FROM (SELECT a AS z, b AS z FROM t) WHERE z = 1",
		"SELECT * FROM (SELECT <int> a AS z FROM t) WHERE z = 1",
		"SELECT * FROM (SELECT a AS z FROM t SPLIT z) WHERE z = 1",
		"SELECT * FROM (SELECT a, count() AS n FROM t GROUP BY a) WHERE a = 1",
		"SELECT * FROM (SELECT VALUE a FROM t) WHERE $this = 1",
		"SELECT * FROM (SELECT * FROM [{ b: 1 }, { b: 2 }]) WHERE b = 2",
		"SELECT * FROM (SELECT a AS z FROM t) WHERE $this.z = 1",
	] {
		let mut ir = bind(sql).await;
		let before = filters(&ir);
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
		assert_eq!(filters(&ir), before, "`{sql}`");
	}
}

/// A filter stays above a subquery whose rows a `LIMIT` or `START` cut, whatever its projection
/// and its order; an ordering alone does not cut the rows, so the filter still moves through it.
#[tokio::test]
async fn a_filter_stays_above_a_limit_or_start() {
	for sql in [
		"SELECT * FROM (SELECT * FROM t LIMIT 2) WHERE id = t:3",
		"SELECT * FROM (SELECT * FROM t START 2) WHERE a = 1",
		"SELECT * FROM (SELECT * FROM t LIMIT 2 START 1) WHERE a = 1",
		"SELECT * FROM (SELECT * FROM t ORDER BY a LIMIT 2) WHERE a = 1",
		"SELECT * FROM (SELECT a AS z, id FROM t LIMIT 2) WHERE z = 1",
		"SELECT * FROM (SELECT * FROM t ORDER BY b[0]) WHERE a = 1",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
	}
	let mut ir = bind("SELECT * FROM (SELECT * FROM t ORDER BY a) WHERE a = 1").await;
	assert_eq!(push(&mut ir).await.rewrites, 1);
}

/// A filter stays above a subquery whose `WHERE` clause holds a `NearestNeighbor` (`<|...|>`)
/// operator, because the inner KNN selection already caps its output to the top-k neighbours and
/// pushing the outer filter inside would turn post-filtering into pre-filtering.
#[tokio::test]
async fn a_filter_stays_above_a_knn_subquery() {
	for sql in [
		"SELECT * FROM (SELECT * FROM t WHERE vec <|5, 40|> [0f, 0f]) WHERE category = 'a'",
		"SELECT * FROM (SELECT * FROM t WHERE tenant = 'x' AND vec <|5, EUCLIDEAN|> [0f, 0f]) WHERE category = 'a'",
		"SELECT id, category FROM (SELECT id, category, vec FROM t WHERE vec <|3, 40|> $q) WHERE category = 'a'",
	] {
		let mut ir = bind(sql).await;
		let before = filters(&ir);
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
		assert_eq!(filters(&ir), before, "`{sql}`");
	}
}

/// A conjunct that may raise does not move past an earlier conjunct that stays above the
/// subquery, and no conjunct moves past an earlier conjunct that stays and may raise.
#[tokio::test]
async fn a_filter_respects_short_circuit_fallibility_when_splitting() {
	for sql in [
		"SELECT * FROM (SELECT a AS z FROM t) WHERE b = 1 AND <int> z > 0",
		"SELECT * FROM (SELECT a AS z FROM t) WHERE <int> b > 0 AND z = 1",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
		assert_eq!(filters(&ir), 1, "`{sql}`");
	}
	for sql in [
		"SELECT * FROM (SELECT a AS z FROM t) WHERE b = 1 AND z = 1",
		"SELECT * FROM (SELECT a AS z FROM t) WHERE <int> z > 0 AND b = 1",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 1, "`{sql}`");
		assert_eq!(filters(&ir), 2, "`{sql}`");
		let env = Env::new();
		verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
	}
}

/// Conjuncts the projection admits move and the rest stay above the subquery.
#[tokio::test]
async fn a_filter_splits_between_the_subquery_and_its_statement() {
	let mut ir =
		bind("SELECT * FROM (SELECT a AS z FROM t) WHERE z = 1 AND z = $parent.x AND b = 2").await;
	assert_eq!(filters(&ir), 1);
	let report = push(&mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_eq!(filters(&ir), 2);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A conjunct holding a `MATCHES` moves only when every name it reads is the stored field of that
/// name: a name projected as a literal, or under another name, holds content the full-text index
/// entry of the record does not, so the conjunct stays above and judges the row's own content.
#[tokio::test]
async fn a_matches_conjunct_moves_only_over_stored_fields() {
	for sql in [
		"SELECT * FROM (SELECT id, text FROM t) WHERE text @@ 'quick'",
		"SELECT * FROM (SELECT *, 1 AS n FROM t) WHERE text @@ 'quick'",
	] {
		let mut ir = bind(sql).await;
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 1, "`{sql}`");
		let env = Env::new();
		verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
	}
	for sql in [
		"SELECT * FROM (SELECT id, 'unrelated' AS text FROM t) WHERE text @@ 'quick'",
		"SELECT * FROM (SELECT id, body AS text FROM t) WHERE text @@ 'quick'",
		"SELECT * FROM (SELECT *, 'unrelated' AS text FROM t) WHERE text @@ 'quick'",
	] {
		let mut ir = bind(sql).await;
		let before = filters(&ir);
		let report = push(&mut ir).await;
		assert_eq!(report.rewrites, 0, "`{sql}`");
		assert_eq!(filters(&ir), before, "`{sql}`");
	}
}
