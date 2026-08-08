//! Foreach operator with deferred planning.
//!
//! The ForeachPlan operator implements FOR loop iteration over arrays/ranges,
//! using deferred planning like SequencePlan. It handles BREAK/CONTINUE control
//! flow signals within the loop body.

use std::sync::Arc;

use common::range::IntegerRangeIter;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::EngineError;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{
	block_required_context, evaluate_body_expr, evaluate_expr_at_depth, expr_required_context,
};
use crate::exec::{
	AccessMode, CardinalityHint, Error as ExecError, ExecOperator, FlowResult, OperatorMetrics,
	OutputShape, ValueBatch, ValueBatchStream,
};
use crate::expr::{Block, ControlFlow, ControlFlowExt, Error as ExprError, Expr, Param};
use crate::val::Value;

/// Foreach operator with deferred planning.
///
/// Iterates over an array or integer range, executing the body block for
/// each element with the loop variable bound in the context.
///
/// Example:
/// ```surql
/// FOR $item IN [1, 2, 3] {
///     CREATE foo SET value = $item;
/// }
/// ```
#[derive(Debug)]
pub struct ForeachPlan {
	/// Loop variable parameter
	pub param: Param,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Range expression (evaluates to Array or Range)
	pub range: Expr,
	/// Loop body block
	pub body: Block,
	/// Expression-nesting depth recorded when this operator was planned. The
	/// deferred range/body planning below is seeded with it so re-entry nodes
	/// (eval/UDF) keep counting toward `max_computation_depth`.
	plan_depth: u32,
}

impl ForeachPlan {
	pub(crate) fn new(param: Param, range: Expr, body: Block, plan_depth: u32) -> Self {
		Self {
			param,
			range,
			body,
			metrics: Arc::new(OperatorMetrics::new()),
			plan_depth,
		}
	}
}

/// Iterator enum for foreach - handles both arrays and integer ranges.
enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<IntegerRangeIter, fn(i64) -> Value>),
}

impl Iterator for ForeachIter {
	type Item = Value;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			ForeachIter::Array(iter) => iter.next(),
			ForeachIter::Range(iter) => iter.next(),
		}
	}
}
impl ExecOperator for ForeachPlan {
	fn name(&self) -> &'static str {
		"Foreach"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("param".to_string(), self.param.to_string()),
			("statements".to_string(), self.body.0.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		// Derive the required context from the range expression and body block
		expr_required_context(&self.range).max(block_required_context(&self.body))
	}

	fn access_mode(&self) -> AccessMode {
		// Check if range and body require write access
		let range_read_only = self.range.read_only();
		let body_read_only = self.body.read_only();

		if range_read_only && body_read_only {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let param = self.param.clone();
		let range = self.range.clone();
		let body = self.body.clone();
		// Range/body are re-planned one re-entry deeper than this operator, so the
		// depth count continues at `plan_depth + 1` toward `max_computation_depth`.
		let depth = self.plan_depth + 1;
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_foreach(&param, &range, &body, &ctx, depth).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_shape(&self) -> OutputShape {
		// FOR loops return a single value (NONE)
		OutputShape::Scalar
	}
}

/// Execute the FOR loop with deferred planning.
async fn execute_foreach(
	param: &Param,
	range: &Expr,
	body: &Block,
	ctx: &ExecutionContext,
	depth: u32,
) -> crate::expr::FlowResult<ValueBatch> {
	// First, evaluate the range expression
	let range_value = evaluate_expr_at_depth(range, ctx, depth).await?;

	// Create the iterator based on the range value
	let iter = match range_value {
		Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
		Value::Range(r) => {
			let r =
				r.coerce_to_typed::<i64>().map_err(ExprError::from).context("Invalid FOR range")?;
			ForeachIter::Range(r.iter().map(Value::from))
		}
		v => {
			return Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidStatementTarget {
				value: v.to_raw_string(),
			})));
		}
	};

	// Loop variable name
	let param_name = param.as_str().to_owned();

	// Iterate over each value
	for v in iter {
		// Check timeout (also yields for cooperative scheduling)
		ctx.ctx().expect_not_timedout().await.map_err(ControlFlow::Err)?;
		// Check for cancellation via the streaming executor's token
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}

		// Create a new context with the loop variable bound
		// This is the base context for this iteration - LET statements will build on this
		let mut current_ctx = ctx.with_param(param_name.clone(), v.clone());

		// Execute each statement in the body
		for expr in body.0.iter() {
			let result = evaluate_body_expr(expr, &mut current_ctx, &param_name, &v, depth).await;

			// Handle control flow signals
			match result {
				Ok(_) => {
					// Continue to next statement
				}
				Err(ControlFlow::Continue) => {
					// Skip remaining statements, move to next iteration
					break;
				}
				Err(ControlFlow::Break) => {
					// Exit the loop entirely
					return Ok(ValueBatch::new(vec![Value::None]));
				}
				Err(ctrl) => {
					// Propagate RETURN and errors upward
					return Err(ctrl);
				}
			}
		}
	}

	// Loop completed normally - return NONE
	Ok(ValueBatch::new(vec![Value::None]))
}

impl ToSql for ForeachPlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("FOR ");
		self.param.fmt_sql(f, fmt);
		f.push_str(" IN ");
		self.range.fmt_sql(f, fmt);
		f.push(' ');
		self.body.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::exec::operators::test_util::{
		TestDb, collect, drain_err, parse_expr, root_ctx, try_collect,
	};
	use crate::expr::Literal;
	use crate::expr::statements::ForeachStatement;
	#[tokio::test]
	async fn a_context_without_a_transaction_yields_an_error_not_a_panic() {
		// The range expression is planned at execute time, which needs a
		// transaction to resolve catalog definitions. The executor always attaches
		// one; a context assembled without one has to fail rather than panic.
		let plan = ForeachPlan::new(
			Param::from("i".to_string()),
			Expr::Literal(Literal::Array(vec![Expr::Literal(Literal::Integer(1))])),
			Block(vec![Expr::Literal(Literal::Integer(1))]),
			0,
		);
		let err = drain_err(&plan, &root_ctx()).await;
		assert!(
			format!("{err}").contains("requires a transaction"),
			"expected a missing-transaction error, got: {err}"
		);
	}
	/// A database-level context over a read transaction.
	///
	/// `ForeachPlan` re-plans its range and body at execute time, and the planner
	/// reads the transaction out of the context unconditionally, so a
	/// transaction-less root context cannot drive this operator.
	async fn db_ctx() -> ExecutionContext {
		TestDb::new("").await.exec_ctx().await
	}

	/// Build a `ForeachPlan` from SurrealQL source the way the planner does: the
	/// range expression and the body block are stored unplanned and planned again
	/// at execute time.
	fn plan(src: &str) -> Arc<dyn ExecOperator> {
		match parse_expr(src) {
			Expr::Foreach(stmt) => {
				let ForeachStatement {
					param,
					range,
					block,
				} = *stmt;
				Arc::new(ForeachPlan::new(param, range, block, 0))
			}
			other => panic!("expected a FOR statement for {src:?}, got {other:?}"),
		}
	}

	/// The message carried by a `THROW`n error. Because a completed loop always
	/// emits the same single NONE row, the tests use `THROW` as the observable
	/// side effect that names which iteration and statement actually ran.
	fn thrown(flow: ControlFlow) -> String {
		match flow {
			ControlFlow::Err(e) => match e.downcast_ref::<ExecError>() {
				Some(ExecError::Thrown(msg)) => msg.clone(),
				_ => panic!("expected a THROWn error, got {e:?}"),
			},
			other => panic!("expected an error, got {other}"),
		}
	}

	#[tokio::test]
	async fn an_array_target_is_iterated_front_to_back() {
		let ctx = db_ctx().await;

		// The first iteration binds the first element.
		let op = plan("FOR $i IN [10, 20, 30] { THROW $i }");
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "10");

		// Later elements are reached in order.
		let op =
			plan(r#"FOR $i IN [10, 20, 30] { IF $i = 30 { THROW "reached the last element" } }"#);
		let flow = try_collect(&op, &ctx).await.expect_err("the last element must be reached");
		assert_eq!(thrown(flow), "reached the last element");
	}

	#[tokio::test]
	async fn an_integer_range_target_is_iterated_and_excludes_the_open_end() {
		let ctx = db_ctx().await;

		let op = plan("FOR $i IN 0..3 { THROW $i }");
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "0");

		// `0..3` yields 0, 1, 2 — the exclusive end is never bound.
		let op = plan(r#"FOR $i IN 0..3 { IF $i = 3 { THROW "bound the exclusive end" } }"#);
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn an_empty_target_never_enters_the_body_and_still_emits_one_none_row() {
		let ctx = db_ctx().await;
		// FOR declares `is_scalar()` and `CardinalityHint::AtMostOne`, so it must
		// always emit exactly one row; a loop that ran zero iterations is no
		// exception.
		for empty in ["[]", "0..0"] {
			let op = plan(&format!(r#"FOR $i IN {empty} {{ THROW "body ran" }}"#));
			assert_eq!(collect(&op, &ctx).await, vec![Value::None], "{empty} should be empty");
		}
	}

	#[tokio::test]
	async fn a_target_that_is_neither_array_nor_range_is_rejected() {
		let ctx = db_ctx().await;
		// Only Array and Range are iterable. A set is a distinct `Value` variant
		// and is refused along with the scalars.
		for target in ["3", r#""abc""#, "{ a: 1 }", "NONE", "NULL", "true", "<set>[1, 2]"] {
			let op = plan(&format!(r#"FOR $i IN {target} {{ THROW "body ran" }}"#));
			let ControlFlow::Err(e) = try_collect(&op, &ctx)
				.await
				.err()
				.unwrap_or_else(|| panic!("{target} is not iterable and must fail"))
			else {
				panic!("expected an error for {target}");
			};
			assert!(
				matches!(
					e.downcast_ref::<ExecError>(),
					Some(ExecError::InvalidStatementTarget { .. })
				),
				"expected InvalidStatementTarget for {target}, got {e:?}"
			);
		}
	}

	#[tokio::test]
	async fn a_range_that_does_not_coerce_to_integers_is_rejected() {
		let ctx = db_ctx().await;
		let op = plan(r#"FOR $i IN "a".."z" { THROW "body ran" }"#);
		let flow = try_collect(&op, &ctx).await.expect_err("a non-integer range must fail");
		let ControlFlow::Err(e) = flow else {
			panic!("expected an error, got {flow}");
		};
		assert!(
			format!("{e:#}").contains("Invalid FOR range"),
			"the coercion failure should be reported as an invalid FOR range, got {e:#}"
		);
	}

	#[tokio::test]
	async fn the_loop_variable_is_bound_in_the_body() {
		let ctx = db_ctx().await;
		let op = plan("FOR $item IN [7] { THROW $item }");
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "7");
	}

	#[tokio::test]
	async fn a_body_binding_lives_for_one_iteration_only() {
		let ctx = db_ctx().await;

		// Within an iteration, a LET is visible to the statements that follow it.
		let op = plan(
			r#"FOR $i IN [1] {
				LET $carry = "kept";
				IF $carry != "kept" { THROW "let not visible" };
				THROW "let visible"
			}"#,
		);
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "let visible");

		// Each iteration rebuilds its context from the context the loop started
		// with, so the binding does not survive into the next iteration.
		let op = plan(
			r#"FOR $i IN [1, 2] {
				IF $i = 2 AND $carry = "kept" { THROW "binding carried over" };
				IF $i = 2 { THROW "binding reset" };
				LET $carry = "kept"
			}"#,
		);
		let flow = try_collect(&op, &ctx).await.expect_err("the second iteration must run");
		assert_eq!(thrown(flow), "binding reset");
	}

	#[tokio::test]
	async fn the_loop_never_publishes_a_context_so_the_variable_cannot_escape() {
		// The executor only threads a new context to following statements when
		// `mutates_context()` is true. FOR leaves it false, which is what confines
		// the loop variable (and any body LET) to the loop.
		assert!(!plan("FOR $i IN [1] { LET $x = $i }").mutates_context());
	}

	#[tokio::test]
	async fn break_stops_the_loop_and_reports_normal_completion() {
		let ctx = db_ctx().await;
		// If BREAK did not stop iteration, the second element would throw.
		let op = plan("FOR $i IN [1, 2] { IF $i = 1 { BREAK }; THROW $i }");
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn continue_skips_the_rest_of_its_iteration_but_not_the_loop() {
		let ctx = db_ctx().await;
		// Iteration 1 continues before reaching the THROW; iteration 2 still runs
		// it, so the loop was not abandoned.
		let op = plan("FOR $i IN [1, 2] { IF $i = 1 { CONTINUE }; THROW $i }");
		let flow = try_collect(&op, &ctx).await.expect_err("the second iteration must run");
		assert_eq!(thrown(flow), "2");
	}

	#[tokio::test]
	async fn a_return_in_the_body_escapes_the_whole_loop() {
		let ctx = db_ctx().await;
		let op = plan("FOR $i IN [1, 2] { RETURN $i }");
		let flow = try_collect(&op, &ctx).await.expect_err("RETURN must propagate");
		match flow {
			ControlFlow::Return(v) => assert_eq!(v, Value::from(1i64)),
			other => panic!("expected RETURN from the first iteration, got {other}"),
		}
	}

	#[tokio::test]
	async fn an_error_mid_iteration_aborts_the_loop() {
		let ctx = db_ctx().await;
		let op = plan(r#"FOR $i IN [1, 2, 3] { IF $i = 2 { THROW "failed on 2" } }"#);
		let flow = try_collect(&op, &ctx).await.expect_err("the error must propagate");
		assert_eq!(thrown(flow), "failed on 2");
	}

	#[tokio::test]
	async fn access_mode_is_readwrite_when_the_range_or_the_body_can_write() {
		// The executor picks the transaction type from the plan's access mode, so
		// a write anywhere under the loop has to be reported here.
		assert_eq!(plan("FOR $i IN [1] { $i }").access_mode(), AccessMode::ReadOnly);
		assert_eq!(
			plan("FOR $i IN [1] { CREATE foo SET n = $i }").access_mode(),
			AccessMode::ReadWrite
		);
		assert_eq!(plan("FOR $i IN (CREATE foo) { $i }").access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn required_context_is_the_maximum_of_the_range_and_the_body() {
		// The executor validates the declared context level before execution, so
		// under-reporting would let the body run without a database.
		assert_eq!(plan("FOR $i IN [1] { $i }").required_context(), ContextLevel::Root);
		assert_eq!(
			plan("FOR $i IN [1] { SELECT * FROM foo }").required_context(),
			ContextLevel::Database
		);
		assert_eq!(
			plan("FOR $i IN (SELECT * FROM foo) { $i }").required_context(),
			ContextLevel::Database
		);
		assert_eq!(
			plan("FOR $i IN [1] { INFO FOR NS }").required_context(),
			ContextLevel::Namespace
		);
	}
}
