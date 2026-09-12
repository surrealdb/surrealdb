//! Foreach operator: `FOR $param IN range { body }`.
//!
//! [`ForeachPlan`] evaluates its range once, then runs its compiled body once
//! per element with the loop variable bound in a child context. `BREAK` and
//! `CONTINUE` raised by the body are the loop's to act on.

use std::sync::Arc;

use common::range::IntegerRangeIter;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::EngineError;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::collect_stream;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, Error as ExecError, ExecOperator, FlowResult, OperatorMetrics,
	OutputShape, ValueBatch, ValueBatchStream,
};
use crate::expr::{ControlFlow, ControlFlowExt, Error as ExprError, Param};
use crate::val::Value;

/// Iterates over an array or integer range, running the body for each element
/// with the loop variable bound in the context.
///
/// The range and the body are compiled when the loop is. The loop variable is
/// a runtime binding the body reads through `$param`; each iteration starts
/// from the context the loop was given, so a `LET` in the body lives for one
/// iteration only and nothing escapes the loop.
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
	/// The compiled range expression (evaluates to an array or range).
	pub range: Arc<dyn PhysicalExpr>,
	/// The compiled loop body.
	pub body: Arc<dyn ExecOperator>,
}

impl ForeachPlan {
	pub(crate) fn new(
		param: Param,
		range: Arc<dyn PhysicalExpr>,
		body: Arc<dyn ExecOperator>,
	) -> Self {
		Self {
			param,
			range,
			body,
			metrics: Arc::new(OperatorMetrics::new()),
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
		vec![("param".to_string(), self.param.to_sql()), ("range".to_string(), self.range.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		self.range.required_context().max(self.body.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.range.access_mode().combine(self.body.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let param = self.param.clone();
		let range = Arc::clone(&self.range);
		let body = Arc::clone(&self.body);
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_foreach(&param, &*range, &*body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.body]
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("range", &self.range)]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_shape(&self) -> OutputShape {
		// FOR loops return a single value (NONE)
		OutputShape::Scalar
	}
}

/// Run the loop: evaluate the range once, then the body per element.
async fn execute_foreach(
	param: &Param,
	range: &dyn PhysicalExpr,
	body: &dyn ExecOperator,
	ctx: &ExecutionContext,
) -> FlowResult<ValueBatch> {
	// A `FOR` in scalar position reads the enclosing row as its document.
	let eval_ctx = match ctx.current_row() {
		Some(row) => EvalContext::from_exec_ctx(ctx).with_value_and_doc(row),
		None => EvalContext::from_exec_ctx(ctx),
	};
	let range_value = range.evaluate(eval_ctx).await?;

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

	let param_name = param.as_str().to_owned();

	for v in iter {
		// Check timeout (also yields for cooperative scheduling)
		ctx.ctx().expect_not_timedout().await.map_err(ControlFlow::Err)?;
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}

		// Each iteration starts from the loop's own context with the variable
		// bound; a binding the body makes lives in this child context only.
		let iteration_ctx = ctx.with_param(param_name.clone(), v);

		match run_body(body, &iteration_ctx).await {
			Ok(()) => {}
			// Skip the rest of this iteration.
			Err(ControlFlow::Continue) => continue,
			// Leave the loop; a completed loop yields NONE.
			Err(ControlFlow::Break) => return Ok(ValueBatch::new(vec![Value::None])),
			// RETURN and errors belong to the statement boundary above.
			Err(ctrl) => return Err(ctrl),
		}
	}

	Ok(ValueBatch::new(vec![Value::None]))
}

/// Run the body once. Its value is discarded; a body that binds (a bare `LET`
/// or a block containing one) runs through `output_context` so the binding is
/// evaluated, and the resulting context is dropped with the iteration.
async fn run_body(body: &dyn ExecOperator, ctx: &ExecutionContext) -> FlowResult<()> {
	if body.mutates_context() {
		body.output_context(ctx).await?;
	} else {
		let stream = body.execute(ctx)?;
		collect_stream(stream).await?;
	}
	Ok(())
}

impl ToSql for ForeachPlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("FOR ");
		self.param.fmt_sql(f, fmt);
		f.push_str(" IN ");
		self.range.fmt_sql(f, fmt);
		f.push_str(" { ");
		f.push_str(self.body.name());
		f.push_str(" }");
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{TestDb, collect, plan_statement, try_collect};

	/// A database-level context over a read transaction.
	async fn db_ctx() -> ExecutionContext {
		TestDb::new("").await.exec_ctx().await
	}

	/// Compile a `FOR` statement the way the planner does: the range as a
	/// physical expression and the body as an operator, both up front.
	async fn plan(src: &str, ctx: &ExecutionContext) -> Arc<dyn ExecOperator> {
		let op = plan_statement(src, ctx).await;
		assert_eq!(op.name(), "Foreach", "{src:?} should compile to a Foreach operator");
		op
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
		let op = plan("FOR $i IN [10, 20, 30] { THROW $i }", &ctx).await;
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "10");

		// Later elements are reached in order.
		let op = plan(
			r#"FOR $i IN [10, 20, 30] { IF $i = 30 { THROW "reached the last element" } }"#,
			&ctx,
		)
		.await;
		let flow = try_collect(&op, &ctx).await.expect_err("the last element must be reached");
		assert_eq!(thrown(flow), "reached the last element");
	}

	#[tokio::test]
	async fn an_integer_range_target_is_iterated_and_excludes_the_open_end() {
		let ctx = db_ctx().await;

		let op = plan("FOR $i IN 0..3 { THROW $i }", &ctx).await;
		let flow = try_collect(&op, &ctx).await.expect_err("the body must run");
		assert_eq!(thrown(flow), "0");

		// `0..3` yields 0, 1, 2 — the exclusive end is never bound.
		let op =
			plan(r#"FOR $i IN 0..3 { IF $i = 3 { THROW "bound the exclusive end" } }"#, &ctx).await;
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn an_empty_target_never_enters_the_body_and_still_emits_one_none_row() {
		let ctx = db_ctx().await;
		// FOR declares `is_scalar()` and `CardinalityHint::AtMostOne`, so it must
		// always emit exactly one row; a loop that ran zero iterations is no
		// exception.
		for empty in ["[]", "0..0"] {
			let op = plan(&format!(r#"FOR $i IN {empty} {{ THROW "body ran" }}"#), &ctx).await;
			assert_eq!(collect(&op, &ctx).await, vec![Value::None], "{empty} should be empty");
		}
	}

	#[tokio::test]
	async fn a_target_that_is_neither_array_nor_range_is_rejected() {
		let ctx = db_ctx().await;
		// Only Array and Range are iterable. A set is a distinct `Value` variant
		// and is refused along with the scalars.
		for target in ["3", r#""abc""#, "{ a: 1 }", "NONE", "NULL", "true", "<set>[1, 2]"] {
			let op = plan(&format!(r#"FOR $i IN {target} {{ THROW "body ran" }}"#), &ctx).await;
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
		let op = plan(r#"FOR $i IN "a".."z" { THROW "body ran" }"#, &ctx).await;
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
		let op = plan("FOR $item IN [7] { THROW $item }", &ctx).await;
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
			&ctx,
		)
		.await;
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
			&ctx,
		)
		.await;
		let flow = try_collect(&op, &ctx).await.expect_err("the second iteration must run");
		assert_eq!(thrown(flow), "binding reset");
	}

	#[tokio::test]
	async fn the_loop_never_publishes_a_context_so_the_variable_cannot_escape() {
		// The executor only threads a new context to following statements when
		// `mutates_context()` is true. FOR leaves it false, which is what confines
		// the loop variable (and any body LET) to the loop.
		let ctx = db_ctx().await;
		assert!(!plan("FOR $i IN [1] { LET $x = $i }", &ctx).await.mutates_context());
	}

	#[tokio::test]
	async fn a_binding_body_is_still_evaluated() {
		// A body that only binds runs through `output_context`; its value
		// expression must still be evaluated, side effects included.
		let ctx = db_ctx().await;
		let op = plan(r#"FOR $i IN [1] { LET $x = { THROW "let body ran" } }"#, &ctx).await;
		let flow = try_collect(&op, &ctx).await.expect_err("the LET's value must be evaluated");
		assert_eq!(thrown(flow), "let body ran");
	}

	#[tokio::test]
	async fn break_stops_the_loop_and_reports_normal_completion() {
		let ctx = db_ctx().await;
		// If BREAK did not stop iteration, the second element would throw.
		let op = plan("FOR $i IN [1, 2] { IF $i = 1 { BREAK }; THROW $i }", &ctx).await;
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn break_after_a_binding_in_the_body_still_stops_the_loop() {
		let ctx = db_ctx().await;
		// A body that binds runs through the block's `output_context`, which has
		// to hand the loop signal back to the loop rather than reject it.
		let op =
			plan("FOR $i IN [1, 2] { LET $x = $i; IF $x = 1 { BREAK }; THROW $x }", &ctx).await;
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn continue_skips_the_rest_of_the_iteration_only() {
		let ctx = db_ctx().await;
		// The first iteration is skipped past its THROW; the second runs it.
		let op = plan("FOR $i IN [1, 2] { IF $i = 1 { CONTINUE }; THROW $i }", &ctx).await;
		let flow = try_collect(&op, &ctx).await.expect_err("the second iteration must throw");
		assert_eq!(thrown(flow), "2");
	}

	#[tokio::test]
	async fn return_propagates_out_of_the_loop() {
		let ctx = db_ctx().await;
		let op = plan("FOR $i IN [1, 2] { RETURN $i }", &ctx).await;
		let flow = try_collect(&op, &ctx).await.expect_err("RETURN must propagate");
		match flow {
			ControlFlow::Return(v) => assert_eq!(v, Value::from(1i64)),
			other => panic!("expected RETURN, got {other}"),
		}
	}

	#[tokio::test]
	async fn access_mode_reflects_the_range_and_the_body() {
		// A write statement has no logical form, so a body writes through a
		// function whose body writes; the call's access mode is resolved
		// through the call graph at plan time.
		let db = TestDb::new(
			"DEFINE TABLE log SCHEMALESS;
			 DEFINE FUNCTION fn::log($v: any) { CREATE log SET v = $v; };",
		)
		.await;
		let ctx = db.exec_ctx().await;
		assert_eq!(plan("FOR $i IN [1] { $i }", &ctx).await.access_mode(), AccessMode::ReadOnly);
		assert_eq!(
			plan("FOR $i IN [1] { fn::log($i) }", &ctx).await.access_mode(),
			AccessMode::ReadWrite
		);
	}
}
