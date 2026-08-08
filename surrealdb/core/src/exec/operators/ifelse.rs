//! IfElse operator with deferred planning.
//!
//! The IfElsePlan operator evaluates IF/ELSE IF/ELSE conditional branches,
//! using deferred planning like SequencePlan. Each condition is evaluated
//! sequentially, and the first truthy branch's body is executed.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::EngineError;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{evaluate_expr_at_depth, expr_required_context};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, OutputShape,
	ValueBatch, ValueBatchStream,
};
use crate::expr::{ControlFlow, Expr};
use crate::val::Value;

/// IfElse operator with deferred planning.
///
/// Stores the original condition-body pairs and optional else body.
/// Plans and executes each condition at runtime, executing the first
/// truthy branch's body.
///
/// Example:
/// ```surql
/// IF $x > 10 {
///     "large"
/// } ELSE IF $x > 5 {
///     "medium"
/// } ELSE {
///     "small"
/// }
/// ```
#[derive(Debug)]
pub struct IfElsePlan {
	/// Condition-body pairs: Vec<(condition_expr, body_expr)>
	pub branches: Vec<(Expr, Expr)>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Optional else body
	pub else_body: Option<Expr>,
	/// Expression-nesting depth recorded when this operator was planned. The
	/// deferred condition/body planning below is seeded with it so re-entry
	/// nodes (eval/UDF) keep counting toward `max_computation_depth`.
	plan_depth: u32,
}

impl IfElsePlan {
	pub(crate) fn new(
		branches: Vec<(Expr, Expr)>,
		else_body: Option<Expr>,
		plan_depth: u32,
	) -> Self {
		Self {
			branches,
			else_body,
			metrics: Arc::new(OperatorMetrics::new()),
			plan_depth,
		}
	}
}
impl ExecOperator for IfElsePlan {
	fn name(&self) -> &'static str {
		"IfElse"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("branches".to_string(), self.branches.len().to_string())];
		if self.else_body.is_some() {
			attrs.push(("has_else".to_string(), "true".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Derive the required context from condition and body expressions
		let branches_ctx = self
			.branches
			.iter()
			.flat_map(|(cond, body)| [expr_required_context(cond), expr_required_context(body)])
			.max()
			.unwrap_or(ContextLevel::Root);
		let else_ctx =
			self.else_body.as_ref().map(expr_required_context).unwrap_or(ContextLevel::Root);
		branches_ctx.max(else_ctx)
	}

	fn access_mode(&self) -> AccessMode {
		// Check if any branch requires write access
		let branches_read_only =
			self.branches.iter().all(|(cond, body)| cond.read_only() && body.read_only());
		let else_read_only = self.else_body.as_ref().map(|e| e.read_only()).unwrap_or(true);

		if branches_read_only && else_read_only {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let branches = self.branches.clone();
		let else_body = self.else_body.clone();
		// Branch bodies are re-planned one re-entry deeper than this operator, so
		// the depth count continues at `plan_depth + 1` toward `max_computation_depth`.
		let depth = self.plan_depth + 1;
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_ifelse(&branches, &else_body, &ctx, depth).await });

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
		// IF/ELSE expressions return a single value
		OutputShape::Scalar
	}
}

/// Execute the IF/ELSE logic with deferred planning.
async fn execute_ifelse(
	branches: &[(Expr, Expr)],
	else_body: &Option<Expr>,
	ctx: &ExecutionContext,
	depth: u32,
) -> crate::expr::FlowResult<ValueBatch> {
	for (cond, body) in branches {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}
		let cond_value = evaluate_expr_at_depth(cond, ctx, depth).await?;

		if cond_value.is_truthy() {
			let result = evaluate_expr_at_depth(body, ctx, depth).await?;
			return Ok(ValueBatch::new(vec![result]));
		}
	}

	// No branch matched - check for else body
	if let Some(else_expr) = else_body {
		let result = evaluate_expr_at_depth(else_expr, ctx, depth).await?;
		Ok(ValueBatch::new(vec![result]))
	} else {
		// No else - return NONE
		Ok(ValueBatch::new(vec![Value::None]))
	}
}

impl ToSql for IfElsePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		for (i, (cond, body)) in self.branches.iter().enumerate() {
			if i == 0 {
				f.push_str("IF ");
			} else {
				f.push_str(" ELSE IF ");
			}
			cond.fmt_sql(f, fmt);
			f.push(' ');
			body.fmt_sql(f, fmt);
		}
		if let Some(ref else_body) = self.else_body {
			f.push_str(" ELSE ");
			else_body.fmt_sql(f, fmt);
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::exec::Error as ExecError;
	use crate::exec::operators::test_util::{
		TestDb, collect, drain_err, parse_expr, root_ctx, try_collect,
	};
	use crate::expr::Literal;
	use crate::expr::statements::IfelseStatement;
	#[tokio::test]
	async fn a_context_without_a_transaction_yields_an_error_not_a_panic() {
		// Conditions are planned at execute time, which needs a transaction to
		// resolve catalog definitions. The executor always attaches one; a context
		// assembled without one has to fail rather than panic on the way in.
		let plan = IfElsePlan::new(
			vec![(Expr::Literal(Literal::Bool(true)), Expr::Literal(Literal::Integer(1)))],
			None,
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
	/// `IfElsePlan` re-plans its conditions and bodies at execute time, and the
	/// planner reads the transaction out of the context unconditionally, so a
	/// transaction-less root context cannot drive this operator.
	async fn db_ctx() -> ExecutionContext {
		TestDb::new("").await.exec_ctx().await
	}

	/// Build an `IfElsePlan` from SurrealQL source the way the planner does:
	/// conditions, branch bodies and the ELSE body are stored unplanned and
	/// planned again at execute time.
	fn plan(src: &str) -> Arc<dyn ExecOperator> {
		match parse_expr(src) {
			Expr::IfElse(stmt) => {
				let IfelseStatement {
					exprs,
					close,
				} = *stmt;
				Arc::new(IfElsePlan::new(exprs, close, 0))
			}
			other => panic!("expected an IF statement for {src:?}, got {other:?}"),
		}
	}

	/// The message carried by a `THROW`n error. Tests use `THROW` as a probe: a
	/// branch that must not run throws, so a thrown message names the branch that
	/// actually executed.
	fn thrown(flow: ControlFlow) -> String {
		match flow {
			ControlFlow::Err(e) => match e.downcast_ref::<ExecError>() {
				Some(ExecError::Thrown(msg)) => msg.clone(),
				_ => panic!("expected a THROWn error, got {e:?}"),
			},
			other => panic!("expected an error, got {other}"),
		}
	}

	/// The value carried by a `ControlFlow::Return`.
	fn returned(flow: ControlFlow) -> Value {
		match flow {
			ControlFlow::Return(v) => v,
			other => panic!("expected RETURN, got {other}"),
		}
	}

	#[tokio::test]
	async fn a_truthy_condition_runs_its_body_and_leaves_the_else_body_unevaluated() {
		let ctx = db_ctx().await;
		let op = plan(r#"IF true { 1 } ELSE { THROW "else body ran" }"#);
		assert_eq!(collect(&op, &ctx).await, vec![Value::from(1i64)]);
	}

	#[tokio::test]
	async fn a_false_condition_leaves_its_body_unevaluated_and_falls_through_to_else() {
		let ctx = db_ctx().await;
		let op = plan(r#"IF false { THROW "if body ran" } ELSE { 2 }"#);
		assert_eq!(collect(&op, &ctx).await, vec![Value::from(2i64)]);
	}

	#[tokio::test]
	async fn an_else_if_chain_stops_at_the_first_truthy_condition() {
		let ctx = db_ctx().await;
		// Conditions are evaluated in order and evaluation stops at the first
		// truthy one: neither the later condition nor any other body may run.
		let op = plan(
			r#"IF false { THROW "first body ran" }
			ELSE IF true { "second" }
			ELSE IF (THROW "third condition ran") { THROW "third body ran" }
			ELSE { THROW "else body ran" }"#,
		);
		assert_eq!(collect(&op, &ctx).await, vec![Value::from("second")]);
	}

	#[tokio::test]
	async fn no_else_and_no_truthy_condition_emits_exactly_one_none_row() {
		let ctx = db_ctx().await;
		// IfElse declares `is_scalar()` and `CardinalityHint::AtMostOne`, so
		// consumers unwrap a single row. The no-match-no-ELSE case must therefore
		// still emit one row, carrying NONE — not an empty stream.
		let op = plan("IF false { 1 }");
		assert_eq!(collect(&op, &ctx).await, vec![Value::None]);
	}

	#[tokio::test]
	async fn condition_truthiness_follows_value_is_truthy() {
		let ctx = db_ctx().await;
		for falsy in ["NONE", "NULL", "false", "0", "0.0", r#""""#, "[]", "{}", "0s"] {
			let op = plan(&format!("IF {falsy} {{ \"taken\" }} ELSE {{ \"not taken\" }}"));
			assert_eq!(
				collect(&op, &ctx).await,
				vec![Value::from("not taken")],
				"{falsy} should not be truthy"
			);
		}
		for truthy in ["true", "1", "-1", "0.5", r#""x""#, "[0]", "{ a: 0 }", "1s"] {
			let op = plan(&format!("IF {truthy} {{ \"taken\" }} ELSE {{ \"not taken\" }}"));
			assert_eq!(
				collect(&op, &ctx).await,
				vec![Value::from("taken")],
				"{truthy} should be truthy"
			);
		}
	}

	#[tokio::test]
	async fn an_error_from_a_condition_aborts_before_any_body_runs() {
		let ctx = db_ctx().await;
		let op = plan(
			r#"IF (THROW "bad condition") { THROW "if body ran" } ELSE { THROW "else body ran" }"#,
		);
		let flow = try_collect(&op, &ctx).await.expect_err("the condition error must propagate");
		assert_eq!(thrown(flow), "bad condition");
	}

	#[tokio::test]
	async fn an_error_from_the_taken_body_propagates() {
		let ctx = db_ctx().await;
		let op = plan(r#"IF true { THROW "body failed" } ELSE { 2 }"#);
		let flow = try_collect(&op, &ctx).await.expect_err("the body error must propagate");
		assert_eq!(thrown(flow), "body failed");
	}

	#[tokio::test]
	async fn a_return_from_the_taken_body_propagates_as_control_flow() {
		let ctx = db_ctx().await;
		let op = plan("IF true { RETURN 7 }");
		let flow = try_collect(&op, &ctx).await.expect_err("RETURN must propagate");
		assert_eq!(returned(flow), Value::from(7i64));
	}

	#[tokio::test]
	async fn a_break_from_the_taken_body_propagates_so_an_enclosing_loop_sees_it() {
		let ctx = db_ctx().await;
		let op = plan("IF true { BREAK }");
		let flow = try_collect(&op, &ctx).await.expect_err("BREAK must propagate");
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");
	}

	#[tokio::test]
	async fn a_return_from_a_condition_propagates_before_any_body_runs() {
		let ctx = db_ctx().await;
		let op = plan(r#"IF (RETURN 9) { THROW "if body ran" } ELSE { THROW "else body ran" }"#);
		let flow = try_collect(&op, &ctx).await.expect_err("RETURN must propagate");
		assert_eq!(returned(flow), Value::from(9i64));
	}

	#[tokio::test]
	async fn access_mode_is_readwrite_when_any_condition_or_body_can_write() {
		// The executor picks the transaction type from the plan's access mode, so
		// a write hidden in a branch that is only sometimes taken must still be
		// reported here.
		assert_eq!(plan("IF true { 1 } ELSE { 2 }").access_mode(), AccessMode::ReadOnly);
		assert_eq!(plan("IF true { CREATE foo } ELSE { 2 }").access_mode(), AccessMode::ReadWrite);
		// The ELSE body is a separate field from the branch list; it must be
		// included in the scan.
		assert_eq!(plan("IF true { 1 } ELSE { CREATE foo }").access_mode(), AccessMode::ReadWrite);
		// So must the conditions.
		assert_eq!(plan("IF (CREATE foo) { 1 } ELSE { 2 }").access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn required_context_is_the_maximum_over_conditions_bodies_and_else() {
		// The executor validates the declared context level before execution, so
		// under-reporting here would let a branch run without a database.
		assert_eq!(plan("IF true { 1 } ELSE { 2 }").required_context(), ContextLevel::Root);
		assert_eq!(
			plan("IF true { 1 } ELSE { SELECT * FROM foo }").required_context(),
			ContextLevel::Database
		);
		assert_eq!(
			plan("IF true { SELECT * FROM foo } ELSE { 2 }").required_context(),
			ContextLevel::Database
		);
		assert_eq!(plan("IF true { INFO FOR NS }").required_context(), ContextLevel::Namespace);
	}
}
