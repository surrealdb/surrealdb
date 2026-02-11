//! Shared helpers for the plan-or-compute fallback pattern.
//!
//! Operators that use deferred planning (foreach, ifelse, sequence, block) share
//! a common pattern: try to plan an expression with the streaming engine, and if
//! the planner returns `PlannerUnsupported` or `PlannerUnimplemented`, fall back
//! to the legacy `Expr::compute()` path.
//!
//! This module centralises that logic so each operator does not need its own copy.

use futures::StreamExt;
use reblessive::tree::TreeStack;

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::context::ExecutionContext;
use crate::exec::planner::try_plan_expr;
use crate::exec::{FlowResult, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt, Expr};
use crate::val::Value;

// ============================================================================
// Legacy Context Helpers
// ============================================================================

/// Extract the `Options` and `FrozenContext` needed for legacy `Expr::compute()`.
///
/// The `ExecutionContext`'s `FrozenContext` is the single source of truth for
/// parameters, transactions, capabilities, and all legacy context fields.
pub(crate) fn get_legacy_context(
	exec_ctx: &ExecutionContext,
) -> Result<(&crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;
	Ok((options, exec_ctx.ctx().clone()))
}

/// Extract the `Options` and `FrozenContext` for legacy fallback, adding a loop
/// variable to the context.
///
/// Used by the `ForeachPlan` operator to inject the current iteration value.
pub(crate) fn get_legacy_context_with_param<'a>(
	exec_ctx: &'a ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> Result<(&'a crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;

	let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());
	ctx.add_value(param_name.to_string(), std::sync::Arc::new(param_value.clone()));

	Ok((options, ctx.freeze()))
}

// ============================================================================
// Plan-or-Compute Evaluation
// ============================================================================

/// Plan and evaluate an expression, falling back to legacy compute if the
/// planner returns `PlannerUnsupported` or `PlannerUnimplemented`.
///
/// This is the simple variant used when no context mutation is needed
/// (e.g. evaluating a FOR range, an IF condition, or an IF/ELSE branch body).
pub(crate) async fn evaluate_expr(
	expr: &Expr,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<Value> {
	match try_plan_expr(expr, ctx.ctx()) {
		Ok(plan) => {
			let stream = plan.execute(ctx)?;
			collect_single_value(stream).await
		}
		Err(Error::PlannerUnsupported(_) | Error::PlannerUnimplemented(_)) => {
			let (opt, frozen) =
				get_legacy_context(ctx).context("Legacy compute fallback context unavailable")?;
			let mut stack = TreeStack::new();
			stack.enter(|stk| expr.compute(stk, &frozen, opt, None)).finish().await
		}
		Err(e) => Err(ControlFlow::Err(e.into())),
	}
}

/// Plan and evaluate a body expression that may mutate the execution context
/// (e.g. a LET statement inside a FOR loop body).
///
/// When the planned operator has `mutates_context() == true`, the context is
/// updated via `output_context()`. The legacy fallback for loop bodies injects
/// the loop variable into the context before calling `Expr::compute()`.
pub(crate) async fn evaluate_body_expr(
	expr: &Expr,
	ctx: &mut ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> crate::expr::FlowResult<Value> {
	let frozen_ctx = ctx.ctx().clone();

	match try_plan_expr(expr, &frozen_ctx) {
		Ok(plan) => {
			if plan.mutates_context() {
				*ctx = plan.output_context(ctx).await.map_err(|e| ControlFlow::Err(e.into()))?;
				Ok(Value::None)
			} else {
				let stream = plan.execute(ctx)?;
				collect_single_value(stream).await
			}
		}
		Err(Error::PlannerUnsupported(_) | Error::PlannerUnimplemented(_)) => {
			let (opt, frozen) = get_legacy_context_with_param(ctx, param_name, param_value)
				.context("Legacy compute fallback context unavailable")?;
			let mut stack = TreeStack::new();
			stack.enter(|stk| expr.compute(stk, &frozen, opt, None)).finish().await
		}
		Err(e) => Err(ControlFlow::Err(e.into())),
	}
}

// ============================================================================
// Stream Collection Helpers
// ============================================================================

/// Collect values from a stream into a single value.
///
/// - Empty stream → `Value::None`
/// - Single value → that value
/// - Multiple values → wrapped in an array (for query results like SELECT)
///
/// Propagates control flow signals (BREAK, CONTINUE, RETURN, errors).
pub(crate) async fn collect_single_value(
	stream: ValueBatchStream,
) -> crate::expr::FlowResult<Value> {
	let mut values = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => values.extend(batch.values),
			Err(ctrl) => return Err(ctrl),
		}
	}

	if values.is_empty() {
		Ok(Value::None)
	} else if values.len() == 1 {
		Ok(values.into_iter().next().expect("values verified non-empty"))
	} else {
		Ok(Value::Array(crate::val::Array(values)))
	}
}

/// Collect all values from a stream into a `Vec`.
///
/// Propagates control flow signals directly.
pub(crate) async fn collect_stream(stream: ValueBatchStream) -> FlowResult<Vec<Value>> {
	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => return Err(ctrl),
		}
	}

	Ok(results)
}
