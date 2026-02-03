//! Foreach operator with deferred planning.
//!
//! The ForeachPlan operator implements FOR loop iteration over arrays/ranges,
//! using deferred planning like SequencePlan. It handles BREAK/CONTINUE control
//! flow signals within the loop body.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use reblessive::tree::TreeStack;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::planner::try_plan_expr;
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatch, ValueBatchStream};
use crate::expr::{Block, ControlFlow, Expr, Param};
use crate::val::Value;
use crate::val::range::IntegerRangeIter;

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
	/// Range expression (evaluates to Array or Range)
	pub range: Expr,
	/// Loop body block
	pub body: Block,
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

/// Create a FrozenContext for planning that includes the loop variable.
///
/// This creates a child context from the ExecutionContext's FrozenContext,
/// which inherits sequences and other context fields, and adds the loop variable.
fn create_loop_planning_context(
	exec_ctx: &ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> FrozenContext {
	// Create a child context that inherits sequences and other context fields
	let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());

	// Add all current params from execution context
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	// Add the loop variable
	ctx.add_value(param_name.to_string(), Arc::new(param_value.clone()));

	ctx.freeze()
}

/// Get the Options and create a FrozenContext for legacy compute fallback.
///
/// This creates a child context that includes the loop variable for proper
/// evaluation in the legacy compute path.
fn get_legacy_context_with_param<'a>(
	exec_ctx: &'a ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> Result<(&'a crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;

	// Create a child context with the loop variable
	let frozen = create_loop_planning_context(exec_ctx, param_name, param_value);

	Ok((options, frozen))
}

/// Get the Options and FrozenContext for legacy compute fallback (without loop variable).
fn get_legacy_context(
	exec_ctx: &ExecutionContext,
) -> Result<(&crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;

	// Create a child context that inherits sequences and other context fields
	let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	Ok((options, ctx.freeze()))
}

#[async_trait]
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
		// Conservative: require database context since we don't know
		// what the inner expressions need without analyzing them
		ContextLevel::Database
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let param = self.param.clone();
		let range = self.range.clone();
		let body = self.body.clone();
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_foreach(&param, &range, &body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn is_scalar(&self) -> bool {
		// FOR loops return a single value (NONE)
		true
	}
}

/// Execute the FOR loop with deferred planning.
async fn execute_foreach(
	param: &Param,
	range: &Expr,
	body: &Block,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	// First, evaluate the range expression
	let range_value = evaluate_expr(range, ctx).await?;

	// Create the iterator based on the range value
	let iter = match range_value {
		Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
		Value::Range(r) => {
			let r = r
				.coerce_to_typed::<i64>()
				.map_err(Error::from)
				.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
			ForeachIter::Range(r.iter().map(Value::from))
		}
		v => {
			return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatementTarget {
				value: v.to_raw_string(),
			})));
		}
	};

	// Loop variable name
	let param_name = param.as_str().to_owned();

	// Iterate over each value
	for v in iter {
		// Check timeout (TODO: needs proper timeout integration with ExecutionContext)

		// Create a new context with the loop variable bound
		// This is the base context for this iteration - LET statements will build on this
		let mut current_ctx = ctx.with_param(param_name.clone(), v.clone());

		// Execute each statement in the body
		for expr in body.0.iter() {
			let result = execute_body_expr(expr, &mut current_ctx, &param_name, &v).await;

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
					return Ok(ValueBatch {
						values: vec![Value::None],
					});
				}
				Err(ctrl) => {
					// Propagate RETURN and errors upward
					return Err(ctrl);
				}
			}
		}

		// Cooperative yielding for long-running loops
		tokio::task::yield_now().await;
	}

	// Loop completed normally - return NONE
	Ok(ValueBatch {
		values: vec![Value::None],
	})
}

/// Evaluate an expression using deferred planning.
///
/// Tries to plan the expression with the streaming engine first,
/// falling back to legacy compute if unimplemented.
/// This is used for the range expression before the loop starts.
async fn evaluate_expr(expr: &Expr, ctx: &ExecutionContext) -> crate::expr::FlowResult<Value> {
	// Create a planning context that inherits sequences and other fields
	let mut planning_ctx = crate::ctx::Context::new(ctx.ctx());
	for (name, value) in ctx.params().iter() {
		planning_ctx.add_value(name.clone(), value.clone());
	}
	let frozen_ctx = planning_ctx.freeze();

	match try_plan_expr(expr.clone(), &frozen_ctx) {
		Ok(plan) => {
			// Execute the plan and collect the result
			let stream = plan.execute(ctx)?;
			let value = collect_single_value(stream).await?;
			Ok(value)
		}
		Err(Error::Unimplemented(_)) => {
			// Fallback to legacy compute path
			let (opt, frozen) = get_legacy_context(ctx)
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!(e.to_string())))?;
			let mut stack = TreeStack::new();
			stack.enter(|stk| expr.compute(stk, &frozen, opt, None)).finish().await
		}
		Err(e) => Err(ControlFlow::Err(anyhow::anyhow!(e.to_string()))),
	}
}

/// Execute a body expression with the loop variable in context.
///
/// LET statements are context-mutating - when they execute successfully via
/// the streaming engine, the context is updated so subsequent statements can
/// access the new parameter. When falling back to legacy compute, the LET
/// binding is handled internally by the legacy executor.
async fn execute_body_expr(
	expr: &Expr,
	ctx: &mut ExecutionContext,
	param_name: &str,
	param_value: &Value,
) -> crate::expr::FlowResult<Value> {
	// Create a planning context that includes the loop variable
	let frozen_ctx = create_loop_planning_context(ctx, param_name, param_value);

	match try_plan_expr(expr.clone(), &frozen_ctx) {
		Ok(plan) => {
			// Handle context-mutating operators (like LET)
			if plan.mutates_context() {
				// Get the output context - this also executes the plan
				*ctx = plan
					.output_context(ctx)
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!(e.to_string())))?;
				Ok(Value::None)
			} else {
				// Execute the plan normally
				let stream = plan.execute(ctx)?;
				collect_single_value(stream).await
			}
		}
		Err(Error::Unimplemented(_)) => {
			// Fallback to legacy compute path with the loop variable
			let (opt, frozen) = get_legacy_context_with_param(ctx, param_name, param_value)
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!(e.to_string())))?;
			let mut stack = TreeStack::new();
			stack.enter(|stk| expr.compute(stk, &frozen, opt, None)).finish().await
		}
		Err(e) => Err(ControlFlow::Err(anyhow::anyhow!(e.to_string()))),
	}
}

/// Collect values from a stream into a single value.
///
/// For scalar expressions (single value), returns that value.
/// For query expressions (multiple values), returns an array.
/// Propagates control flow signals appropriately.
async fn collect_single_value(stream: ValueBatchStream) -> crate::expr::FlowResult<Value> {
	let mut values = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => values.extend(batch.values),
			Err(ctrl) => return Err(ctrl),
		}
	}

	// Return the value appropriately:
	// - Empty: NONE
	// - Single value: that value
	// - Multiple values: wrap in array (for query results like SELECT)
	if values.is_empty() {
		Ok(Value::None)
	} else if values.len() == 1 {
		Ok(values.into_iter().next().unwrap())
	} else {
		Ok(Value::Array(crate::val::Array(values)))
	}
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
