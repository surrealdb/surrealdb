//! IfElse operator with deferred planning.
//!
//! The IfElsePlan operator evaluates IF/ELSE IF/ELSE conditional branches,
//! using deferred planning like SequencePlan. Each condition is evaluated
//! sequentially, and the first truthy branch's body is executed.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use reblessive::tree::TreeStack;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::planner::try_plan_expr;
use crate::exec::{AccessMode, FlowResult, OperatorPlan, ValueBatch, ValueBatchStream};
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
	/// Optional else body
	pub else_body: Option<Expr>,
}

/// Create a FrozenContext for planning that includes the current parameters.
fn create_planning_context(exec_ctx: &ExecutionContext) -> FrozenContext {
	let mut ctx = crate::ctx::Context::background();
	ctx.set_transaction(exec_ctx.txn().clone());
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}
	ctx.freeze()
}

/// Get the Options and FrozenContext for legacy compute fallback.
fn get_legacy_context(
	exec_ctx: &ExecutionContext,
) -> Result<(&crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;

	let mut ctx = crate::ctx::Context::background();
	ctx.set_transaction(exec_ctx.txn().clone());
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	Ok((options, ctx.freeze()))
}

#[async_trait]
impl OperatorPlan for IfElsePlan {
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
		// Conservative: require database context since we don't know
		// what the inner expressions need without analyzing them
		ContextLevel::Database
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let branches = self.branches.clone();
		let else_body = self.else_body.clone();
		let ctx = ctx.clone();

		let stream = stream::once(async move { execute_ifelse(&branches, &else_body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn is_scalar(&self) -> bool {
		// IF/ELSE expressions return a single value
		true
	}
}

/// Execute the IF/ELSE logic with deferred planning.
async fn execute_ifelse(
	branches: &[(Expr, Expr)],
	else_body: &Option<Expr>,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	// Evaluate each condition in order
	for (cond, body) in branches {
		let cond_value = evaluate_expr(cond, ctx).await?;

		if cond_value.is_truthy() {
			// Execute the body of the first truthy branch
			let result = evaluate_expr(body, ctx).await?;
			return Ok(ValueBatch {
				values: vec![result],
			});
		}
	}

	// No branch matched - check for else body
	if let Some(else_expr) = else_body {
		let result = evaluate_expr(else_expr, ctx).await?;
		Ok(ValueBatch {
			values: vec![result],
		})
	} else {
		// No else - return NONE
		Ok(ValueBatch {
			values: vec![Value::None],
		})
	}
}

/// Evaluate an expression using deferred planning.
///
/// Tries to plan the expression with the streaming engine first,
/// falling back to legacy compute if unimplemented.
async fn evaluate_expr(expr: &Expr, ctx: &ExecutionContext) -> crate::expr::FlowResult<Value> {
	let frozen_ctx = create_planning_context(ctx);

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

/// Collect a single value from a stream.
///
/// For scalar expressions, this returns the single value.
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

	// Return the single value, or NONE if empty
	Ok(values.into_iter().next().unwrap_or(Value::None))
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
			f.push_str(" ");
			body.fmt_sql(f, fmt);
		}
		if let Some(ref else_body) = self.else_body {
			f.push_str(" ELSE ");
			else_body.fmt_sql(f, fmt);
		}
	}
}
