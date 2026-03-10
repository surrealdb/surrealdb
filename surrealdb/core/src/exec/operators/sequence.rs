//! Sequence operator with deferred planning.
//!
//! The SequencePlan operator executes a sequence of expressions (a Block)
//! in order, threading the execution context through to enable LET bindings
//! to inform subsequent expression planning. This mirrors how the top-level
//! script executor handles multiple statements.
//!
//! When planning fails with `PlannerUnsupported` or `PlannerUnimplemented`,
//! the sequence falls back to the legacy `Expr::compute` path, similar to how
//! the top-level executor handles unplanned expressions.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::{Context, FrozenContext};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{block_required_context, collect_stream, legacy_compute};
use crate::exec::planner::try_plan_expr;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::{Block, ControlFlow, ControlFlowExt, Expr};
use crate::val::{Array, Value};

/// Sequence operator with deferred planning.
///
/// Stores the original block and plans each statement just before
/// execution, threading the execution context through to enable
/// LET bindings to inform subsequent statement planning.
///
/// Example where deferred planning helps:
/// ```surql
/// {
///     LET $table = "users";
///     SELECT * FROM type::table($table);  -- Planner knows $table = "users"
/// }
/// ```
#[derive(Debug)]
pub struct SequencePlan {
	/// The original block containing Expr values
	pub block: Block,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SequencePlan {
	pub(crate) fn new(block: Block) -> Self {
		Self {
			block,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SequencePlan {
	fn name(&self) -> &'static str {
		"Sequence"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("statements".to_string(), self.block.0.len().to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		// Derive the required context from the block's expressions
		block_required_context(&self.block)
	}

	fn access_mode(&self) -> AccessMode {
		if self.block.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let block = self.block.clone();
		let initial_ctx = ctx.clone();

		let stream = stream::once(async move {
			let (result, _) = execute_block_with_context(&block, &initial_ctx).await?;
			Ok(ValueBatch {
				values: vec![result],
			})
		});

		Ok(Box::pin(stream))
	}

	fn mutates_context(&self) -> bool {
		self.block.0.iter().any(|expr| matches!(expr, Expr::Let(_)))
	}

	async fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		let (_result, final_ctx) =
			execute_block_with_context(&self.block, input).await.map_err(|ctrl| match ctrl {
				ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
					// BREAK/CONTINUE/RETURN at top-level LET binding context is invalid
					Error::InvalidControlFlow
				}
				ControlFlow::Err(e) => Error::Thrown(e.to_string()),
			})?;
		Ok(final_ctx)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

/// Execute a block and return both the result and the final execution context.
///
/// Returns `FlowResult` to allow BREAK/CONTINUE/RETURN to propagate through
/// block expressions nested inside FOR loops.
async fn execute_block_with_context(
	block: &Block,
	initial_ctx: &ExecutionContext,
) -> crate::expr::FlowResult<(Value, ExecutionContext)> {
	// Empty block returns NONE
	if block.0.is_empty() {
		return Ok((Value::None, initial_ctx.clone()));
	}

	let mut current_ctx = initial_ctx.clone();
	let mut result = Value::None;

	// Track a mutable frozen context for legacy compute fallback
	let mut legacy_ctx: Option<FrozenContext> = None;

	for expr in block.0.iter() {
		// Check for cancellation between statements
		if current_ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryCancelled)));
		}

		let frozen_ctx = current_ctx.ctx().clone();

		// Try to plan the expression with current context
		match try_plan_expr!(expr, &frozen_ctx, current_ctx.txn()) {
			Ok(plan) => {
				if plan.mutates_context() {
					current_ctx = plan.output_context(&current_ctx).await?;
					result = Value::None;
				} else {
					let stream = plan.execute(&current_ctx)?;
					let values = collect_stream(stream).await?;

					result = if plan.is_scalar() {
						values.into_iter().next().unwrap_or(Value::None)
					} else {
						Value::Array(Array(values))
					};
				}
			}
			Err(e @ (Error::PlannerUnsupported(_) | Error::PlannerUnimplemented(_))) => {
				if let Error::PlannerUnimplemented(msg) = &e {
					tracing::warn!("PlannerUnimplemented fallback in sequence: {msg}");
				}
				// Fallback to legacy compute path
				let (opt, frozen) = get_legacy_context_cached(&current_ctx, &mut legacy_ctx)
					.context("Legacy compute fallback context unavailable")?;

				if let Expr::Let(set_stmt) = expr {
					let value = legacy_compute(&set_stmt.what, &frozen, opt, None).await?;

					// Update context with the new variable
					current_ctx = current_ctx.with_param(set_stmt.name.clone(), value.clone());
					// Update the legacy context too
					if let Some(ref mut ctx) = legacy_ctx {
						let mut new_ctx = Context::new(ctx);
						new_ctx.add_value(set_stmt.name.clone(), Arc::new(value));
						*ctx = new_ctx.freeze();
					}
					result = Value::None;
				} else {
					result = legacy_compute(expr, &frozen, opt, None).await?;
				}
			}
			Err(e) => return Err(ControlFlow::Err(e.into())),
		}
	}

	Ok((result, current_ctx))
}

/// Get the Options and FrozenContext for legacy compute fallback, with caching.
///
/// Sequence needs a cached legacy context because LET statements may update it
/// incrementally across iterations of the block.
fn get_legacy_context_cached<'a>(
	exec_ctx: &'a ExecutionContext,
	cached_ctx: &mut Option<FrozenContext>,
) -> Result<(&'a crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx.options().ok_or_else(|| {
		Error::Internal("Options not available for legacy compute fallback".into())
	})?;

	let frozen = cached_ctx.clone().unwrap_or_else(|| exec_ctx.ctx().clone());
	*cached_ctx = Some(frozen.clone());

	Ok((options, frozen))
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
