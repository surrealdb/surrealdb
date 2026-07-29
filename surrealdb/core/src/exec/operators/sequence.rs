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

use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::err::{EngineError, Error};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{block_required_context, collect_stream, legacy_compute};
use crate::exec::planner::try_plan_expr;
use crate::exec::{
	AccessMode, BoxFut, CardinalityHint, Error as ExecError, ExecOperator, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream,
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
	/// Expression-nesting depth recorded when this operator was planned. The
	/// deferred per-statement planning below is seeded with it so re-entry nodes
	/// (eval/UDF) keep counting toward `max_computation_depth`.
	plan_depth: u32,
}

impl SequencePlan {
	pub(crate) fn new(block: Block, plan_depth: u32) -> Self {
		Self {
			block,
			metrics: Arc::new(OperatorMetrics::new()),
			plan_depth,
		}
	}
}
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
		// Statements are re-planned one re-entry deeper than this operator, so the
		// depth count continues at `plan_depth + 1` toward `max_computation_depth`.
		let depth = self.plan_depth + 1;
		let initial_ctx = ctx.clone();

		let stream = stream::once(async move {
			let (result, _) = execute_block_with_context(&block, &initial_ctx, depth).await?;
			Ok(ValueBatch {
				values: vec![result],
			})
		});

		Ok(Box::pin(stream))
	}

	fn mutates_context(&self) -> bool {
		self.block.0.iter().any(|expr| matches!(expr, Expr::Let(_)))
	}

	fn output_context<'a>(
		&'a self,
		input: &'a ExecutionContext,
	) -> BoxFut<'a, anyhow::Result<ExecutionContext>> {
		Box::pin(async move {
			let (_result, final_ctx) =
				execute_block_with_context(&self.block, input, self.plan_depth + 1).await.map_err(
					|ctrl| match ctrl {
						ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_) => {
							// BREAK/CONTINUE/RETURN at top-level LET binding context is invalid
							anyhow::Error::new(ExecError::InvalidControlFlow)
						}
						// Unchanged, not stringified: a write conflict raised in
						// here has to stay downcastable or the transactor will
						// not retry it, and a cancelled or timed-out block has
						// to stay recognisable as such.
						ControlFlow::Err(e) => e,
					},
				)?;
			Ok(final_ctx)
		})
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
	depth: u32,
) -> crate::expr::FlowResult<(Value, ExecutionContext)> {
	// Empty block returns NONE
	if block.0.is_empty() {
		return Ok((Value::None, initial_ctx.clone()));
	}

	let mut current_ctx = initial_ctx.clone();
	let mut result = Value::None;

	for expr in block.0.iter() {
		// Check for cancellation between statements
		if current_ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}

		let frozen_ctx = Arc::clone(current_ctx.ctx());
		let auth = current_ctx.options().map(|o| Arc::clone(&o.auth));

		// Try to plan the expression with current context, continuing the depth
		// count so re-entry nodes inside the block stay bounded.
		match try_plan_expr!(expr, &frozen_ctx, current_ctx.txn(), auth, depth) {
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
			Err(Error::Exec(
				e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
			)) => {
				match &e {
					ExecError::PlannerUnimplemented(msg) => {
						tracing::warn!("PlannerUnimplemented fallback in sequence: {msg}");
					}
					ExecError::PlannerUnsupported(msg) => {
						tracing::debug!("PlannerUnsupported fallback in sequence: {msg}",);
					}
					_ => {}
				}
				// Fallback to legacy compute path, continuing the depth count from
				// `depth` rather than handing it a fresh budget.
				let (opt, frozen) = legacy_context_for_fallback(&current_ctx)
					.context("Legacy compute fallback context unavailable")?;
				let opt = opt.with_dive_consumed(depth);

				if let Expr::Let(set_stmt) = expr {
					let value = legacy_compute(&set_stmt.what, &frozen, &opt, None).await?;

					// Update context with the new variable
					current_ctx = current_ctx.with_param(set_stmt.name.clone(), value.clone());
					result = Value::None;
				} else {
					result = legacy_compute(expr, &frozen, &opt, None).await?;
				}
			}
			Err(e) => return Err(ControlFlow::Err(e.into())),
		}
	}

	Ok((result, current_ctx))
}

/// Options and frozen context for [`legacy_compute`] when the planner falls back.
///
/// Always clones from [`ExecutionContext`]: LET bindings from the planned path
/// (`with_param` / `output_context`) must be visible to legacy `Expr::compute`
/// (issue #7131).
fn legacy_context_for_fallback(
	exec_ctx: &ExecutionContext,
) -> Result<(crate::dbs::Options, FrozenContext), Error> {
	let options = exec_ctx.options().ok_or_else(|| {
		EngineError::Internal("Options not available for legacy compute fallback".into())
	})?;
	// Block write side effects when this sequence is evaluated inside a
	// PERMISSIONS predicate (signalled by `skip_fetch_perms`), so a predicate
	// cannot mutate data via the legacy compute fallback (GHSA-66r2-5gwj-gxm2).
	let options = if exec_ctx.root().skip_fetch_perms {
		options.new_for_permission_predicate()
	} else {
		options.clone()
	};
	Ok((options, Arc::clone(exec_ctx.ctx())))
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
