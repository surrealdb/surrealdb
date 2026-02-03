//! Sequence operator with deferred planning.
//!
//! The SequencePlan operator executes a sequence of expressions (a Block)
//! in order, threading the execution context through to enable LET bindings
//! to inform subsequent expression planning. This mirrors how the top-level
//! script executor handles multiple statements.
//!
//! When planning fails with `Error::Unimplemented`, the sequence falls back to
//! the legacy `Expr::compute` path, similar to how the top-level executor
//! handles unimplemented expressions.

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
use crate::expr::Block;
use crate::val::Value;

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
}

/// Create a FrozenContext for planning that includes the current parameters.
///
/// This creates a child context from the ExecutionContext's FrozenContext,
/// which inherits sequences and other context fields needed for expression
/// planning during block evaluation.
fn create_planning_context(exec_ctx: &ExecutionContext) -> FrozenContext {
	// Create a child context that inherits sequences and other context fields
	let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());

	// Add all current params from execution context (may shadow parent values)
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	ctx.freeze()
}

#[async_trait]
impl ExecOperator for SequencePlan {
	fn name(&self) -> &'static str {
		"Sequence"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("statements".to_string(), self.block.0.len().to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		// Conservative: require database context since we don't know
		// what the inner expressions need without analyzing them
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Use the block's read_only analysis
		if self.block.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// We need to execute each statement in sequence with deferred planning
		// Since execute() is sync but we need async, we create a stream that
		// will do the work when polled

		let block = self.block.clone();
		let initial_ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_block_sequence(&block, &initial_ctx).await });

		Ok(Box::pin(stream))
	}

	fn mutates_context(&self) -> bool {
		// Check if any expression in the block is a LET statement
		self.block.0.iter().any(|expr| matches!(expr, crate::expr::Expr::Let(_)))
	}

	async fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		// Execute all statements and return the final context
		let (_result, final_ctx) = execute_block_with_context(&self.block, input).await?;
		Ok(final_ctx)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn is_scalar(&self) -> bool {
		// Blocks are scalar expressions - they return a single value
		true
	}
}

/// Execute a block sequence and return the result as a ValueBatch
async fn execute_block_sequence(
	block: &Block,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	let (result, _final_ctx) = execute_block_with_context(block, ctx)
		.await
		.map_err(|e| crate::expr::ControlFlow::Err(e.into()))?;

	Ok(ValueBatch {
		values: vec![result],
	})
}

/// Execute a block and return both the result and the final execution context
async fn execute_block_with_context(
	block: &Block,
	initial_ctx: &ExecutionContext,
) -> Result<(Value, ExecutionContext), Error> {
	// Empty block returns NONE
	if block.0.is_empty() {
		return Ok((Value::None, initial_ctx.clone()));
	}

	let mut current_ctx = initial_ctx.clone();
	let mut result = Value::None;

	// Track a mutable frozen context for legacy compute fallback
	let mut legacy_ctx: Option<FrozenContext> = None;

	for expr in block.0.iter() {
		// Create a frozen context for planning that includes current params
		let frozen_ctx = create_planning_context(&current_ctx);

		// Try to plan the expression with current context
		match try_plan_expr(expr.clone(), &frozen_ctx) {
			Ok(plan) => {
				// Handle context-mutating operators (like LET)
				if plan.mutates_context() {
					// Get the output context (this also executes the plan)
					current_ctx = plan.output_context(&current_ctx).await?;
					result = Value::None; // Context-mutating statements return NONE
				} else {
					// Execute the plan and get the result
					// Handle control flow signals from execute()
					let stream = match plan.execute(&current_ctx) {
						Ok(s) => s,
						Err(crate::expr::ControlFlow::Return(v)) => {
							// RETURN statement - return immediately
							return Ok((v, current_ctx));
						}
						Err(crate::expr::ControlFlow::Break) => {
							// BREAK outside of loop context is an error
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Continue) => {
							// CONTINUE outside of loop context is an error
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Err(e)) => {
							return Err(Error::Thrown(e.to_string()));
						}
					};

					let values = match collect_stream(stream).await {
						Ok(v) => v,
						Err(crate::expr::ControlFlow::Return(v)) => {
							// RETURN statement - propagate immediately
							return Ok((v, current_ctx));
						}
						Err(crate::expr::ControlFlow::Err(e)) => {
							return Err(Error::Thrown(e.to_string()));
						}
						Err(ctrl) => {
							// Break/Continue outside loop context
							return Err(Error::Thrown(format!("Invalid control flow: {}", ctrl)));
						}
					};

					// For scalar expressions, return the single value
					// For queries, the result is already an array from the plan
					result = if plan.is_scalar() {
						values.into_iter().next().unwrap_or(Value::None)
					} else {
						// Queries return an array
						Value::Array(crate::val::Array(values))
					};
				}
			}
			Err(Error::Unimplemented(_)) => {
				// Fallback to legacy compute path
				let (opt, frozen) = get_legacy_context(&current_ctx, &mut legacy_ctx)?;

				// Handle LET statements specially - only compute the value expression
				if let crate::expr::Expr::Let(set_stmt) = expr {
					let mut stack = TreeStack::new();
					let value = match stack
						.enter(|stk| set_stmt.what.compute(stk, &frozen, opt, None))
						.finish()
						.await
					{
						Ok(v) => v,
						Err(crate::expr::ControlFlow::Return(v)) => {
							// RETURN in LET value - return immediately
							return Ok((v, current_ctx));
						}
						Err(crate::expr::ControlFlow::Break) => {
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Continue) => {
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Err(e)) => {
							return Err(Error::Thrown(e.to_string()));
						}
					};

					// Update context with the new variable
					current_ctx = current_ctx.with_param(set_stmt.name.clone(), value.clone());
					// Update the legacy context too
					if let Some(ref mut ctx) = legacy_ctx {
						let mut new_ctx = crate::ctx::Context::new(ctx);
						new_ctx.add_value(set_stmt.name.clone(), std::sync::Arc::new(value));
						*ctx = new_ctx.freeze();
					}
					result = Value::None;
				} else {
					// For other expressions, compute the whole expression
					let mut stack = TreeStack::new();
					result = match stack
						.enter(|stk| expr.compute(stk, &frozen, opt, None))
						.finish()
						.await
					{
						Ok(v) => v,
						Err(crate::expr::ControlFlow::Return(v)) => {
							// RETURN statement - return immediately from sequence
							return Ok((v, current_ctx));
						}
						Err(crate::expr::ControlFlow::Break) => {
							// BREAK outside of loop context is an error
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Continue) => {
							// CONTINUE outside of loop context is an error
							return Err(Error::InvalidControlFlow);
						}
						Err(crate::expr::ControlFlow::Err(e)) => {
							// Propagate the actual error
							return Err(Error::Thrown(e.to_string()));
						}
					};
				}
			}
			Err(e) => return Err(e),
		}
	}

	Ok((result, current_ctx))
}

/// Get the Options and FrozenContext for legacy compute fallback.
///
/// This returns a reference to Options from the ExecutionContext and creates
/// or reuses a FrozenContext for the legacy compute path. The FrozenContext
/// is created as a child of the ExecutionContext's context to inherit
/// sequences and other context fields.
fn get_legacy_context<'a>(
	exec_ctx: &'a ExecutionContext,
	cached_ctx: &mut Option<FrozenContext>,
) -> Result<(&'a crate::dbs::Options, FrozenContext), Error> {
	// Get Options from ExecutionContext - required for fallback
	let options = exec_ctx
		.options()
		.ok_or_else(|| Error::Thrown("Options not available for legacy compute fallback".into()))?;

	// Create or reuse the FrozenContext
	let frozen = if let Some(ctx) = cached_ctx.take() {
		ctx
	} else {
		// Create a child context that inherits sequences and other context fields
		let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());
		for (name, value) in exec_ctx.params().iter() {
			ctx.add_value(name.clone(), value.clone());
		}
		ctx.freeze()
	};

	// Store the context back for potential reuse
	*cached_ctx = Some(frozen.clone());

	Ok((options, frozen))
}

/// Collect all values from a stream into a Vec
///
/// Returns `FlowResult` to properly propagate control flow signals:
/// - `Ok(values)` - normal completion with collected values
/// - `Err(ControlFlow::Return(v))` - early return from RETURN statement
/// - `Err(ControlFlow::Err(e))` - error during execution
async fn collect_stream(stream: ValueBatchStream) -> FlowResult<Vec<Value>> {
	use crate::expr::ControlFlow;

	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => match ctrl {
				ControlFlow::Break | ControlFlow::Continue => continue,
				ControlFlow::Return(v) => {
					// Propagate RETURN as control flow signal
					return Err(ControlFlow::Return(v));
				}
				ControlFlow::Err(e) => {
					return Err(ControlFlow::Err(e));
				}
			},
		}
	}

	Ok(results)
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
