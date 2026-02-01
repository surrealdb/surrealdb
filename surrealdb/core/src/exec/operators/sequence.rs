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
use crate::exec::{AccessMode, OperatorPlan, ValueBatch, ValueBatchStream};
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
/// This creates a minimal context with the transaction and parameters needed
/// for expression planning during block evaluation.
fn create_planning_context(exec_ctx: &ExecutionContext) -> FrozenContext {
	let mut ctx = crate::ctx::Context::background();

	// Set the transaction
	ctx.set_transaction(exec_ctx.txn().clone());

	// Add all current params from execution context
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	ctx.freeze()
}

#[async_trait]
impl OperatorPlan for SequencePlan {
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

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
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

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
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
					let stream = plan.execute(&current_ctx)?;
					let values =
						collect_stream(stream).await.map_err(|e| Error::Thrown(e.to_string()))?;

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
				let mut stack = TreeStack::new();
				result =
					match stack.enter(|stk| expr.compute(stk, &frozen, opt, None)).finish().await {
						Ok(v) => v,
						Err(crate::expr::ControlFlow::Return(v)) => {
							// RETURN statement - return immediately from sequence
							return Ok((v, current_ctx));
						}
						Err(e) => {
							return Err(Error::Thrown(format!("Legacy compute failed: {:?}", e)));
						}
					};

				// If this was a LET statement, we need to update the context
				if let crate::expr::Expr::Let(set_stmt) = expr {
					// For LET, evaluate the value and add to context
					current_ctx = current_ctx.with_param(set_stmt.name.clone(), result.clone());
					// Update the legacy context too
					if let Some(ref mut ctx) = legacy_ctx {
						let mut new_ctx = crate::ctx::Context::new(ctx);
						new_ctx
							.add_value(set_stmt.name.clone(), std::sync::Arc::new(result.clone()));
						*ctx = new_ctx.freeze();
					}
					result = Value::None;
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
/// or reuses a FrozenContext for the legacy compute path.
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
		// Create a new context with the transaction and parameters
		let mut ctx = crate::ctx::Context::background();
		ctx.set_transaction(exec_ctx.txn().clone());
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
async fn collect_stream(stream: ValueBatchStream) -> anyhow::Result<Vec<Value>> {
	let mut results = Vec::new();
	futures::pin_mut!(stream);

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => {
				use crate::expr::ControlFlow;
				match ctrl {
					ControlFlow::Break | ControlFlow::Continue => continue,
					ControlFlow::Return(v) => {
						results.push(v);
						break;
					}
					ControlFlow::Err(e) => {
						return Err(e);
					}
				}
			}
		}
	}

	Ok(results)
}

impl ToSql for SequencePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
