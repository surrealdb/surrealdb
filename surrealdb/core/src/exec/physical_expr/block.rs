//! Block expression with deferred planning.
//!
//! Block expressions (`{ stmt1; stmt2; ... }`) store the original `Expr` values
//! and convert them to physical expressions just before evaluation. This allows
//! the planner to use resolved variable values (from LET statements) for
//! optimization of subsequent expressions.
//!
//! When planning fails with `Error::Unimplemented`, the block falls back to the
//! legacy `Expr::compute` path, similar to how the top-level executor handles
//! unimplemented expressions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use reblessive::tree::TreeStack;
use surrealdb_types::{SqlFormat, ToSql};

use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::planner::expr_to_physical_expr;
use crate::expr::{Block, Expr};
use crate::val::Value;

/// Block expression with deferred planning.
///
/// Stores the original block containing `Expr` values and converts them to
/// physical expressions just before evaluation. This enables the planner to
/// use resolved LET bindings when planning subsequent expressions.
///
/// Example where deferred planning helps:
/// ```surql
/// {
///     LET $table = "users";
///     SELECT * FROM type::table($table);  -- Planner knows $table = "users"
/// }
/// ```
#[derive(Debug, Clone)]
pub struct BlockPhysicalExpr {
	/// The original block containing Expr values
	pub(crate) block: Block,
}

/// Create a FrozenContext for planning that includes the current parameters.
///
/// This creates a minimal context with the transaction and parameters needed
/// for expression planning during block evaluation.
fn create_planning_context(
	exec_ctx: &crate::exec::ExecutionContext,
	local_params: &HashMap<String, Value>,
) -> FrozenContext {
	let mut ctx = crate::ctx::Context::background();

	// Set the transaction
	ctx.set_transaction(exec_ctx.txn().clone());

	// Add all current params from execution context
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}

	// Add local params (these shadow global params with the same name)
	for (name, value) in local_params.iter() {
		ctx.add_value(name.clone(), Arc::new(value.clone()));
	}

	ctx.freeze()
}

/// Get the Options and FrozenContext for legacy compute fallback.
///
/// This returns a reference to Options from the ExecutionContext and creates
/// or reuses a FrozenContext for the legacy compute path.
fn get_legacy_context<'a>(
	exec_ctx: &'a crate::exec::ExecutionContext,
	cached_ctx: &mut Option<FrozenContext>,
) -> anyhow::Result<(&'a crate::dbs::Options, FrozenContext)> {
	// Get Options from ExecutionContext - required for fallback
	let options = exec_ctx
		.options()
		.ok_or_else(|| anyhow::anyhow!("Options not available for legacy compute fallback"))?;

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

#[async_trait]
impl PhysicalExpr for BlockPhysicalExpr {
	fn name(&self) -> &'static str {
		"Block"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Empty block returns NONE
		if self.block.0.is_empty() {
			return Ok(Value::None);
		}

		// Track block-local parameters (from LET statements)
		let mut local_params: HashMap<String, Value> = HashMap::new();

		// Track the result of the last expression
		let mut result = Value::None;

		// Track updated execution context (for LET bindings)
		let mut current_exec_ctx = ctx.exec_ctx.clone();

		// Track a mutable frozen context for legacy compute fallback
		let mut legacy_ctx: Option<FrozenContext> = None;

		for expr in self.block.0.iter() {
			match expr {
				Expr::Let(set_stmt) => {
					// Check for protected parameter names
					if PROTECTED_PARAM_NAMES.contains(&set_stmt.name.as_str()) {
						return Err(Error::InvalidParam {
							name: set_stmt.name.clone(),
						}
						.into());
					}

					// Create a frozen context for planning that includes current params
					let frozen_ctx = create_planning_context(&current_exec_ctx, &local_params);

					// Try to plan and evaluate the value expression
					let value = match expr_to_physical_expr(set_stmt.what.clone(), &frozen_ctx) {
						Ok(phys_expr) => {
							let eval_ctx = EvalContext {
								exec_ctx: &current_exec_ctx,
								current_value: ctx.current_value,
								local_params: if local_params.is_empty() {
									None
								} else {
									Some(&local_params)
								},
							};
							phys_expr.evaluate(eval_ctx).await?
						}
						Err(Error::Unimplemented(_)) => {
							// Fallback to legacy compute path
							let (opt, frozen) =
								get_legacy_context(&current_exec_ctx, &mut legacy_ctx)?;
							let mut stack = TreeStack::new();
							match stack
								.enter(|stk| set_stmt.what.compute(stk, &frozen, opt, None))
								.finish()
								.await
							{
								Ok(v) => v,
								Err(crate::expr::ControlFlow::Return(v)) => v,
								Err(e) => {
									return Err(anyhow::anyhow!("Legacy compute failed: {:?}", e));
								}
							}
						}
						Err(e) => {
							return Err(anyhow::anyhow!("Failed to plan LET expression: {}", e));
						}
					};

					// Apply type coercion if specified
					let value = if let Some(kind) = &set_stmt.kind {
						value.coerce_to_kind(kind).map_err(|e| Error::SetCoerce {
							name: set_stmt.name.clone(),
							error: Box::new(e),
						})?
					} else {
						value
					};

					// Store in local params and update execution context
					local_params.insert(set_stmt.name.clone(), value.clone());
					current_exec_ctx =
						current_exec_ctx.with_param(set_stmt.name.clone(), value.clone());

					// Update the legacy context with the new parameter
					if let Some(ref mut ctx) = legacy_ctx {
						let mut new_ctx = crate::ctx::Context::new(ctx);
						new_ctx.add_value(set_stmt.name.clone(), Arc::new(value));
						*ctx = new_ctx.freeze();
					}

					// LET returns NONE
					result = Value::None;
				}
				other => {
					// Create a frozen context for planning that includes current params
					let frozen_ctx = create_planning_context(&current_exec_ctx, &local_params);

					// Try to plan and evaluate the expression
					result = match expr_to_physical_expr(other.clone(), &frozen_ctx) {
						Ok(phys_expr) => {
							let eval_ctx = EvalContext {
								exec_ctx: &current_exec_ctx,
								current_value: ctx.current_value,
								local_params: if local_params.is_empty() {
									None
								} else {
									Some(&local_params)
								},
							};
							phys_expr.evaluate(eval_ctx).await?
						}
						Err(Error::Unimplemented(_)) => {
							// Fallback to legacy compute path
							let (opt, frozen) =
								get_legacy_context(&current_exec_ctx, &mut legacy_ctx)?;
							let mut stack = TreeStack::new();
							match stack
								.enter(|stk| other.compute(stk, &frozen, opt, None))
								.finish()
								.await
							{
								Ok(v) => v,
								Err(crate::expr::ControlFlow::Return(v)) => {
									// RETURN statement - return immediately from block
									return Ok(v);
								}
								Err(e) => {
									return Err(anyhow::anyhow!("Legacy compute failed: {:?}", e));
								}
							}
						}
						Err(e) => {
							return Err(anyhow::anyhow!("Failed to plan block expression: {}", e));
						}
					};
				}
			}
		}

		Ok(result)
	}

	fn references_current_value(&self) -> bool {
		// Conservative: blocks might reference current value
		// We can't know without analyzing all expressions
		true
	}

	fn access_mode(&self) -> AccessMode {
		// Conservative: blocks might contain mutations
		// We can't know without analyzing all expressions
		// A more sophisticated implementation could analyze the block
		if self.block.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}
}

impl ToSql for BlockPhysicalExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
