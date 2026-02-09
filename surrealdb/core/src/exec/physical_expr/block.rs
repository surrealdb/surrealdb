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
use crate::dbs::NewPlannerStrategy;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::planner::expr_to_physical_expr;
use crate::expr::{Block, ControlFlow, Expr, FlowResult};
use crate::val::Value;

/// Check if an expression is a DDL or DML statement that should always
/// fall back to the legacy compute path regardless of planner strategy.
fn is_ddl_or_dml(expr: &Expr) -> bool {
	matches!(
		expr,
		Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Insert(_)
			| Expr::Relate(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
	)
}

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

/// Create a FrozenContext for planning that includes block-local parameters.
///
/// The ExecutionContext's FrozenContext already has the correct global params.
/// This creates a child context that adds local block params on top.
fn create_planning_context(
	exec_ctx: &crate::exec::ExecutionContext,
	local_params: &HashMap<String, Value>,
) -> FrozenContext {
	if local_params.is_empty() {
		return exec_ctx.ctx().clone();
	}

	// Create a child context that adds local params (shadowing global params)
	let mut ctx = crate::ctx::Context::new(exec_ctx.ctx());
	for (name, value) in local_params.iter() {
		ctx.add_value(name.clone(), Arc::new(value.clone()));
	}
	ctx.freeze()
}

/// Get the Options and FrozenContext for legacy compute fallback.
///
/// Since the ExecutionContext's FrozenContext is the single source of truth
/// for parameters, we can use it directly without reconstruction.
fn get_legacy_context<'a>(
	exec_ctx: &'a crate::exec::ExecutionContext,
	cached_ctx: &mut Option<FrozenContext>,
) -> anyhow::Result<(&'a crate::dbs::Options, FrozenContext)> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| anyhow::anyhow!("Options not available for legacy compute fallback"))?;

	// Use or create a cached context for legacy compute
	let frozen = if let Some(ctx) = cached_ctx.take() {
		ctx
	} else {
		exec_ctx.ctx().clone()
	};

	*cached_ctx = Some(frozen.clone());

	Ok((options, frozen))
}

#[async_trait]
impl PhysicalExpr for BlockPhysicalExpr {
	fn name(&self) -> &'static str {
		"Block"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Blocks can contain anything and are planned dynamically,
		// so we conservatively require database context
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
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

		// Store the current value for $this - used in legacy compute fallback
		let current_value_for_legacy = ctx.current_value.cloned();

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
								recursion_ctx: None,
							};
							// Control flow (BREAK/CONTINUE/RETURN) propagates directly
							phys_expr.evaluate(eval_ctx).await?
						}
						Err(Error::Unimplemented(ref msg))
							if *frozen_ctx.new_planner_strategy()
								== NewPlannerStrategy::AllReadOnlyStatements
								&& !is_ddl_or_dml(&set_stmt.what) =>
						{
							// Hard fail: non-DDL/DML expression can't be planned
							return Err(ControlFlow::Err(anyhow::anyhow!(Error::Query {
								message: format!("New executor does not support: {msg}"),
							})));
						}
						Err(Error::Unimplemented(_)) => {
							// Fallback to legacy compute path
							let (opt, frozen) =
								get_legacy_context(&current_exec_ctx, &mut legacy_ctx)?;
							// Create a CursorDoc from the current value for $this resolution
							let doc = current_value_for_legacy
								.as_ref()
								.map(|v| CursorDoc::new(None, None, v.clone()));
							let mut stack = TreeStack::new();
							// Legacy compute returns FlowResult directly - propagate as-is
							stack
								.enter(|stk| set_stmt.what.compute(stk, &frozen, opt, doc.as_ref()))
								.finish()
								.await?
						}
						Err(e) => {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								"Failed to plan LET expression: {}",
								e
							)));
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
								recursion_ctx: None,
							};
							// Control flow (BREAK/CONTINUE/RETURN) propagates directly
							phys_expr.evaluate(eval_ctx).await?
						}
						Err(Error::Unimplemented(ref msg))
							if *frozen_ctx.new_planner_strategy()
								== NewPlannerStrategy::AllReadOnlyStatements
								&& !is_ddl_or_dml(other) =>
						{
							// Hard fail: non-DDL/DML expression can't be planned
							return Err(ControlFlow::Err(anyhow::anyhow!(Error::Query {
								message: format!("New executor does not support: {msg}"),
							})));
						}
						Err(Error::Unimplemented(_)) => {
							// Fallback to legacy compute path
							let (opt, frozen) =
								get_legacy_context(&current_exec_ctx, &mut legacy_ctx)?;
							// Create a CursorDoc from the current value for $this resolution
							let doc = current_value_for_legacy
								.as_ref()
								.map(|v| CursorDoc::new(None, None, v.clone()));
							let mut stack = TreeStack::new();
							// Legacy compute returns FlowResult directly - propagate as-is
							stack
								.enter(|stk| other.compute(stk, &frozen, opt, doc.as_ref()))
								.finish()
								.await?
						}
						Err(e) => {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								"Failed to plan block expression: {}",
								e
							)));
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
