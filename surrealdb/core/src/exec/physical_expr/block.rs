//! Block expression with deferred planning.
//!
//! Block expressions (`{ stmt1; stmt2; ... }`) store the original `Expr` values
//! and convert them to physical expressions just before evaluation. This allows
//! the planner to use resolved variable values (from LET statements) for
//! optimization of subsequent expressions.
//!
//! When planning fails with `PlannerUnsupported` or `PlannerUnimplemented`,
//! the block falls back to the legacy `Expr::compute` path, similar to how the
//! top-level executor handles unplanned expressions.

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::NewPlannerStrategy;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::plan_or_compute::{block_required_context, legacy_compute};
use crate::exec::planner::expr_to_physical_expr_at_depth;
use crate::exec::{AccessMode, BoxFut, Error as ExecError};
use crate::expr::{Block, ControlFlow, Expr, FlowResult};
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

/// Create a FrozenContext for planning that includes block-local parameters.
///
/// The ExecutionContext's FrozenContext already has the correct global params.
/// This creates a child context that adds local block params on top.
fn create_planning_context(
	exec_ctx: &crate::exec::ExecutionContext,
	local_params: &HashMap<Strand, Value>,
) -> FrozenContext {
	if local_params.is_empty() {
		return Arc::clone(exec_ctx.ctx());
	}

	// Create a child context that adds local params (shadowing global params)
	let mut ctx = crate::ctx::Context::new_child(exec_ctx.ctx());
	for (name, value) in local_params.iter() {
		ctx.add_value(name.clone(), Arc::new(value.clone()));
	}
	ctx.freeze()
}

/// Get the Options and FrozenContext for legacy compute fallback, with caching.
///
/// Since the ExecutionContext's FrozenContext is the single source of truth
/// for parameters, we can use it directly without reconstruction.
fn get_legacy_context(
	exec_ctx: &crate::exec::ExecutionContext,
	cached_ctx: &mut Option<FrozenContext>,
	permission_predicate: bool,
) -> anyhow::Result<(crate::dbs::Options, FrozenContext)> {
	let options = exec_ctx
		.options()
		.ok_or_else(|| anyhow::anyhow!("Options not available for legacy compute fallback"))?;

	let options = crate::exec::plan_or_compute::legacy_fallback_options(
		options,
		permission_predicate,
		exec_ctx.root().computing_field,
	);

	// Use or create a cached context for legacy compute
	let frozen = if let Some(ctx) = cached_ctx.take() {
		ctx
	} else {
		Arc::clone(exec_ctx.ctx())
	};

	*cached_ctx = Some(Arc::clone(&frozen));

	Ok((options, frozen))
}
impl PhysicalExpr for BlockPhysicalExpr {
	fn name(&self) -> &'static str {
		"Block"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Derive the required context from the block's expressions
		block_required_context(&self.block)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Empty block returns NONE
			if self.block.0.is_empty() {
				return Ok(Value::None);
			}

			// Track block-local parameters (from LET statements)
			let mut local_params: HashMap<Strand, Value> = HashMap::new();

			// Track the result of the last expression
			let mut result = Value::None;

			// Track updated execution context (for LET bindings)
			let mut current_exec_ctx = ctx.exec_ctx.clone();

			// Track a mutable frozen context for legacy compute fallback
			let mut legacy_ctx: Option<FrozenContext> = None;

			// Store the current value for $this - used in legacy compute fallback
			let current_value_for_legacy = ctx.current_value.cloned();

			for expr in self.block.0.iter() {
				result = self
					.evaluate_block_entry(
						expr,
						&ctx,
						&mut local_params,
						&mut current_exec_ctx,
						&mut legacy_ctx,
						current_value_for_legacy.as_ref(),
					)
					.await?;
			}

			Ok(result)
		})
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

impl BlockPhysicalExpr {
	/// Evaluate a single entry in the block, handling LET bindings and expression planning.
	///
	/// Returns the evaluated value or a control flow signal.
	#[allow(clippy::too_many_arguments)]
	async fn evaluate_block_entry(
		&self,
		expr: &Expr,
		ctx: &EvalContext<'_>,
		local_params: &mut HashMap<Strand, Value>,
		current_exec_ctx: &mut crate::exec::ExecutionContext,
		legacy_ctx: &mut Option<FrozenContext>,
		current_value_for_legacy: Option<&Value>,
	) -> FlowResult<Value> {
		// Block statements are re-planned one re-entry deeper than the block, so
		// the depth count continues toward `max_computation_depth`.
		let depth = ctx.plan_depth + 1;
		match expr {
			Expr::Let(set_stmt) => {
				// Check for protected parameter names
				if PROTECTED_PARAM_NAMES.contains(&set_stmt.name.as_str()) {
					return Err(ExecError::InvalidParam {
						name: set_stmt.name.to_string(),
					}
					.into());
				}

				// Create a frozen context for planning that includes current params
				let frozen_ctx = create_planning_context(current_exec_ctx, local_params);

				// Try to plan and evaluate the value expression. Seed the planner
				// with the block's nesting depth so re-entry nodes inside it
				// (eval/UDF) keep counting toward `max_computation_depth`.
				let value = match expr_to_physical_expr_at_depth(
					set_stmt.what.clone(),
					&frozen_ctx,
					current_exec_ctx.function_registry(),
					depth,
				)
				.await
				{
					Ok(phys_expr) => {
						let eval_ctx = EvalContext {
							exec_ctx: current_exec_ctx,
							current_value: ctx.current_value,
							local_params: if local_params.is_empty() {
								None
							} else {
								Some(local_params)
							},
							recursion_ctx: None,
							document_root: ctx.document_root,
							skip_fetch_perms: ctx.skip_fetch_perms,
							computing_record: ctx.computing_record.clone(),
							plan_depth: depth,
						};
						phys_expr.evaluate(eval_ctx).await?
					}
					Err(Error::Exec(ExecError::PlannerUnimplemented(ref msg)))
						if *frozen_ctx.new_planner_strategy()
							== NewPlannerStrategy::AllReadOnlyStatements =>
					{
						return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::Query {
							message: format!("New executor does not support: {msg}"),
						})));
					}
					Err(Error::Exec(
						e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
					)) => {
						match &e {
							ExecError::PlannerUnimplemented(msg) => {
								tracing::warn!(
									"PlannerUnimplemented fallback in block (LET): {msg}"
								);
							}
							ExecError::PlannerUnsupported(msg) => {
								tracing::debug!(
									"PlannerUnsupported fallback in block (LET): {msg}",
								);
							}
							_ => {}
						}
						let (opt, frozen) =
							get_legacy_context(current_exec_ctx, legacy_ctx, ctx.skip_fetch_perms)?;
						let opt = &opt.with_dive_consumed(depth);
						let doc =
							current_value_for_legacy.map(|v| CursorDoc::new(None, None, v.clone()));
						legacy_compute(&set_stmt.what, &frozen, opt, doc.as_ref()).await?
					}
					Err(e) => {
						return Err(ControlFlow::Err(e.into()));
					}
				};

				// Apply type coercion if specified
				let value = if let Some(kind) = &set_stmt.kind {
					value.coerce_to_kind(kind).map_err(|e| ExecError::SetCoerce {
						name: set_stmt.name.to_string(),
						error: Box::new(e),
					})?
				} else {
					value
				};

				// Store in local params and update execution context
				let name: Strand = set_stmt.name.clone();
				local_params.insert(name.clone(), value.clone());
				*current_exec_ctx = current_exec_ctx.with_param(name.clone(), value.clone());

				// Update the legacy context with the new parameter
				if let Some(ctx) = legacy_ctx {
					let mut new_ctx = crate::ctx::Context::new_child(ctx);
					new_ctx.add_value(name, Arc::new(value));
					*ctx = new_ctx.freeze();
				}

				Ok(Value::None)
			}
			other => {
				// Create a frozen context for planning that includes current params
				let frozen_ctx = create_planning_context(current_exec_ctx, local_params);

				// Try to plan and evaluate the expression. Seed the planner with the
				// block's nesting depth so re-entry nodes inside it (eval/UDF) keep
				// counting toward `max_computation_depth`.
				match expr_to_physical_expr_at_depth(
					other.clone(),
					&frozen_ctx,
					current_exec_ctx.function_registry(),
					depth,
				)
				.await
				{
					Ok(phys_expr) => {
						let eval_ctx = EvalContext {
							exec_ctx: current_exec_ctx,
							current_value: ctx.current_value,
							local_params: if local_params.is_empty() {
								None
							} else {
								Some(local_params)
							},
							recursion_ctx: None,
							document_root: ctx.document_root,
							skip_fetch_perms: ctx.skip_fetch_perms,
							computing_record: ctx.computing_record.clone(),
							plan_depth: depth,
						};
						phys_expr.evaluate(eval_ctx).await
					}
					Err(Error::Exec(ExecError::PlannerUnimplemented(ref msg)))
						if *frozen_ctx.new_planner_strategy()
							== NewPlannerStrategy::AllReadOnlyStatements =>
					{
						Err(ControlFlow::Err(anyhow::anyhow!(ExecError::Query {
							message: format!("New executor does not support: {msg}"),
						})))
					}
					Err(Error::Exec(
						e @ (ExecError::PlannerUnsupported(_) | ExecError::PlannerUnimplemented(_)),
					)) => {
						match &e {
							ExecError::PlannerUnimplemented(msg) => {
								tracing::warn!(
									"PlannerUnimplemented fallback in block (expr): {msg}"
								);
							}
							ExecError::PlannerUnsupported(msg) => {
								tracing::debug!(
									"PlannerUnsupported fallback in block (expr): {msg}",
								);
							}
							_ => {}
						}
						let (opt, frozen) =
							get_legacy_context(current_exec_ctx, legacy_ctx, ctx.skip_fetch_perms)?;
						let opt = &opt.with_dive_consumed(depth);
						let doc =
							current_value_for_legacy.map(|v| CursorDoc::new(None, None, v.clone()));
						legacy_compute(other, &frozen, opt, doc.as_ref()).await
					}
					Err(e) => Err(ControlFlow::Err(e.into())),
				}
			}
		}
	}
}

impl ToSql for BlockPhysicalExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.block.fmt_sql(f, fmt);
	}
}
