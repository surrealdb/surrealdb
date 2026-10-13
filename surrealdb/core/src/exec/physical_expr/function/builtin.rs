//! Built-in function expression - math::abs(), string::len(), etc.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context, evaluate_args};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut};
use crate::expr::FlowResult;
use crate::val::Value;

/// Whether the named builtin can evaluate arbitrary nested statements, and so
/// needs a write transaction regardless of what its arguments look like.
pub(crate) fn builtin_can_write(name: &str) -> bool {
	crate::expr::method::is_writer_builtin(name)
}

/// Built-in function expression - math::abs(), string::len(), etc.
///
/// These functions are registered in the FunctionRegistry at startup.
#[derive(Debug, Clone)]
pub struct BuiltinFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The required context level for this function (looked up at planning time).
	pub(crate) func_required_context: crate::exec::ContextLevel,
	/// Expression-nesting depth recorded when this node was planned. Only
	/// meaningful for `eval::*`, which uses it to continue the depth count when
	/// it re-plans its query string (see `exec/function/builtin/eval.rs`).
	pub(crate) plan_depth: u32,
}
impl PhysicalExpr for BuiltinFunctionExec {
	fn name(&self) -> &'static str {
		"BuiltinFunction"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Built-in functions need either their declared context level or
		// whatever context their arguments need, whichever is higher
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Check if function is allowed by capabilities
			ctx.check_allowed_function(&self.name)?;

			// Look up the function in the registry
			let registry = ctx.exec_ctx.function_registry();
			let func = registry.get(&self.name).ok_or_else(|| {
				anyhow::anyhow!("Unknown function '{}' - not found in function registry", self.name)
			})?;

			// Evaluate all arguments
			let args = evaluate_args(&self.arguments, ctx.clone()).await?;

			// A pure built-in behind an experimental target carries no context
			// of its own, so the gate the legacy dispatcher applies is applied
			// here instead — after the arguments, which is where the legacy
			// dispatcher applies it, so an argument's own error surfaces first.
			if let Some(target) = crate::exec::function::experimental_target(&self.name)
				&& !ctx.capabilities().allows_experimental(&target)
			{
				return Err(crate::exec::Error::InvalidFunction {
					name: self.name.clone(),
					message: format!("Experimental feature {target} is not enabled"),
				}
				.into());
			}

			// Invoke the function based on whether it's pure or needs context
			if func.is_pure() && !func.is_async() {
				Ok(func.invoke(args)?)
			} else {
				// Surface the plan-time nesting depth so `eval::*` can continue
				// counting toward `max_computation_depth` when it re-plans its
				// query string. Harmless for every other builtin (they ignore it).
				let mut ctx = ctx;
				ctx.plan_depth = self.plan_depth;
				Ok(func.invoke_async(&ctx, args).await?)
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		let func_mode = if builtin_can_write(&self.name) {
			AccessMode::ReadWrite
		} else {
			AccessMode::ReadOnly
		};
		func_mode.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for BuiltinFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}
