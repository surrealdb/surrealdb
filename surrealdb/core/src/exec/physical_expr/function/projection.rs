//! Projection function expression - type::field, type::fields, etc.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context, evaluate_args};
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
use crate::expr::idiom::Idiom;
use crate::val::Value;

/// Projection function expression - type::field(), type::fields(), etc.
///
/// These functions produce field bindings rather than single values.
/// When used in SELECT projections, they expand into multiple output fields
/// with names derived from their arguments at runtime.
#[derive(Debug, Clone)]
pub struct ProjectionFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The required context level for this function.
	pub(crate) func_required_context: crate::exec::ContextLevel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for ProjectionFunctionExec {
	fn name(&self) -> &'static str {
		"ProjectionFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// When evaluated as a regular expression (not in projection context),
		// return the first value from the projection bindings, or None if empty.
		// This handles cases like: RETURN type::field("name")
		if let Some(bindings) = self.evaluate_projection(ctx).await? {
			if bindings.len() == 1 {
				Ok(bindings.into_iter().next().expect("bindings verified non-empty").1)
			} else {
				// Multiple bindings - return as array of values
				Ok(Value::Array(bindings.into_iter().map(|(_, v)| v).collect()))
			}
		} else {
			Ok(Value::None)
		}
	}

	fn access_mode(&self) -> AccessMode {
		args_access_mode(&self.arguments)
	}

	fn is_projection_function(&self) -> bool {
		true
	}

	async fn evaluate_projection(
		&self,
		ctx: EvalContext<'_>,
	) -> FlowResult<Option<Vec<(Idiom, Value)>>> {
		// Look up the projection function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get_projection(&self.name).ok_or_else(|| {
			anyhow::anyhow!(
				"Unknown projection function '{}' - not found in function registry",
				self.name
			)
		})?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Invoke the projection function to get field bindings
		let bindings = func.invoke_async(&ctx, args).await?;

		Ok(Some(bindings))
	}
}

impl ToSql for ProjectionFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}
