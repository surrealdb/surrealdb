//! Index function expression - search::highlight, search::score, search::offsets,
//! vector::distance::knn.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::{args_access_mode, args_required_context, evaluate_args};
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
use crate::val::Value;

/// Index function expression - functions bound to WHERE clause predicates.
///
/// These functions are associated with an index operator in the WHERE condition
/// (e.g., MATCHES for full-text, `<|k,ef|>` for KNN). The planner resolves
/// the appropriate [`IndexContext`] at plan time based on the function's
/// declared [`IndexContextKind`].
///
/// Any plan-time reference arguments (e.g., match_ref) are extracted by the
/// planner and are NOT included in the stored arguments.
#[derive(Debug)]
pub struct IndexFunctionExec {
	pub(crate) name: String,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// Resolved index context (FullText or Knn).
	pub(crate) index_ctx: crate::exec::function::IndexContext,
	/// The required context level for this function.
	pub(crate) func_required_context: crate::exec::ContextLevel,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for IndexFunctionExec {
	fn name(&self) -> &'static str {
		"IndexFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		let args_ctx = args_required_context(&self.arguments);
		args_ctx.max(self.func_required_context)
	}

	fn access_mode(&self) -> AccessMode {
		args_access_mode(&self.arguments)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Look up the index function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get_index_function(&self.name).ok_or_else(|| {
			anyhow::anyhow!(
				"Unknown index function '{}' - not found in function registry",
				self.name
			)
		})?;

		// Evaluate all arguments (any plan-time ref args were already extracted)
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Invoke the index function with the resolved index context
		Ok(func.invoke_async(&ctx, &self.index_ctx, args).await?)
	}
}

impl ToSql for IndexFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.name);
		f.push_str("(...)");
	}
}

impl Clone for IndexFunctionExec {
	fn clone(&self) -> Self {
		Self {
			name: self.name.clone(),
			arguments: self.arguments.clone(),
			index_ctx: self.index_ctx.clone(),
			func_required_context: self.func_required_context,
		}
	}
}
