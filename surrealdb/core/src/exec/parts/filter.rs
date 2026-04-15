//! Where filtering part -- `[WHERE condition]`.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Filter predicate on arrays - `[WHERE condition]`.
#[derive(Debug, Clone)]
pub struct WherePart {
	pub predicate: Arc<dyn PhysicalExpr>,
	/// Whether the predicate references `$parent`. When false, we skip the
	/// per-element context allocation for binding `$parent`.
	pub needs_parent: bool,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for WherePart {
	fn name(&self) -> &'static str {
		"Where"
	}

	fn required_context(&self) -> ContextLevel {
		self.predicate.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Only bind $parent when the predicate actually references it,
		// avoiding a Value::clone() + context allocation per element for
		// the common case (e.g. `[WHERE age > 30]`).
		let parent_ctx = if self.needs_parent
			&& let Some(parent) = ctx.document_root
		{
			Some(ctx.exec_ctx.with_param("parent", parent.clone()))
		} else {
			None
		};
		let ctx = if let Some(ref pc) = parent_ctx {
			EvalContext {
				exec_ctx: pc,
				current_value: ctx.current_value,
				local_params: ctx.local_params,
				recursion_ctx: ctx.recursion_ctx,
				document_root: ctx.document_root,
				skip_fetch_perms: ctx.skip_fetch_perms,
				computing_record: ctx.computing_record,
			}
		} else {
			ctx
		};

		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		match value {
			Value::Array(arr) => {
				let mut result = Vec::new();
				for item in arr.iter() {
					let item_ctx = ctx.with_value(item);
					let matches = self.predicate.evaluate(item_ctx).await?.is_truthy();
					if matches {
						result.push(item.clone());
					}
				}
				Ok(Value::Array(result.into()))
			}
			// For non-arrays, check if the single value matches
			other => {
				let item_ctx = ctx.with_value(&other);
				let matches = self.predicate.evaluate(item_ctx).await?.is_truthy();
				if matches {
					Ok(Value::Array(vec![other].into()))
				} else {
					Ok(Value::Array(crate::val::Array::default()))
				}
			}
		}
	}

	fn access_mode(&self) -> AccessMode {
		self.predicate.access_mode()
	}
}

impl ToSql for WherePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("[WHERE ");
		self.predicate.fmt_sql(f, fmt);
		f.push(']');
	}
}
