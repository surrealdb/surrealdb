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

	fn references_current_value(&self) -> bool {
		true
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
