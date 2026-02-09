//! Computed index access part -- `[expr]`.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Computed index access - `[expr]`.
#[derive(Debug, Clone)]
pub struct IndexPart {
	pub expr: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for IndexPart {
	fn name(&self) -> &'static str {
		"Index"
	}

	fn required_context(&self) -> ContextLevel {
		self.expr.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		let index = self.expr.evaluate(ctx).await?;
		Ok(evaluate_index(&value, &index)?)
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		self.expr.access_mode()
	}
}

impl ToSql for IndexPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('[');
		self.expr.fmt_sql(f, fmt);
		f.push(']');
	}
}

/// Index access on arrays, sets, objects, and record IDs.
pub(crate) fn evaluate_index(value: &Value, index: &Value) -> anyhow::Result<Value> {
	use crate::val::record_id::RecordIdKey;

	match (value, index) {
		// Array with numeric index
		(Value::Array(arr), Value::Number(n)) => {
			let idx = n.to_usize();
			Ok(arr.get(idx).cloned().unwrap_or(Value::None))
		}
		// Set with numeric index
		(Value::Set(set), Value::Number(n)) => {
			let idx = n.to_usize();
			Ok(set.nth(idx).cloned().unwrap_or(Value::None))
		}
		// Array with range
		(Value::Array(arr), Value::Range(range)) => {
			let slice = range
				.as_ref()
				.clone()
				.coerce_to_typed::<i64>()
				.map_err(|e| anyhow::anyhow!("Invalid range: {}", e))?
				.slice(arr.as_slice())
				.map(|s| Value::Array(s.to_vec().into()))
				.unwrap_or(Value::None);
			Ok(slice)
		}
		// Object with string key
		(Value::Object(obj), Value::String(key)) => {
			Ok(obj.get(key.as_str()).cloned().unwrap_or(Value::None))
		}
		// Object with numeric key (converted to string)
		(Value::Object(obj), Value::Number(n)) => {
			let key = n.to_string();
			Ok(obj.get(&key).cloned().unwrap_or(Value::None))
		}
		// RecordId with numeric index - only array keys support indexing
		(Value::RecordId(rid), Value::Number(n)) => match &rid.key {
			RecordIdKey::Array(arr) => {
				let idx = n.to_usize();
				Ok(arr.get(idx).cloned().unwrap_or(Value::None))
			}
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}
