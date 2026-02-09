//! Field access part -- `foo` in `obj.foo`.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::fetch_record_with_computed_fields;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::val::{RecordId, Value};

/// Simple field access on an object - `foo`.
///
/// When applied to a RecordId, the record is automatically fetched from the
/// database and the field is accessed on the fetched object.
#[derive(Debug, Clone)]
pub struct FieldPart {
	pub name: String,
}

#[async_trait]
impl PhysicalExpr for FieldPart {
	fn name(&self) -> &'static str {
		"Field"
	}

	fn required_context(&self) -> ContextLevel {
		// Field access might trigger record fetch if applied to RecordId,
		// so we conservatively require database context.
		ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		Ok(evaluate_field(&value, &self.name, ctx).await?)
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for FieldPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('.');
		f.push_str(&self.name);
	}
}

/// Field access on objects, with support for RecordId auto-fetch.
///
/// When accessing a field on a RecordId, the record is automatically fetched
/// from the database and the field is accessed on the fetched object.
pub(crate) async fn evaluate_field(
	value: &Value,
	name: &str,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	match value {
		Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),

		Value::RecordId(rid) => {
			// Fetch the record with computed fields evaluated.
			let fetched = fetch_record_with_computed_fields(rid, ctx).await?;
			match fetched {
				Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
				_ => Ok(Value::None),
			}
		}

		Value::Array(arr) => {
			// Apply field access to each element (may involve fetches)
			let mut results = Vec::with_capacity(arr.len());
			for v in arr.iter() {
				results.push(Box::pin(evaluate_field(v, name, ctx.clone())).await?);
			}
			Ok(Value::Array(results.into()))
		}

		_ => Ok(Value::None),
	}
}
