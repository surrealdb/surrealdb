//! Destructure part -- `{ field1, field2: path, ... }`.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::{evaluate_physical_path, fetch_record_with_computed_fields};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel};
use crate::expr::{ControlFlowExt, FlowResult};
use crate::val::Value;

/// Destructure - extract fields into a new object `{ field1, field2: path }`.
#[derive(Debug, Clone)]
pub struct DestructurePart {
	pub fields: Vec<DestructureField>,
}

/// A field in a destructure pattern.
#[derive(Debug, Clone)]
pub enum DestructureField {
	/// Include all fields from a nested object.
	All(String),
	/// Include a single field by name.
	Field(String),
	/// Include a field with an aliased path.
	Aliased {
		field: String,
		path: Vec<Arc<dyn PhysicalExpr>>,
	},
	/// Nested destructure on a field.
	Nested {
		field: String,
		parts: Vec<DestructureField>,
	},
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for DestructurePart {
	fn name(&self) -> &'static str {
		"Destructure"
	}

	fn required_context(&self) -> ContextLevel {
		fn field_context(fields: &[DestructureField]) -> ContextLevel {
			fields
				.iter()
				.map(|f| match f {
					DestructureField::All(_) | DestructureField::Field(_) => ContextLevel::Root,
					DestructureField::Aliased {
						path,
						..
					} => path
						.iter()
						.map(|p| p.required_context())
						.max()
						.unwrap_or(ContextLevel::Root),
					DestructureField::Nested {
						parts,
						..
					} => field_context(parts),
				})
				.max()
				.unwrap_or(ContextLevel::Root)
		}
		// Destructure may need to fetch records (when applied to RecordId)
		ContextLevel::Database.max(field_context(&self.fields))
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);
		evaluate_destructure(&value, &self.fields, ctx).await
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		fn field_access(fields: &[DestructureField]) -> AccessMode {
			fields
				.iter()
				.map(|f| match f {
					DestructureField::All(_) | DestructureField::Field(_) => AccessMode::ReadOnly,
					DestructureField::Aliased {
						path,
						..
					} => path.iter().map(|p| p.access_mode()).combine_all(),
					DestructureField::Nested {
						parts,
						..
					} => field_access(parts),
				})
				.combine_all()
		}
		field_access(&self.fields)
	}
}

impl ToSql for DestructurePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('{');
		for (i, field) in self.fields.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			field.fmt_sql(f, fmt);
		}
		f.push('}');
	}
}

impl ToSql for DestructureField {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			DestructureField::All(name) => {
				f.push_str(name);
				f.push_str(".*");
			}
			DestructureField::Field(name) => {
				f.push_str(name);
			}
			DestructureField::Aliased {
				field,
				path,
			} => {
				f.push_str(field);
				f.push_str(": ");
				for part in path {
					part.fmt_sql(f, fmt);
				}
			}
			DestructureField::Nested {
				field,
				parts,
			} => {
				f.push_str(field);
				f.push_str(": {");
				for (i, part) in parts.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					part.fmt_sql(f, fmt);
				}
				f.push('}');
			}
		}
	}
}

/// Destructure evaluation - extract fields into a new object.
async fn evaluate_destructure(
	value: &Value,
	fields: &[DestructureField],
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	match value {
		Value::Object(obj) => {
			let mut result = std::collections::BTreeMap::new();

			for field in fields {
				match field {
					DestructureField::All(name) => {
						// Include all fields from the nested object
						if let Some(Value::Object(nested)) = obj.get(name.as_str()) {
							for (k, v) in nested.iter() {
								result.insert(k.clone(), v.clone());
							}
						}
					}
					DestructureField::Field(name) => {
						let v = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						result.insert(name.clone(), v);
					}
					DestructureField::Aliased {
						field: name,
						path,
					} => {
						// Evaluate the aliased path starting from the current value
						// (not from obj.get(field)). The field name is just the output label.
						let v = evaluate_physical_path(value, path, ctx.clone()).await?;
						result.insert(name.clone(), v);
					}
					DestructureField::Nested {
						field: name,
						parts,
					} => {
						let nested_value = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						let v = Box::pin(evaluate_destructure(&nested_value, parts, ctx.clone()))
							.await?;
						result.insert(name.clone(), v);
					}
				}
			}

			Ok(Value::Object(crate::val::Object(result)))
		}
		Value::RecordId(rid) => {
			// Fetch the record with computed fields evaluated, then destructure it.
			// Using fetch_record_with_computed_fields ensures that DEFINE FIELD ... COMPUTED
			// fields are properly evaluated before destructuring.
			let fetched = fetch_record_with_computed_fields(rid, ctx.clone())
				.await
				.context("Failed to fetch record")?;
			if fetched.is_none() {
				return Ok(Value::None);
			}

			// Continue destructure on the fetched object
			Box::pin(evaluate_destructure(&fetched, fields, ctx)).await
		}
		Value::Array(arr) => {
			// Apply destructure to each element
			let mut results = Vec::with_capacity(arr.len());
			for item in arr.iter() {
				let v = Box::pin(evaluate_destructure(item, fields, ctx.clone())).await?;
				results.push(v);
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}
