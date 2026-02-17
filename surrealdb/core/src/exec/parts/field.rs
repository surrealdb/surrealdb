//! Field access part -- `foo` in `obj.foo`.

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::fetch_record_with_computed_fields;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Threshold below which we evaluate sequentially (no parallelism overhead).
const PARALLEL_BATCH_THRESHOLD: usize = 2;

/// Simple field access on an object - `foo`.
///
/// When applied to a RecordId, the record is automatically fetched from the
/// database and the field is accessed on the fetched object.
#[derive(Debug, Clone)]
pub struct FieldPart {
	pub name: String,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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
		let none = Value::None;
		let value = ctx.current_value.unwrap_or(&none);
		Ok(evaluate_field(value, &self.name, ctx).await?)
	}

	/// Parallel batch evaluation for field access.
	///
	/// Field access on RecordIds triggers record fetches, which are I/O-bound.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		if values.len() < PARALLEL_BATCH_THRESHOLD {
			// Small batches: avoid parallelism overhead
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value(value)).await?);
			}
			return Ok(results);
		}
		let futures: Vec<_> =
			values.iter().map(|value| self.evaluate(ctx.with_value(value))).collect();
		futures::future::try_join_all(futures).await
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn try_simple_field(&self) -> Option<&str> {
		Some(&self.name)
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

		Value::Geometry(geo) => {
			// Geometry values support GeoJSON field access (type, coordinates, geometries)
			let obj = geo.as_object();
			Ok(obj.get(name).cloned().unwrap_or(Value::None))
		}

		_ => Ok(Value::None),
	}
}
