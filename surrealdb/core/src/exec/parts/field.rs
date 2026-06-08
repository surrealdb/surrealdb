//! Field access part -- `foo` in `obj.foo`.

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
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
impl PhysicalExpr for FieldPart {
	fn name(&self) -> &'static str {
		"Field"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// Field access might trigger record fetch if applied to RecordId,
		// so we conservatively require database context.
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_field(value, &self.name, ctx).await
		})
	}

	/// Parallel batch evaluation for field access.
	///
	/// Field access on RecordIds triggers record fetches, which are I/O-bound.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
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
		})
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
) -> FlowResult<Value> {
	match value {
		Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),

		Value::RecordId(rid) => {
			// SECURITY: when the record-id key is an Object and the field
			// name resolves to one of its components, return the immutable
			// key component directly. Falling through to `fetch_record`
			// runs `SELECT *` with permissions disabled and re-binds
			// `id.tenant` to the row's `tenant` document field — letting
			// permission predicates like
			// `WHERE id.tenant = $token.tenant` be spoofed (Codex finding
			// c67c7232), and during UPDATE the same self-fetch returns the
			// already-stored new value for both `o` and `n` in
			// `store_index_data`, leaving stale indexes (Codex finding
			// 9c442c96). When the requested name is not a key component,
			// keep the existing remote-fetch semantics (`record:foo.id`
			// returns the rid itself via the standard `id` projection).
			if let crate::val::RecordIdKey::Object(obj) = &rid.key
				&& obj.contains_key(name)
			{
				return Ok(obj.get(name).cloned().unwrap_or(Value::None));
			}
			// When we are already computing fields for this record, fetch the
			// raw stored data without re-evaluating computed fields. Otherwise
			// a computed field like `{ return $this.id.prop }` would re-enter
			// compute_fields_for_value for the same record and stack-overflow.
			if ctx.computing_record.as_ref() == Some(rid) {
				let version = ctx.exec_ctx.version_stamp();
				let raw =
					crate::exec::operators::fetch::fetch_raw_record(ctx.exec_ctx, rid, version)
						.await?;
				return match raw {
					Some(Value::Object(obj)) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
					_ => Ok(Value::None),
				};
			}
			let fetched = if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await?
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await?
			};
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
