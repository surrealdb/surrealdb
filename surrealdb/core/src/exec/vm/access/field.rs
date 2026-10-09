//! Field access part -- `foo` in `obj.foo`.

use crate::exec::EvalContext;
use crate::expr::FlowResult;
use crate::val::Value;

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
			// When we are already computing fields for this record, answer the
			// dereference directly rather than fetching the record. Otherwise
			// a computed field like `{ return $this.id.prop }` would re-enter
			// compute_fields_for_value for the same record and stack-overflow.
			//
			// SECURITY: prefer the caller's reduced view of the row. Reading
			// storage here sees the record as written, so a computed field
			// spelled `id.secret` returns a field the caller's
			// `PERMISSIONS FOR select` removed, whatever order the computed
			// fields and the permission passes ran in.
			if let Some(computing) =
				ctx.computing_record.as_ref().filter(|computing| &computing.rid == rid)
			{
				if let Some(stored) = &computing.stored {
					return Ok(match stored {
						Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
						_ => Value::None,
					});
				}
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

#[cfg(test)]
mod tests {

	// =========================================================================
	// Object / array / scalar inputs
	// =========================================================================

	// =========================================================================
	// Record-id inputs
	// =========================================================================

	// =========================================================================
	// Batch evaluation
	// =========================================================================

	// =========================================================================
	// Declared metadata
	// =========================================================================
}
