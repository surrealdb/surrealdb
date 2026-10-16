//! Field access part -- `foo` in `obj.foo`.

use crate::exec::EvalContext;
use crate::exec::operators::link_window::{Slot, Windows};
use crate::expr::FlowResult;
use crate::val::{RecordId, RecordIdKey, Value};

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

		Value::RecordId(rid) => field_of_record(rid, name, &ctx, Slot::Unread).await,

		Value::Array(arr) => Ok(Value::Array(field_of_each(&arr.0, name, ctx).await?.into())),

		// A set is read element by element as an array is, its results
		// collected back into a set.
		Value::Set(set) => Ok(Value::Set(
			field_of_each(set.0.iter().as_slice(), name, ctx).await?.into_iter().collect(),
		)),

		Value::Geometry(geo) => {
			// Geometry values support GeoJSON field access (type, coordinates, geometries)
			let obj = geo.as_object();
			Ok(obj.get(name).cloned().unwrap_or(Value::None))
		}

		_ => Ok(Value::None),
	}
}

/// `name` of each of `elements`, in order, as [`evaluate_field`] reads it of each. Where the
/// evaluation admits windows and two or more of the elements are record ids that are read from
/// storage, their records are read a window at a time.
async fn field_of_each(
	elements: &[Value],
	name: &str,
	ctx: EvalContext<'_>,
) -> FlowResult<Vec<Value>> {
	let include = |rid: &RecordId| reads_storage(rid, name, &ctx);
	let mut windows = Windows::of(ctx.link_windows, elements, &include);
	let mut results = Vec::with_capacity(elements.len());
	for (at, v) in elements.iter().enumerate() {
		results.push(match v {
			Value::RecordId(rid) if include(rid) => {
				let slot = match &mut windows {
					Some(windows) => {
						windows.prepare(ctx.exec_ctx, elements, at, &include).await?;
						windows.slot(at)
					}
					None => Slot::Unread,
				};
				Box::pin(field_of_record(rid, name, &ctx, slot)).await?
			}
			_ => Box::pin(evaluate_field(v, name, ctx.clone())).await?,
		});
	}
	Ok(results)
}

/// Whether `name` of the record `rid` is read from storage: not a component of its key, and not
/// the record whose fields are being computed.
fn reads_storage(rid: &RecordId, name: &str, ctx: &EvalContext<'_>) -> bool {
	if let RecordIdKey::Object(obj) = &rid.key
		&& obj.contains_key(name)
	{
		return false;
	}
	ctx.computing_record.as_ref().is_none_or(|computing| &computing.rid != rid)
}

/// `name` of the record `rid` names, fetched from `slot` where a window holds it.
async fn field_of_record(
	rid: &RecordId,
	name: &str,
	ctx: &EvalContext<'_>,
	slot: Slot<'_>,
) -> FlowResult<Value> {
	// SECURITY: when the record-id key is an Object and the field
	// name resolves to one of its components, return the immutable
	// key component directly. Falling through to `fetch_record`
	// runs `SELECT *` with permissions disabled and re-binds
	// `id.tenant` to the row's `tenant` document field — letting
	// permission predicates like
	// `WHERE id.tenant = $token.tenant` be spoofed, and during UPDATE
	// the same self-fetch returns the already-stored new value for
	// both `o` and `n` in `store_index_data`, leaving stale indexes.
	// When the requested name is not a key component, keep the
	// remote-fetch semantics (`record:foo.id` returns the rid itself
	// via the standard `id` projection).
	if let RecordIdKey::Object(obj) = &rid.key
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
	if let Some(computing) = ctx.computing_record.as_ref().filter(|computing| &computing.rid == rid)
	{
		if let Some(stored) = &computing.stored {
			return Ok(match stored {
				Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
				_ => Value::None,
			});
		}
		let version = ctx.exec_ctx.version_stamp();
		let raw =
			crate::exec::operators::fetch::fetch_raw_record(ctx.exec_ctx, rid, version).await?;
		return match raw {
			Some(Value::Object(obj)) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
			_ => Ok(Value::None),
		};
	}
	// The fetch computes the record's COMPUTED fields, which may read
	// a field through a link again, so it spends a unit of the
	// computation-depth budget: a cycle of links fails with
	// `ComputationDepthExceeded` rather than recursing without bound.
	let exec_ctx = ctx.exec_ctx.dive()?;
	let fetched = if ctx.skip_fetch_perms {
		crate::exec::operators::fetch::fetch_record_no_perms_slot(&exec_ctx, rid, slot).await?
	} else {
		crate::exec::operators::fetch::fetch_record_slot(&exec_ctx, rid, slot).await?
	};
	match fetched {
		Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
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
