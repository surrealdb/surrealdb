//! Array operation parts -- `[*]`, `...`, `[$]`, `[~]`.

use crate::exec::EvalContext;
use crate::exec::operators::fetch::{
	fetch_record, fetch_record_no_perms, fetch_record_no_perms_slot, fetch_record_slot,
};
use crate::exec::operators::link_window::{Slot, Windows};
use crate::expr::FlowResult;
use crate::val::{RecordId, Value};

// ============================================================================
// AllPart -- [*] or .*
// ============================================================================

/// All elements evaluation.
///
/// An array or a set yields its elements, each record-id element fetched and
/// every other element kept; a set's results are collected back into a set.
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> FlowResult<Value> {
	match value {
		Value::Array(arr) => Ok(match fetch_record_elements(&arr.0, ctx).await? {
			Some(results) => Value::Array(results.into()),
			None => value.clone(),
		}),
		Value::Set(set) => Ok(match fetch_record_elements(set.0.iter().as_slice(), ctx).await? {
			Some(results) => Value::Set(results.into_iter().collect()),
			None => value.clone(),
		}),
		Value::Object(_) => Ok(value.clone()),
		// The fetch computes the record's COMPUTED fields, which may reach
		// `.*` over a link again, so it spends a unit of the computation-depth
		// budget: a cycle of links fails with `ComputationDepthExceeded`
		// rather than recursing without bound.
		Value::RecordId(rid) => {
			let exec_ctx = ctx.exec_ctx.dive()?;
			if ctx.skip_fetch_perms {
				fetch_record_no_perms(&exec_ctx, rid).await
			} else {
				fetch_record(&exec_ctx, rid).await
			}
		}
		// Anything else (NONE, NULL, scalars, geometries) returns NONE rather than a
		// single-element array: `none.*` is `NONE`, not `[NONE]`.
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => Ok(Value::None),
	}
}

/// The record `rid` names, fetched as `.*` reads it, from `slot` where a window holds it.
async fn fetch_linked(rid: &RecordId, ctx: &EvalContext<'_>, slot: Slot<'_>) -> FlowResult<Value> {
	let exec_ctx = ctx.exec_ctx.dive()?;
	if ctx.skip_fetch_perms {
		fetch_record_no_perms_slot(&exec_ctx, rid, slot).await
	} else {
		fetch_record_slot(&exec_ctx, rid, slot).await
	}
}

/// The elements of a collection under `.*`, each record id fetched and every
/// other element kept, in order; `None` when no element is a record id, so
/// the collection is its own answer.
///
/// Where the evaluation admits windows and the collection holds two or more record ids, their
/// records are read a window at a time.
async fn fetch_record_elements(
	elements: &[Value],
	ctx: EvalContext<'_>,
) -> FlowResult<Option<Vec<Value>>> {
	if !elements.iter().any(|v| matches!(v, Value::RecordId(_))) {
		return Ok(None);
	}
	let mut windows = Windows::of(ctx.link_windows, elements, &|_| true);
	let mut results = Vec::with_capacity(elements.len());
	for (at, item) in elements.iter().enumerate() {
		let processed = if let Value::RecordId(rid) = item {
			let slot = match &mut windows {
				Some(windows) => {
					windows.prepare(ctx.exec_ctx, elements, at, &|_| true).await?;
					windows.slot(at)
				}
				None => Slot::Unread,
			};
			Box::pin(fetch_linked(rid, &ctx, slot)).await?
		} else {
			item.clone()
		};
		results.push(processed);
	}
	Ok(Some(results))
}

// ============================================================================
// FlattenPart -- ...
// ============================================================================

/// Flatten nested arrays.
pub(crate) fn evaluate_flatten(value: &Value) -> anyhow::Result<Value> {
	if let Value::Array(arr) = value {
		let mut result = Vec::new();
		for item in arr.iter() {
			if let Value::Array(inner) = item {
				result.extend(inner.iter().cloned());
			} else {
				result.push(item.clone());
			}
		}
		Ok(Value::Array(result.into()))
	} else {
		Ok(value.clone())
	}
}

// ============================================================================
// LastPart -- [$]
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{Array, Number};

	// =========================================================================
	// AllPart -- [*] / .*
	// =========================================================================

	// =========================================================================
	// FlattenPart -- ...
	// =========================================================================

	// =========================================================================
	// FirstPart / LastPart -- [0] / [$]
	// =========================================================================

	#[test]
	fn test_evaluate_flatten() {
		let nested = Value::Array(Array::from(vec![
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
			Value::Array(Array::from(vec![Value::Number(Number::Int(3))])),
		]));

		let result = evaluate_flatten(&nested).unwrap();
		assert_eq!(
			result,
			Value::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
				Value::Number(Number::Int(3)),
			]))
		);
	}

	#[test]
	fn test_evaluate_first_and_last() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		// Test first element
		let first = match &arr {
			Value::Array(a) => a.first().cloned().unwrap_or(Value::None),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => arr.clone(),
		};
		assert_eq!(first, Value::Number(Number::Int(1)));

		// Test last element
		let last = match &arr {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => arr.clone(),
		};
		assert_eq!(last, Value::Number(Number::Int(3)));

		// Empty array
		let empty = Value::Array(Array::from(Vec::<Value>::new()));
		let first_empty = match &empty {
			Value::Array(a) => a.first().cloned().unwrap_or(Value::None),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => empty.clone(),
		};
		let last_empty = match &empty {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => empty.clone(),
		};
		assert_eq!(first_empty, Value::None);
		assert_eq!(last_empty, Value::None);
	}
}
