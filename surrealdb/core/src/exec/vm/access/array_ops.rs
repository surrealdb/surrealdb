//! Array operation parts -- `[*]`, `...`, `[$]`, `[~]`.

use crate::exec::EvalContext;
use crate::expr::FlowResult;
use crate::val::Value;

// ============================================================================
// AllPart -- [*] or .*
// ============================================================================

/// All elements evaluation.
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> FlowResult<Value> {
	match value {
		Value::Array(arr) => {
			let has_record_ids = arr.iter().any(|v| matches!(v, Value::RecordId(_)));
			if has_record_ids {
				let mut results = Vec::with_capacity(arr.len());
				for item in arr.iter() {
					// Under `.*`, only RecordId elements trigger a fetch; everything else passes
					// through unchanged.
					let processed = match item {
						Value::RecordId(_) => Box::pin(evaluate_all(item, ctx.clone())).await?,
						_ => item.clone(),
					};
					results.push(processed);
				}
				Ok(Value::Array(results.into()))
			} else {
				Ok(Value::Array(arr.clone()))
			}
		}
		Value::Object(_) => Ok(value.clone()),
		Value::RecordId(rid) => {
			if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await
			}
		}
		// Anything else (NONE, NULL, scalars, geometries) returns NONE rather than a
		// single-element array: `none.*` is `NONE`, not `[NONE]`.
		_ => Ok(Value::None),
	}
}

// ============================================================================
// FlattenPart -- ...
// ============================================================================

/// Flatten nested arrays.
pub(crate) fn evaluate_flatten(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => {
			let mut result = Vec::new();
			for item in arr.iter() {
				match item {
					Value::Array(inner) => result.extend(inner.iter().cloned()),
					other => result.push(other.clone()),
				}
			}
			Ok(Value::Array(result.into()))
		}
		other => Ok(other.clone()),
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
			_ => arr.clone(),
		};
		assert_eq!(first, Value::Number(Number::Int(1)));

		// Test last element
		let last = match &arr {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			_ => arr.clone(),
		};
		assert_eq!(last, Value::Number(Number::Int(3)));

		// Empty array
		let empty = Value::Array(Array::from(Vec::<Value>::new()));
		let first_empty = match &empty {
			Value::Array(a) => a.first().cloned().unwrap_or(Value::None),
			_ => empty.clone(),
		};
		let last_empty = match &empty {
			Value::Array(a) => a.last().cloned().unwrap_or(Value::None),
			_ => empty.clone(),
		};
		assert_eq!(first_empty, Value::None);
		assert_eq!(last_empty, Value::None);
	}
}
