//! Computed index access part -- `[expr]`.

use crate::val::Value;

/// Index access on arrays, sets, objects, and record IDs.
pub(crate) fn evaluate_index(value: &Value, index: &Value) -> anyhow::Result<Value> {
	use crate::val::record_id::RecordIdKey;

	match (value, index) {
		// Array with numeric index
		(Value::Array(arr), Value::Number(n)) => {
			Ok(n.as_array_index().and_then(|idx| arr.get(idx).cloned()).unwrap_or(Value::None))
		}
		// Set with numeric index
		(Value::Set(set), Value::Number(n)) => {
			Ok(n.as_array_index().and_then(|idx| set.nth(idx).cloned()).unwrap_or(Value::None))
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
		(Value::RecordId(rid), Value::Number(n)) => {
			if let RecordIdKey::Array(arr) = &rid.key {
				Ok(n.as_array_index().and_then(|idx| arr.get(idx).cloned()).unwrap_or(Value::None))
			} else {
				Ok(Value::None)
			}
		}
		_ => Ok(Value::None),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Array, Number, Object, Set};

	// =========================================================================
	// evaluate_index — container/index type matrix
	// =========================================================================

	#[tokio::test]
	async fn a_set_is_indexed_in_its_own_sorted_order() {
		// A set stores its members sorted and deduplicated, so position 0 is the
		// smallest member rather than the first one written.
		let set =
			Value::Set(Set::from_iter(vec![Value::from(30), Value::from(10), Value::from(20)]));
		assert_eq!(evaluate_index(&set, &Value::from(0)).unwrap(), Value::from(10));
		assert_eq!(evaluate_index(&set, &Value::from(2)).unwrap(), Value::from(30));
		assert_eq!(evaluate_index(&set, &Value::from(3)).unwrap(), Value::None);
	}

	// =========================================================================
	// IndexPart — the key expression is evaluated against the document root
	// =========================================================================

	#[test]
	fn test_evaluate_index_on_array() {
		let arr = Value::Array(Array::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		]));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(0))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(1)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(2))).unwrap();
		assert_eq!(result, Value::Number(Number::Int(3)));

		let result = evaluate_index(&arr, &Value::Number(Number::Int(5))).unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn test_evaluate_index_on_object() {
		let obj = Value::Object(Object::from_iter([(
			"key1".to_string(),
			Value::String(Strand::new_static("value1")),
		)]));

		let result = evaluate_index(&obj, &Value::String(Strand::new_static("key1"))).unwrap();
		assert_eq!(result, Value::String(Strand::new_static("value1")));
	}
}
