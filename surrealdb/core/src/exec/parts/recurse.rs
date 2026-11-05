//! Recursion helpers shared by the recursion strategies of `{min..max}`, `{..}` and `@`.

use crate::val::Value;

/// Helper function to create a hash for value deduplication in graph traversal.
///
/// Optimized for the common case of RecordId values, which are the primary
/// target for cycle detection in graph traversal.
pub(crate) fn value_hash(value: &Value) -> u64 {
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();

	match value {
		Value::RecordId(rid) => {
			0u8.hash(&mut hasher);
			rid.hash(&mut hasher);
		}
		Value::None => {
			1u8.hash(&mut hasher);
		}
		Value::Null => {
			2u8.hash(&mut hasher);
		}
		Value::Bool(b) => {
			3u8.hash(&mut hasher);
			b.hash(&mut hasher);
		}
		Value::String(s) => {
			4u8.hash(&mut hasher);
			s.hash(&mut hasher);
		}
		Value::Number(n) => {
			5u8.hash(&mut hasher);
			n.to_string().hash(&mut hasher);
		}
		Value::Uuid(u) => {
			6u8.hash(&mut hasher);
			u.0.hash(&mut hasher);
		}
		Value::Array(arr) => {
			7u8.hash(&mut hasher);
			arr.len().hash(&mut hasher);
			for (i, v) in arr.iter().enumerate() {
				if i >= 8 {
					break;
				}
				value_hash(v).hash(&mut hasher);
			}
		}
		Value::Object(obj) => {
			8u8.hash(&mut hasher);
			obj.len().hash(&mut hasher);
			for (i, (k, v)) in obj.iter().enumerate() {
				if i >= 8 {
					break;
				}
				k.hash(&mut hasher);
				value_hash(v).hash(&mut hasher);
			}
		}
		Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => {
			255u8.hash(&mut hasher);
			format!("{:?}", value).hash(&mut hasher);
		}
	}

	hasher.finish()
}

#[cfg(test)]
mod tests {

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::{Number, Object, RecordId, RecordIdKey};

	fn rid(table: &str, key: i64) -> Value {
		Value::RecordId(RecordId {
			table: table.into(),
			key: RecordIdKey::Number(key),
		})
	}

	fn arr(values: Vec<Value>) -> Value {
		Value::from(values)
	}

	// =========================================================================
	// value_hash -- the identity used for per-level dedup and the assembly cache
	// =========================================================================

	#[test]
	fn value_hash_separates_the_three_falsey_variants() {
		// Cycle detection keys on this hash, so NONE, NULL and `false` must not
		// be conflated: each variant carries its own discriminant byte.
		let none = value_hash(&Value::None);
		let null = value_hash(&Value::Null);
		let no = value_hash(&Value::Bool(false));
		assert_ne!(none, null);
		assert_ne!(none, no);
		assert_ne!(null, no);
	}

	#[test]
	fn value_hash_treats_a_record_id_key_type_as_part_of_its_identity() {
		// RecordId hashes through its own `Hash` impl, so the table and the key
		// (including the key's type) all take part.
		let numeric = rid("person", 1);
		let string_key = Value::RecordId(RecordId {
			table: "person".into(),
			key: RecordIdKey::String("1".into()),
		});
		let other_table = rid("company", 1);

		assert_eq!(value_hash(&numeric), value_hash(&rid("person", 1)));
		assert_ne!(value_hash(&numeric), value_hash(&string_key));
		assert_ne!(value_hash(&numeric), value_hash(&other_table));
	}

	#[test]
	fn value_hash_of_a_number_is_its_decimal_rendering_not_its_representation() {
		// Numbers are hashed via `to_string()`, so an integer, a float and a
		// decimal that all render as "1" share one hash. This is what makes
		// `1` and `1f` a single node during traversal.
		let int = Value::Number(Number::Int(1));
		let float = Value::Number(Number::Float(1.0));
		let decimal = Value::Number(Number::Decimal(Decimal::from(1)));

		assert_eq!(value_hash(&int), value_hash(&float));
		assert_eq!(value_hash(&int), value_hash(&decimal));
	}

	#[test]
	fn value_hash_splits_numerically_equal_numbers_that_render_differently() {
		// The rendering is not canonicalised, so trailing decimal scale and a
		// negative zero produce different hashes from the numerically equal
		// plain integer. Two values that compare equal therefore hash apart,
		// and the traversal treats them as distinct nodes.
		let one = Value::Number(Number::Int(1));
		let one_scaled = Value::Number(Number::Decimal("1.0".parse().expect("decimal literal")));
		assert_eq!(one, one_scaled, "these compare equal");
		assert_ne!(value_hash(&one), value_hash(&one_scaled), "but they do not hash equal");

		let zero = Value::Number(Number::Int(0));
		let negative_zero = Value::Number(Number::Float(-0.0));
		assert_eq!(zero, negative_zero, "these compare equal");
		assert_ne!(value_hash(&zero), value_hash(&negative_zero), "but they do not hash equal");
	}

	#[test]
	fn value_hash_of_an_array_covers_its_length_and_only_the_first_eight_items() {
		let short = arr(vec![Value::from(1), Value::from(2)]);
		let longer = arr(vec![Value::from(1), Value::from(2), Value::from(3)]);
		assert_ne!(value_hash(&short), value_hash(&longer), "length takes part in the hash");

		// Element hashing stops after index 7. Two nine-element arrays that
		// share their first eight elements collide, so a traversal that
		// deduplicated on such values would treat them as the same node.
		let mut a: Vec<Value> = (0..9).map(Value::from).collect();
		let mut b = a.clone();
		a[8] = Value::from(100);
		b[8] = Value::from(200);
		assert_eq!(value_hash(&arr(a)), value_hash(&arr(b)));
	}

	#[test]
	fn value_hash_of_an_object_covers_its_size_and_only_the_first_eight_entries() {
		let small = Value::Object(Object::from_iter([("a".to_string(), Value::from(1))]));
		let bigger = Value::Object(Object::from_iter([
			("a".to_string(), Value::from(1)),
			("b".to_string(), Value::from(2)),
		]));
		assert_ne!(value_hash(&small), value_hash(&bigger), "entry count takes part in the hash");

		// Entries are visited in key order and hashing stops after the eighth,
		// so two nine-entry objects differing only in the last key collide.
		let entries = |last: Value| {
			let mut e: Vec<(String, Value)> =
				(0..8).map(|i| (format!("k{i}"), Value::from(i))).collect();
			e.push(("k8".to_string(), last));
			Value::Object(Object::from_iter(e))
		};
		assert_eq!(value_hash(&entries(Value::from(100))), value_hash(&entries(Value::from(200))));
	}

	// =========================================================================
	// RepeatRecursePart -- the two-phase discovery / assembly protocol
	// =========================================================================

	// =========================================================================
	// RecursePart -- draining the recursion operator
	// =========================================================================

	// =========================================================================
	// RecursePart -- SQL rendering of the depth range
	// =========================================================================

	// =========================================================================
	// End-to-end: the two phases over real record links
	// =========================================================================

	#[test]
	fn test_value_hash_consistency() {
		let v1 = Value::Number(Number::Int(42));
		let v2 = Value::Number(Number::Int(42));
		let v3 = Value::Number(Number::Int(43));

		assert_eq!(value_hash(&v1), value_hash(&v2));
		assert_ne!(value_hash(&v1), value_hash(&v3));
	}
}
