use std::cmp::Ordering;

use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	pub(crate) fn compare(
		&self,
		other: &Self,
		path: &[Part],
		collate: bool,
		numeric: bool,
	) -> Option<Ordering> {
		let Some(p) = path.first() else {
			return match (collate, numeric) {
				(true, true) => self.natural_lexical_cmp(other),
				(true, false) => self.lexical_cmp(other),
				(false, true) => self.natural_cmp(other),
				_ => self.partial_cmp(other),
			};
		};

		match (self, other) {
			// Current path part is an object
			(Value::Object(a), Value::Object(b)) => match p {
				Part::Field(f) => match (a.get(&**f), b.get(&**f)) {
					(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
					(Some(_), None) => Some(Ordering::Greater),
					(None, Some(_)) => Some(Ordering::Less),
					(_, _) => Some(Ordering::Equal),
				},
				_ => None,
			},
			// Current path part is an array
			(Value::Array(a), Value::Array(b)) => match p {
				Part::All => {
					for (a, b) in a.iter().zip(b.iter()) {
						match a.compare(b, path.next(), collate, numeric) {
							Some(Ordering::Equal) => continue,
							None => continue,
							o => return o,
						}
					}
					match (a.len(), b.len()) {
						(a, b) if a > b => Some(Ordering::Greater),
						(a, b) if a < b => Some(Ordering::Less),
						_ => Some(Ordering::Equal),
					}
				}
				Part::First => match (a.first(), b.first()) {
					(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
					(Some(_), None) => Some(Ordering::Greater),
					(None, Some(_)) => Some(Ordering::Less),
					(_, _) => Some(Ordering::Equal),
				},
				Part::Last => match (a.last(), b.last()) {
					(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
					(Some(_), None) => Some(Ordering::Greater),
					(None, Some(_)) => Some(Ordering::Less),
					(_, _) => Some(Ordering::Equal),
				},
				//TODO: It is kind of weird that a[1] works but `a[+(1)]` or `let $b = 1;
				// a[$b]` for example doesn't as
				x => {
					if let Some(idx) = x.as_old_index() {
						match (a.get(idx), b.get(idx)) {
							(Some(a), Some(b)) => a.compare(b, path.next(), collate, numeric),
							(Some(_), None) => Some(Ordering::Greater),
							(None, Some(_)) => Some(Ordering::Less),
							(_, _) => Some(Ordering::Equal),
						}
					} else {
						for (a, b) in a.iter().zip(b.iter()) {
							match a.compare(b, path, collate, numeric) {
								Some(Ordering::Equal) => continue,
								None => continue,
								o => return o,
							}
						}
						Some(a.len().cmp(&b.len()))
					}
				}
			},
			// Handle field access on mixed or non-object types
			(a, b) => match p {
				Part::Field(f) => match (a, b) {
					// If one is an Object and the other is not, treat non-Object as missing field
					(Value::Object(a), _) => match a.get(&**f) {
						Some(a) => a.compare(&Value::None, path.next(), collate, numeric),
						None => Some(Ordering::Equal),
					},
					(_, Value::Object(b)) => match b.get(&**f) {
						Some(b) => Value::None.compare(b, path.next(), collate, numeric),
						None => Some(Ordering::Equal),
					},
					// Both are non-object types, so both are missing the field
					_ => Some(Ordering::Equal),
				},
				// For non-field path parts, continue comparing with remaining path
				_ => a.compare(b, path.next(), collate, numeric),
			},
		}
	}
}

#[cfg(test)]
mod tests {

	use rstest::rstest;

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[rstest]
	#[case::none_eq_none(Value::None, Value::None, Idiom::default(), Some(Ordering::Equal))]
	#[case::none_eq_none(parse_val!("{ test: { other: null, something: 456 } }"), parse_val!("{ test: { other: null, something: 123 } }"), syn::idiom("test.something").unwrap().into(), Some(Ordering::Greater))]
	fn test_compare(
		#[case] a: Value,
		#[case] b: Value,
		#[case] path: Idiom,
		#[case] expected: Option<Ordering>,
	) {
		let res = a.compare(&b, &path, false, false);
		assert_eq!(res, expected);
	}

	#[test]
	fn compare_none() {
		let idi: Idiom = Default::default();
		let one = parse_val!("{ test: { other: null, something: 456 } }");
		let two = parse_val!("{ test: { other: null, something: 123 } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: 456 } }");
		let two = parse_val!("{ test: { other: null, something: 123 } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_basic_missing_left() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = parse_val!("{ test: { other: null } }");
		let two = parse_val!("{ test: { other: null, something: 123 } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_basic_missing_right() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: 456 } }");
		let two = parse_val!("{ test: { other: null } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [4, 5, 6] } }");
		let two = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }");
		let two = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_longer_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let two = parse_val!("{ test: { other: null, something: [1, 2, 3, 4, 5, 6] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: null } }");
		let two = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [4, 5, 6] } }");
		let two = parse_val!("{ test: { other: null, something: null } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_array_missing_value_left() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [1, null, 3] } }");
		let two = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_array_missing_value_right() {
		let idi: Idiom = syn::idiom("test.something.*").unwrap().into();
		let one = parse_val!("{ test: { other: null, something: [1, 2, 3] } }");
		let two = parse_val!("{ test: { other: null, something: [1, null, 3] } }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_last() {
		let idi: Idiom = syn::idiom("test[$]").unwrap().into();
		let one = parse_val!("{ test: [1,5] }");
		let two = parse_val!("{ test: [2,4] }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Greater))
	}

	#[test]
	fn compare_field_on_record_id() {
		// Both are RecordIds, trying to access a field should be Equal (both missing field)
		let idi: Idiom = syn::idiom("city.name").unwrap().into();
		let one = parse_val!("{ city: city:1 }");
		let two = parse_val!("{ city: city:2 }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_field_on_record_id_different_order() {
		// Even with different record IDs, field access should treat them as equal (both missing)
		let idi: Idiom = syn::idiom("city.name").unwrap().into();
		let one = parse_val!("{ city: city:100 }");
		let two = parse_val!("{ city: city:1 }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_field_object_vs_record_id() {
		// Object with field vs RecordId without field
		let idi: Idiom = syn::idiom("city.name").unwrap().into();
		let one = parse_val!("{ city: { name: 'San Francisco' } }");
		let two = parse_val!("{ city: city:1 }");
		let res = one.compare(&two, &idi, false, false);
		// Object has field, RecordId doesn't, so Object > RecordId (Greater)
		assert_eq!(res, Some(Ordering::Greater));
	}

	#[test]
	fn compare_field_record_id_vs_object() {
		// RecordId without field vs Object with field
		let idi: Idiom = syn::idiom("city.name").unwrap().into();
		let one = parse_val!("{ city: city:1 }");
		let two = parse_val!("{ city: { name: 'San Francisco' } }");
		let res = one.compare(&two, &idi, false, false);
		// RecordId doesn't have field, Object does, so RecordId < Object (Less)
		assert_eq!(res, Some(Ordering::Less));
	}

	#[test]
	fn compare_field_object_missing_vs_record_id() {
		// Object without the specific field vs RecordId
		let idi: Idiom = syn::idiom("city.name").unwrap().into();
		let one = parse_val!("{ city: { country: 'USA' } }");
		let two = parse_val!("{ city: city:1 }");
		let res = one.compare(&two, &idi, false, false);
		// Both missing field, should be Equal
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_field_on_string() {
		// Both are Strings, trying to access a field should be Equal (both missing field)
		let idi: Idiom = syn::idiom("value.nested").unwrap().into();
		let one = parse_val!("{ value: 'hello' }");
		let two = parse_val!("{ value: 'world' }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_field_on_number() {
		// Both are Numbers, trying to access a field should be Equal (both missing field)
		let idi: Idiom = syn::idiom("value.nested").unwrap().into();
		let one = parse_val!("{ value: 42 }");
		let two = parse_val!("{ value: 100 }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_field_mixed_types() {
		// String vs Number, both can't access field, should be Equal
		let idi: Idiom = syn::idiom("value.nested").unwrap().into();
		let one = parse_val!("{ value: 'hello' }");
		let two = parse_val!("{ value: 42 }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}

	#[test]
	fn compare_nested_field_on_record_id() {
		// Deeply nested field access on RecordId
		let idi: Idiom = syn::idiom("user.city.name").unwrap().into();
		let one = parse_val!("{ user: user:1 }");
		let two = parse_val!("{ user: user:2 }");
		let res = one.compare(&two, &idi, false, false);
		assert_eq!(res, Some(Ordering::Equal));
	}
}
