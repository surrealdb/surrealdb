use std::borrow::Cow;

use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	/// Synchronous method for getting a field from a `Value`
	pub fn pick(&self, path: &[Part]) -> Self {
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					Part::All => v.values().map(|v| v.pick(path.next())).collect::<Vec<_>>().into(),
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(&idx.to_string()) {
								Some(v) => v.pick(path.next()),
								None => Value::None,
							}
						} else {
							Value::None
						}
					}
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					Part::All => v.iter().map(|v| v.pick(path.next())).collect::<Vec<_>>().into(),
					Part::First => match v.first() {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					Part::Last => match v.last() {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(idx) {
								Some(v) => v.pick(path.next()),
								None => Value::None,
							}
						} else {
							v.iter().map(|v| v.pick(path)).collect::<Vec<_>>().into()
						}
					}
				},
				// Current value at path is a set
				Value::Set(v) => match p {
					Part::All => Value::Set(v.iter().map(|v| v.pick(path.next())).collect()),
					Part::First => match v.first() {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					Part::Last => match v.last() {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.nth(idx) {
								Some(v) => v.pick(path.next()),
								None => Value::None,
							}
						} else {
							Value::Set(v.iter().map(|v| v.pick(path)).collect())
						}
					}
				},
				// Ignore everything else
				_ => Value::None,
			},
			// No more parts so get the value
			None => self.clone(),
		}
	}

	/// Like [`pick`], but returns a [`Cow`] to avoid cloning when the path
	/// resolves by borrowing directly into `self`.
	///
	/// Simple field and index navigation borrows the target value without
	/// allocating. An owned [`Value`] is produced only when the result must
	/// be synthesised (e.g. array/set projection, or a missing path that
	/// yields [`Value::None`]).
	pub fn pick_cow<'a>(&'a self, path: &[Part]) -> Cow<'a, Value> {
		match path.first() {
			Some(p) => match self {
				Value::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v.pick_cow(path.next()),
						None => Cow::Owned(Value::None),
					},
					Part::All => Cow::Owned(
						v.values().map(|v| v.pick(path.next())).collect::<Vec<_>>().into(),
					),
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(&idx.to_string()) {
								Some(v) => v.pick_cow(path.next()),
								None => Cow::Owned(Value::None),
							}
						} else {
							Cow::Owned(Value::None)
						}
					}
				},
				Value::Array(v) => match p {
					Part::All => {
						Cow::Owned(v.iter().map(|v| v.pick(path.next())).collect::<Vec<_>>().into())
					}
					Part::First => match v.first() {
						Some(v) => v.pick_cow(path.next()),
						None => Cow::Owned(Value::None),
					},
					Part::Last => match v.last() {
						Some(v) => v.pick_cow(path.next()),
						None => Cow::Owned(Value::None),
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(idx) {
								Some(v) => v.pick_cow(path.next()),
								None => Cow::Owned(Value::None),
							}
						} else {
							Cow::Owned(v.iter().map(|v| v.pick(path)).collect::<Vec<_>>().into())
						}
					}
				},
				Value::Set(v) => match p {
					Part::All => {
						Cow::Owned(Value::Set(v.iter().map(|v| v.pick(path.next())).collect()))
					}
					Part::First => match v.first() {
						Some(v) => v.pick_cow(path.next()),
						None => Cow::Owned(Value::None),
					},
					Part::Last => match v.last() {
						Some(v) => v.pick_cow(path.next()),
						None => Cow::Owned(Value::None),
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.nth(idx) {
								Some(v) => v.pick_cow(path.next()),
								None => Cow::Owned(Value::None),
							}
						} else {
							Cow::Owned(Value::Set(v.iter().map(|v| v.pick(path)).collect()))
						}
					}
				},
				_ => Cow::Owned(Value::None),
			},
			// Empty path: borrow self directly, no clone needed.
			None => Cow::Borrowed(self),
		}
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn;
	use crate::val::{RecordId, RecordIdKey};

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn pick_none() {
		let idi: Idiom = SqlIdiom::default().into();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(res, val);
	}

	#[test]
	fn pick_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(123));
	}

	#[test]
	fn pick_thing() {
		let idi: Idiom = syn::idiom("test.other").unwrap().into();
		let val = parse_val!("{ test: { other: test:tobie, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(RecordId {
				table: "test".into(),
				key: RecordIdKey::String(Strand::new_static("tobie"))
			})
		);
	}

	#[test]
	fn pick_array() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val = parse_val!("{ test: { something: [123, 456, 789] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(456));
	}

	#[test]
	fn pick_array_thing() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val = parse_val!("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(RecordId {
				table: "test".into(),
				key: RecordIdKey::String(Strand::new_static("jaime"))
			})
		);
	}

	#[test]
	fn pick_array_field() {
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(36));
	}

	#[test]
	fn pick_array_fields() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}

	#[test]
	fn pick_array_fields_flat() {
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}

	// pick_cow tests: verify that simple navigations borrow without cloning.

	#[test]
	fn pick_cow_empty_path_borrows() {
		// An empty path should return Cow::Borrowed pointing into `val`.
		let idi: Idiom = SqlIdiom::default().into();
		let val = parse_val!("{ test: 123 }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, val);
		assert!(matches!(res, Cow::Borrowed(_)), "empty path must borrow, not clone");
	}

	#[test]
	fn pick_cow_simple_field_borrows() {
		// A single-field path on an object should borrow the field value directly.
		let idi: Idiom = syn::idiom("something").unwrap().into();
		let val = parse_val!("{ something: 42 }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, Value::from(42));
		assert!(matches!(res, Cow::Borrowed(_)), "simple field access must borrow");
	}

	#[test]
	fn pick_cow_nested_field_borrows() {
		// Chained field access should borrow end-to-end.
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { something: 123 } }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, Value::from(123));
		assert!(matches!(res, Cow::Borrowed(_)), "nested field access must borrow");
	}

	#[test]
	fn pick_cow_missing_field_is_owned_none() {
		// A missing field synthesises Value::None, so it must be Cow::Owned.
		let idi: Idiom = syn::idiom("missing").unwrap().into();
		let val = parse_val!("{ something: 42 }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, Value::None);
		assert!(matches!(res, Cow::Owned(_)), "missing field must be owned Value::None");
	}

	#[test]
	fn pick_cow_array_index_borrows() {
		// Integer index access on an array should borrow the element.
		let idi: Idiom = syn::idiom("items[1]").unwrap().into();
		let val = parse_val!("{ items: [10, 20, 30] }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, Value::from(20));
		assert!(matches!(res, Cow::Borrowed(_)), "array index access must borrow");
	}

	#[test]
	fn pick_cow_array_first_borrows() {
		// Part::First on an array should borrow the first element.
		let idi: Idiom = syn::idiom("items[0]").unwrap().into();
		let val = parse_val!("{ items: [10, 20, 30] }");
		let res = val.pick_cow(&idi);
		assert_eq!(*res, Value::from(10));
		assert!(matches!(res, Cow::Borrowed(_)), "first element access must borrow");
	}

	#[test]
	fn pick_cow_all_is_owned() {
		// Part::All synthesises a new array, so it must be Cow::Owned.
		let idi: Idiom = syn::idiom("items[*]").unwrap().into();
		let val = parse_val!("{ items: [10, 20, 30] }");
		let res = val.pick_cow(&idi);
		assert!(matches!(res, Cow::Owned(_)), "wildcard projection must be owned");
	}

	#[test]
	fn pick_cow_matches_pick() {
		// pick_cow must return the same value as pick for all cases.
		let cases: Vec<(&str, &str)> = vec![
			("test.something", "{ test: { something: 123 } }"),
			("missing", "{ something: 42 }"),
			("items[1]", "{ items: [10, 20, 30] }"),
			("items[*].x", "{ items: [{ x: 1 }, { x: 2 }] }"),
		];
		for (path_str, val_str) in cases {
			let idi: Idiom = syn::idiom(path_str).unwrap().into();
			let val = parse_val!(val_str);
			let owned = val.pick(&idi);
			let cow = val.pick_cow(&idi);
			assert_eq!(owned, *cow, "pick_cow mismatch for path={path_str}, val={val_str}");
		}
	}
}
