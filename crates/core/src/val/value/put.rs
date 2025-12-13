use surrealdb_types::ToSql;

use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	/// Synchronous method for setting a field on a `Value`
	pub fn put(&mut self, path: &[Part], val: Value) {
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Lookup(lookup) => {
						let entry = v.entry(lookup.to_sql()).or_insert_with(Value::empty_object);
						if !entry.is_nullish() {
							entry.put(path.next(), val);
						} else {
							let mut obj = Value::empty_object();
							obj.put(path.next(), val);
							v.insert(lookup.to_sql(), obj);
						}
					}
					Part::Field(f) => {
						let entry = v.entry(f.clone()).or_insert_with(Value::empty_object);
						entry.put(path.next(), val);
					}
					Part::All => {
						let path = path.next();
						*v = v
							.iter()
							.map(|(k, v)| {
								let mut v = v.clone();
								v.put(path, val.clone());
								(k.clone(), v)
							})
							.collect();
					}
					x => {
						if let Some(idx) = x.as_old_index() {
							let entry =
								v.entry(idx.to_string()).or_insert_with(Value::empty_object);
							entry.put(path.next(), val);
						}
					}
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();
						v.iter_mut().for_each(|v| v.put(path, val.clone()));
					}
					Part::First => {
						if let Some(v) = v.first_mut() {
							v.put(path.next(), val)
						}
					}
					Part::Last => {
						if let Some(v) = v.last_mut() {
							v.put(path.next(), val)
						}
					}
					x => {
						if let Some(idx) = x.as_old_index() {
							if let Some(v) = v.get_mut(idx) {
								v.put(path.next(), val)
							}
						} else {
							v.iter_mut().for_each(|v| v.put(path, val.clone()));
						}
					}
				},
				// Current value at path is empty
				Value::Null => {
					*self = Value::empty_object();
					self.put(path, val)
				}
				// Current value at path is empty
				Value::None => {
					*self = Value::empty_object();
					self.put(path, val)
				}
				// Ignore everything else
				_ => (),
			},
			// No more parts so put the value
			None => {
				*self = val;
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn put_none() {
		let idi: Idiom = Idiom::default();
		let mut val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Value = parse_val!("999");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_empty() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = Value::None;
		let res: Value = parse_val!("{ test: 999 }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_blank() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = Value::None;
		let res: Value = parse_val!("{ test: { something: 999 } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_reput() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Value = parse_val!("{ test: 999 }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Value = parse_val!("{ test: { other: null, something: 999 } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_allow() {
		let idi: Idiom = syn::idiom("test.something.allow").unwrap().into();
		let mut val: Value = parse_val!("{ test: { other: null } }");
		let res: Value = parse_val!("{ test: { other: null, something: { allow: 999 } } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_wrong() {
		let idi: Idiom = syn::idiom("test.something.wrong").unwrap().into();
		let mut val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Value = parse_val!("{ test: { other: null, something: 123 } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_other() {
		let idi: Idiom = syn::idiom("test.other.something").unwrap().into();
		let mut val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Value = parse_val!("{ test: { other: { something: 999 }, something: 123 } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let mut val: Value = parse_val!("{ test: { something: [123, 456, 789] } }");
		let res: Value = parse_val!("{ test: { something: [123, 999, 789] } }");
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_field() {
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let mut val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let mut val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Value = parse_val!("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields_flat() {
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let mut val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Value = parse_val!("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}
}
