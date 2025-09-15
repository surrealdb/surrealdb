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
					Part::Lookup(g) => {
						let entry = v.entry(g.to_raw()).or_insert_with(Value::empty_object);
						if !entry.is_nullish() {
							entry.put(path.next(), val);
						} else {
							let mut obj = Value::empty_object();
							obj.put(path.next(), val);
							v.insert(g.to_raw(), obj);
						}
					}
					Part::Field(f) => {
						let entry = v.entry(f.to_raw_string()).or_insert_with(Value::empty_object);
						entry.put(path.next(), val);
					}
					Part::All => {
						let path = path.next();
						v.iter_mut().for_each(|(_, v)| v.put(path, val.clone()));
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

	#[tokio::test]
	async fn put_none() {
		let idi: Idiom = Idiom::default();
		let mut val: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Value = syn::value("999").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_empty() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = Value::None;
		let res: Value = syn::value("{ test: 999 }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_blank() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = Value::None;
		let res: Value = syn::value("{ test: { something: 999 } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_reput() {
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Value = syn::value("{ test: 999 }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Value = syn::value("{ test: { other: null, something: 999 } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_allow() {
		let idi: Idiom = syn::idiom("test.something.allow").unwrap().into();
		let mut val: Value = syn::value("{ test: { other: null } }").unwrap();
		let res: Value =
			syn::value("{ test: { other: null, something: { allow: 999 } } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_wrong() {
		let idi: Idiom = syn::idiom("test.something.wrong").unwrap().into();
		let mut val: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_other() {
		let idi: Idiom = syn::idiom("test.other.something").unwrap().into();
		let mut val: Value = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Value =
			syn::value("{ test: { other: { something: 999 }, something: 123 } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let mut val: Value = syn::value("{ test: { something: [123, 456, 789] } }").unwrap();
		let res: Value = syn::value("{ test: { something: [123, 999, 789] } }").unwrap();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_field() {
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let mut val: Value =
			syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Value = syn::value("{ test: { something: [{ age: 34 }, { age: 21 }] } }").unwrap();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let mut val: Value =
			syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Value = syn::value("{ test: { something: [{ age: 21 }, { age: 21 }] } }").unwrap();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields_flat() {
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let mut val: Value =
			syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Value = syn::value("{ test: { something: [{ age: 21 }, { age: 21 }] } }").unwrap();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}
}
