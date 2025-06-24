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
					Part::Graph(g) => {
						let entry = v.entry(g.to_raw()).or_insert_with(Value::empty_object);
						if !entry.is_nullish() {
							entry.put(path.next(), val);
						}
					}
					Part::Field(f) => {
						let entry = v.entry(f.to_raw()).or_insert_with(Value::empty_object);
						if !entry.is_nullish() {
							entry.put(path.next(), val);
						}
					}
					Part::All => {
						let path = path.next();
						v.iter_mut().for_each(|(_, v)| v.put(path, val.clone()));
					}
					x => {
						if let Some(idx) = x.as_old_index() {
							let entry =
								v.entry(idx.to_string()).or_insert_with(Value::empty_object);
							if !entry.is_nullish() {
								entry.put(path.next(), val);
							}
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

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::sql::SqlValue;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn put_none() {
		let idi: Idiom = SqlIdiom::default().into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("999").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_empty() {
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val = Value::None;
		let res: Value = SqlValue::parse("{ test: 999 }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_blank() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let mut val = Value::None;
		let res: Value = SqlValue::parse("{ test: { something: 999 } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_reput() {
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: 999 }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_basic() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 999 } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_allow() {
		let idi: Idiom = SqlIdiom::parse("test.something.allow").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null } }").into();
		let res: Value =
			SqlValue::parse("{ test: { other: null, something: { allow: 999 } } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_wrong() {
		let idi: Idiom = SqlIdiom::parse("test.something.wrong").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_other() {
		let idi: Idiom = SqlIdiom::parse("test.other.something").into();
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Value =
			SqlValue::parse("{ test: { other: { something: 999 }, something: 123 } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array() {
		let idi: Idiom = SqlIdiom::parse("test.something[1]").into();
		let mut val: Value = SqlValue::parse("{ test: { something: [123, 456, 789] } }").into();
		let res: Value = SqlValue::parse("{ test: { something: [123, 999, 789] } }").into();
		val.put(&idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_field() {
		let idi: Idiom = SqlIdiom::parse("test.something[1].age").into();
		let mut val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }").into();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].age").into();
		let mut val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }").into();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn put_array_fields_flat() {
		let idi: Idiom = SqlIdiom::parse("test.something.age").into();
		let mut val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Value =
			SqlValue::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }").into();
		val.put(&idi, Value::from(21));
		assert_eq!(res, val);
	}
}*/
