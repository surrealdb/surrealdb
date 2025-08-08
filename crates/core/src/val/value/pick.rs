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
					Part::All => {
						v.iter().map(|(_, v)| v.pick(path.next())).collect::<Vec<_>>().into()
					}
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
				// Ignore everything else
				_ => Value::None,
			},
			// No more parts so get the value
			None => self.clone(),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::idiom::Idiom;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn;
	use crate::val::{RecordId, RecordIdKey};

	#[test]
	fn pick_none() {
		let idi: Idiom = SqlIdiom::default().into();
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, val);
	}

	#[test]
	fn pick_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(123));
	}

	#[test]
	fn pick_thing() {
		let idi: Idiom = syn::idiom("test.other").unwrap().into();
		let val = syn::value("{ test: { other: test:tobie, something: 123 } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(RecordId {
				table: String::from("test"),
				key: RecordIdKey::String("tobie".to_owned())
			})
		);
	}

	#[test]
	fn pick_array() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val = syn::value("{ test: { something: [123, 456, 789] } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(456));
	}

	#[test]
	fn pick_array_thing() {
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val = syn::value("{ test: { something: [test:tobie, test:jaime] } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(RecordId {
				table: String::from("test"),
				key: RecordIdKey::String("jaime".to_owned())
			})
		);
	}

	#[test]
	fn pick_array_field() {
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(36));
	}

	#[test]
	fn pick_array_fields() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}

	#[test]
	fn pick_array_fields_flat() {
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res = val.pick(&idi);
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}
}
