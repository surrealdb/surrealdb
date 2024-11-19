use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;

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
					Part::Index(i) => match v.get(&i.to_string()) {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					Part::All => v.iter()
						.map(|(_, v)| v.pick(path.next()))
						.collect::<Vec<_>>()
						.into(),
					_ => Value::None,
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
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v.pick(path.next()),
						None => Value::None,
					},
					_ => v.iter().map(|v| v.pick(path)).collect::<Vec<_>>().into(),
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
	use crate::sql::id::Id;
	use crate::sql::idiom::Idiom;
	use crate::sql::thing::Thing;
	use crate::syn::Parse;

	#[test]
	fn pick_none() {
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(res, val);
	}

	#[test]
	fn pick_basic() {
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(123));
	}

	#[test]
	fn pick_thing() {
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: Id::from("tobie")
			})
		);
	}

	#[test]
	fn pick_array() {
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(456));
	}

	#[test]
	fn pick_array_thing() {
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.pick(&idi);
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: Id::from("jaime")
			})
		);
	}

	#[test]
	fn pick_array_field() {
		let idi = Idiom::parse("test.something[1].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(36));
	}

	#[test]
	fn pick_array_fields() {
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[test]
	fn pick_array_fields_flat() {
		let idi = Idiom::parse("test.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.pick(&idi);
		assert_eq!(res, Value::from(vec![34, 36]));
	}
}
