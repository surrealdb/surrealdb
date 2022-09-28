use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn walk(&self, path: &[Part]) -> Vec<(Idiom, Self)> {
		self._walk(path, Idiom::default())
	}
	fn _walk(&self, path: &[Part], prev: Idiom) -> Vec<(Idiom, Self)> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => Value::None._walk(path.next(), prev.push(p.clone())),
					},
					Part::All => self._walk(path.next(), prev.push(p.clone())),
					_ => vec![],
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => v
						.iter()
						.enumerate()
						.flat_map(|(i, v)| v._walk(path.next(), prev.clone().push(Part::from(i))))
						.collect::<Vec<_>>(),
					Part::First => match v.first() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Last => match v.last() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					_ => v
						.iter()
						.enumerate()
						.flat_map(|(i, v)| v._walk(path.next(), prev.clone().push(Part::from(i))))
						.collect::<Vec<_>>(),
				},
				// Ignore everything else
				_ => match p {
					Part::Field(_) => Value::None._walk(path.next(), prev.push(p.clone())),
					Part::Index(_) => Value::None._walk(path.next(), prev.push(p.clone())),
					_ => vec![],
				},
			},
			// No more parts so get the value
			None => vec![(prev, self.clone())],
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::idiom::Idiom;
	use crate::sql::test::Parse;

	#[test]
	fn walk_blank() {
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res =
			vec![(Idiom::default(), Value::parse("{ test: { other: null, something: 123 } }"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = vec![(Idiom::parse("test.something"), Value::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi = Idiom::parse("test.missing");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = vec![(Idiom::parse("test.missing"), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi = Idiom::parse("none.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![(Idiom::parse("none.something.age"), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi = Idiom::parse("none.something.*.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi = Idiom::parse("none.something[0].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![(Idiom::parse("none.something[0].age"), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res =
			vec![(Idiom::parse("test.something"), Value::parse("[{ age: 34 }, { age: 36 }]"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].age"), Value::from(34)),
			(Idiom::parse("test.something[1].age"), Value::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi = Idiom::parse("test.something[*].tags");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags"), Value::parse("['code', 'databases']")),
			(Idiom::parse("test.something[1].tags"), Value::parse("['design', 'operations']")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi = Idiom::parse("test.something[*].tags[1]");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags[1]"), Value::from("databases")),
			(Idiom::parse("test.something[1].tags[1]"), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi = Idiom::parse("test.something[*].tags[*]");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags[0]"), Value::from("code")),
			(Idiom::parse("test.something[0].tags[1]"), Value::from("databases")),
			(Idiom::parse("test.something[1].tags[0]"), Value::from("design")),
			(Idiom::parse("test.something[1].tags[1]"), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}
}
