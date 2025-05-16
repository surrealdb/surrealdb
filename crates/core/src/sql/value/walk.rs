use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub fn walk(&self, path: &[Part]) -> Vec<(Idiom, Self)> {
		self._walk(path, Idiom::default())
	}
	fn _walk(&self, path: &[Part], prev: Idiom) -> Vec<(Idiom, Self)> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				SqlValue::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => SqlValue::None._walk(path.next(), prev.push(p.clone())),
					},
					Part::Index(i) => match v.get(&i.to_string()) {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => SqlValue::None._walk(path.next(), prev.push(p.clone())),
					},
					Part::All => v
						.iter()
						.flat_map(|(field, v)| {
							v._walk(
								path.next(),
								prev.clone().push(Part::Field(field.to_owned().into())),
							)
						})
						.collect::<Vec<_>>(),
					_ => vec![],
				},
				// Current path part is an array
				SqlValue::Array(v) => match p {
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
					Part::Field(_) => SqlValue::None._walk(path.next(), prev.push(p.clone())),
					Part::Index(_) => SqlValue::None._walk(path.next(), prev.push(p.clone())),
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
	use crate::syn::Parse;

	#[test]
	fn walk_blank() {
		let idi = Idiom::default();
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res =
			vec![(Idiom::default(), SqlValue::parse("{ test: { other: null, something: 123 } }"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi = Idiom::parse("test.something");
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = vec![(Idiom::parse("test.something"), SqlValue::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi = Idiom::parse("test.missing");
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = vec![(Idiom::parse("test.missing"), SqlValue::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi = Idiom::parse("none.something.age");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![(Idiom::parse("none.something.age"), SqlValue::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi = Idiom::parse("none.something.*.age");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, SqlValue)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi = Idiom::parse("none.something[0].age");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![(Idiom::parse("none.something[0].age"), SqlValue::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi = Idiom::parse("test.something");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res =
			vec![(Idiom::parse("test.something"), SqlValue::parse("[{ age: 34 }, { age: 36 }]"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi = Idiom::parse("test.something[*].age");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].age"), SqlValue::from(34)),
			(Idiom::parse("test.something[1].age"), SqlValue::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi = Idiom::parse("test.something[*].tags");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags"), SqlValue::parse("['code', 'databases']")),
			(Idiom::parse("test.something[1].tags"), SqlValue::parse("['design', 'operations']")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi = Idiom::parse("test.something[*].tags[1]");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags[1]"), SqlValue::from("databases")),
			(Idiom::parse("test.something[1].tags[1]"), SqlValue::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi = Idiom::parse("test.something[*].tags[*]");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			(Idiom::parse("test.something[0].tags[0]"), SqlValue::from("code")),
			(Idiom::parse("test.something[0].tags[1]"), SqlValue::from("databases")),
			(Idiom::parse("test.something[1].tags[0]"), SqlValue::from("design")),
			(Idiom::parse("test.something[1].tags[1]"), SqlValue::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}
}
