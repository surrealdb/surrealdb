use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub(crate) fn each(&self, path: &[Part]) -> Vec<Idiom> {
		self._each(path, Idiom::default())
	}
	fn _each(&self, path: &[Part], prev: Idiom) -> Vec<Idiom> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				SqlValue::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v._each(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::All => v
						.iter()
						.flat_map(|(field, v)| {
							v._each(
								path.next(),
								prev.clone().push(Part::Field(field.to_owned().into())),
							)
						})
						.collect::<Vec<_>>(),
					_ => vec![],
				},
				// Current path part is an array
				SqlValue::Array(v) => match p {
					Part::All => v
						.iter()
						.enumerate()
						.flat_map(|(i, v)| v._each(path.next(), prev.clone().push(Part::from(i))))
						.collect::<Vec<_>>(),
					Part::First => match v.first() {
						Some(v) => v._each(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Last => match v.last() {
						Some(v) => v._each(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v._each(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					_ => v
						.iter()
						.enumerate()
						.flat_map(|(i, v)| v._each(path.next(), prev.clone().push(Part::from(i))))
						.collect::<Vec<_>>(),
				},
				// Ignore everything else
				_ => vec![],
			},
			// No more parts so get the value
			None => vec![prev],
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[test]
	fn each_none() {
		let idi = Idiom::default();
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = vec![Idiom::default()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::parse("{ test: { other: null, something: 123 } }"));
	}

	#[test]
	fn each_basic() {
		let idi = Idiom::parse("test.something");
		let val = SqlValue::parse("{ test: { other: null, something: 123 } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::from(123));
	}

	#[test]
	fn each_array() {
		let idi = Idiom::parse("test.something");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::parse("[{ age: 34 }, { age: 36 }]"));
	}

	#[test]
	fn each_array_field() {
		let idi = Idiom::parse("test.something[*].age");
		let val = SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res =
			vec![Idiom::parse("test.something[0].age"), Idiom::parse("test.something[1].age")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::from(34));
		assert_eq!(val.pick(&res[1]), SqlValue::from(36));
	}

	#[test]
	fn each_array_field_embedded() {
		let idi = Idiom::parse("test.something[*].tags");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res =
			vec![Idiom::parse("test.something[0].tags"), Idiom::parse("test.something[1].tags")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::parse("['code', 'databases']"));
		assert_eq!(val.pick(&res[1]), SqlValue::parse("['design', 'operations']"));
	}

	#[test]
	fn each_array_field_embedded_index() {
		let idi = Idiom::parse("test.something[*].tags[1]");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[1].tags[1]"),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::from("databases"));
		assert_eq!(val.pick(&res[1]), SqlValue::from("operations"));
	}

	#[test]
	fn each_array_field_embedded_index_all() {
		let idi = Idiom::parse("test.something[*].tags[*]");
		let val = SqlValue::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something[0].tags[0]"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[1].tags[1]"),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), SqlValue::from("code"));
		assert_eq!(val.pick(&res[1]), SqlValue::from("databases"));
		assert_eq!(val.pick(&res[2]), SqlValue::from("design"));
		assert_eq!(val.pick(&res[3]), SqlValue::from("operations"));
	}

	#[test]
	fn each_wildcards() {
		let val = SqlValue::parse(
			"{ test: { a: { color: 'red' }, b: { color: 'blue' }, c: { color: 'green' } } }",
		);

		let res = vec![
			Idiom::parse("test.a.color"),
			Idiom::parse("test.b.color"),
			Idiom::parse("test.c.color"),
		];

		assert_eq!(res, val.each(&Idiom::parse("test.*.color")));
	}
}
