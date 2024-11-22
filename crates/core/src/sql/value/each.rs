use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn each(&self, path: &[Part]) -> Vec<Idiom> {
		self._each(path, Idiom::default())
	}
	fn _each(&self, path: &[Part], prev: Idiom) -> Vec<Idiom> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
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
				Value::Array(v) => match p {
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
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = vec![Idiom::default()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::parse("{ test: { other: null, something: 123 } }"));
	}

	#[test]
	fn each_basic() {
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(123));
	}

	#[test]
	fn each_array() {
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::parse("[{ age: 34 }, { age: 36 }]"));
	}

	#[test]
	fn each_array_field() {
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res =
			vec![Idiom::parse("test.something[0].age"), Idiom::parse("test.something[1].age")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(34));
		assert_eq!(val.pick(&res[1]), Value::from(36));
	}

	#[test]
	fn each_array_field_embedded() {
		let idi = Idiom::parse("test.something[*].tags");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res =
			vec![Idiom::parse("test.something[0].tags"), Idiom::parse("test.something[1].tags")];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::parse("['code', 'databases']"));
		assert_eq!(val.pick(&res[1]), Value::parse("['design', 'operations']"));
	}

	#[test]
	fn each_array_field_embedded_index() {
		let idi = Idiom::parse("test.something[*].tags[1]");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[1].tags[1]"),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("databases"));
		assert_eq!(val.pick(&res[1]), Value::from("operations"));
	}

	#[test]
	fn each_array_field_embedded_index_all() {
		let idi = Idiom::parse("test.something[*].tags[*]");
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something[0].tags[0]"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[1].tags[1]"),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("code"));
		assert_eq!(val.pick(&res[1]), Value::from("databases"));
		assert_eq!(val.pick(&res[2]), Value::from("design"));
		assert_eq!(val.pick(&res[3]), Value::from("operations"));
	}
}
