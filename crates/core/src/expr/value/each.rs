use crate::expr::idiom::Idiom;
use crate::expr::part::Next;
use crate::expr::part::Part;
use crate::expr::value::Value;

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
	use crate::sql::Idiom as SqlIdiom;
	use crate::{sql::SqlValue, syn::Parse};

	#[test]
	fn each_none() {
		let idi: Idiom = Idiom::default();
		let val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Vec<Idiom> = vec![Idiom::default()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::parse("{ test: { other: null, something: 123 } }"));
	}

	#[test]
	fn each_basic() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Vec<Idiom> = vec![SqlIdiom::parse("test.something").into()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(123));
	}

	#[test]
	fn each_array() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<Idiom> = vec![SqlIdiom::parse("test.something").into()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::parse("[{ age: 34 }, { age: 36 }]"));
	}

	#[test]
	fn each_array_field() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].age").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[1].age").into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(34));
		assert_eq!(val.pick(&res[1]), Value::from(36));
	}

	#[test]
	fn each_array_field_embedded() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(SqlValue::parse("['code', 'databases']")));
		assert_eq!(val.pick(&res[1]), Value::from(SqlValue::parse("['design', 'operations']")));
	}

	#[test]
	fn each_array_field_embedded_index() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags[1]").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("databases"));
		assert_eq!(val.pick(&res[1]), Value::from("operations"));
	}

	#[test]
	fn each_array_field_embedded_index_all() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags[*]").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something[0].tags[0]").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("code"));
		assert_eq!(val.pick(&res[1]), Value::from("databases"));
		assert_eq!(val.pick(&res[2]), Value::from("design"));
		assert_eq!(val.pick(&res[3]), Value::from("operations"));
	}

	#[test]
	fn each_wildcards() {
		let val: Value = SqlValue::parse(
			"{ test: { a: { color: 'red' }, b: { color: 'blue' }, c: { color: 'green' } } }",
		)
		.into();

		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.a.color").into(),
			SqlIdiom::parse("test.b.color").into(),
			SqlIdiom::parse("test.c.color").into(),
		];

		assert_eq!(res, val.each(&Idiom::from(SqlIdiom::parse("test.*.color"))));
	}
}
