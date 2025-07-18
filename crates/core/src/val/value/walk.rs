use crate::expr::idiom::Idiom;
use crate::expr::part::{Next, Part};
use crate::val::Value;

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
					Part::All => v
						.iter()
						.flat_map(|(field, v)| {
							v._walk(
								path.next(),
								// TODO: null byte validity.
								prev.clone().push(Part::field(field.clone()).unwrap()),
							)
						})
						.collect::<Vec<_>>(),
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(&idx.to_string()) {
								Some(v) => v._walk(path.next(), prev.push(p.clone())),
								None => Value::None._walk(path.next(), prev.push(p.clone())),
							}
						} else {
							vec![]
						}
					}
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::First => match v.first() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Last => match v.last() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(idx) {
								Some(v) => v._walk(path.next(), prev.push(p.clone())),
								None => vec![],
							}
						} else {
							v.iter()
								.enumerate()
								.flat_map(|(i, v)| {
									v._walk(
										path.next(),
										prev.clone().push(Part::index_int(i as i64)),
									)
								})
								.collect::<Vec<_>>()
						}
					}
				},
				// Ignore everything else
				_ => match p {
					Part::Field(_) => Value::None._walk(path.next(), prev.push(p.clone())),
					x => {
						if let Some(_) = x.as_old_index() {
							Value::None._walk(path.next(), prev.push(p.clone()))
						} else {
							vec![]
						}
					}
				},
			},
			// No more parts so get the value
			None => vec![(prev, self.clone())],
		}
	}
}

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::Idiom;
	use crate::sql::SqlValue;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn::Parse;

	#[test]
	fn walk_blank() {
		let idi: Idiom = SqlIdiom::default().into();
		let val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Vec<(Idiom, Value)> = vec![(
			Idiom::default(),
			SqlValue::parse("{ test: { other: null, something: 123 } }").into(),
		)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Vec<(Idiom, Value)> =
			vec![(SqlIdiom::parse("test.something").into(), Value::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi: Idiom = SqlIdiom::parse("test.missing").into();
		let val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let res: Vec<(Idiom, Value)> = vec![(SqlIdiom::parse("test.missing").into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi: Idiom = SqlIdiom::parse("none.something.age").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<(Idiom, Value)> =
			vec![(SqlIdiom::parse("none.something.age").into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi: Idiom = SqlIdiom::parse("none.something.*.age").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<(Idiom, Value)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi: Idiom = SqlIdiom::parse("none.something[0].age").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<(Idiom, Value)> =
			vec![(SqlIdiom::parse("none.something[0].age").into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi: Idiom = SqlIdiom::parse("test.something").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res = vec![(
			SqlIdiom::parse("test.something").into(),
			SqlValue::parse("[{ age: 34 }, { age: 36 }]").into(),
		)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].age").into();
		let val: Value =
			SqlValue::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }").into();
		let res: Vec<(Idiom, Value)> = vec![
			(SqlIdiom::parse("test.something[0].age").into(), Value::from(34)),
			(SqlIdiom::parse("test.something[1].age").into(), Value::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<(Idiom, Value)> = vec![
			(
				SqlIdiom::parse("test.something[0].tags").into(),
				SqlValue::parse("['code', 'databases']").into(),
			),
			(
				SqlIdiom::parse("test.something[1].tags").into(),
				SqlValue::parse("['design', 'operations']").into(),
			),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags[1]").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<(Idiom, Value)> = vec![
			(SqlIdiom::parse("test.something[0].tags[1]").into(), Value::from("databases")),
			(SqlIdiom::parse("test.something[1].tags[1]").into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi: Idiom = SqlIdiom::parse("test.something[*].tags[*]").into();
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<(Idiom, Value)> = vec![
			(SqlIdiom::parse("test.something[0].tags[0]").into(), Value::from("code")),
			(SqlIdiom::parse("test.something[0].tags[1]").into(), Value::from("databases")),
			(SqlIdiom::parse("test.something[1].tags[0]").into(), Value::from("design")),
			(SqlIdiom::parse("test.something[1].tags[1]").into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}
}*/
