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
						if x.as_old_index().is_some() {
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

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::Idiom;
	use crate::syn;

	#[test]
	fn walk_blank() {
		let idi: Idiom = Default::default();
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Vec<(Idiom, Value)> = vec![(
			Idiom::default(),
			syn::value("{ test: { other: null, something: 123 } }").unwrap(),
		)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test.something").unwrap().into(), Value::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi: Idiom = syn::idiom("test.missing").unwrap().into();
		let val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test.missing").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi: Idiom = syn::idiom("none.something.age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something.age").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi: Idiom = syn::idiom("none.something.*.age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Vec<(Idiom, Value)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi: Idiom = syn::idiom("none.something[0].age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something[0].age").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res = vec![(
			syn::idiom("test.something").unwrap().into(),
			syn::value("[{ age: 34 }, { age: 36 }]").unwrap(),
		)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val = syn::value("{ test: { something: [{ age: 34 }, { age: 36 }] } }").unwrap();
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].age").unwrap().into(), Value::from(34)),
			(syn::idiom("test.something[1].age").unwrap().into(), Value::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi: Idiom = syn::idiom("test.something[*].tags").unwrap().into();
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<(Idiom, Value)> = vec![
			(
				syn::idiom("test.something[0].tags").unwrap().into(),
				syn::value("['code', 'databases']").unwrap(),
			),
			(
				syn::idiom("test.something[1].tags").unwrap().into(),
				syn::value("['design', 'operations']").unwrap(),
			),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi: Idiom = syn::idiom("test.something[*].tags[1]").unwrap().into();
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[1]").unwrap().into(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[1]").unwrap().into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi: Idiom = syn::idiom("test.something[*].tags[*]").unwrap().into();
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[0]").unwrap().into(), Value::from("code")),
			(syn::idiom("test.something[0].tags[1]").unwrap().into(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[0]").unwrap().into(), Value::from("design")),
			(syn::idiom("test.something[1].tags[1]").unwrap().into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}
}
