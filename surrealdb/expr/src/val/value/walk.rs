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
							v._walk(path.next(), prev.clone().push(Part::Field(field.clone())))
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
				// Current path part is a set
				Value::Set(v) => match p {
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
							match v.nth(idx) {
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

	/// Visits every value [`Value::walk`] would return, in the same order, by
	/// reference.
	///
	/// `walk` answers with owned values and the paths that name them, so a
	/// caller that only measures what it finds deep-clones every match — a
	/// whole array for an array field — and drops it again. This is the same
	/// traversal with the path and the clone left out, and the two must stay
	/// in step: a measurement taken here has to be a measurement of what a
	/// caller reading `walk` would see.
	pub fn walk_ref(&self, path: &[Part], visit: &mut impl FnMut(&Self)) {
		let Some(p) = path.first() else {
			// No more parts so visit the value
			visit(self);
			return;
		};
		match self {
			// Current path part is an object
			Value::Object(v) => match p {
				Part::Field(f) => match v.get(f as &str) {
					Some(v) => v.walk_ref(path.next(), visit),
					None => Value::None.walk_ref(path.next(), visit),
				},
				Part::All => {
					for (_, v) in v.iter() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => {
					if let Some(idx) = x.as_old_index() {
						match v.get(&idx.to_string()) {
							Some(v) => v.walk_ref(path.next(), visit),
							None => Value::None.walk_ref(path.next(), visit),
						}
					}
				}
			},
			// Current path part is an array
			Value::Array(v) => match p {
				Part::First => {
					if let Some(v) = v.first() {
						v.walk_ref(path.next(), visit);
					}
				}
				Part::Last => {
					if let Some(v) = v.last() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.get(idx) {
							v.walk_ref(path.next(), visit);
						}
					}
					None => {
						for v in v.iter() {
							v.walk_ref(path.next(), visit);
						}
					}
				},
			},
			// Current path part is a set
			Value::Set(v) => match p {
				Part::First => {
					if let Some(v) = v.first() {
						v.walk_ref(path.next(), visit);
					}
				}
				Part::Last => {
					if let Some(v) = v.last() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.nth(idx) {
							v.walk_ref(path.next(), visit);
						}
					}
					None => {
						for v in v.iter() {
							v.walk_ref(path.next(), visit);
						}
					}
				},
			},
			// Ignore everything else
			_ => match p {
				Part::Field(_) => Value::None.walk_ref(path.next(), visit),
				x => {
					if x.as_old_index().is_some() {
						Value::None.walk_ref(path.next(), visit);
					}
				}
			},
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn walk_blank() {
		let idi: Idiom = Default::default();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> =
			vec![(Idiom::default(), parse_val!("{ test: { other: null, something: 123 } }"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test.something").unwrap().into(), Value::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi: Idiom = syn::idiom("test.missing").unwrap().into();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test.missing").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi: Idiom = syn::idiom("none.something.age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something.age").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi: Idiom = syn::idiom("none.something.*.age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi: Idiom = syn::idiom("none.something[0].age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something[0].age").unwrap().into(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = vec![(
			syn::idiom("test.something").unwrap().into(),
			parse_val!("[{ age: 34 }, { age: 36 }]"),
		)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].age").unwrap().into(), Value::from(34)),
			(syn::idiom("test.something[1].age").unwrap().into(), Value::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi: Idiom = syn::idiom("test.something[*].tags").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(
				syn::idiom("test.something[0].tags").unwrap().into(),
				parse_val!("['code', 'databases']"),
			),
			(
				syn::idiom("test.something[1].tags").unwrap().into(),
				parse_val!("['design', 'operations']"),
			),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi: Idiom = syn::idiom("test.something[*].tags[1]").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[1]").unwrap().into(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[1]").unwrap().into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_set_field_all() {
		let idi: Idiom = syn::idiom("test.*").unwrap().into();
		let val = parse_val!("{ test: { ' hello ', } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test[0]").unwrap().into(), Value::from(" hello "))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi: Idiom = syn::idiom("test.something[*].tags[*]").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[0]").unwrap().into(), Value::from("code")),
			(syn::idiom("test.something[0].tags[1]").unwrap().into(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[0]").unwrap().into(), Value::from("design")),
			(syn::idiom("test.something[1].tags[1]").unwrap().into(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	/// The borrowing walk is the owning one with the clone left out, so it must
	/// visit exactly the values `walk` returns, in the same order. A caller
	/// measuring through it is measuring what a caller reading `walk` sees.
	#[test]
	fn walk_ref_visits_what_walk_returns() {
		let val = parse_val!(
			"{ test: { other: null, something: [{ age: 34, tags: ['code', 'databases'] }, \
			 { age: 36, tags: [] }] }, tags: ['a', 'b', 'c'], empty: [], set: { 'x', 'y', }, n: 1 }"
		);
		for path in [
			"test",
			"test.something",
			"test.missing",
			"test.*",
			"test.something[*].age",
			"test.something[0].tags[1]",
			"test.something[*].tags[*]",
			"tags",
			"tags[*]",
			"tags[1]",
			"tags[5]",
			"empty",
			"empty[*]",
			"empty[0]",
			"set",
			"set[*]",
			"set[0]",
			"n",
			"n.missing",
			"missing.deep[*]",
		] {
			let idi: Idiom = syn::idiom(path).unwrap().into();
			let mut visited = Vec::new();
			val.walk_ref(&idi, &mut |v| visited.push(v.clone()));
			let walked: Vec<Value> = val.walk(&idi).into_iter().map(|(_, v)| v).collect();
			assert_eq!(visited, walked, "the two walks must agree on `{path}`");
		}
	}
}
