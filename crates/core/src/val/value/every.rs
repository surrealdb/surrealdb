use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::expr::{Expr, Literal};
use crate::val::Value;

impl Value {
	/// Returns a list of idioms for then entries of a possibly nested value.
	///
	/// Exact behavior of this function is dictated by the ArrayBehaviour param and steps.
	/// Steps enables intermediate idioms instead of only the leaf values.
	/// For the changes in behavior with ArrayBehaviour see the docs for that enum.
	pub(crate) fn every(
		&self,
		path: Option<&[Part]>,
		steps: bool,
		behavior: ArrayBehaviour,
	) -> Vec<Idiom> {
		let mut accum = if let Some(x) = path {
			let mut res = x.to_vec();
			while res.ends_with(&[Part::All]) {
				res.pop();
			}
			res
		} else {
			Vec::new()
		};

		let mut build = Vec::new();
		match path {
			Some(path) => self.pick(path)._every(steps, behavior, &mut accum, &mut build),
			None => self._every(steps, behavior, &mut accum, &mut build),
		}
		build
	}

	/// Recursive version of public fn every.
	fn _every(
		&self,
		steps: bool,
		behavior: ArrayBehaviour,
		accum: &mut Vec<Part>,
		build: &mut Vec<Idiom>,
	) {
		match self {
			// Current path part is an object and is not empty
			Value::Object(v) => {
				if (steps || v.is_empty()) && !accum.is_empty() {
					build.push(Idiom(accum.clone()))
				}

				for (k, v) in v.0.iter() {
					// TODO: null byte validity.
					accum.push(Part::field(k.clone()).unwrap());
					v._every(steps, behavior, accum, build);
					accum.pop();
				}
			}
			// Current path part is an array and is not empty
			Value::Array(v) => {
				if !accum.is_empty() {
					build.push(Idiom(accum.clone()))
				}

				// Check if we should log individual array items
				match behavior {
					// Let's log all individual array items
					ArrayBehaviour::Full => {
						for (i, v) in v.iter().enumerate() {
							accum.push(Part::Value(Expr::Literal(Literal::Integer(i as i64))));
							v._every(steps, behavior, accum, build);
							accum.pop();
						}
					}
					// Let's skip this array's values entirely
					ArrayBehaviour::Ignore => {}
				}
			}
			// Process every other path
			_ => {
				if !accum.is_empty() {
					build.push(Idiom(accum.clone()))
				}
			}
		}
	}
}

// Assuming a value like: { foo: [{ bar: 123 }] }
#[derive(Clone, Copy, Debug)]
pub enum ArrayBehaviour {
	// Do not process this array at all
	// [foo]
	Ignore,
	// Give back all nested paths and all indexes of the array
	// [foo, foo[0], foo[0].bar]
	Full,
}

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::{sql::SqlValue, syn::Parse};

	#[test]
	fn every_empty() {
		let val: Value = SqlValue::parse("{}").into();
		let res: Vec<Idiom> = vec![];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_with_empty_objects_arrays() {
		let val: Value =
			SqlValue::parse("{ test: {}, status: false, something: {age: 45}, tags: []}").into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("something.age").into(),
			SqlIdiom::parse("status").into(),
			SqlIdiom::parse("tags").into(),
			SqlIdiom::parse("test").into(),
		];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_without_array_indexes() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![SqlIdiom::parse("test.something").into()];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_recursive_without_array_indexes() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
		];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Nested));
	}

	#[test]
	fn every_including_array_indexes() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[0].tags[0]").into(),
		];
		assert_eq!(res, val.every(None, false, true));
	}

	#[test]
	fn every_including_intermediary_nodes_without_array_indexes() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> =
			vec![SqlIdiom::parse("test").into(), SqlIdiom::parse("test.something").into()];
		assert_eq!(res, val.every(None, true, false));
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test").into(),
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1]").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[0]").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[0].tags[0]").into(),
		];
		assert_eq!(res, val.every(None, true, true));
	}

	#[test]
	fn every_given_one_path_part() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test").into(),
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1]").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[0]").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[0].tags[0]").into(),
		];
		assert_eq!(res, val.every(Some(&Idiom::from(SqlIdiom::parse("test"))), true, true));
	}

	#[test]
	fn every_given_two_path_parts() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1]").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[0]").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[0].tags[0]").into(),
		];
		assert_eq!(
			res,
			val.every(Some(&Idiom::from(SqlIdiom::parse("test.something"))), true, true)
		);
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes_ending_all() {
		let val: Value = SqlValue::parse(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).into();
		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.something").into(),
			SqlIdiom::parse("test.something[1]").into(),
			SqlIdiom::parse("test.something[1].age").into(),
			SqlIdiom::parse("test.something[1].tags").into(),
			SqlIdiom::parse("test.something[1].tags[1]").into(),
			SqlIdiom::parse("test.something[1].tags[0]").into(),
			SqlIdiom::parse("test.something[0]").into(),
			SqlIdiom::parse("test.something[0].age").into(),
			SqlIdiom::parse("test.something[0].tags").into(),
			SqlIdiom::parse("test.something[0].tags[1]").into(),
			SqlIdiom::parse("test.something[0].tags[0]").into(),
		];
		assert_eq!(
			res,
			val.every(Some(&Idiom::from(SqlIdiom::parse("test.something.*"))), true, true)
		);
	}

	#[test]
	fn every_wildcards() {
		let val: Value = SqlValue::parse(
			"{ test: { a: { color: 'red' }, b: { color: 'blue' }, c: { color: 'green' } } }",
		)
		.into();

		let res: Vec<Idiom> = vec![
			SqlIdiom::parse("test.*.color").into(),
			SqlIdiom::parse("test.*.color[2]").into(),
			SqlIdiom::parse("test.*.color[1]").into(),
			SqlIdiom::parse("test.*.color[0]").into(),
		];

		assert_eq!(res, val.every(Some(&Idiom::from(SqlIdiom::parse("test.*.color"))), true, true));
	}
}*/
