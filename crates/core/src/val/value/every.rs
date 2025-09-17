use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::expr::{Expr, Literal};
use crate::val::Value;

impl Value {
	/// Returns a list of idioms for then entries of a possibly nested value.
	///
	/// Exact behavior of this function is dictated by the ArrayBehaviour param
	/// and steps. Steps enables intermediate idioms instead of only the leaf
	/// values. For the changes in behavior with ArrayBehaviour see the docs
	/// for that enum.
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
						// For some reason this is in reverse order.
						for (i, v) in v.iter().enumerate().rev() {
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

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	#[test]
	fn every_empty() {
		let val = syn::value("{}").unwrap();
		let res: Vec<Idiom> = vec![];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Ignore));
	}

	#[test]
	fn every_with_empty_objects_arrays() {
		let val = syn::value("{ test: {}, status: false, something: {age: 45}, tags: []}").unwrap();
		let res: Vec<Idiom> = vec![
			syn::idiom("something.age").unwrap().into(),
			syn::idiom("status").unwrap().into(),
			syn::idiom("tags").unwrap().into(),
			syn::idiom("test").unwrap().into(),
		];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Ignore));
	}

	#[test]
	fn every_without_array_indexes() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> = vec![syn::idiom("test.something").unwrap().into()];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Ignore));
	}

	#[test]
	fn every_including_array_indexes() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
		];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Full));
	}

	#[test]
	fn every_including_intermediary_nodes_without_array_indexes() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> =
			vec![syn::idiom("test").unwrap().into(), syn::idiom("test.something").unwrap().into()];
		assert_eq!(res, val.every(None, true, ArrayBehaviour::Ignore));
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> = vec![
			syn::idiom("test").unwrap().into(),
			syn::idiom("test.something").unwrap().into(),
			syn::idiom("test.something[1]").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[0]").unwrap().into(),
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
		];
		assert_eq!(res, val.every(None, true, ArrayBehaviour::Full));
	}

	#[test]
	fn every_given_one_path_part() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();

		let val =
			val.every(Some(&Idiom::from(syn::idiom("test").unwrap())), true, ArrayBehaviour::Full);
		for v in val.iter() {
			println!("{}", v);
		}

		let res: Vec<Idiom> = vec![
			syn::idiom("test").unwrap().into(),
			syn::idiom("test.something").unwrap().into(),
			syn::idiom("test.something[1]").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[0]").unwrap().into(),
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
		];
		assert_eq!(res, val,);
	}

	#[test]
	fn every_given_two_path_parts() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something").unwrap().into(),
			syn::idiom("test.something[1]").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[0]").unwrap().into(),
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
		];
		assert_eq!(
			res,
			val.every(
				Some(&Idiom::from(syn::idiom("test.something").unwrap())),
				true,
				ArrayBehaviour::Full
			)
		);
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes_ending_all() {
		let val = syn::value(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }",
		).unwrap();
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something").unwrap().into(),
			syn::idiom("test.something[1]").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[0]").unwrap().into(),
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
		];
		assert_eq!(
			res,
			val.every(
				Some(&Idiom::from(syn::idiom("test.something.*").unwrap())),
				true,
				ArrayBehaviour::Full
			)
		);
	}

	#[test]
	fn every_wildcards() {
		let val = syn::value(
			"{ test: { a: { color: 'red' }, b: { color: 'blue' }, c: { color: 'green' } } }",
		)
		.unwrap();

		let res: Vec<Idiom> = vec![
			syn::idiom("test.*.color").unwrap().into(),
			syn::idiom("test.*.color[2]").unwrap().into(),
			syn::idiom("test.*.color[1]").unwrap().into(),
			syn::idiom("test.*.color[0]").unwrap().into(),
		];

		assert_eq!(
			res,
			val.every(
				Some(&Idiom::from(syn::idiom("test.*.color").unwrap())),
				true,
				ArrayBehaviour::Full
			)
		);
	}
}
