use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	pub(crate) fn every(
		&self,
		path: Option<&[Part]>,
		steps: bool,
		arrays: impl Into<ArrayBehaviour>,
	) -> Vec<Idiom> {
		match path {
			Some(path) => self.pick(path)._every(steps, arrays.into(), Idiom::from(path)),
			None => self._every(steps, arrays.into(), Idiom::default()),
		}
	}

	fn _every(&self, steps: bool, arrays: ArrayBehaviour, mut prev: Idiom) -> Vec<Idiom> {
		match self {
			// Current path part is an object and is not empty
			Value::Object(v) if !v.is_empty() => {
				// Remove any trailing * path parts
				prev.remove_trailing_all();
				// Check if we should log intermediary nodes
				match steps {
					// Let's log all intermediary nodes
					true if !prev.is_empty() => Some(prev.clone())
						.into_iter()
						.chain(v.iter().flat_map(|(k, v)| {
							let p = Part::from(k.to_owned());
							v._every(steps, arrays, prev.clone().push(p))
						}))
						.collect::<Vec<_>>(),
					// Let's not log intermediary nodes
					_ => v
						.iter()
						.flat_map(|(k, v)| {
							let p = Part::from(k.to_owned());
							v._every(steps, arrays, prev.clone().push(p))
						})
						.collect::<Vec<_>>(),
				}
			}
			// Current path part is an array and is not empty
			Value::Array(v) if !v.is_empty() => {
				// Remove any trailing * path parts
				prev.remove_trailing_all();
				// Check if we should log individual array items
				match arrays {
					// Let's log all individual array items
					ArrayBehaviour::Full => std::iter::once(prev.clone())
						.chain(v.iter().enumerate().rev().flat_map(|(i, v)| {
							let p = Part::from(i.to_owned());
							v._every(steps, arrays, prev.clone().push(p))
						}))
						.collect::<Vec<_>>(),
					// Let's log all nested paths found in the array items
					ArrayBehaviour::Nested => std::iter::once(prev.clone())
						.chain(v.iter().enumerate().rev().flat_map(|(i, v)| {
							let p = Part::from(i.to_owned());
							let prev = prev.clone().push(p);
							let r = v._every(steps, arrays, prev.clone());
							if r.first() != Some(&prev) {
								r
							} else {
								r[1..].to_vec()
							}
						}))
						.collect::<Vec<_>>(),
					// Let's skip this array's values entirely
					ArrayBehaviour::Ignore => vec![prev],
				}
			}
			// Process every other path
			_ if !prev.is_empty() => vec![prev],
			// Nothing to do
			_ => vec![],
		}
	}
}

// Assuming a value like: { foo: [{ bar: 123 }] }
#[derive(Clone, Copy, Debug)]
pub enum ArrayBehaviour {
	// Do not process this array at all
	// [foo]
	Ignore,
	// Only give back nested paths, but skip the array indexes themselves
	// [foo, foo[0].bar ]
	Nested,
	// Give back all nested paths and all indexes of the array
	// [foo, foo[0], foo[0].bar]
	Full,
}

impl From<bool> for ArrayBehaviour {
	fn from(value: bool) -> Self {
		if value {
			ArrayBehaviour::Full
		} else {
			ArrayBehaviour::Ignore
		}
	}
}

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
}
