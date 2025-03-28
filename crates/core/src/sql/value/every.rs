use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

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
	use crate::syn::Parse;

	#[test]
	fn every_empty() {
		let val = Value::parse("{}");
		let res: Vec<Idiom> = vec![];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_with_empty_objects_arrays() {
		let val = Value::parse("{ test: {}, status: false, something: {age: 45}, tags: []}");
		let res = vec![
			Idiom::parse("something.age"),
			Idiom::parse("status"),
			Idiom::parse("tags"),
			Idiom::parse("test"),
		];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_without_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.every(None, false, false));
	}

	#[test]
	fn every_recursive_without_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something"),
			Idiom::parse("test.something[1].age"),
			Idiom::parse("test.something[1].tags"),
			Idiom::parse("test.something[0].age"),
			Idiom::parse("test.something[0].tags"),
		];
		assert_eq!(res, val.every(None, false, ArrayBehaviour::Nested));
	}

	#[test]
	fn every_including_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something"),
			Idiom::parse("test.something[1].age"),
			Idiom::parse("test.something[1].tags"),
			Idiom::parse("test.something[1].tags[1]"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[0].age"),
			Idiom::parse("test.something[0].tags"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[0].tags[0]"),
		];
		assert_eq!(res, val.every(None, false, true));
	}

	#[test]
	fn every_including_intermediary_nodes_without_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![Idiom::parse("test"), Idiom::parse("test.something")];
		assert_eq!(res, val.every(None, true, false));
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test"),
			Idiom::parse("test.something"),
			Idiom::parse("test.something[1]"),
			Idiom::parse("test.something[1].age"),
			Idiom::parse("test.something[1].tags"),
			Idiom::parse("test.something[1].tags[1]"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[0]"),
			Idiom::parse("test.something[0].age"),
			Idiom::parse("test.something[0].tags"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[0].tags[0]"),
		];
		assert_eq!(res, val.every(None, true, true));
	}

	#[test]
	fn every_including_intermediary_nodes_including_array_indexes_ending_all() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something"),
			Idiom::parse("test.something[1]"),
			Idiom::parse("test.something[1].age"),
			Idiom::parse("test.something[1].tags"),
			Idiom::parse("test.something[1].tags[1]"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[0]"),
			Idiom::parse("test.something[0].age"),
			Idiom::parse("test.something[0].tags"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[0].tags[0]"),
		];
		assert_eq!(res, val.every(Some(&Idiom::parse("test.something.*")), true, true));
	}
}
