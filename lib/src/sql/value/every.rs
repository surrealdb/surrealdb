use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn every(&self, steps: bool, arrays: bool) -> Vec<Idiom> {
		self._every(steps, arrays, Idiom::default())
	}
	fn _every(&self, steps: bool, arrays: bool, prev: Idiom) -> Vec<Idiom> {
		match self {
			// Current path part is an object
			Value::Object(v) => match steps {
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
			},
			// Current path part is an array
			Value::Array(v) => match arrays {
				// Let's log all individual array items
				true => std::iter::once(prev.clone())
					.chain(v.iter().enumerate().rev().flat_map(|(i, v)| {
						let p = Part::from(i.to_owned());
						v._every(steps, arrays, prev.clone().push(p))
					}))
					.collect::<Vec<_>>(),
				// Let's not log individual array items
				false => vec![prev],
			},
			// Process everything else
			_ => vec![prev],
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::idiom::Idiom;
	use crate::sql::test::Parse;

	#[test]
	fn every_without_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![Idiom::parse("test.something")];
		assert_eq!(res, val.every(false, false));
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
		assert_eq!(res, val.every(false, true));
	}

	#[test]
	fn every_including_intermediary_nodes_without_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![Idiom::parse("test"), Idiom::parse("test.something")];
		assert_eq!(res, val.every(true, false));
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
		assert_eq!(res, val.every(true, true));
	}
}
