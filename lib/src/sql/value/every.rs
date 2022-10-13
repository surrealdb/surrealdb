use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn every(&self, split: bool) -> Vec<Idiom> {
		self._every(split, Idiom::default())
	}
	fn _every(&self, split: bool, prev: Idiom) -> Vec<Idiom> {
		match self {
			// Current path part is an object
			Value::Object(v) => v
				.iter()
				.flat_map(|(k, v)| {
					let p = Part::from(k.to_owned());
					v._every(split, prev.clone().push(p))
				})
				.collect::<Vec<_>>(),
			// Current path part is an array
			Value::Array(v) => match split {
				true => v
					.iter()
					.enumerate()
					.flat_map(|(i, v)| {
						let p = Part::from(i.to_owned());
						v._every(split, prev.clone().push(p))
					})
					.collect::<Vec<_>>(),
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
		assert_eq!(res, val.every(false));
	}

	#[test]
	fn every_including_array_indexes() {
		let val = Value::parse("{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }");
		let res = vec![
			Idiom::parse("test.something[0].age"),
			Idiom::parse("test.something[0].tags[0]"),
			Idiom::parse("test.something[0].tags[1]"),
			Idiom::parse("test.something[1].age"),
			Idiom::parse("test.something[1].tags[0]"),
			Idiom::parse("test.something[1].tags[1]"),
		];
		assert_eq!(res, val.every(true));
	}
}
