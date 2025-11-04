use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::Value;

impl Value {
	/// Widens an idiom into a list of idioms with the given object.
	/// Resolving .* to all the different fields or indexes in that position.
	///
	/// For example `a.*.*` with the object `{ a: { b:  [1,2], c: 1} }` resolves
	/// to a.b[0], a.b[1], a.c.
	/// and `a.$` with object `{ a: [1,2,3] }` resolves to `a[2]`.
	pub(crate) fn each(&self, path: &[Part]) -> Vec<Idiom> {
		let mut accum = Vec::new();
		let mut build = Vec::new();
		self._each(path, &mut accum, &mut build);
		build
	}

	fn _each(&self, path: &[Part], accum: &mut Vec<Part>, build: &mut Vec<Idiom>) {
		let Some((first, rest)) = path.split_first() else {
			build.push(Idiom(accum.clone()));
			return;
		};

		// Get the current path part
		match self {
			// Current path part is an object
			Value::Object(v) => match first {
				Part::Field(f) => {
					if let Some(v) = v.get(&**f) {
						accum.push(Part::Field(f.clone()));
						v._each(rest, accum, build);
						accum.pop();
					}
				}
				Part::All => {
					for (k, v) in v.iter() {
						accum.push(Part::Field(k.clone()));
						v._each(rest, accum, build);
						accum.pop();
					}
				}
				_ => {}
			},
			// Current path part is an array
			Value::Array(v) => match first {
				Part::All => {
					for (idx, v) in v.iter().enumerate() {
						accum.push(Part::index_int(idx as i64));
						v._each(rest, accum, build);
						accum.pop();
					}
				}
				Part::First => {
					if !v.is_empty() {
						// NOTE: We previously did not add an index into the resulting path here.
						// That seemed like an bug but it might not be.
						accum.push(Part::index_int(0));
						v[0]._each(rest, accum, build);
						accum.pop();
					}
				}
				Part::Last => {
					let len = v.len();
					if len > 0 {
						// NOTE: We previously did not add an index into the resulting path here.
						// That seemed like an bug but it might not be.
						accum.push(Part::index_int(len as i64 - 1));
						v[len]._each(rest, accum, build);
						accum.pop();
					}
				}
				x => {
					if let Some(idx) = x.as_old_index() {
						if let Some(v) = v.get(idx) {
							// NOTE: We previously did not add an index into the resulting path
							// here. That seemed like an bug but it might not be.
							accum.push(x.clone());
							v._each(rest, accum, build);
							accum.pop();
						}
					} else {
						for (idx, v) in v.iter().enumerate() {
							accum.push(Part::index_int(idx as i64));
							v._each(rest, accum, build);
							accum.pop();
						}
					}
				}
			},
			// Ignore everything else
			_ => {}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn each_none() {
		let idi: Idiom = Idiom::default();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<Idiom> = vec![Idiom::default()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), parse_val!("{ test: { other: null, something: 123 } }"));
	}

	#[test]
	fn each_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<Idiom> = vec![syn::idiom("test.something").unwrap().into()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(123));
	}

	#[test]
	fn each_array() {
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<Idiom> = vec![syn::idiom("test.something").unwrap().into()];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), parse_val!("[{ age: 34 }, { age: 36 }]"));
	}

	#[test]
	fn each_array_field() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something[0].age").unwrap().into(),
			syn::idiom("test.something[1].age").unwrap().into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from(34));
		assert_eq!(val.pick(&res[1]), Value::from(36));
	}

	#[test]
	fn each_array_field_embedded() {
		let idi: Idiom = syn::idiom("test.something[*].tags").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something[0].tags").unwrap().into(),
			syn::idiom("test.something[1].tags").unwrap().into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), parse_val!("['code', 'databases']"));
		assert_eq!(val.pick(&res[1]), parse_val!("['design', 'operations']"));
	}

	#[test]
	fn each_array_field_embedded_index() {
		let idi: Idiom = syn::idiom("test.something[*].tags[1]").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("databases"));
		assert_eq!(val.pick(&res[1]), Value::from("operations"));
	}

	#[test]
	fn each_array_field_embedded_index_all() {
		let idi: Idiom = syn::idiom("test.something[*].tags[*]").unwrap().into();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<Idiom> = vec![
			syn::idiom("test.something[0].tags[0]").unwrap().into(),
			syn::idiom("test.something[0].tags[1]").unwrap().into(),
			syn::idiom("test.something[1].tags[0]").unwrap().into(),
			syn::idiom("test.something[1].tags[1]").unwrap().into(),
		];
		assert_eq!(res, val.each(&idi));
		assert_eq!(val.pick(&res[0]), Value::from("code"));
		assert_eq!(val.pick(&res[1]), Value::from("databases"));
		assert_eq!(val.pick(&res[2]), Value::from("design"));
		assert_eq!(val.pick(&res[3]), Value::from("operations"));
	}

	#[test]
	fn each_wildcards() {
		let val = parse_val!(
			"{ test: { a: { color: 'red' }, b: { color: 'blue' }, c: { color: 'green' } } }"
		);

		let res: Vec<Idiom> = vec![
			syn::idiom("test.a.color").unwrap().into(),
			syn::idiom("test.b.color").unwrap().into(),
			syn::idiom("test.c.color").unwrap().into(),
		];

		assert_eq!(res, val.each(&Idiom::from(syn::idiom("test.*.color").unwrap())));
	}
}
