use crate::expr::idiom::Idiom;
use crate::val::Value;

impl Value {
	pub(crate) fn changed(&self, val: &Value) -> Value {
		match (self, val) {
			(Value::Object(a), Value::Object(b)) => {
				// Create an object
				let mut chg = Value::empty_object();
				// Loop over old keys
				for (key, _) in a.iter() {
					if !b.contains_key(key) {
						let path = Idiom::field(key.clone());
						chg.put(&path, Value::None);
					}
				}
				// Loop over new keys
				for (key, val) in b.iter() {
					match a.get(key) {
						// Key did not exist
						None => {
							let path = Idiom::field(key.clone());
							chg.put(&path, val.clone());
						}
						Some(old) => {
							if old != val {
								let path = Idiom::field(key.clone());
								chg.put(&path, old.changed(val));
							}
						}
					}
				}
				//
				chg
			}
			(_, _) => val.clone(),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::syn;
	use crate::val::convert_public_value_to_internal;

	macro_rules! parse_val {
		($input:expr) => {
			convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn changed_none() {
		let old = parse_val!("{ test: true, text: 'text', other: { something: true } }");
		let now = parse_val!("{ test: true, text: 'text', other: { something: true } }");
		let res = parse_val!("{}");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add() {
		let old = parse_val!("{ test: true }");
		let now = parse_val!("{ test: true, other: 'test' }");
		let res = parse_val!("{ other: 'test' }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_remove() {
		let old = parse_val!("{ test: true, other: 'test' }");
		let now = parse_val!("{ test: true }");
		let res = parse_val!("{ other: NONE }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add_array() {
		let old = parse_val!("{ test: [1,2,3] }");
		let now = parse_val!("{ test: [1,2,3,4] }");
		let res = parse_val!("{ test: [1,2,3,4] }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_replace_embedded() {
		let old = parse_val!("{ test: { other: 'test' } }");
		let now = parse_val!("{ test: { other: false } }");
		let res = parse_val!("{ test: { other: false } }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_change_text() {
		let old = parse_val!("{ test: { other: 'test' } }");
		let now = parse_val!("{ test: { other: 'text' } }");
		let res = parse_val!("{ test: { other: 'text' } }");
		assert_eq!(res, old.changed(&now));
	}
}
