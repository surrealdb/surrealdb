use crate::sql::idiom::Idiom;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn changed(&self, val: &Value) -> Value {
		match (self, val) {
			(Value::Object(a), Value::Object(b)) => {
				// Create an object
				let mut chg = Value::base();
				// Loop over old keys
				for (key, _) in a.iter() {
					if !b.contains_key(key) {
						let path = Idiom::from(key.clone());
						chg.put(&path, Value::None);
					}
				}
				// Loop over new keys
				for (key, val) in b.iter() {
					match a.get(key) {
						// Key did not exist
						None => {
							let path = Idiom::from(key.clone());
							chg.put(&path, val.clone());
						}
						Some(old) => {
							if old != val {
								let path = Idiom::from(key.clone());
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

	use super::*;
	use crate::syn::Parse;

	#[test]
	fn changed_none() {
		let old = Value::parse("{ test: true, text: 'text', other: { something: true } }");
		let now = Value::parse("{ test: true, text: 'text', other: { something: true } }");
		let res = Value::parse("{}");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add() {
		let old = Value::parse("{ test: true }");
		let now = Value::parse("{ test: true, other: 'test' }");
		let res = Value::parse("{ other: 'test' }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_remove() {
		let old = Value::parse("{ test: true, other: 'test' }");
		let now = Value::parse("{ test: true }");
		let res = Value::parse("{ other: NONE }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add_array() {
		let old = Value::parse("{ test: [1,2,3] }");
		let now = Value::parse("{ test: [1,2,3,4] }");
		let res = Value::parse("{ test: [1,2,3,4] }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_replace_embedded() {
		let old = Value::parse("{ test: { other: 'test' } }");
		let now = Value::parse("{ test: { other: false } }");
		let res = Value::parse("{ test: { other: false } }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_change_text() {
		let old = Value::parse("{ test: { other: 'test' } }");
		let now = Value::parse("{ test: { other: 'text' } }");
		let res = Value::parse("{ test: { other: 'text' } }");
		assert_eq!(res, old.changed(&now));
	}
}
