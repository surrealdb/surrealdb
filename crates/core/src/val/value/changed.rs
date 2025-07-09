use crate::expr::Ident;
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
						// TODO: null byte validity.
						let path = Idiom::field(Ident::new(key.clone()).unwrap());
						chg.put(&path, Value::None);
					}
				}
				// Loop over new keys
				for (key, val) in b.iter() {
					match a.get(key) {
						// Key did not exist
						None => {
							// TODO: null byte validity.
							let path = Idiom::field(Ident::new(key.clone()).unwrap());
							chg.put(&path, val.clone());
						}
						Some(old) => {
							if old != val {
								// TODO: null byte validity.
								let path = Idiom::field(Ident::new(key.clone()).unwrap());
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

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::{sql::SqlValue, syn::Parse};

	#[test]
	fn changed_none() {
		let old: Value =
			SqlValue::parse("{ test: true, text: 'text', other: { something: true } }").into();
		let now: Value =
			SqlValue::parse("{ test: true, text: 'text', other: { something: true } }").into();
		let res: Value = SqlValue::parse("{}").into();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add() {
		let old: Value = SqlValue::parse("{ test: true }").into();
		let now: Value = SqlValue::parse("{ test: true, other: 'test' }").into();
		let res: Value = SqlValue::parse("{ other: 'test' }").into();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_remove() {
		let old: Value = SqlValue::parse("{ test: true, other: 'test' }").into();
		let now: Value = SqlValue::parse("{ test: true }").into();
		let res: Value = SqlValue::parse("{ other: NONE }").into();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add_array() {
		let old: Value = SqlValue::parse("{ test: [1,2,3] }").into();
		let now: Value = SqlValue::parse("{ test: [1,2,3,4] }").into();
		let res: Value = SqlValue::parse("{ test: [1,2,3,4] }").into();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_replace_embedded() {
		let old: Value = SqlValue::parse("{ test: { other: 'test' } }").into();
		let now: Value = SqlValue::parse("{ test: { other: false } }").into();
		let res: Value = SqlValue::parse("{ test: { other: false } }").into();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_change_text() {
		let old: Value = SqlValue::parse("{ test: { other: 'test' } }").into();
		let now: Value = SqlValue::parse("{ test: { other: 'text' } }").into();
		let res: Value = SqlValue::parse("{ test: { other: 'text' } }").into();
		assert_eq!(res, old.changed(&now));
	}
}
*/
