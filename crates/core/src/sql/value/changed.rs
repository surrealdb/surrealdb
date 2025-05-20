use crate::sql::idiom::Idiom;
use crate::sql::value::SqlValue;

impl SqlValue {
	pub(crate) fn changed(&self, val: &SqlValue) -> SqlValue {
		match (self, val) {
			(SqlValue::Object(a), SqlValue::Object(b)) => {
				// Create an object
				let mut chg = SqlValue::base();
				// Loop over old keys
				for (key, _) in a.iter() {
					if !b.contains_key(key) {
						let path = Idiom::from(key.clone());
						chg.put(&path, SqlValue::None);
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
		let old = SqlValue::parse("{ test: true, text: 'text', other: { something: true } }");
		let now = SqlValue::parse("{ test: true, text: 'text', other: { something: true } }");
		let res = SqlValue::parse("{}");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add() {
		let old = SqlValue::parse("{ test: true }");
		let now = SqlValue::parse("{ test: true, other: 'test' }");
		let res = SqlValue::parse("{ other: 'test' }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_remove() {
		let old = SqlValue::parse("{ test: true, other: 'test' }");
		let now = SqlValue::parse("{ test: true }");
		let res = SqlValue::parse("{ other: NONE }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add_array() {
		let old = SqlValue::parse("{ test: [1,2,3] }");
		let now = SqlValue::parse("{ test: [1,2,3,4] }");
		let res = SqlValue::parse("{ test: [1,2,3,4] }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_replace_embedded() {
		let old = SqlValue::parse("{ test: { other: 'test' } }");
		let now = SqlValue::parse("{ test: { other: false } }");
		let res = SqlValue::parse("{ test: { other: false } }");
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_change_text() {
		let old = SqlValue::parse("{ test: { other: 'test' } }");
		let now = SqlValue::parse("{ test: { other: 'text' } }");
		let res = SqlValue::parse("{ test: { other: 'text' } }");
		assert_eq!(res, old.changed(&now));
	}
}
