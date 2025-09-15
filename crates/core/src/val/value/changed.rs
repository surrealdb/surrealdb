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

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn changed_none() {
		let old = syn::value("{ test: true, text: 'text', other: { something: true } }").unwrap();
		let now = syn::value("{ test: true, text: 'text', other: { something: true } }").unwrap();
		let res = syn::value("{}").unwrap();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add() {
		let old = syn::value("{ test: true }").unwrap();
		let now = syn::value("{ test: true, other: 'test' }").unwrap();
		let res = syn::value("{ other: 'test' }").unwrap();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_remove() {
		let old = syn::value("{ test: true, other: 'test' }").unwrap();
		let now = syn::value("{ test: true }").unwrap();
		let res = syn::value("{ other: NONE }").unwrap();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_add_array() {
		let old = syn::value("{ test: [1,2,3] }").unwrap();
		let now = syn::value("{ test: [1,2,3,4] }").unwrap();
		let res = syn::value("{ test: [1,2,3,4] }").unwrap();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_replace_embedded() {
		let old = syn::value("{ test: { other: 'test' } }").unwrap();
		let now = syn::value("{ test: { other: false } }").unwrap();
		let res = syn::value("{ test: { other: false } }").unwrap();
		assert_eq!(res, old.changed(&now));
	}

	#[test]
	fn changed_change_text() {
		let old = syn::value("{ test: { other: 'test' } }").unwrap();
		let now = syn::value("{ test: { other: 'text' } }").unwrap();
		let res = syn::value("{ test: { other: 'text' } }").unwrap();
		assert_eq!(res, old.changed(&now));
	}
}
