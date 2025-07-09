use crate::expr::Part;
use crate::expr::idiom::Idiom;
use crate::expr::operation::Operation;
use crate::val::Value;
use std::cmp::min;

impl Value {
	pub(crate) fn diff(&self, val: &Value, path: Idiom) -> Vec<Operation> {
		let mut ops: Vec<Operation> = vec![];
		match (self, val) {
			(Value::Object(a), Value::Object(b)) if a != b => {
				// Loop over old keys
				for (key, _) in a.iter() {
					if !b.contains_key(key) {
						ops.push(Operation::Remove {
							// TODO: null byte validity.
							path: path.clone().push(Part::field(key.clone()).unwrap()),
						})
					}
				}
				// Loop over new keys
				for (key, val) in b.iter() {
					match a.get(key) {
						None => ops.push(Operation::Add {
							// TODO: null byte validity.
							path: path.clone().push(Part::field(key.clone())),
							value: val.clone(),
						}),
						Some(old) => {
							// TODO: null byte validity.
							let path = path.clone().push(Part::field(key.clone()));
							ops.append(&mut old.diff(val, path))
						}
					}
				}
			}
			(Value::Array(a), Value::Array(b)) if a != b => {
				let mut n = 0;
				while n < min(a.len(), b.len()) {
					// TODO: null byte validity.
					let path = path.clone().push(Part::index_int(n));
					ops.append(&mut a[n].diff(&b[n], path));
					n += 1;
				}
				while n < b.len() {
					if n >= a.len() {
						ops.push(Operation::Add {
							// TODO: null byte validity.
							path: path.clone().push(Part::index_int(n)),
							value: b[n].clone(),
						})
					}
					n += 1;
				}
				while n < a.len() {
					if n >= b.len() {
						ops.push(Operation::Remove {
							// TODO: null byte validity.
							path: path.clone().push(Part::index_int(n)),
						})
					}
					n += 1;
				}
			}
			(Value::Strand(a), Value::Strand(b)) if a != b => ops.push(Operation::Change {
				path,
				value: {
					let dmp = dmp::new();
					let pch = dmp.patch_make1(a, b);
					let txt = dmp.patch_to_text(&pch);
					txt.into()
				},
			}),
			(a, b) if a != b => ops.push(Operation::Replace {
				path,
				value: val.clone(),
			}),
			(_, _) => (),
		}
		ops
	}
}

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::Idiom;
	use crate::{sql::SqlValue, syn::Parse};

	#[test]
	fn diff_none() {
		let old: Value =
			SqlValue::parse("{ test: true, text: 'text', other: { something: true } }").into();
		let now: Value =
			SqlValue::parse("{ test: true, text: 'text', other: { something: true } }").into();
		let res: Value = SqlValue::parse("[]").into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}

	#[test]
	fn diff_add() {
		let old: Value = SqlValue::parse("{ test: true }").into();
		let now: Value = SqlValue::parse("{ test: true, other: 'test' }").into();
		let res: Value = SqlValue::parse("[{ op: 'add', path: '/other', value: 'test' }]").into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}

	#[test]
	fn diff_remove() {
		let old: Value = SqlValue::parse("{ test: true, other: 'test' }").into();
		let now: Value = SqlValue::parse("{ test: true }").into();
		let res: Value = SqlValue::parse("[{ op: 'remove', path: '/other' }]").into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}

	#[test]
	fn diff_add_array() {
		let old: Value = SqlValue::parse("{ test: [1,2,3] }").into();
		let now: Value = SqlValue::parse("{ test: [1,2,3,4] }").into();
		let res: Value = SqlValue::parse("[{ op: 'add', path: '/test/3', value: 4 }]").into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}

	#[test]
	fn diff_replace_embedded() {
		let old: Value = SqlValue::parse("{ test: { other: 'test' } }").into();
		let now: Value = SqlValue::parse("{ test: { other: false } }").into();
		let res: Value =
			SqlValue::parse("[{ op: 'replace', path: '/test/other', value: false }]").into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}

	#[test]
	fn diff_change_text() {
		let old: Value = SqlValue::parse("{ test: { other: 'test' } }").into();
		let now: Value = SqlValue::parse("{ test: { other: 'text' } }").into();
		let res: Value = SqlValue::parse(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.into();
		assert_eq!(res.to_operations().unwrap(), old.diff(&now, Idiom::default()));
	}
}*/
