use crate::expr::operation::Operation;
use crate::val::Value;

impl Value {
	pub(crate) fn diff(&self, val: &Value) -> Vec<Operation> {
		let mut res = Vec::new();
		let mut path = Vec::new();

		self.diff_rec(val, &mut path, &mut res);

		res
	}

	fn diff_rec(&self, val: &Value, path: &mut Vec<String>, ops: &mut Vec<Operation>) {
		match (self, val) {
			(Value::Object(a), Value::Object(b)) if a != b => {
				// Loop over old keys
				for (key, _) in a.iter() {
					if !b.contains_key(key) {
						let mut path = path.clone();
						path.push(key.clone());
						ops.push(Operation::Remove {
							path,
						});
					}
				}
				// Loop over new keys
				for (key, val) in b.iter() {
					match a.get(key) {
						None => {
							let mut path = path.clone();
							path.push(key.clone());
							ops.push(Operation::Add {
								// TODO: null byte validity.
								path,
								value: val.clone(),
							});
						}
						Some(old) => {
							path.push(key.clone());
							old.diff_rec(val, path, ops);
							path.pop();
						}
					}
				}
			}
			(Value::Array(a), Value::Array(b)) if a != b => {
				let min_len = a.len().min(b.len());
				for n in 0..min_len {
					// TODO: null byte validity.
					path.push(n.to_string());
					a[n].diff_rec(&b[n], path, ops);
					path.pop();
				}
				for n in min_len..b.len() {
					let mut path = path.clone();
					path.push(n.to_string());
					ops.push(Operation::Add {
						path,
						value: b[n].clone(),
					})
				}
				for n in min_len..a.len() {
					let mut path = path.clone();
					path.push(n.to_string());
					ops.push(Operation::Remove {
						path,
					})
				}
			}
			(Value::Strand(a), Value::Strand(b)) if a != b => ops.push(Operation::Change {
				path: path.clone(),
				value: {
					let dmp = dmp::new();
					let pch = dmp.patch_make1(a, b);
					let txt = dmp.patch_to_text(&pch);
					txt.into()
				},
			}),
			(a, b) if a != b => ops.push(Operation::Replace {
				path: path.clone(),
				value: val.clone(),
			}),
			(_, _) => (),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	#[test]
	fn diff_none() {
		let old = syn::value("{ test: true, text: 'text', other: { something: true } }").unwrap();
		let now = syn::value("{ test: true, text: 'text', other: { something: true } }").unwrap();
		let res = syn::value("[]").unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_add() {
		let old = syn::value("{ test: true }").unwrap();
		let now = syn::value("{ test: true, other: 'test' }").unwrap();
		let res = syn::value("[{ op: 'add', path: '/other', value: 'test' }]").unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_remove() {
		let old = syn::value("{ test: true, other: 'test' }").unwrap();
		let now = syn::value("{ test: true }").unwrap();
		let res = syn::value("[{ op: 'remove', path: '/other' }]").unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_add_array() {
		let old = syn::value("{ test: [1,2,3] }").unwrap();
		let now = syn::value("{ test: [1,2,3,4] }").unwrap();
		let res = syn::value("[{ op: 'add', path: '/test/3', value: 4 }]").unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_replace_embedded() {
		let old = syn::value("{ test: { other: 'test' } }").unwrap();
		let now = syn::value("{ test: { other: false } }").unwrap();
		let res = syn::value("[{ op: 'replace', path: '/test/other', value: false }]").unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_change_text() {
		let old = syn::value("{ test: { other: 'test' } }").unwrap();
		let now = syn::value("{ test: { other: 'text' } }").unwrap();
		let res = syn::value(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.unwrap();
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}
}
