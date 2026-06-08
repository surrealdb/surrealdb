use crate::expr::operation::Operation;
use crate::val::{Strand, Value};

impl Value {
	pub(crate) fn diff(&self, val: &Value) -> Vec<Operation> {
		let mut res = Vec::new();
		let mut path = Vec::new();

		self.diff_rec(val, &mut path, &mut res);

		res
	}

	fn diff_rec(&self, val: &Value, path: &mut Vec<Strand>, ops: &mut Vec<Operation>) {
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
					path.push(n.to_string().into());
					a[n].diff_rec(&b[n], path, ops);
					path.pop();
				}
				for n in min_len..b.len() {
					let mut path = path.clone();
					path.push(n.to_string().into());
					ops.push(Operation::Add {
						path,
						value: b[n].clone(),
					})
				}
				for n in min_len..a.len() {
					let mut path = path.clone();
					path.push(n.to_string().into());
					ops.push(Operation::Remove {
						path,
					})
				}
			}
			(Value::String(a), Value::String(b)) if a != b => ops.push(Operation::Change {
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

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn diff_none() {
		let old = parse_val!("{ test: true, text: 'text', other: { something: true } }");
		let now = parse_val!("{ test: true, text: 'text', other: { something: true } }");
		let res = parse_val!("[]");
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_add() {
		let old = parse_val!("{ test: true }");
		let now = parse_val!("{ test: true, other: 'test' }");
		let res = parse_val!("[{ op: 'add', path: '/other', value: 'test' }]");
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_remove() {
		let old = parse_val!("{ test: true, other: 'test' }");
		let now = parse_val!("{ test: true }");
		let res = parse_val!("[{ op: 'remove', path: '/other' }]");
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_add_array() {
		let old = parse_val!("{ test: [1,2,3] }");
		let now = parse_val!("{ test: [1,2,3,4] }");
		let res = parse_val!("[{ op: 'add', path: '/test/3', value: 4 }]");
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_replace_embedded() {
		let old = parse_val!("{ test: { other: 'test' } }");
		let now = parse_val!("{ test: { other: false } }");
		let res = parse_val!("[{ op: 'replace', path: '/test/other', value: false }]");
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_change_text() {
		let old = parse_val!("{ test: { other: 'test' } }");
		let now = parse_val!("{ test: { other: 'text' } }");
		let res = parse_val!(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]"
		);
		let res = Operation::value_to_operations(res).unwrap();
		assert_eq!(res, old.diff(&now));
	}

	#[test]
	fn diff_change_text_root() {
		// Issue 7239: diffing two strings at the root must emit an empty path,
		// which serializes to "" (RFC 6901 root pointer).
		let old = parse_val!("'test'");
		let now = parse_val!("'text'");
		let ops = old.diff(&now);
		assert_eq!(ops.len(), 1);
		match &ops[0] {
			Operation::Change {
				path,
				value: _,
			} => {
				assert!(path.is_empty(), "expected empty path, got {path:?}");
			}
			other => panic!("expected Operation::Change, got {other:?}"),
		}
		// Verify the JSON Patch serialization produces path "".
		let patch = Operation::operations_to_value(ops);
		let expected =
			parse_val!("[{ op: 'change', path: '', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]");
		assert_eq!(patch, expected);
	}

	#[test]
	fn diff_replace_root() {
		// Issue 7239: diffing two non-string scalars at the root must emit
		// an Operation::Replace with an empty path.
		let old = parse_val!("1");
		let now = parse_val!("2");
		let ops = old.diff(&now);
		assert_eq!(ops.len(), 1);
		match &ops[0] {
			Operation::Replace {
				path,
				value: _,
			} => {
				assert!(path.is_empty(), "expected empty path, got {path:?}");
			}
			other => panic!("expected Operation::Replace, got {other:?}"),
		}
		let patch = Operation::operations_to_value(ops);
		let expected = parse_val!("[{ op: 'replace', path: '', value: 2 }]");
		assert_eq!(patch, expected);
	}
}
