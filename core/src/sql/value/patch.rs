use crate::err::Error;
use crate::sql::operation::Operation;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn patch(&mut self, ops: Value) -> Result<(), Error> {
		// Create a new object for testing and patching
		let mut new = self.clone();
		// Loop over the patch operations and apply them
		for operation in ops.to_operations()?.into_iter() {
			match operation {
				// Add a value
				Operation::Add {
					path,
					value,
				} => {
					// Split the last path part from the path
					match path.split_last() {
						// Check what the last path part is
						Some((last, left)) => match last {
							Part::Index(i) => match new.pick(left) {
								Value::Array(mut v) => match v.len() > i.clone().as_usize() {
									true => {
										v.insert((*i).as_usize(), value);
										new.put(left, Value::Array(v));
									}
									false => {
										v.push(value);
										new.put(left, Value::Array(v));
									}
								},
								_ => new.put(left, value),
							},
							Part::Field(v) if v.is_dash() => match new.pick(left) {
								Value::Array(mut v) => {
									v.push(value);
									new.put(left, Value::Array(v));
								}
								_ => new.put(left, value),
							},
							_ => match new.pick(&path) {
								Value::Array(_) => new.inc(&path, value),
								_ => new.put(&path, value),
							},
						},
						None => match new.pick(&path) {
							Value::Array(_) => new.inc(&path, value),
							_ => new.put(&path, value),
						},
					}
				}
				// Remove a value at the specified path
				Operation::Remove {
					path,
				} => new.cut(&path),
				// Replace a value at the specified path
				Operation::Replace {
					path,
					value,
				} => new.put(&path, value),
				// Modify a string at the specified path
				Operation::Change {
					path,
					value,
				} => {
					if let Value::Strand(p) = value {
						if let Value::Strand(v) = new.pick(&path) {
							let dmp = dmp::new();
							let pch = dmp.patch_from_text(p.as_string()).map_err(|e| {
								Error::InvalidPatch {
									message: format!("{e:?}"),
								}
							})?;
							let (txt, _) = dmp.patch_apply(&pch, v.as_str()).map_err(|e| {
								Error::InvalidPatch {
									message: format!("{e:?}"),
								}
							})?;
							let txt = txt.into_iter().collect::<String>();
							new.put(&path, Value::from(txt));
						}
					}
				}
				// Copy a value from one field to another
				Operation::Copy {
					path,
					from,
				} => {
					let val = new.pick(&from);
					new.put(&path, val);
				}
				// Move a value from one field to another
				Operation::Move {
					path,
					from,
				} => {
					let val = new.pick(&from);
					new.put(&path, val);
					new.cut(&from);
				}
				// Test whether a value matches another value
				Operation::Test {
					path,
					value,
				} => {
					let val = new.pick(&path);
					if value != val {
						return Err(Error::PatchTest {
							expected: value.to_string(),
							got: val.to_string(),
						});
					}
				}
			}
		}
		// Set the document to the updated document
		*self = new;
		// Everything ok
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn::Parse;

	#[tokio::test]
	async fn patch_add_simple() {
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Value::parse("[{ op: 'add', path: '/temp', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/temp' }]");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'replace', path: '/temp', value: 'text' }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: 'test' }");
		let ops = Value::parse(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_simple() {
		let mut val = Value::parse("{ test: 123, temp: true }");
		let ops = Value::parse("[{ op: 'copy', path: '/temp', from: '/test' }]");
		let res = Value::parse("{ test: 123, temp: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_simple() {
		let mut val = Value::parse("{ temp: true, some: 123 }");
		let ops = Value::parse("[{ op: 'move', path: '/other', from: '/temp' }]");
		let res = Value::parse("{ other: true, some: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_simple() {
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/test/something' }, { op: 'test', path: '/temp', value: true }]");
		let res = Value::parse("{ test: { other: 'test' }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Value::parse("[{ op: 'add', path: '/temp/test', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: { test: true } }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/test/other' }]");
		let res = Value::parse("{ test: { something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'replace', path: '/test/other', value: 'text' }]");
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Value::parse(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_embedded() {
		let mut val = Value::parse("{ test: { other: null }, temp: 123 }");
		let ops = Value::parse("[{ op: 'copy', path: '/test/other', from: '/temp' }]");
		let res = Value::parse("{ test: { other: 123 }, temp: 123 }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_embedded() {
		let mut val = Value::parse("{ test: { other: ':3', some: 123 }}");
		let ops = Value::parse("[{ op: 'move', path: '/temp', from: '/test/other' }]");
		let res = Value::parse("{ test: { some: 123 }, temp: ':3' }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_embedded() {
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 123 }]");
		let res = Value::parse("{ test: { something: 123 }, temp: true }");
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_invalid() {
		// See https://github.com/surrealdb/surrealdb/issues/2001
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'change', path: '/test/other', value: 'text' }]");
		assert!(val.patch(ops).is_err());
	}

	#[tokio::test]
	async fn patch_test_invalid() {
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let should = val.clone();
		let ops = Value::parse("[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 'not same' }]");
		assert!(val.patch(ops).is_err());
		// It is important to test if patches applied even if test operation fails
		assert_eq!(val, should);
	}
}
