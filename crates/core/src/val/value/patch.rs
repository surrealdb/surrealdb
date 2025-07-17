use crate::expr::Ident;
use crate::expr::operation::PatchError;
use crate::expr::part::Part;
use crate::val::Value;
use crate::{err::Error, expr::Operation};
use anyhow::{Result, ensure};

impl Value {
	pub(crate) fn patch(&mut self, ops: Value) -> Result<()> {
		// Create a new object for testing and patching
		// Loop over the patch operations and apply them
		for operation in Operation::value_to_operations(ops)
			.map_err(Error::InvalidPatch)
			.map_err(anyhow::Error::new)?
			.into_iter()
		{
			match operation {
				// Add a value
				Operation::Add {
					mut path,
					value,
				} => {
					// Split the last path part from the path
					if let Some(last) = path.pop() {
						let path = path
							.into_iter()
							.map(|x| Part::Field(Ident::new(x).unwrap()))
							.collect::<Vec<_>>();

						if let Ok(x) = last.parse::<usize>() {
							// TODO: Fix behavior on overload.
							match self.pick(&path) {
								Value::Array(mut v) => {
									if v.len() > x {
										v.insert(x, value);
										self.put(&path, Value::Array(v));
									} else {
										v.push(value);
										self.put(&path, Value::Array(v));
									}
								}
								_ => self.put(&path, value),
							}
						} else if last == "-" {
							match self.pick(&path) {
								Value::Array(mut v) => {
									v.push(value);
									self.put(&path, Value::Array(v));
								}
								_ => self.put(&path, value),
							}
						} else {
							match self.pick(&path) {
								Value::Array(_) => self.inc(&path, value),
								_ => self.put(&path, value),
							}
						}
					} else {
						// no path
						*self = value
					}
				}
				// Remove a value at the specified path
				Operation::Remove {
					path,
				} => {
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					self.cut(&path)
				}
				// Replace a value at the specified path
				Operation::Replace {
					path,
					value,
				} => {
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					self.put(&path, value)
				}
				// Modify a string at the specified path
				Operation::Change {
					path,
					value,
				} => {
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					if let Value::Strand(p) = value {
						if let Value::Strand(v) = self.pick(&path) {
							let dmp = dmp::new();
							let pch = dmp.patch_from_text(p.into_string()).map_err(|e| {
								Error::InvalidPatch(PatchError {
									message: format!("{e:?}"),
								})
							})?;
							let (txt, _) = dmp.patch_apply(&pch, v.as_str()).map_err(|e| {
								Error::InvalidPatch(PatchError {
									message: format!("{e:?}"),
								})
							})?;
							let txt = txt.into_iter().collect::<String>();
							self.put(&path, Value::from(txt));
						}
					}
				}
				// Copy a value from one field to another
				Operation::Copy {
					path,
					from,
				} => {
					// TODO: NUll byte validity
					let from = from
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();

					let val = self.pick(&from);
					self.put(&path, val);
				}
				// Move a value from one field to another
				Operation::Move {
					path,
					from,
				} => {
					let from = from
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();

					let val = self.pick(&from);
					self.put(&path, val);
					self.cut(&from);
				}
				// Test whether a value matches another value
				Operation::Test {
					path,
					value,
				} => {
					let path = path
						.into_iter()
						.map(|x| Part::Field(Ident::new(x).unwrap()))
						.collect::<Vec<_>>();
					let val = self.pick(&path);
					ensure!(
						value == val,
						Error::PatchTest {
							expected: value.to_string(),
							got: val.to_string(),
						}
					);
				}
			}
		}
		// Everything ok
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::syn;

	#[tokio::test]
	async fn patch_add_simple() {
		let mut val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let ops = syn::value("[{ op: 'add', path: '/temp', value: true }]").unwrap();
		let res = syn::value("{ test: { other: null, something: 123 }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let mut val = syn::value("{ test: { other: null, something: 123 }, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'remove', path: '/temp' }]").unwrap();
		let res = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let mut val = syn::value("{ test: { other: null, something: 123 }, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'replace', path: '/temp', value: 'text' }]").unwrap();
		let res = syn::value("{ test: { other: null, something: 123 }, temp: 'text' }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let mut val =
			syn::value("{ test: { other: null, something: 123 }, temp: 'test' }").unwrap();
		let ops = syn::value(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.unwrap();
		let res = syn::value("{ test: { other: null, something: 123 }, temp: 'text' }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_simple() {
		let mut val = syn::value("{ test: 123, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'copy', path: '/temp', from: '/test' }]").unwrap();
		let res = syn::value("{ test: 123, temp: 123 }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_simple() {
		let mut val = syn::value("{ temp: true, some: 123 }").unwrap();
		let ops = syn::value("[{ op: 'move', path: '/other', from: '/temp' }]").unwrap();
		let res = syn::value("{ other: true, some: 123 }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_simple() {
		let mut val =
			syn::value("{ test: { other: 'test', something: 123 }, temp: true }").unwrap();
		let ops = syn::value(
			"[{ op: 'remove', path: '/test/something' }, { op: 'test', path: '/temp', value: true }]",
		)
		.unwrap()
		.into();
		let res = syn::value("{ test: { other: 'test' }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let mut val = syn::value("{ test: { other: null, something: 123 } }").unwrap();
		let ops = syn::value("[{ op: 'add', path: '/temp/test', value: true }]").unwrap();
		let res =
			syn::value("{ test: { other: null, something: 123 }, temp: { test: true } }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let mut val = syn::value("{ test: { other: null, something: 123 }, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'remove', path: '/test/other' }]").unwrap();
		let res = syn::value("{ test: { something: 123 }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let mut val = syn::value("{ test: { other: null, something: 123 }, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'replace', path: '/test/other', value: 'text' }]").unwrap();
		let res = syn::value("{ test: { other: 'text', something: 123 }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let mut val =
			syn::value("{ test: { other: 'test', something: 123 }, temp: true }").unwrap();
		let ops = syn::value(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.unwrap();
		let res = syn::value("{ test: { other: 'text', something: 123 }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_embedded() {
		let mut val = syn::value("{ test: { other: null }, temp: 123 }").unwrap();
		let ops = syn::value("[{ op: 'copy', path: '/test/other', from: '/temp' }]").unwrap();
		let res = syn::value("{ test: { other: 123 }, temp: 123 }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_embedded() {
		let mut val = syn::value("{ test: { other: ':3', some: 123 }}").unwrap();
		let ops = syn::value("[{ op: 'move', path: '/temp', from: '/test/other' }]").unwrap();
		let res = syn::value("{ test: { some: 123 }, temp: ':3' }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_embedded() {
		let mut val =
			syn::value("{ test: { other: 'test', something: 123 }, temp: true }").unwrap();
		let ops = syn::value(
			"[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 123 }]",
		)
		.unwrap()
		.into();
		let res = syn::value("{ test: { something: 123 }, temp: true }").unwrap();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_invalid() {
		// See https://github.com/surrealdb/surrealdb/issues/2001
		let mut val =
			syn::value("{ test: { other: 'test', something: 123 }, temp: true }").unwrap();
		let ops = syn::value("[{ op: 'change', path: '/test/other', value: 'text' }]").unwrap();
		assert!(val.patch(ops).is_err());
	}

	#[tokio::test]
	async fn patch_test_invalid() {
		let mut val =
			syn::value("{ test: { other: 'test', something: 123 }, temp: true }").unwrap();
		let should = val.clone();
		let ops = syn::value( "[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 'not same' }]",).unwrap();
		assert!(val.patch(ops).is_err());
		// It is important to test if patches applied even if test operation fails
		assert_eq!(val, should);
	}
}
