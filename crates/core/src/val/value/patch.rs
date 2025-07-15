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

/*
#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::SqlValue;
	use crate::syn::Parse;

	#[tokio::test]
	async fn patch_add_simple() {
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let ops: Value = SqlValue::parse("[{ op: 'add', path: '/temp', value: true }]").into();
		let res: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: true }").into();
		let ops: Value = SqlValue::parse("[{ op: 'remove', path: '/temp' }]").into();
		let res: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: true }").into();
		let ops: Value =
			SqlValue::parse("[{ op: 'replace', path: '/temp', value: 'text' }]").into();
		let res: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: 'text' }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: 'test' }").into();
		let ops: Value = SqlValue::parse(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.into();
		let res: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: 'text' }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_simple() {
		let mut val: Value = SqlValue::parse("{ test: 123, temp: true }").into();
		let ops: Value = SqlValue::parse("[{ op: 'copy', path: '/temp', from: '/test' }]").into();
		let res: Value = SqlValue::parse("{ test: 123, temp: 123 }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_simple() {
		let mut val: Value = SqlValue::parse("{ temp: true, some: 123 }").into();
		let ops: Value = SqlValue::parse("[{ op: 'move', path: '/other', from: '/temp' }]").into();
		let res: Value = SqlValue::parse("{ other: true, some: 123 }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_simple() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: 'test', something: 123 }, temp: true }").into();
		let ops: Value = SqlValue::parse(
			"[{ op: 'remove', path: '/test/something' }, { op: 'test', path: '/temp', value: true }]",
		)
		.into();
		let res: Value = SqlValue::parse("{ test: { other: 'test' }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let mut val: Value = SqlValue::parse("{ test: { other: null, something: 123 } }").into();
		let ops: Value = SqlValue::parse("[{ op: 'add', path: '/temp/test', value: true }]").into();
		let res: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: { test: true } }")
				.into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: true }").into();
		let ops: Value = SqlValue::parse("[{ op: 'remove', path: '/test/other' }]").into();
		let res: Value = SqlValue::parse("{ test: { something: 123 }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: null, something: 123 }, temp: true }").into();
		let ops: Value =
			SqlValue::parse("[{ op: 'replace', path: '/test/other', value: 'text' }]").into();
		let res: Value =
			SqlValue::parse("{ test: { other: 'text', something: 123 }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: 'test', something: 123 }, temp: true }").into();
		let ops: Value = SqlValue::parse(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		)
		.into();
		let res: Value =
			SqlValue::parse("{ test: { other: 'text', something: 123 }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_copy_embedded() {
		let mut val: Value = SqlValue::parse("{ test: { other: null }, temp: 123 }").into();
		let ops: Value =
			SqlValue::parse("[{ op: 'copy', path: '/test/other', from: '/temp' }]").into();
		let res: Value = SqlValue::parse("{ test: { other: 123 }, temp: 123 }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_move_embedded() {
		let mut val: Value = SqlValue::parse("{ test: { other: ':3', some: 123 }}").into();
		let ops: Value =
			SqlValue::parse("[{ op: 'move', path: '/temp', from: '/test/other' }]").into();
		let res: Value = SqlValue::parse("{ test: { some: 123 }, temp: ':3' }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_test_embedded() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: 'test', something: 123 }, temp: true }").into();
		let ops: Value = SqlValue::parse(
			"[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 123 }]",
		)
		.into();
		let res: Value = SqlValue::parse("{ test: { something: 123 }, temp: true }").into();
		val.patch(ops).unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_invalid() {
		// See https://github.com/surrealdb/surrealdb/issues/2001
		let mut val: Value =
			SqlValue::parse("{ test: { other: 'test', something: 123 }, temp: true }").into();
		let ops: Value =
			SqlValue::parse("[{ op: 'change', path: '/test/other', value: 'text' }]").into();
		assert!(val.patch(ops).is_err());
	}

	#[tokio::test]
	async fn patch_test_invalid() {
		let mut val: Value =
			SqlValue::parse("{ test: { other: 'test', something: 123 }, temp: true }").into();
		let should = val.clone();
		let ops: Value = SqlValue::parse(
			"[{ op: 'remove', path: '/test/other' }, { op: 'test', path: '/test/something', value: 'not same' }]",
		).into();
		assert!(val.patch(ops).is_err());
		// It is important to test if patches applied even if test operation fails
		assert_eq!(val, should);
	}
}*/
