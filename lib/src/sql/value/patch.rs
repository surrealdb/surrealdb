use crate::err::Error;
use crate::sql::operation::Op;
use crate::sql::value::Value;

impl Value {
	pub(crate) fn patch(&mut self, val: Value) -> Result<(), Error> {
		let mut tmp_val = self.clone();

		for o in val.to_operations()?.into_iter() {
			match o.op {
				Op::Add => match tmp_val.pick(&o.path) {
					Value::Array(_) => tmp_val.inc(&o.path, o.value),
					_ => tmp_val.put(&o.path, o.value),
				},
				Op::Remove => tmp_val.cut(&o.path),
				Op::Replace => tmp_val.put(&o.path, o.value),
				Op::Change => {
					if let Value::Strand(p) = o.value {
						if let Value::Strand(v) = tmp_val.pick(&o.path) {
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
							tmp_val.put(&o.path, Value::from(txt));
						}
					}
				}
				Op::Test => {
					let found_val = tmp_val.pick(&o.path);

					if o.value != tmp_val.pick(&o.path) {
						return Err(Error::PatchTestFail {
							expected: o.value,
							got: found_val,
						});
					}
				}
				Op::None => (),
			}
		}

		*self = tmp_val;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

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
