use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::operation::Op;
use crate::sql::value::Value;

impl Value {
	pub async fn patch(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		val: &Array,
	) -> Result<(), Error> {
		for o in val.to_operations()?.into_iter() {
			match o.op {
				Op::Add => match self.get(ctx, opt, exe, &o.path).await? {
					Value::Array(_) => self.increment(ctx, opt, exe, &o.path, o.value).await?,
					_ => self.set(ctx, opt, exe, &o.path, o.value).await?,
				},
				Op::Remove => self.del(ctx, opt, exe, &o.path).await?,
				Op::Replace => self.set(ctx, opt, exe, &o.path, o.value).await?,
				Op::Change => match o.value {
					Value::Strand(p) => match self.get(ctx, opt, exe, &o.path).await? {
						Value::Strand(v) => {
							let mut dmp = dmp::new();
							let mut pch = dmp.patch_from_text(p.value);
							let (txt, _) = dmp.patch_apply(&mut pch, &v.value);
							let txt = txt.into_iter().collect::<String>();
							self.set(ctx, opt, exe, &o.path, Value::from(txt)).await?;
							()
						}
						_ => (),
					},
					_ => (),
				},
				_ => (),
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn patch_add_simple() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Array::parse("[{ op: 'add', path: '/temp', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Array::parse("[{ op: 'remove', path: '/temp' }]");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Array::parse("[{ op: 'replace', path: '/temp', value: 'text' }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: 'test' }");
		let ops = Array::parse(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Array::parse("[{ op: 'add', path: '/temp/test', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: { test: true } }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Array::parse("[{ op: 'remove', path: '/test/other' }]");
		let res = Value::parse("{ test: { something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Array::parse("[{ op: 'replace', path: '/test/other', value: 'text' }]");
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Array::parse(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &exe, &ops).await.unwrap();
		assert_eq!(res, val);
	}
}
