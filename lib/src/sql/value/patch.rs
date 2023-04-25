use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::operation::Op;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn patch(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: Value,
	) -> Result<(), Error> {
		for o in val.to_operations()?.into_iter() {
			match o.op {
				Op::Add => match self.get(ctx, opt, txn, &o.path).await? {
					Value::Array(_) => self.increment(ctx, opt, txn, &o.path, o.value).await?,
					_ => self.set(ctx, opt, txn, &o.path, o.value).await?,
				},
				Op::Remove => self.del(ctx, opt, txn, &o.path).await?,
				Op::Replace => self.set(ctx, opt, txn, &o.path, o.value).await?,
				Op::Change => {
					if let Value::Strand(p) = o.value {
						if let Value::Strand(v) = self.get(ctx, opt, txn, &o.path).await? {
							let mut dmp = dmp::new();
							let mut pch = dmp.patch_from_text(p.as_string());
							let (txt, _) = dmp.patch_apply(&mut pch, v.as_str());
							let txt = txt.into_iter().collect::<String>();
							self.set(ctx, opt, txn, &o.path, Value::from(txt)).await?;
						}
					}
				}
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
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Value::parse("[{ op: 'add', path: '/temp', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_simple() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/temp' }]");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_simple() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'replace', path: '/temp', value: 'text' }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_simple() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: 'test' }");
		let ops = Value::parse(
			"[{ op: 'change', path: '/temp', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: 'text' }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_add_embedded() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let ops = Value::parse("[{ op: 'add', path: '/temp/test', value: true }]");
		let res = Value::parse("{ test: { other: null, something: 123 }, temp: { test: true } }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_remove_embedded() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'remove', path: '/test/other' }]");
		let res = Value::parse("{ test: { something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_replace_embedded() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 }, temp: true }");
		let ops = Value::parse("[{ op: 'replace', path: '/test/other', value: 'text' }]");
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn patch_change_embedded() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: 'test', something: 123 }, temp: true }");
		let ops = Value::parse(
			"[{ op: 'change', path: '/test/other', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }]",
		);
		let res = Value::parse("{ test: { other: 'text', something: 123 }, temp: true }");
		val.patch(&ctx, &opt, &txn, ops).await.unwrap();
		assert_eq!(res, val);
	}
}
