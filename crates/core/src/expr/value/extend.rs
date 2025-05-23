use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::FlowResultExt as _;
use crate::expr::array::Uniq;
use crate::expr::part::Part;
use crate::expr::value::Value;
use anyhow::Result;
use reblessive::tree::Stk;

impl Value {
	pub(crate) async fn extend(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<()> {
		match self.get(stk, ctx, opt, None, path).await.catch_return()? {
			Value::Array(v) => match val {
				Value::Array(x) => self.set(stk, ctx, opt, path, Value::from((v + x).uniq())).await,
				x => self.set(stk, ctx, opt, path, Value::from((v + x).uniq())).await,
			},
			Value::None => match val {
				Value::Array(x) => self.set(stk, ctx, opt, path, Value::from(x)).await,
				x => self.set(stk, ctx, opt, path, Value::from(vec![x])).await,
			},
			_ => Ok(()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::sql::SqlValue;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn extend_array_value() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let res: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.extend(stk, &ctx, &opt, &idi, Value::from(200)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn extend_array_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let res: Value = SqlValue::parse("{ test: [100, 200, 300, 400, 500] }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				val.extend(stk, &ctx, &opt, &idi, SqlValue::parse("[100, 300, 400, 500]").into())
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
