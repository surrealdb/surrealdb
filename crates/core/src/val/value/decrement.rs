use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::FlowResultExt as _;
use crate::expr::part::Part;
use crate::val::Number;
use crate::val::Value;
use anyhow::Result;
use reblessive::tree::Stk;

impl Value {
	/// Asynchronous method for decrementing a field in a `Value`
	pub(crate) async fn decrement(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<()> {
		match self.get(stk, ctx, opt, None, path).await.catch_return()? {
			Value::Number(v) => match val {
				Value::Number(x) => self.set(stk, ctx, opt, path, Value::from(v - x)).await,
				_ => Ok(()),
			},
			Value::Array(v) => match val {
				Value::Array(x) => self.set(stk, ctx, opt, path, Value::from(v - x)).await,
				x => self.set(stk, ctx, opt, path, Value::from(v - x)).await,
			},
			Value::None => match val {
				Value::Number(x) => {
					self.set(stk, ctx, opt, path, Value::from(Number::from(0) - x)).await
				}
				_ => Ok(()),
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
	async fn decrement_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("other").into();
		let mut val: Value = SqlValue::parse("{ test: 100 }").into();
		let res: Value = SqlValue::parse("{ test: 100, other: -10 }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_number() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: 100 }").into();
		let res: Value = SqlValue::parse("{ test: 90 }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_number() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test[1]").into();
		let mut val: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let res: Value = SqlValue::parse("{ test: [100, 190, 300] }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_value() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let res: Value = SqlValue::parse("{ test: [100, 300] }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, Value::from(200)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::parse("test").into();
		let mut val: Value = SqlValue::parse("{ test: [100, 200, 300] }").into();
		let res: Value = SqlValue::parse("{ test: [200] }").into();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, SqlValue::parse("[100,300]").into()))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
