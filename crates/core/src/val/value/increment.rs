use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::FlowResultExt as _;
use crate::expr::part::Part;
use crate::val::{Number, Value};

impl Value {
	/// Asynchronous method for incrementing a field in a `Value`
	pub(crate) async fn increment(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<()> {
		match self.get(stk, ctx, opt, None, path).await.catch_return()? {
			Value::Number(v) => match val {
				Value::Number(x) => self.set(stk, ctx, opt, path, Value::Number(v + x)).await,
				_ => Ok(()),
			},
			Value::Array(v) => match val {
				Value::Array(x) => self.set(stk, ctx, opt, path, Value::Array(v.concat(x))).await,
				x => self.set(stk, ctx, opt, path, Value::Array(v.with_push(x))).await,
			},
			Value::None => match val {
				Value::Number(x) => {
					self.set(stk, ctx, opt, path, Value::Number(Number::Int(0) + x)).await
				}
				Value::Array(x) => self.set(stk, ctx, opt, path, Value::Array(x)).await,
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
	use crate::syn;

	#[tokio::test]
	async fn increment_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("other").unwrap().into();
		let mut val = syn::value("{ test: 100 }").unwrap();
		let res = syn::value("{ test: 100, other: +10 }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_number() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = syn::value("{ test: 100 }").unwrap();
		let res = syn::value("{ test: 110 }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_number() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test[1]").unwrap().into();
		let mut val = syn::value("{ test: [100, 200, 300] }").unwrap();
		let res = syn::value("{ test: [100, 210, 300] }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_value() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = syn::value("{ test: [100, 200, 300] }").unwrap();
		let res = syn::value("{ test: [100, 200, 300, 200] }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(200)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_array_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = syn::value("{ test: [100, 200, 300] }").unwrap();
		let res = syn::value("{ test: [100, 200, 300, 100, 300, 400, 500] }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				val.increment(stk, &ctx, &opt, &idi, syn::value("[100, 300, 400, 500]").unwrap())
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
