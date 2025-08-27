use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::FlowResultExt as _;
use crate::expr::part::Part;
use crate::val::Value;
use crate::val::array::Uniq;

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
				Value::Array(x) => {
					self.set(stk, ctx, opt, path, Value::from(v.concat(x).uniq())).await
				}
				x => self.set(stk, ctx, opt, path, Value::from(v.with_push(x).uniq())).await,
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
	use crate::syn;

	#[tokio::test]
	async fn extend_array_value() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = syn::value("{ test: [100, 200, 300] }").unwrap();
		let res = syn::value("{ test: [100, 200, 300] }").unwrap();
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
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = syn::value("{ test: [100, 200, 300] }").unwrap();
		let res = syn::value("{ test: [100, 200, 300, 400, 500] }").unwrap();
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				val.extend(stk, &ctx, &opt, &idi, syn::value("[100, 300, 400, 500]").unwrap())
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
