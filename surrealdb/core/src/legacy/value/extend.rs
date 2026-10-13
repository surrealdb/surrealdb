use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exe::FlowResultExt as _;
use crate::expr::part::Part;
use crate::val::Value;
use crate::val::array::Uniq;

pub(crate) async fn value_extend(
	this: &mut Value,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	path: &[Part],
	val: Value,
) -> Result<()> {
	match crate::legacy::value_get(this, stk, ctx, opt, None, path).await.catch_return()? {
		Value::Array(v) => match val {
			Value::Array(x) => {
				crate::legacy::value_set(this, stk, ctx, opt, path, Value::from(v.concat(x).uniq()))
					.await
			}
			x => {
				crate::legacy::value_set(
					this,
					stk,
					ctx,
					opt,
					path,
					Value::from(v.with_push(x).uniq()),
				)
				.await
			}
		},
		Value::None => match val {
			Value::Array(x) => {
				crate::legacy::value_set(this, stk, ctx, opt, path, Value::from(x)).await
			}
			x => crate::legacy::value_set(this, stk, ctx, opt, path, Value::from(vec![x])).await,
		},
		v => bail!(crate::expr::Error::TryExtend(v.kind_of().to_owned())),
	}
}

#[cfg(test)]
mod tests {

	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::syn;
	use crate::val::Value;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn extend_array_value() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300] }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_extend(&mut val, stk, &ctx, &opt, &idi, Value::from(200))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn extend_array_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 400, 500] }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_extend(
					&mut val,
					stk,
					&ctx,
					&opt,
					&idi,
					parse_val!("[100, 300, 400, 500]"),
				)
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn extend_number_errors() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: 100 }");
		let mut stack = reblessive::TreeStack::new();
		let result = stack
			.enter(|stk| {
				crate::legacy::value_extend(&mut val, stk, &ctx, &opt, &idi, Value::from(200))
			})
			.finish()
			.await;
		assert!(result.is_err());
	}

	#[tokio::test]
	async fn extend_object_errors() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { a: 1 } }");
		let mut stack = reblessive::TreeStack::new();
		let result = stack
			.enter(|stk| {
				crate::legacy::value_extend(
					&mut val,
					stk,
					&ctx,
					&opt,
					&idi,
					parse_val!("[1, 2, 3]"),
				)
			})
			.finish()
			.await;
		assert!(result.is_err());
	}
}
