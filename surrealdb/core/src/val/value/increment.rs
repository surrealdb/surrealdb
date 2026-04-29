use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::FlowResultExt as _;
use crate::expr::part::Part;
use crate::val::{Number, TryAdd, Value};

impl Value {
	/// Asynchronous method for incrementing a field in a `Value`
	pub(crate) async fn increment(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<()> {
		let current = self.get(stk, ctx, opt, None, path).await.catch_return()?;

		let next = match current {
			Value::None => match val {
				Value::Number(x) => Value::Number(Number::Int(0) + x),
				Value::Duration(x) => Value::Duration(x),
				Value::Array(x) => Value::Array(x),
				Value::Set(x) => Value::Set(x),
				x => Value::from(vec![x]),
			},

			Value::Array(v) => match val {
				Value::Array(x) => Value::Array(v.concat(x)),
				Value::Set(x) => Value::Array(v.concat_set(x)),
				x => Value::Array(v.with_push(x)),
			},

			Value::Set(mut s) => match val {
				Value::Set(x) => {
					s.0.extend(x.0);
					Value::Set(s)
				}
				Value::Array(x) => {
					s.0.extend(x.0);
					Value::Set(s)
				}
				x => {
					s.insert(x);
					Value::Set(s)
				}
			},

			v => v.try_add(val)?,
		};

		self.set(stk, ctx, opt, path, next).await
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn increment_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("other").unwrap().into();
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 100, other: +10 }");
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
		let mut val = parse_val!("{ test: 100 }");
		let res = parse_val!("{ test: 110 }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(10)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_string() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: 'hello' }");
		let res = parse_val!("{ test: 'hello world' }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(" world")))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_object() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { a: 1 } }");
		let res = parse_val!("{ test: { a: 1, b: 2 } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, parse_val!("{ b: 2 }")))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn increment_object_number_errors() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { a: 1 } }");
		let mut stack = reblessive::TreeStack::new();
		let result =
			stack.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from(10))).finish().await;
		assert!(result.is_err());
	}

	#[tokio::test]
	async fn increment_number_string_errors() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: 100 }");
		let mut stack = reblessive::TreeStack::new();
		let result = stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, Value::from("hello")))
			.finish()
			.await;
		assert!(result.is_err());
	}

	#[tokio::test]
	async fn increment_array_number() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test[1]").unwrap().into();
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 210, 300] }");
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
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 200] }");
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
		let mut val = parse_val!("{ test: [100, 200, 300] }");
		let res = parse_val!("{ test: [100, 200, 300, 100, 300, 400, 500] }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.increment(stk, &ctx, &opt, &idi, parse_val!("[100, 300, 400, 500]")))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
