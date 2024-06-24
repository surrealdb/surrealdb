use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::number::Number;
use crate::sql::part::Part;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Value {
	/// Asynchronous method for decrementing a field in a `Value`
	pub(crate) async fn decrement(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		match self.get(stk, ctx, opt, None, path).await? {
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
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn decrement_none() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("other");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 100, other: -10 }");
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
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 90 }");
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
		let idi = Idiom::parse("test[1]");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 190, 300] }");
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
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 300] }");
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
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [200] }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.decrement(stk, &ctx, &opt, &idi, Value::parse("[100,300]")))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
