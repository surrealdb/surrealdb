use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::array::Uniq;
use crate::sql::part::Part;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Value {
	pub(crate) async fn extend(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		match self.get(stk, ctx, opt, None, path).await? {
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
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn extend_array_value() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 200, 300] }");
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
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 200, 300, 400, 500] }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.extend(stk, &ctx, &opt, &idi, Value::parse("[100, 300, 400, 500]")))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
