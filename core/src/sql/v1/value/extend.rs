use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::sql::array::Uniq;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn extend(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		match self.get(ctx, opt, txn, None, path).await? {
			Value::Array(v) => match val {
				Value::Array(x) => self.set(ctx, opt, txn, path, Value::from((v + x).uniq())).await,
				x => self.set(ctx, opt, txn, path, Value::from((v + x).uniq())).await,
			},
			Value::None => match val {
				Value::Array(x) => self.set(ctx, opt, txn, path, Value::from(x)).await,
				x => self.set(ctx, opt, txn, path, Value::from(vec![x])).await,
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
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 200, 300] }");
		val.extend(&ctx, &opt, &txn, &idi, Value::from(200)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn extend_array_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 200, 300, 400, 500] }");
		val.extend(&ctx, &opt, &txn, &idi, Value::parse("[100, 300, 400, 500]")).await.unwrap();
		assert_eq!(res, val);
	}
}
