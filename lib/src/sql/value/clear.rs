use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub async fn clear(
		&mut self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
	) -> Result<(), Error> {
		*self = Value::base();
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn clear_none() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{}");
		val.clear(&ctx, &opt, &txn).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn clear_path() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{}");
		val.clear(&ctx, &opt, &txn).await.unwrap();
		assert_eq!(res, val);
	}
}
