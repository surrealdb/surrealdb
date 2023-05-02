use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn replace(
		&mut self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		val: Value,
	) -> Result<(), Error> {
		// Clear all entries
		match val {
			Value::Object(v) => {
				*self = Value::from(v);
				Ok(())
			}
			_ => Ok(()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn replace() {
		let (ctx, opt, txn) = mock().await;
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ other: true }");
		let obj = Value::parse("{ other: true }");
		val.replace(&ctx, &opt, &txn, obj).await.unwrap();
		assert_eq!(res, val);
	}
}
