use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::number::Number;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn decrement(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		match self.get(ctx, opt, txn, path).await? {
			Value::Number(v) => match val {
				Value::Number(x) => self.set(ctx, opt, txn, path, Value::from(v - x)).await,
				_ => Ok(()),
			},
			Value::Array(v) => match val {
				Value::Array(x) => self.set(ctx, opt, txn, path, Value::from(v - x)).await,
				x => self.set(ctx, opt, txn, path, Value::from(v - x)).await,
			},
			Value::None => match val {
				Value::Number(x) => {
					self.set(ctx, opt, txn, path, Value::from(Number::from(0) - x)).await
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
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn decrement_none() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("other");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 100, other: -10 }");
		val.decrement(&ctx, &opt, &txn, &idi, Value::from(10)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_number() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 90 }");
		val.decrement(&ctx, &opt, &txn, &idi, Value::from(10)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_number() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test[1]");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 190, 300] }");
		val.decrement(&ctx, &opt, &txn, &idi, Value::from(10)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_value() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 300] }");
		val.decrement(&ctx, &opt, &txn, &idi, Value::from(200)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn decrement_array_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [200] }");
		val.decrement(&ctx, &opt, &txn, &idi, Value::parse("[100, 300]")).await.unwrap();
		assert_eq!(res, val);
	}
}
