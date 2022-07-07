use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub async fn merge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: Value,
	) -> Result<(), Error> {
		match val {
			Value::Object(v) => {
				for (k, v) in v {
					self.set(ctx, opt, txn, &[Part::from(k)], v).await?;
				}
				Ok(())
			}
			_ => Ok(()),
		}
	}
}
