use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub async fn merge(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		val: &Value,
	) -> Result<(), Error> {
		match val.compute(ctx, opt, txn, Some(self)).await? {
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
