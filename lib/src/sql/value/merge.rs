use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
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
			v if v.is_object() => {
				for k in v.every(false).iter() {
					match v.get(ctx, opt, txn, &k.0).await? {
						Value::None => self.del(ctx, opt, txn, &k.0).await?,
						v => self.set(ctx, opt, txn, &k.0, v).await?,
					}
				}
				Ok(())
			}
			_ => Ok(()),
		}
	}
}
