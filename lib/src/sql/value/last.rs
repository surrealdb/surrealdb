use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub async fn last(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Self, Error> {
		self.get(ctx, opt, txn, &[Part::Last]).await
	}
}
