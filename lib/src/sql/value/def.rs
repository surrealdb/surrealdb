use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn def(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: &Thing,
	) -> Result<(), Error> {
		self.set(ctx, opt, txn, ID.as_ref(), val.clone().into()).await
	}
}
