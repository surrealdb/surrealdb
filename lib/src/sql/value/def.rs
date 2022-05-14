use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::part::Part;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use once_cell::sync::Lazy;

static RID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);

impl Value {
	pub async fn def(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: &Thing,
	) -> Result<(), Error> {
		self.set(ctx, opt, txn, RID.as_ref(), val.clone().into()).await
	}
}
