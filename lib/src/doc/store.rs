use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::key::thing;

impl<'a> Document<'a> {
	pub async fn store(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement,
	) -> Result<(), Error> {
		let md = self.id.as_ref().unwrap();
		let key = thing::new(opt.ns(), opt.db(), &md.tb, &md.id);
		txn.clone().lock().await.set(key, self).await?;
		Ok(())
	}
}
