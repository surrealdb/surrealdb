use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn changefeeds(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		//
		let tb = self.tb(opt, txn).await?;
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Get the database and the table for the record
		let db = run.add_and_cache_db(opt.ns(), opt.db(), opt.strict).await?;
		// Check if changefeeds are enabled
		if let Some(cf) = db.as_ref().changefeed.as_ref().or(tb.as_ref().changefeed.as_ref()) {
			// Get the arguments
			let tb = tb.name.as_str();
			let id = self.id.as_ref().unwrap();
			// Create the changefeed entry
			run.record_change(
				opt.ns(),
				opt.db(),
				tb,
				id,
				self.initial.doc.clone(),
				self.current.doc.clone(),
				cf.store_original,
			);
		}
		// Carry on
		Ok(())
	}
}
