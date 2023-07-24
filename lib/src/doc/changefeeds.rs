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
		// Get the table for the record
		let tb = self.tb(opt, txn).await?;
		// Check if changefeeds are enabled
		if tb.changefeed.is_some() {
			// Clone transaction
			let run = txn.clone();
			// Claim transaction
			let mut run = run.lock().await;
			// Get the arguments
			let ns = opt.ns();
			let db = opt.db();
			let tb = tb.name.as_str();
			let id = self.id.as_ref().unwrap();
			// Create the changefeed entry
			run.record_change(ns, db, tb, id, self.current.doc.clone());
		}
		// Carry on
		Ok(())
	}
}
