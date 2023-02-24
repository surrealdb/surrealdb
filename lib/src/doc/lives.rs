use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let _ = self.id.as_ref().unwrap();
		// Loop through all index statements
		for lv in self.lv(opt, txn).await?.iter() {
			// Create a new statement
			let lq = Statement::from(lv);
			// TODO check auth tokens/scope here
			// Check LIVE SELECT where condition
			if self.check(ctx, opt, txn, &lq).await.is_err() {
				continue;
			}
			// Check what type of data change this is
			if stm.is_delete() {
				// Send a DELETE notification to the WebSocket
			} else if self.is_new() {
				// Process the CREATE notification to send
				let _ = self.pluck(ctx, opt, txn, &lq).await?; // TODO the value based on the LQ. Diff vs fields
			} else {
				// Process the CREATE notification to send
				let _ = self.pluck(ctx, opt, txn, &lq).await?;
			};
		}
		// Carry on
		Ok(())
	}
}
