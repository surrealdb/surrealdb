use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn changefeeds(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let txn = ctx.try_clone_transaction()?;
		let _ = self.id.as_ref().unwrap();
		let tb = self.tb(opt, &txn).await?;
		let tb = tb.as_ref();
		if tb.changefeed.is_some() {
			// Clone transaction
			let txn = ctx.try_clone_transaction()?;
			// Claim transaction
			let mut run = txn.lock().await;

			let id = &(*self.id.as_ref().unwrap()).clone();
			// Create the changefeed entry
			run.record_change(tb, id, self.current.clone());
		}
		// Carry on
		Ok(())
	}
}
