use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub async fn process_changefeeds(
		&self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Get the namespace
		let ns = opt.ns()?;
		// Get the database
		let db = opt.db()?;
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the database and the table for the record
		let cf = txn.get_or_add_db(ns, db, opt.strict).await?;
		// Get the changefeed definition on the database
		let dbcf = cf.as_ref().changefeed.as_ref();
		// Get the changefeed definition on the table
		let tbcf = tb.as_ref().changefeed.as_ref();
		// Check if changefeeds are enabled
		if let Some(cf) = dbcf.or(tbcf) {
			// Create the changefeed entry
			if let Some(id) = &self.id {
				txn.lock().await.record_change(
					ns,
					db,
					tb.name.as_str(),
					id.as_ref(),
					self.initial.doc.clone(),
					self.current.doc.clone(),
					cf.store_diff,
				);
			}
		}
		// Carry on
		Ok(())
	}
}
