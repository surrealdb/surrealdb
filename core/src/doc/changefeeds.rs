use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub async fn changefeeds(
		&self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the database and the table for the record
		let db = txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
		// Check if changefeeds are enabled
		if let Some(cf) = db.as_ref().changefeed.as_ref().or(tb.as_ref().changefeed.as_ref()) {
			// Create the changefeed entry
			if let Some(id) = &self.id {
				txn.lock().await.record_change(
					opt.ns()?,
					opt.db()?,
					tb.name.as_str(),
					id.as_ref(),
					self.initial.doc.as_ref(),
					self.current.doc.as_ref(),
					cf.store_diff,
				);
			}
		}
		// Carry on
		Ok(())
	}
}
