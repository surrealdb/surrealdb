use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::{Options, Statement};
use crate::doc::Document;

impl Document {
	pub async fn process_changefeeds(
		&self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<()> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Get the NS + DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the table for this record
		let tbv = self.tb(ctx, opt).await?;
		// Get the database for this record
		let dbv = self.db(ctx, opt).await?;
		// Get the changefeed definition on the database
		let dbcf = dbv.as_ref().changefeed.as_ref();
		// Get the changefeed definition on the table
		let tbcf = tbv.as_ref().changefeed.as_ref();
		// Check if changefeeds are enabled
		if let Some(cf) = dbcf.or(tbcf) {
			// Create the changefeed entry
			if let Some(id) = &self.id {
				ctx.tx().lock().await.record_change(
					ns,
					db,
					tbv.name.as_str(),
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
