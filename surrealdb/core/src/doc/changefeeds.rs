use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::Document;

impl Document {
	pub async fn process_changefeeds(&self, ctx: &FrozenContext, opt: &Options) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Check if changed
		if !self.is_modified() {
			return Ok(());
		}
		// Get the namespace for this record
		let ns = self.doc_ctx.ns();
		// Get the database for this record
		let db = self.doc_ctx.db();
		// Get the table for this record
		let tb = self.doc_ctx.tb()?;
		// Get the changefeed definition on the database
		let dbcf = db.changefeed.as_ref();
		// Get the changefeed definition on the table
		let tbcf = tb.changefeed.as_ref();
		// Check if changefeeds are enabled
		if let Some(cf) = dbcf.or(tbcf) {
			// Create the changefeed entry
			if let Some(id) = &self.id {
				ctx.tx().changefeed_buffer_record_change(
					ns.namespace_id,
					db.database_id,
					&tb.name,
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
