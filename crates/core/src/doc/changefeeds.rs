use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::kvs::cache;

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
		// Get the NS + DB
		let ns = opt.ns()?;
		let db = opt.db()?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the table
		let tbv = self.tb(ctx, opt).await?;
		// Get the database and the table for the record
		let dbv = match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) if txn.local() => {
				// Get the cache entry key
				let key = cache::ds::Lookup::Db(ns, db);
				// Get or update the cache entry
				match cache.get(&key) {
					Some(val) => val,
					None => {
						let val = txn.get_or_add_db(ns, db, opt.strict).await?;
						let val = cache::ds::Entry::Any(val.clone());
						cache.insert(key, val.clone());
						val
					}
				}
				.try_into_type()
			}
			// No cache is present on the context
			_ => txn.get_or_add_db(ns, db, opt.strict).await,
		}?;
		// Get the changefeed definition on the database
		let dbcf = dbv.as_ref().changefeed.as_ref();
		// Get the changefeed definition on the table
		let tbcf = tbv.as_ref().changefeed.as_ref();
		// Check if changefeeds are enabled
		if let Some(cf) = dbcf.or(tbcf) {
			// Create the changefeed entry
			if let Some(id) = &self.id {
				txn.lock().await.record_change(
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
