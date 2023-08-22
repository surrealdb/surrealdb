use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn store(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Claim transaction
		let mut run = txn.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get ns and db ids
		let ns = run.add_and_cache_ns(opt.ns(), opt.strict).await?;
		let ns = ns.id.unwrap();
		let db = run.add_and_cache_db(opt.ns(), opt.db(), opt.strict).await?;
		let db = db.id.unwrap();
		let tb = run.add_and_cache_tb(opt.ns(), opt.db(), &rid.tb, opt.strict).await?;
		let tb = tb.id.unwrap();
		// Store the record data
		let key = crate::key::thing::new(ns, db, tb, &rid.id);
		run.set(key, self).await?;
		// Carry on
		Ok(())
	}
}
