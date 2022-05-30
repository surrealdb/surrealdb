use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::graph::Dir;

impl<'a> Document<'a> {
	pub async fn edges(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Store the record edges
		if let Workable::Relate(l, r) = &self.extras {
			// Store the left pointer edge
			let key = crate::key::graph::new(opt.ns(), opt.db(), &l.tb, &l.id, &Dir::Out, rid);
			run.set(key, self).await?;
			// Store the left inner edge
			let key = crate::key::graph::new(opt.ns(), opt.db(), &rid.tb, &rid.id, &Dir::In, l);
			run.set(key, self).await?;
			// Store the right inner edge
			let key = crate::key::graph::new(opt.ns(), opt.db(), &rid.tb, &rid.id, &Dir::Out, r);
			run.set(key, self).await?;
			// Store the right pointer edge
			let key = crate::key::graph::new(opt.ns(), opt.db(), &r.tb, &r.id, &Dir::In, rid);
			run.set(key, self).await?;
		}
		// Carry on
		Ok(())
	}
}
