use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use std::ops::Deref;

impl<'a> Document<'a> {
	pub async fn event(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check events
		if !opt.events {
			return Ok(());
		}
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Don't run permissions
		let opt = &opt.perms(false);
		// Loop through all event statements
		for ev in self.ev(opt, txn).await?.iter() {
			// Get the event action
			let met = if stm.is_delete() {
				Value::from("DELETE")
			} else if self.is_new() {
				Value::from("CREATE")
			} else {
				Value::from("UPDATE")
			};
			// Configure the context
			let mut ctx = Context::new(ctx);
			ctx.add_value("event".into(), met);
			ctx.add_value("value".into(), self.current.deref());
			ctx.add_value("after".into(), self.current.deref());
			ctx.add_value("before".into(), self.initial.deref());
			// Process conditional clause
			let val = ev.when.compute(&ctx, opt, txn, Some(&self.current)).await?;
			// Execute event if value is truthy
			if val.is_truthy() {
				for v in ev.then.iter() {
					v.compute(&ctx, opt, txn, Some(&self.current)).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
