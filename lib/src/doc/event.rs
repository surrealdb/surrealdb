use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use std::ops::Deref;

impl<'a> Document<'a> {
	pub async fn event(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check events
		if !opt.events {
			return Ok(());
		}
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Loop through all event statements
		for ev in self.ev(opt, txn).await?.iter() {
			// Get the event action
			let met = if self.initial.is_none() {
				Value::from("CREATE")
			} else if self.current.is_none() {
				Value::from("DELETE")
			} else {
				Value::from("UPDATE")
			};
			// Configure the context
			let mut ctx = Context::new(ctx);
			ctx.add_value("event".into(), met);
			ctx.add_value("value".into(), self.current.deref().clone());
			ctx.add_value("after".into(), self.current.deref().clone());
			ctx.add_value("before".into(), self.initial.deref().clone());
			let ctx = ctx.freeze();
			// Ensure event queries run
			let opt = &opt.perms(false);
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
