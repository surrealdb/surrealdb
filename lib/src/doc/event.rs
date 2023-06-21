use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use std::ops::Deref;

impl<'a> Document<'a> {
	pub async fn event(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
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
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Loop through all event statements
		for ev in self.ev(opt, &txn).await?.iter() {
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
			ctx.add_value("event", met);
			ctx.add_value("value", self.current.deref());
			ctx.add_value("after", self.current.deref());
			ctx.add_value("before", self.initial.deref());
			ctx.add_cursor_doc(&self.current);
			// Process conditional clause
			let val = ev.when.compute(&ctx, opt).await?;
			// Execute event if value is truthy
			if val.is_truthy() {
				for v in ev.then.iter() {
					v.compute(&ctx, opt).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
