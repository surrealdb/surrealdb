use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
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
		// Check import
		if opt.import {
			return Ok(());
		}
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Don't run permissions
		let opt = &opt.new_with_perms(false);
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
			// Depending on type of event, how do we populate the document
			let doc = match stm.is_delete() {
				true => &self.initial,
				false => &self.current,
			};
			// Configure the context
			let mut ctx = Context::new(ctx);
			ctx.add_value("event", met);
			ctx.add_value("value", doc.doc.deref());
			ctx.add_value("after", self.current.doc.deref());
			ctx.add_value("before", self.initial.doc.deref());
			// Process conditional clause
			let val = ev.when.compute(&ctx, opt, txn, Some(doc)).await?;
			// Execute event if value is truthy
			if val.is_truthy() {
				for v in ev.then.iter() {
					v.compute(&ctx, opt, txn, Some(doc)).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
