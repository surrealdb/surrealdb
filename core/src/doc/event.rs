use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use std::ops::Deref;

impl<'a> Document<'a> {
	pub async fn event(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
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
		for ev in self.ev(ctx, opt).await?.iter() {
			// Get the event action
			let evt = if stm.is_delete() {
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
			ctx.add_value("event", evt);
			ctx.add_value("value", doc.doc.deref());
			ctx.add_value("after", self.current.doc.deref());
			ctx.add_value("before", self.initial.doc.deref());
			// Process conditional clause
			let val = ev.when.compute_bordered(stk, &ctx, opt, Some(doc)).await?;
			// Execute event if value is truthy
			if val.is_truthy() {
				for v in ev.then.iter() {
					v.compute_bordered(stk, &ctx, opt, Some(doc)).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
