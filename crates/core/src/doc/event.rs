use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::expr::FlowResultExt as _;
use crate::val::Value;

impl Document {
	/// Processes any DEFINE EVENT clauses which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the events and processes them all
	/// within the currently running transaction.
	pub(super) async fn process_table_events(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
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
			let after = self.current.doc.as_arc();
			let before = self.initial.doc.as_arc();
			// Depending on type of event, how do we populate the document
			let doc = if stm.is_delete() {
				&mut self.initial
			} else {
				&mut self.current
			};
			// Configure the context
			let mut ctx = MutableContext::new(ctx);
			ctx.add_value("event", evt.into());
			ctx.add_value("value", doc.doc.as_arc());
			ctx.add_value("after", after);
			ctx.add_value("before", before);
			// Freeze the context
			let ctx = ctx.freeze();
			// Process conditional clause
			let val =
				stk.run(|stk| ev.when.compute(stk, &ctx, opt, Some(doc))).await.catch_return()?;
			// Execute event if value is truthy
			if val.is_truthy() {
				for v in ev.then.iter() {
					stk.run(|stk| v.compute(stk, &ctx, opt, Some(&*doc))).await.catch_return()?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
