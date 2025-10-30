use anyhow::Result;
use reblessive::tree::{Stk, TreeStack};

use crate::catalog::EventDefinition;
use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Statement};
use crate::doc::{CursorDoc, Document};
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
			let input = self.compute_input_value(stk, ctx, opt, stm).await?;
			// Depending on type of event, how do we populate the document
			let doc = if stm.is_delete() {
				&mut self.initial
			} else {
				&mut self.current
			};
			// Configure the context
			#[cfg(not(target_family = "wasm"))]
			let mut ctx = if ev.concurrently {
				// Only available in non-wasm environments
				MutableContext::new_concurrent(ctx)
			} else {
				MutableContext::new(ctx)
			};
			#[cfg(target_family = "wasm")]
			let mut ctx = MutableContext::new(ctx);
			ctx.add_value("event", evt.into());
			ctx.add_value("value", doc.doc.as_arc());
			ctx.add_value("after", after);
			ctx.add_value("before", before);
			ctx.add_value("input", input.unwrap_or_default());
			// Freeze the context
			let ctx = ctx.freeze();
			// Process conditional clause
			#[cfg(not(target_family = "wasm"))]
			if ev.concurrently {
				let ev = ev.clone();
				let opt = opt.clone();
				let doc = doc.clone();
				tokio::spawn(async move {
					let mut stack = TreeStack::new();
					stack.enter(|stk| process_event(stk, &ev, ctx, &opt, &doc)).finish().await
				});
			} else {
				process_event(stk, ev, ctx, opt, doc).await?;
			}
			#[cfg(target_family = "wasm")]
			{
				if ev.concurrently {
					warn!(
						"CONCURRENTLY running events are not supported in WASM, they will run synchronously like regular events"
					);
				}
				process_event(stk, ev, ctx, opt, doc).await?;
			}
		}
		// Carry on
		Ok(())
	}
}

async fn process_event(
	stk: &mut Stk,
	ev: &EventDefinition,
	ctx: Context,
	opt: &Options,
	doc: &CursorDoc,
) -> Result<()> {
	let val = stk
		.run(|stk| ev.when.compute(stk, &ctx, opt, Some(doc)))
		.await
		.catch_return()
		.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
	// Execute event if value is truthy
	if val.is_truthy() {
		for v in ev.then.iter() {
			stk.run(|stk| v.compute(stk, &ctx, opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
		}
	}
	Ok(())
}
