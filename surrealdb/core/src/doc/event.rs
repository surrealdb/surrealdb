use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;

use crate::catalog::EventDefinition;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::{Action, CursorDoc, Document, DocumentContext};
use crate::expr::FlowResultExt as _;
use crate::iam::AuthLimit;
use crate::kvs::{HlcTimestamp, Transaction, impl_kv_value_revisioned};
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
		ctx: &FrozenContext,
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

		if self.ev(ctx, opt).await?.is_empty() {
			return Ok(());
		}

		let input = self.compute_input_value(stk, ctx, opt, stm).await?;

		let action = if stm.is_delete() {
			Action::Delete
		} else if self.is_new() {
			Action::Create
		} else {
			Action::Update
		};

		self.process_events(stk, ctx, opt, action, input).await
	}

	pub(super) async fn process_events(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		action: Action,
		input: Option<Arc<Value>>,
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
			// Limit auth
			let opt = AuthLimit::try_from(&ev.auth_limit)?.limit_opt(opt);
			// Get the event action
			let evt = match action {
				Action::Create => Value::from("CREATE"),
				Action::Update => Value::from("UPDATE"),
				Action::Delete => Value::from("DELETE"),
			};
			// Get the event action
			let after = self.current.doc.as_arc();
			let before = self.initial.doc.as_arc();
			// Depending on type of event, how do we populate the document
			let doc = if action == Action::Delete {
				&mut self.initial
			} else {
				&mut self.current
			};
			// Configure the context
			let event_record = EventRecord::new(
				evt.into(),
				doc.doc.as_arc(),
				after,
				before,
				input.clone().unwrap_or_default(),
			);
			let ctx = event_record.build_event_context(ctx);
			// Process conditional clause
			let val = stk
				.run(|stk| ev.when.compute(stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
			// Execute event if value is truthy
			if val.is_truthy() {
				if ev.asynchronous {
					Self::process_async(ctx, opt, event_record, ev, &self.doc_ctx).await?;
				} else {
					Self::process_sync(stk, ctx, opt, ev, doc).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn process_async(
		ctx: FrozenContext,
		opt: Options,
		event_record: EventRecord,
		ev: &EventDefinition,
		doc_ctx: &DocumentContext,
	) -> Result<()> {
		let node_id = opt.id();
		let ts = HlcTimestamp::next();
		let db = doc_ctx.db();
		let tx = ctx.tx();
		let key = crate::key::table::eq::Eq::new(
			db.namespace_id,
			db.database_id,
			&ev.target_table,
			&ev.name,
			ts,
			node_id,
		);
		tx.put(&key, &event_record, None).await?;
		Ok(())
	}

	async fn process_sync(
		stk: &mut Stk,
		ctx: FrozenContext,
		opt: Options,
		ev: &EventDefinition,
		doc: &CursorDoc,
	) -> Result<()> {
		for v in ev.then.iter() {
			stk.run(|stk| v.compute(stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
pub(crate) struct EventRecord {
	attempt: u16,
	event: Arc<Value>,
	doc: Arc<Value>,
	after: Arc<Value>,
	before: Arc<Value>,
	input: Arc<Value>,
}

impl_kv_value_revisioned!(EventRecord);

impl EventRecord {
	fn new(
		event: Arc<Value>,
		doc: Arc<Value>,
		after: Arc<Value>,
		before: Arc<Value>,
		input: Arc<Value>,
	) -> Self {
		Self {
			attempt: 0,
			event,
			doc,
			after,
			before,
			input,
		}
	}

	fn build_event_context(&self, ctx: &FrozenContext) -> FrozenContext {
		let mut ctx = Context::new(ctx);
		ctx.add_value("event", self.event.clone());
		ctx.add_value("value", self.doc.clone());
		ctx.add_value("after", self.after.clone());
		ctx.add_value("before", self.before.clone());
		ctx.add_value("input", self.input.clone());
		ctx.freeze()
	}

	pub(crate) async fn process_events(_tx: &Transaction) -> Result<usize> {
		todo!()
	}
}
