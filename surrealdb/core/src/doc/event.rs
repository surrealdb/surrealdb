use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;

use crate::catalog::EventDefinition;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::{Action, CursorDoc, Document, DocumentContext};
use crate::expr::FlowResultExt as _;
use crate::iam::AuthLimit;
use crate::kvs::impl_kv_value_revisioned;
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
			let async_event_record = AsyncEventRecord {
				attempt: 0,
				event: evt.into(),
				doc: doc.doc.as_arc(),
				after,
				before,
				input: input.clone().unwrap_or_default(),
			};
			let ctx = async_event_record.build_event_context(ctx);
			// Process conditional clause
			let val = stk
				.run(|stk| ev.when.compute(stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
			// Execute event if value is truthy
			if val.is_truthy() {
				if ev.asynchronous {
					Self::process_async(ctx, opt, async_event_record, ev, &self.doc_ctx).await?;
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
		event_record: AsyncEventRecord,
		ev: &EventDefinition,
		doc_ctx: &DocumentContext,
	) -> Result<()> {
		let event_id = ctx.try_get_event_manager()?.get_next_event_id()?;
		let node_id = opt.id();
		let dur = UNIX_EPOCH.elapsed()?;
		let ts_id = dur.as_secs() * 1_000_000 + u64::from(dur.subsec_micros());
		let db = doc_ctx.db();
		let tx = ctx.tx();
		let key = crate::key::table::eq::Eq::new(
			db.namespace_id,
			db.database_id,
			&ev.target_table,
			&ev.name,
			ts_id,
			event_id,
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
pub(crate) struct AsyncEventRecord {
	attempt: u16,
	event: Arc<Value>,
	doc: Arc<Value>,
	after: Arc<Value>,
	before: Arc<Value>,
	input: Arc<Value>,
}

impl_kv_value_revisioned!(AsyncEventRecord);

impl AsyncEventRecord {
	fn build_event_context(&self, ctx: &FrozenContext) -> FrozenContext {
		let mut ctx = Context::new(ctx);
		ctx.add_value("event", self.event.clone());
		ctx.add_value("value", self.doc.clone());
		ctx.add_value("after", self.after.clone());
		ctx.add_value("before", self.before.clone());
		ctx.add_value("input", self.input.clone());
		ctx.freeze()
	}
}

#[derive(Default, Clone)]
pub(crate) struct EventManager {
	event_id_sequence: Arc<AtomicU64>,
}

impl EventManager {
	fn get_next_event_id(&self) -> Result<u64> {
		Ok(self.event_id_sequence.fetch_add(1, Ordering::Relaxed))
	}
}
