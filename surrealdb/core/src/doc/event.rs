use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::TreeStack;
use reblessive::tree::Stk;
use revision::revisioned;
use surrealdb_types::ToSql;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{EventDefinition, Record};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Session, Statement};
use crate::doc::{Action, CursorDoc, CursorRecord, Document, DocumentContext};
use crate::err::Error;
use crate::expr::FlowResultExt;
use crate::iam::{Auth, AuthLimit};
use crate::key::root::eq;
use crate::kvs::TransactionType::Write;
use crate::kvs::{
	Datastore, HlcTimestamp, KVValue, LockType, Transaction, TransactionType,
	impl_kv_value_revisioned,
};
use crate::val::{RecordId, Value};

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
			let event_context = EventContext {
				auth: opt.auth.clone(),
				event: evt.into(),
				doc: doc.doc.as_arc(),
				after,
				before,
				input: input.clone().unwrap_or_default(),
			};
			let ctx = event_context.build_event_context(ctx);
			// Process conditional clause
			let val = stk
				.run(|stk| ev.when.compute(stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
			// Execute event if value is truthy
			if val.is_truthy() {
				if ev.asynchronous {
					Self::process_async(ctx, opt, event_context, ev, &self.doc_ctx, doc).await?;
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
		event_context: EventContext,
		ev: &EventDefinition,
		doc_ctx: &DocumentContext,
		cursor_doc: &CursorDoc,
	) -> Result<()> {
		let node_id = opt.id();
		let ts = HlcTimestamp::next();
		let db = doc_ctx.db();
		let tx = ctx.tx();
		let key =
			eq::Eq::new(db.namespace_id, db.database_id, &ev.target_table, &ev.name, ts, node_id);
		let event_record = AsyncEventRecord::new(&opt, event_context, ev, cursor_doc)?;
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
			let res =
				stk.run(|stk| v.compute(stk, &ctx, &opt, Some(doc))).await.catch_return().map_err(
					|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e),
				)?;
			trace!("Async event returns: {}", res.to_sql());
		}
		Ok(())
	}
}

struct EventContext {
	auth: Arc<Auth>,
	event: Arc<Value>,
	doc: Arc<Value>,
	after: Arc<Value>,
	before: Arc<Value>,
	input: Arc<Value>,
}

impl EventContext {
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub struct AsyncEventRecord {
	attempt: u16,
	max_computation_depth: u32,
	rid: Option<Arc<RecordId>>,
	cursor_record: Arc<Record>,
	fields_computed: bool,
	ns: Arc<str>,
	db: Arc<str>,
	auth: Arc<Auth>,
	event: Arc<Value>,
	after: Arc<Value>,
	before: Arc<Value>,
	input: Arc<Value>,
	event_definition: EventDefinition,
}

impl_kv_value_revisioned!(AsyncEventRecord);

impl AsyncEventRecord {
	fn new(
		opt: &Options,
		event_context: EventContext,
		event_definition: &EventDefinition,
		cursor_doc: &CursorDoc,
	) -> Result<Self> {
		let (ns, db) = opt.arc_ns_db()?;
		Ok(Self {
			attempt: 0,
			max_computation_depth: opt.dive,
			rid: cursor_doc.rid.clone(),
			cursor_record: cursor_doc.doc.clone().into_read_only(),
			fields_computed: false,
			ns,
			db,
			auth: event_context.auth,
			event: event_context.event,
			after: event_context.after,
			before: event_context.before,
			input: event_context.input,
			event_definition: event_definition.clone(),
		})
	}

	fn build_event_context(&self, ctx: &FrozenContext) -> FrozenContext {
		let mut ctx = Context::new(ctx);
		ctx.add_value("event", self.event.clone());
		ctx.add_value("value", CursorRecord::from(self.cursor_record.clone()).as_arc());
		ctx.add_value("after", self.after.clone());
		ctx.add_value("before", self.before.clone());
		ctx.add_value("input", self.input.clone());
		ctx.freeze()
	}

	async fn build_event_options(
		&self,
		tx: &Transaction,
		parent_opts: &Options,
		eq: &eq::Eq<'_>,
	) -> Result<Options> {
		let ns = tx.expect_ns_by_name(&self.ns).await?;
		if ns.namespace_id != eq.ns {
			bail!(Error::EvNamespaceMismatch(ns.name.clone()));
		}
		let db = tx.expect_db_by_name(&self.ns, &self.db).await?;
		if db.database_id != eq.db {
			bail!(Error::EvDatabaseMismatch(ns.name.clone()));
		}
		let opt = parent_opts.clone();
		let opt = opt
			.with_auth(self.auth.clone())
			.with_max_computation_depth(self.max_computation_depth)
			.with_ns(Some(self.ns.clone()))
			.with_db(Some(self.db.clone()));
		Ok(opt)
	}

	fn build_event_cursor_doc(&self) -> CursorDoc {
		CursorDoc::new(self.rid.clone(), None, self.cursor_record.clone())
	}

	pub async fn process_next_events_batch(ds: &Datastore) -> Result<usize> {
		let concurrency: usize = num_cpus::get().max(4);
		let sem = Arc::new(Semaphore::new(concurrency));

		// Collect the next batch
		let res = {
			let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
			let (beg, end) = eq::Eq::range();
			let res = catch!(tx, tx.scan(beg..end, *NORMAL_FETCH_SIZE, None).await);
			tx.cancel().await?;
			res
		};
		let count = res.len();
		let mut join_set = JoinSet::new();

		for (k, v) in res {
			let permit = sem.clone().acquire_owned().await?;
			// Setup a context
			let mut ctx = ds.setup_ctx()?;
			// Build default options
			let opt = ds.setup_options(&Session::default());
			// Extract sequences and transaction factory
			let sequences = ds.sequences().clone();
			let tf = ds.transaction_factory().clone();
			join_set.spawn(async move {
				{
					// create transaction
					let tx = tf.transaction(Write, LockType::Optimistic, sequences.clone()).await?;
					ctx.set_transaction(Arc::new(tx));
					let ctx = ctx.freeze();
					let eq = eq::Eq::decode_key(&k)?;
					let mut ev = AsyncEventRecord::kv_decode_value(v)?;
					ev.attempt += 1;
					let ev = ev;
					let tx = ctx.tx();
					match Self::process_event(&ctx, &opt, &eq, &ev).await {
						Ok(_) => {
							catch!(tx, tx.del(&k).await);
						}
						Err(e) => {
							let se: Option<&Error> = e.downcast_ref();
							if matches!(
								se,
								Some(Error::EvNamespaceMismatch(_))
									| Some(Error::EvDatabaseMismatch(_))
							) {
								// This error is final, we won't retry. The namespace or the
								// database has been recreated.
								warn!("Event processing failed: {se:?}");
								catch!(tx, tx.del(&k).await);
							} else {
								catch!(tx, Self::retry_event(&tx, e, &eq, &ev).await);
							}
						}
					}
					if let Err(e) = tx.commit().await {
						tx.cancel().await?;
						let tx =
							tf.transaction(Write, LockType::Optimistic, sequences.clone()).await?;
						catch!(tx, Self::retry_event(&tx, e, &eq, &ev).await);
						tx.commit().await?;
					}
				}
				drop(permit); // releases a slot => producer can send another job
				Ok(())
			});
		}

		while let Some(res) = join_set.join_next().await {
			if let Err(e) = res {
				error!("Error while processing an event: {e}");
			}
		}
		Ok(count)
	}

	async fn retry_event(
		tx: &Transaction,
		e: anyhow::Error,
		eq: &eq::Eq<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		if ev.attempt < ev.event_definition.retry.unwrap_or(0) {
			tx.set(eq, ev, None).await?;
		} else {
			error!("Final error after processing the event `{}` {} times: {e}", eq.ev, ev.attempt);
			tx.del(eq).await?;
		}
		Ok(())
	}

	async fn process_event(
		ctx: &FrozenContext,
		opt: &Options,
		eq: &eq::Eq<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		let ctx = ev.build_event_context(ctx);
		let opt = ev.build_event_options(&ctx.tx(), opt, eq).await?;
		let doc = ev.build_event_cursor_doc();
		let mut stack = TreeStack::new();
		stack
			.enter(|stk| {
				stk.run(|stk| Document::process_sync(stk, ctx, opt, &ev.event_definition, &doc))
			})
			.finish()
			.await?;
		Ok(())
	}
}
