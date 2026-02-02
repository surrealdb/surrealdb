use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::TreeStack;
use reblessive::tree::Stk;
use revision::revisioned;
use surrealdb_types::ToSql;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
#[cfg(not(target_family = "wasm"))]
use tokio::sync::Semaphore;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{EventDefinition, Record};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Session, Statement};
use crate::doc::{Action, CursorDoc, Document, DocumentContext};
use crate::err::Error;
use crate::expr::FlowResultExt;
use crate::iam::{Auth, AuthLimit};
use crate::key::root::eq;
use crate::kvs::TransactionType::Write;
use crate::kvs::sequences::Sequences;
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{
	Datastore, HlcTimestamp, KVValue, Key, LockType, Transaction, TransactionFactory,
	TransactionType, Val, impl_kv_value_revisioned,
};
use crate::val::{RecordId, Value};

impl Document {
	/// Processes any DEFINE EVENT clauses defined for this table.
	/// Synchronous events execute within the current transaction, while async
	/// events are enqueued for background processing.
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
			// Resolve the event action string for the context.
			let evt = match action {
				Action::Create => Value::from("CREATE"),
				Action::Update => Value::from("UPDATE"),
				Action::Delete => Value::from("DELETE"),
			};
			// Capture before/after values for the event context.
			let after = self.current.doc.as_arc();
			let before = self.initial.doc.as_arc();
			// Depending on type of event, how do we populate the document
			let doc = if action == Action::Delete {
				&mut self.initial
			} else {
				&mut self.current
			};
			// Configure the context
			let mut ctx = Context::new(ctx);
			ctx.add_value("event", evt.into());
			ctx.add_value("value", doc.doc.as_arc());
			ctx.add_value("after", after);
			ctx.add_value("before", before);
			ctx.add_value("input", input.clone().unwrap_or_default());
			let ctx = ctx.freeze();
			// Process conditional clause
			let val = stk
				.run(|stk| ev.when.compute(stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e))?;
			// Execute or enqueue the event if the condition is truthy.
			if val.is_truthy() {
				if ev.asynchronous {
					Self::process_async(ctx, opt, ev, &self.doc_ctx, doc).await?;
				} else {
					Self::process_sync(stk, ctx, opt, None, ev, doc).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn process_async(
		ctx: FrozenContext,
		opt: Options,
		ev: &EventDefinition,
		doc_ctx: &DocumentContext,
		cursor_doc: &CursorDoc,
	) -> Result<()> {
		let node_id = opt.id();
		let ts = HlcTimestamp::next();
		let db = doc_ctx.db();
		let tx = ctx.tx();
		// Persist the event payload so it can be processed out-of-band.
		// Use the current transaction so enqueue is atomic with the document change.
		// HLC timestamp + node ID keep the queue key ordered and unique.
		let key =
			eq::Eq::new(db.namespace_id, db.database_id, &ev.target_table, &ev.name, ts, node_id);
		let event_record = AsyncEventRecord::new(&opt, &ctx, ev, cursor_doc)?;
		tx.put(&key, &event_record, None).await?;
		Ok(())
	}

	async fn process_sync(
		stk: &mut Stk,
		ctx: FrozenContext,
		opt: Options,
		lh: Option<LeaseHandler>,
		ev: &EventDefinition,
		doc: &CursorDoc,
	) -> Result<()> {
		// Evaluate each THEN expression in order.
		for v in ev.then.iter() {
			if let Some(lh) = lh.as_ref() {
				lh.try_maintain_lease().await?;
			}
			let res =
				stk.run(|stk| v.compute(stk, &ctx, &opt, Some(doc))).await.catch_return().map_err(
					|e| anyhow::anyhow!("Error while processing event {}: {}", ev.name, e),
				)?;
			trace!("Event statement returns: {}", res.to_sql());
		}
		Ok(())
	}
}

/// Persisted payload for processing DEFINE EVENT ... ASYNC.
#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub struct AsyncEventRecord {
	/// Number of processing attempts already made (incremented before execution).
	attempt: u16,
	/// Async event nesting depth for this record (0 for top-level).
	event_depth: u16,
	rid: Option<Arc<RecordId>>,
	cursor_record: Arc<Record>,
	fields_computed: bool,
	ns: Arc<str>,
	db: Arc<str>,
	perms: bool,
	auth_enabled: bool,
	values: HashMap<Cow<'static, str>, Arc<Value>>,
	auth: Arc<Auth>,
	event_definition: EventDefinition,
}

impl_kv_value_revisioned!(AsyncEventRecord);

impl AsyncEventRecord {
	/// Build a queued event payload from the current cursor document and context.
	fn new(
		opt: &Options,
		ctx: &FrozenContext,
		event_definition: &EventDefinition,
		cursor_doc: &CursorDoc,
	) -> Result<Self> {
		let (ns, db) = opt.arc_ns_db()?;
		// `async_event_depth` tracks the parent depth; refuse to enqueue above max.
		if let Some(d) = opt.async_event_depth()
			&& d >= event_definition.max_depth
		{
			bail!(Error::EvReachMaxDepth(event_definition.name.clone(), d))
		}
		Ok(Self {
			attempt: 0,
			event_depth: opt.async_event_depth().map(|d| d + 1).unwrap_or(0),
			rid: cursor_doc.rid.clone(),
			cursor_record: cursor_doc.doc.clone().into_read_only(),
			fields_computed: cursor_doc.fields_computed,
			ns,
			db,
			perms: opt.perms,
			auth_enabled: opt.auth_enabled,
			values: ctx.collect_values(HashMap::new()),
			auth: opt.auth.clone(),
			event_definition: event_definition.clone(),
			// session: ctx.value("session").map(|v| Arc::new(v.clone())),
		})
	}

	/// Rebuild the event context when processing a queued event.
	fn build_event_context(&self, ctx: &FrozenContext) -> FrozenContext {
		let mut ctx = Context::new(ctx);
		ctx.add_values(self.values.clone());
		ctx.freeze()
	}

	/// Recreate options for queued event evaluation and validate ns/db IDs.
	async fn build_event_options(
		&self,
		tx: &Transaction,
		parent_opts: &Options,
		eq: &eq::Eq<'_>,
	) -> Result<Options> {
		// Resolve namespace/database IDs and ensure they still match the queued key.
		let ns = tx.expect_ns_by_name(&self.ns).await?;
		if ns.namespace_id != eq.ns {
			bail!(Error::EvNamespaceMismatch(self.event_definition.name.clone(), ns.name.clone()));
		}
		let db = tx.expect_db_by_name(&self.ns, &self.db).await?;
		if db.database_id != eq.db {
			bail!(Error::EvDatabaseMismatch(self.event_definition.name.clone(), db.name.clone()));
		}
		let opt = parent_opts.clone();
		let opt = opt
			.with_perms(self.perms)
			.with_auth_enabled(self.auth_enabled)
			.with_auth(self.auth.clone())
			.with_async_event_depth(self.event_depth)
			.with_ns(Some(self.ns.clone()))
			.with_db(Some(self.db.clone()));
		Ok(opt)
	}

	/// Recreate a cursor document from the persisted record snapshot.
	fn build_event_cursor_doc(&self) -> CursorDoc {
		CursorDoc {
			rid: self.rid.clone(),
			ir: None,
			doc: self.cursor_record.clone().into(),
			fields_computed: self.fields_computed,
		}
	}

	/// Process a single batch of queued async events.
	/// Returns the number of events fetched (not necessarily successfully processed).
	pub async fn process_next_events_batch(
		ds: &Datastore,
		lh: Option<&LeaseHandler>,
	) -> Result<usize> {
		// Collect the next batch
		let res = {
			if let Some(lh) = lh.as_ref() {
				lh.try_maintain_lease().await?;
			}
			let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
			let (beg, end) = eq::Eq::range();
			// Read a bounded batch without holding a write transaction.
			let res = catch!(tx, tx.scan(beg..end, *NORMAL_FETCH_SIZE, None).await);
			tx.cancel().await?;
			res
		};
		let count = res.len();
		Self::process_events_batch(ds, res, lh).await?;
		Ok(count)
	}

	#[cfg(not(target_family = "wasm"))]
	async fn process_events_batch(
		ds: &Datastore,
		res: Vec<(Key, Val)>,
		lh: Option<&LeaseHandler>,
	) -> Result<()> {
		// Limit in-flight event processing to avoid oversubscription.
		let concurrency: usize = num_cpus::get().max(4);
		let sem = Arc::new(Semaphore::new(concurrency));

		let mut join_handles = Vec::with_capacity(res.len());

		for (k, v) in res {
			// Acquire a concurrency slot per event.
			let permit = sem.clone().acquire_owned().await?;
			// Setup a context
			let ctx = ds.setup_ctx()?;
			// Build default options
			let opt = ds.setup_options(&Session::default());
			// Extract sequences and transaction factory
			let sequences = ds.sequences().clone();
			let tf = ds.transaction_factory().clone();
			let lh = lh.cloned();
			let jh = spawn(async move {
				Self::run_event_checked(ctx, opt, tf, sequences, lh, k, v).await;
				drop(permit); // releases a slot so the loop can enqueue another task
			});
			join_handles.push(jh);
		}

		for jh in join_handles {
			if let Err(e) = jh.await {
				error!("Error while processing an event: {e}");
			}
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn process_events_batch(
		ds: &Datastore,
		res: Vec<(Key, Val)>,
		lh: Option<&LeaseHandler>,
	) -> Result<()> {
		for (k, v) in res {
			if let Some(lh) = lh {
				lh.try_maintain_lease().await?
			}
			// Setup a context
			let ctx = ds.setup_ctx()?;
			// Build default options
			let opt = ds.setup_options(&Session::default());
			// Extract sequences and transaction factory
			let sequences = ds.sequences().clone();
			let tf = ds.transaction_factory().clone();
			let lh = lh.cloned();
			Self::run_event_checked(ctx, opt, tf, sequences, lh, k, v).await;
		}
		Ok(())
	}

	async fn run_event_checked(
		ctx: Context,
		opt: Options,
		tf: TransactionFactory,
		sequences: Sequences,
		lh: Option<LeaseHandler>,
		k: Key,
		v: Val,
	) {
		if let Err(e) = Self::run_event(ctx, opt, tf, sequences, lh, k, v).await {
			error!("Error while processing an event: {e}");
		}
	}
	async fn run_event(
		mut ctx: Context,
		opt: Options,
		tf: TransactionFactory,
		sequences: Sequences,
		lh: Option<LeaseHandler>,
		k: Key,
		v: Val,
	) -> Result<()> {
		// Process each event in its own write transaction.
		let tx = tf.transaction(Write, LockType::Optimistic, sequences.clone()).await?;
		ctx.set_transaction(Arc::new(tx));
		let ctx = ctx.freeze();
		let eq = eq::Eq::decode_key(&k)?;
		let mut ev = AsyncEventRecord::kv_decode_value(v)?;
		// Count this attempt before processing so retries are bounded.
		ev.attempt += 1;
		let ev = ev;
		let tx = ctx.tx();
		match Self::process_event(&ctx, &opt, lh, &eq, &ev).await {
			Ok(_) => {
				catch!(tx, tx.del(&k).await);
			}
			Err(e) => {
				let se: Option<&Error> = e.downcast_ref();
				if matches!(
					se,
					Some(Error::EvNamespaceMismatch(..)) | Some(Error::EvDatabaseMismatch(..))
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
			// If the commit fails, requeue the event and commit that update.
			tx.cancel().await?;
			let tx = tf.transaction(Write, LockType::Optimistic, sequences.clone()).await?;
			catch!(tx, Self::retry_event(&tx, e, &eq, &ev).await);
			tx.commit().await?;
		}
		Ok(())
	}

	/// Update or remove the queued event based on the retry policy.
	async fn retry_event(
		tx: &Transaction,
		e: anyhow::Error,
		eq: &eq::Eq<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		// `attempt` is incremented before processing; `retry` counts retries, so requeue while
		// attempt <= retry.
		if ev.attempt <= ev.event_definition.retry {
			// Requeue with the same key so the event keeps its original queue position; retries are
			// bounded here and no backoff is applied yet (will be implemented in a future version).
			tx.set(eq, ev, None).await?;
		} else {
			error!("Final error after processing the event `{}` {} times: {e}", eq.ev, ev.attempt);
			tx.del(eq).await?;
		}
		Ok(())
	}

	/// Execute a queued event using a fresh TreeStack.
	async fn process_event(
		ctx: &FrozenContext,
		opt: &Options,
		lh: Option<LeaseHandler>,
		eq: &eq::Eq<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		let ctx = ev.build_event_context(ctx);
		let opt = ev.build_event_options(&ctx.tx(), opt, eq).await?;
		let doc = ev.build_event_cursor_doc();
		let mut stack = TreeStack::new();
		// Run event statements in a new stack scope.
		stack
			.enter(|stk| {
				stk.run(|stk| Document::process_sync(stk, ctx, opt, lh, &ev.event_definition, &doc))
			})
			.finish()
			.await?;
		Ok(())
	}
}
