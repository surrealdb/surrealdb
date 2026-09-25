use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use common::time::sleep;
use reblessive::TreeStack;
use reblessive::tree::Stk;
pub(crate) use surrealdb_datastore::values::event_queue::AsyncEventRecord;
use surrealdb_kvs::TransactionType::Write;
use surrealdb_kvs::timestamp::HlcTimeStamp;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{EventDefinition, FromStored, StoredEventDefinition};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Session};
use crate::doc::{Action, CursorDoc, Document, DocumentContext, Error};
use crate::exe::FlowResultExt as _;
use crate::iam::AuthLimit;
use crate::key::schema::{EventQueueKey, EventQueuePrefix};
use crate::key::{KVKeyDecode, KVValue};
use crate::kvs::sequences::Sequences;
use crate::kvs::tasklease::LeaseHandler;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	Datastore, NORMAL_BATCH_SIZE, Transaction, TransactionFactory, TransactionType,
	is_indeterminate_commit, is_retryable_transaction_conflict,
};
use crate::val::Value;

/// How many times a queued event is re-run in place after its transaction lost
/// a retryable conflict, or its commit ended with an unknown outcome.
///
/// A conflict reports that another writer reached the same keys first, not that
/// the event is wrong, so it must not spend the `RETRY` budget the definition
/// set aside for a failing event: the queue is drained by several workers at
/// once, and contention alone would otherwise exhaust that budget and drop the
/// event. An unknown outcome is not a failure of the event either, and re-running
/// is how it gets resolved: each attempt first checks that the entry is still
/// queued, so an attempt whose commit did apply ends there. The count is bounded
/// so an event that keeps losing the race still reaches a decision rather than
/// holding the drain loop open indefinitely.
const EVENT_CONFLICT_RETRIES: u32 = 3;

/// How long to wait between those re-runs, so the writer this attempt lost to
/// has a chance to commit instead of being raced again immediately.
const EVENT_CONFLICT_RETRY_SLEEP: Duration = Duration::from_millis(50);

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
		action: Action,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Check if changed
		if !self.is_modified() {
			return Ok(());
		}
		// Don't run permissions
		let opt = &opt.new_with_perms(false);

		if self.doc_ctx.ev()?.is_empty() {
			return Ok(());
		}

		let input = self.materialize_input_value(stk, ctx, opt).await?;

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
		if !self.is_modified() {
			return Ok(());
		}
		// Don't run permissions
		let opt = &opt.new_with_perms(false);

		// Loop through all event statements
		for ev in self.doc_ctx.ev()?.iter() {
			// Limit auth
			let opt = opt.limited_by(&AuthLimit::try_from(&ev.auth_limit)?);
			// Get the event action
			let evt = match action {
				Action::Create => Value::from("CREATE"),
				Action::Update => Value::from("UPDATE"),
				Action::Delete => Value::from("DELETE"),
			};
			// Capture documents for the event context
			let after = self.current.doc.as_arc();
			let before = self.initial.doc.as_arc();
			// Populate the relevant event document
			let doc = if action == Action::Delete {
				&mut self.initial
			} else {
				&mut self.current
			};
			// Configure the context
			let mut ctx = Context::new_child(ctx);
			ctx.add_value("after", after);
			ctx.add_value("before", before);
			ctx.add_value("event", evt.into());
			ctx.add_value("value", doc.doc.as_arc());
			ctx.add_value("input", input.clone().unwrap_or_default());
			// Freeze the context
			let ctx = ctx.freeze();
			// Process conditional clause
			let val = stk
				.run(|stk| crate::legacy::expr_compute(&ev.when, stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {e}", ev.name))?;
			// Execute event if value is truthy
			if val.is_truthy() {
				if ev.is_async() {
					Self::process_event_async(ctx, opt, ev, &self.doc_ctx, doc).await?;
				} else {
					Self::process_event_sync(stk, ctx, opt, None, ev, doc).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn process_event_sync(
		stk: &mut Stk,
		ctx: FrozenContext,
		opt: Options,
		_lh: Option<&LeaseHandler>,
		ev: &EventDefinition,
		doc: &CursorDoc,
	) -> Result<()> {
		// Evaluate each THEN expression in order.
		for then in ev.then.iter() {
			stk.run(|stk| crate::legacy::expr_compute(then, stk, &ctx, &opt, Some(doc)))
				.await
				.catch_return()
				.map_err(|e| anyhow::anyhow!("Error while processing event {}: {e}", ev.name))?;
		}
		// Carry on
		Ok(())
	}

	async fn process_event_async(
		ctx: FrozenContext,
		opt: Options,
		ev: &EventDefinition,
		doc_ctx: &DocumentContext,
		cursor_doc: &mut CursorDoc,
	) -> Result<()> {
		let node_id = ctx.node_id();
		let ts = HlcTimeStamp::next();
		let db = doc_ctx.db();
		let tx = ctx.tx();
		// Persist the event payload so it can be processed out-of-band.
		// Use the current transaction so enqueue is atomic with the document change.
		// HLC timestamp + node ID keep the queue key ordered and unique.
		let key = EventQueueKey {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&ev.target_table),
			ev: Cow::Borrowed(&ev.name),
			ts: ts.0,
			node_id,
		};
		// The queued payload persists the stored (text-form) definition,
		// rendered from the compiled one the context carries.
		let event_record = queue_async_event(&opt, &ctx, ev.stored(), cursor_doc)?;
		tx.put_key(&key, &event_record).await?;
		tx.trigger_async_event();
		Ok(())
	}
}

/// Build a queued event payload from the current cursor document and context.
fn queue_async_event(
	opt: &Options,
	ctx: &FrozenContext,
	event_definition: &StoredEventDefinition,
	cursor_doc: &CursorDoc,
) -> Result<AsyncEventRecord> {
	let (ns, db) = opt.arc_ns_db()?;
	// `async_event_depth` tracks the parent depth; refuse to enqueue above max.
	if let Some(d) = opt.async_event_depth()
		&& d >= event_definition.max_depth()
	{
		bail!(Error::EvReachMaxDepth(event_definition.name.to_string(), d))
	}
	Ok(AsyncEventRecord {
		attempt: 0,
		event_depth: opt.async_event_depth().map(|d| d + 1).unwrap_or(0),
		rid: cursor_doc.rid.clone(),
		cursor_record: cursor_doc.doc.clone().into_read_only(),
		fields_computed: cursor_doc.fields_computed,
		ns,
		db,
		perms: opt.perms,
		auth_enabled: ctx.auth_enabled(),
		values: ctx.collect_values(HashMap::new()),
		auth_with_limit: Arc::clone(&opt.auth),
		event_definition: event_definition.clone(),
		// session: ctx.value("session").map(|v| Arc::new(v.clone())),
	})
}

/// Rebuild the event context when processing a queued event.
fn build_event_context(record: &AsyncEventRecord, ctx: &FrozenContext) -> FrozenContext {
	let mut ctx = Context::new_child(ctx);
	ctx.add_values(record.values.clone());
	ctx.auth_enabled = record.auth_enabled;
	ctx.freeze()
}

/// Recreate options for queued event evaluation and validate ns/db IDs.
async fn build_event_options(
	record: &AsyncEventRecord,
	tx: &Transaction,
	parent_opts: &Options,
	eq: &EventQueueKey<'_>,
) -> Result<Options> {
	// Resolve namespace/database IDs and ensure they still match the queued key.
	let ns = tx.expect_ns_by_name(&record.ns).await?;
	if ns.namespace_id != eq.ns {
		bail!(Error::EvNamespaceMismatch(
			record.event_definition.name.to_string(),
			ns.name.to_string(),
		));
	}
	let db = tx.expect_db_by_name(&record.ns, &record.db).await?;
	if db.database_id != eq.db {
		bail!(Error::EvDatabaseMismatch(
			record.event_definition.name.to_string(),
			db.name.to_string(),
		));
	}
	let opt = parent_opts.clone();
	let opt = opt
		.with_perms(record.perms)
		.with_auth(Arc::clone(&record.auth_with_limit))
		.with_async_event_depth(record.event_depth)
		.with_ns(Some(Arc::clone(&record.ns)))
		.with_db(Some(Arc::clone(&record.db)));
	Ok(opt)
}

/// Recreate a cursor document from the persisted record snapshot.
fn build_event_cursor_doc(record: &AsyncEventRecord) -> CursorDoc {
	CursorDoc {
		rid: record.rid.clone(),
		ir: None,
		doc: Arc::clone(&record.cursor_record).into(),
		fields_computed: record.fields_computed,
	}
}

/// Process a single batch of queued async events.
/// Returns the number of events fetched (not necessarily successfully processed).
pub async fn process_next_events_batch(ds: &Datastore, lh: Option<&LeaseHandler>) -> Result<usize> {
	// Collect the next batch
	let res = {
		if let Some(lh) = lh.as_ref() {
			lh.try_maintain_lease().await?;
		}
		let tx = ds.transaction(TransactionType::Read).await?;
		let range = EventQueuePrefix {}.range()?;
		// Read a bounded batch without holding a write transaction. The values stay
		// encoded so that an entry this binary cannot decode — one written by a newer
		// node, or a partial write — is skipped per entry rather than failing the
		// batch. Nothing but a successful run deletes a queue entry, so failing the
		// batch would stall the queue for good.
		let res = catch!(tx, tx.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await);
		tx.cancel().await?;
		res
	};
	let count = res.len();
	process_events_batch(ds, res, lh).await?;
	Ok(count)
}

#[cfg(not(target_family = "wasm"))]
pub(crate) async fn process_events_batch(
	ds: &Datastore,
	res: Vec<(Vec<u8>, Vec<u8>)>,
	lh: Option<&LeaseHandler>,
) -> Result<()> {
	if res.is_empty() {
		return Ok(());
	}
	// Best-effort parallel processing; queue order is not preserved.
	// Limit in-flight event processing to avoid oversubscription.
	let concurrency: usize = num_cpus::get().max(4);
	// Cap workers by batch size and reuse one TreeStack per worker.
	let workers = res.len().min(concurrency);
	// Store the join handles
	let mut join_handles = Vec::with_capacity(workers);
	// Build a producer/consumer channel
	let (sender, receiver) = async_channel::bounded::<AsyncEventContext>(workers);

	// Start consumers
	for _ in 0..workers {
		let receiver = receiver.clone();
		// Spawn a worker
		let jh = spawn(async move {
			// Reuse a stack per worker to amortize allocations.
			let mut stack = TreeStack::new();
			while let Ok(event_context) = receiver.recv().await {
				stack
					.enter(|stk| stk.run(|stk| event_context.run_event_checked(stk)))
					.finish()
					.await;
			}
		});
		join_handles.push(jh);
	}

	// Producer
	for (k, v) in res {
		let Some(v) = decode_queued(&k, &v) else {
			continue;
		};
		match AsyncEventContext::new(ds, lh.cloned(), k, v) {
			Ok(event_context) => {
				sender.send(event_context).await?;
			}
			Err(e) => {
				// Log and skip this entry so other events can still be processed.
				error!("Unexpected Error while processing event: {e}");
			}
		};
		if let Some(lh) = lh {
			lh.try_maintain_lease().await?;
		}
	}
	sender.close();

	// Wait for workers to be done
	for jh in join_handles {
		if let Err(e) = jh.await {
			error!("Error while processing an event: {e}");
		}
	}
	Ok(())
}

#[cfg(target_family = "wasm")]
pub(crate) async fn process_events_batch(
	ds: &Datastore,
	res: Vec<(Vec<u8>, Vec<u8>)>,
	lh: Option<&LeaseHandler>,
) -> Result<()> {
	let mut stack = TreeStack::new();
	for (k, v) in res {
		if let Some(lh) = lh {
			lh.try_maintain_lease().await?;
		}
		let Some(v) = decode_queued(&k, &v) else {
			continue;
		};
		let event_context = AsyncEventContext::new(ds, lh.cloned(), k, v)?;
		stack.enter(|stk| stk.run(|stk| event_context.run_event_checked(stk))).finish().await;
	}
	Ok(())
}

/// Decode one queued event, reporting and discarding an entry this binary
/// cannot read.
///
/// A queue entry is only removed once it has run, so an undecodable entry has
/// to be stepped over rather than propagated: returning an error here would
/// leave the entry in place and fail every later batch the same way.
///
/// Both halves are checked here. The key is validated even though the value is
/// what this returns, because an entry whose *key* cannot be decoded is equally
/// unprocessable: it could never be run and could never be deleted (the delete
/// is keyed off the decoded key), so it stayed in the queue and failed every
/// subsequent batch — and, before the decode moved above the transaction open in
/// `run_event`, leaked a writeable transaction on each of those attempts.
/// Neither half is deleted, matching how the other queue drains treat entries
/// written by a newer node during a rolling upgrade.
fn decode_queued(k: &[u8], v: &[u8]) -> Option<AsyncEventRecord> {
	if let Err(e) = EventQueueKey::decode_key(k) {
		error!("Skipping async event queue entry with an undecodable key: {e} - Key: {k:?}");
		return None;
	}
	match KVValue::kv_decode_value(v, ()) {
		Ok(ev) => Some(ev),
		Err(e) => {
			error!("Skipping undecodable async event queue entry: {e} - Key: {k:?}");
			None
		}
	}
}

struct AsyncEventContext {
	ctx: Option<Context>,
	opt: Options,
	tf: TransactionFactory,
	sequences: Sequences,
	lh: Option<LeaseHandler>,
	k: Vec<u8>,
	v: Option<AsyncEventRecord>,
}

impl AsyncEventContext {
	fn new(
		ds: &Datastore,
		lh: Option<LeaseHandler>,
		k: Vec<u8>,
		v: AsyncEventRecord,
	) -> Result<Self> {
		Ok(Self {
			ctx: Some(ds.setup_ctx()?),
			opt: ds.setup_options(&Session::default()),
			tf: ds.transaction_factory().clone(),
			sequences: ds.sequences().clone(),
			lh,
			k,
			v: Some(v),
		})
	}

	async fn run_event_checked(mut self, stk: &mut Stk) {
		if let Some(ctx) = self.ctx.take()
			&& let Some(v) = self.v.take()
			&& let Err(e) = self.run_event(stk, ctx, v).await
		{
			error!("Unexpected error while processing an event. Error: {e} - Key: {:?}", self.k);
		}
	}

	async fn new_write_tx(&self) -> Result<Transaction> {
		self.tf.transaction(Write, self.sequences.clone()).await
	}

	async fn run_event(
		&mut self,
		stk: &mut Stk,
		ctx: Context,
		mut ev: AsyncEventRecord,
	) -> Result<()> {
		// Decode the key *before* opening the transaction. Decoding it after
		// would let a decode failure return while the writeable transaction is
		// still open, tripping `Transactor::drop`'s "a transaction was dropped
		// without being committed or cancelled" error. Because the queue entry is
		// only removed once it has run, that leak repeated on every batch for as
		// long as the undecodable entry stayed in the queue. `decode_queued` now
		// steps over such an entry before it reaches here, so this is the second
		// line of defence rather than the only one.
		let eq = EventQueueKey::decode_key(&self.k)?;
		// The base context carries no transaction. Each attempt opens its own and
		// installs it on a child, so a re-run never reaches the closed transaction
		// of the attempt before it.
		let base = ctx.freeze();
		let mut conflicts = 0;
		// The error that ends the attempts, if any. Each attempt closes its own
		// transaction, so nothing past this loop has one left to unwind.
		let err = loop {
			let tx = self.new_write_tx().await?;
			let mut ctx = Context::new_child(&base);
			ctx.set_transaction(Arc::new(tx));
			let ctx = ctx.freeze();
			let Err(e) = self.run_event_once(stk, &ctx, &eq, &ev).await else {
				return Ok(());
			};
			// Contention or an unknown commit outcome, not a failure of the event:
			// re-run it rather than charging an attempt against the definition's
			// `RETRY` budget. See [`EVENT_CONFLICT_RETRIES`].
			let transient = is_retryable_transaction_conflict(&e) || is_indeterminate_commit(&e);
			if transient && conflicts < EVENT_CONFLICT_RETRIES {
				conflicts += 1;
				debug!("Re-running the event `{}` in place: {e}", eq.ev);
				sleep(EVENT_CONFLICT_RETRY_SLEEP).await;
				continue;
			}
			break e;
		};
		// The last commit may have applied, removing the entry along with the
		// event's writes. Requeueing it would run the event again if it did, and
		// deleting it would lose the event if it did not, so the entry is left as
		// the commit left it: a later pass either finds it gone or runs it.
		if is_indeterminate_commit(&err) {
			return Err(err);
		}
		// Requeue or delete the entry in a fresh transaction based on the error.
		if let Some(final_error) = Self::is_final_error(&err).await? {
			let tx = self.new_write_tx().await?;
			return Self::final_error(tx, &eq, final_error).await;
		}
		let tx = self.new_write_tx().await?;
		Self::retry_attempt(tx, err, &eq, &mut ev).await
	}

	/// One attempt at a queued event: run it and remove its queue entry in a
	/// single transaction, so the event is neither run twice nor lost.
	///
	/// The transaction the context carries is closed on every path — committed
	/// once both are staged, cancelled otherwise — and the caller must not close
	/// it again. `Transaction::commit` finishes the transaction whether or not it
	/// succeeds, so a cancel after a failed commit reports `TransactionFinished`
	/// and replaces the error that actually ended the attempt. The caller
	/// classifies that error to decide the entry's fate, so losing it strands the
	/// entry: it is then neither re-run nor removed, and every later batch fetches
	/// and fails it the same way.
	///
	/// An entry that is no longer queued has already run, and the attempt ends
	/// without running it again. Only a run removes an entry, and two runs can
	/// reach the same one: batches overlap once a lease lapses, and an earlier
	/// attempt's commit can apply while reporting an unknown outcome. The later
	/// run either starts after the removal and finds the entry gone here, or
	/// started before it and loses the conflict on the entry's key at commit,
	/// and the re-run that follows finds it gone.
	async fn run_event_once(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		eq: &EventQueueKey<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		let tx = ctx.tx();
		match tx.exists_key(eq, None).await {
			Ok(true) => {}
			Ok(false) => return tx.cancel().await,
			Err(e) => {
				let _ = tx.cancel().await;
				return Err(e);
			}
		}
		if let Err(e) = Self::process_event(stk, ctx, &self.opt, self.lh.as_ref(), eq, ev).await {
			// Roll back the event's partial side effects. The cancel outcome is
			// discarded so it cannot displace the error being reported.
			let _ = tx.cancel().await;
			return Err(e);
		}
		if let Err(e) = tx.del_key(eq).await {
			let _ = tx.cancel().await;
			return Err(e);
		}
		// Stands in for a commit that lost a retryable conflict. A real one
		// leaves the transaction finished with its writes discarded, so the
		// injected one has to as well, or it exercises a state the recovery
		// path never sees.
		#[cfg(test)]
		if let Err(e) =
			maybe_inject_retryable_conflict(RetryableConflictSite::AsyncEventCommit, ctx.node_id())
		{
			let _ = tx.cancel().await;
			return Err(e);
		}
		// Stand in for a commit whose outcome is unknown, one for each outcome
		// it may really have had.
		#[cfg(test)]
		if let Err(e) = maybe_inject_non_retryable_error(
			NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown,
			ctx.node_id(),
		) {
			let _ = tx.cancel().await;
			return Err(e);
		}
		let res = tx.commit().await;
		#[cfg(test)]
		if res.is_ok() {
			maybe_inject_non_retryable_error(
				NonRetryableErrorSite::AsyncEventCommitAppliedUnknown,
				ctx.node_id(),
			)?;
		}
		res
	}

	/// Update or remove the queued event based on the retry policy.
	///
	/// An entry that is no longer queued has already run on another consumer —
	/// see [`Self::run_event_once`] — and is left gone: requeueing it would run
	/// the event again.
	async fn retry_attempt(
		tx: Transaction,
		e: anyhow::Error,
		eq: &EventQueueKey<'_>,
		ev: &mut AsyncEventRecord,
	) -> Result<()> {
		if !catch!(tx, tx.exists_key(eq, None).await) {
			return tx.cancel().await;
		}
		// `attempt` is incremented when requeuing; `retry` counts retries, so requeue while
		// attempt <= retry.
		ev.attempt += 1;
		if ev.attempt <= ev.event_definition.retry() {
			// Requeue with the same key so the event keeps its original queue position; retries are
			// bounded here and no backoff is applied.
			catch!(tx, tx.set_key(eq, ev).await);
		} else {
			warn!(
				"Final error after processing the event `{}` on table {} {} times: {e}",
				eq.ev, ev.event_definition.target_table, ev.attempt
			);
			catch!(tx, tx.del_key(eq).await);
		}
		catch!(tx, tx.commit().await);
		Ok(())
	}

	async fn is_final_error(e: &anyhow::Error) -> Result<Option<&Error>> {
		// Check if the error is final
		let se: Option<&Error> = e.downcast_ref();
		if matches!(
			se,
			Some(Error::EvNamespaceMismatch(..))
				| Some(Error::EvDatabaseMismatch(..))
				| Some(Error::EvReachMaxDepth(..))
		) {
			Ok(se)
		} else {
			Ok(None)
		}
	}

	async fn final_error(tx: Transaction, eq: &EventQueueKey<'_>, e: &Error) -> Result<()> {
		// The error is final, we log the final error message and remove the event from the queue
		warn!("Event processing failed: {:?}", e);
		catch!(tx, tx.del_key(eq).await);
		catch!(tx, tx.commit().await);
		// Carry on
		Ok(())
	}

	/// Execute a queued event using the provided stack scope.
	async fn process_event(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		lh: Option<&LeaseHandler>,
		eq: &EventQueueKey<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		let ctx = build_event_context(ev, ctx);
		let opt = build_event_options(ev, &ctx.tx(), opt, eq).await?;
		let doc = build_event_cursor_doc(ev);
		// The queued payload persists the stored (text-form) definition;
		// compile it once per dequeued event before execution.
		let compiled = EventDefinition::from_stored(&ev.event_definition)?;
		Document::process_event_sync(stk, ctx, opt, lh, &compiled, &doc).await
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use uuid::Uuid;

	use super::{AsyncEventContext, decode_queued};
	use crate::catalog::providers::CatalogProvider;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::dbs::Session;
	use crate::key::schema::{EventQueueKey, EventQueuePrefix};
	use crate::key::{KVKey, KVKeyDecode};
	use crate::kvs::Datastore;
	use crate::kvs::TransactionType::{Read, Write};

	fn valid_key() -> Vec<u8> {
		EventQueueKey {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned("tb".into()),
			ev: Cow::Owned("ev".into()),
			ts: 42,
			node_id: Uuid::from_u128(7),
		}
		.encode_key()
		.expect("a well-formed queue key must encode")
		.to_vec()
	}

	/// Guards the key check added to [`decode_queued`] against rejecting
	/// well-formed keys. If this regressed, every async event would be silently
	/// skipped rather than processed, which is far worse than the leak the check
	/// prevents.
	#[test]
	fn a_well_formed_queue_key_still_decodes() {
		let encoded = valid_key();
		let decoded = EventQueueKey::decode_key(&encoded).expect("valid key must decode");
		assert_eq!(decoded.ns, NamespaceId(1));
		assert_eq!(decoded.db, DatabaseId(2));
		assert_eq!(decoded.ts, 42);
		assert_eq!(decoded.node_id, Uuid::from_u128(7));
	}

	/// An entry whose key cannot be decoded must be stepped over here, before a
	/// transaction is opened for it.
	///
	/// Previously it reached `run_event`, which opened a writeable transaction
	/// and only then decoded the key, so the failure returned with the
	/// transaction still open — tripping `Transactor::drop`'s "a transaction was
	/// dropped without being committed or cancelled". Because a queue entry is
	/// only removed once it has run, the entry stayed put and leaked another
	/// transaction on every subsequent batch, indefinitely.
	#[test]
	fn an_undecodable_key_is_skipped() {
		assert!(
			decode_queued(b"/!eq\xff-not-a-valid-entry", &[]).is_none(),
			"an undecodable key must be skipped, not passed on to run_event"
		);
	}

	/// The pre-existing value check must still reject a bad payload behind a
	/// good key, so the new key check has not short-circuited it.
	#[test]
	fn an_undecodable_value_behind_a_valid_key_is_skipped() {
		assert!(
			decode_queued(&valid_key(), b"not-an-async-event-record").is_none(),
			"an undecodable value must still be skipped"
		);
	}

	async fn queued_entries(ds: &Datastore) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let tx = ds.transaction(Read).await?;
		let res = tx.scan_raw(EventQueuePrefix {}.range()?, 1000, 0, None).await;
		tx.cancel().await?;
		res
	}

	/// Only a run removes a queue entry, so a retry that finds its entry gone
	/// must leave it gone: the event ran on another consumer while this one's
	/// attempt was failing, and requeueing it would run the event again.
	/// `RETRY 1` is what makes that observable, since the retry would otherwise
	/// write the entry back.
	#[tokio::test]
	async fn a_retry_does_not_requeue_an_entry_that_already_ran() -> anyhow::Result<()> {
		let ds = Datastore::new("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let tx = ds.transaction(Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		let sql = "DEFINE TABLE person SCHEMALESS;
			DEFINE EVENT log ON person ASYNC RETRY 1 THEN (CREATE logged);
			CREATE person:1 RETURN NONE;";
		for res in ds.execute(sql, &session, None).await? {
			res.result?;
		}

		let (k, v) = queued_entries(&ds).await?.pop().expect("the event must be queued");
		let eq = EventQueueKey::decode_key(&k)?;
		let mut ev = decode_queued(&k, &v).expect("the queued entry must decode");
		// Another consumer runs the event and removes its entry.
		let tx = ds.transaction(Write).await?;
		tx.del_key(&eq).await?;
		tx.commit().await?;

		let tx = ds.transaction(Write).await?;
		AsyncEventContext::retry_attempt(tx, anyhow::anyhow!("the event failed"), &eq, &mut ev)
			.await?;

		assert!(queued_entries(&ds).await?.is_empty(), "the retry must not write the entry back");
		Ok(())
	}
}
