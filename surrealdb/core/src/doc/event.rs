use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use common::time::sleep;
pub(crate) use surrealdb_datastore::values::event_queue::AsyncEventRecord;
use surrealdb_kvs::TransactionType::Write;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{EventDefinition, FromStored};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Session};
use crate::doc::{CursorDoc, Error};
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
		doc: Arc::clone(&record.cursor_record).into(),
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
	// Cap workers by batch size.
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
			while let Ok(event_context) = receiver.recv().await {
				event_context.run_event_checked().await;
			}
		});
		join_handles.push(jh);
	}

	// Producer
	for (k, v) in res {
		let Some(v) = decode_queued(&k, &v) else {
			continue;
		};
		match AsyncEventContext::new(ds, k, v) {
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
	for (k, v) in res {
		if let Some(lh) = lh {
			lh.try_maintain_lease().await?;
		}
		let Some(v) = decode_queued(&k, &v) else {
			continue;
		};
		let event_context = AsyncEventContext::new(ds, k, v)?;
		event_context.run_event_checked().await;
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
/// is keyed off the decoded key), so it would stay in the queue and fail every
/// later batch. The key decode must also run before any transaction is opened,
/// or each of those attempts would leak one. Neither half is deleted, matching
/// how the other queue drains treat entries written by a newer node during a
/// rolling upgrade.
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
	k: Vec<u8>,
	v: Option<AsyncEventRecord>,
}

impl AsyncEventContext {
	fn new(ds: &Datastore, k: Vec<u8>, v: AsyncEventRecord) -> Result<Self> {
		Ok(Self {
			ctx: Some(ds.setup_ctx()?),
			opt: ds.setup_options(&Session::default()),
			tf: ds.transaction_factory().clone(),
			sequences: ds.sequences().clone(),
			k,
			v: Some(v),
		})
	}

	async fn run_event_checked(mut self) {
		if let Some(ctx) = self.ctx.take()
			&& let Some(v) = self.v.take()
			&& let Err(e) = self.run_event(ctx, v).await
		{
			error!("Unexpected error while processing an event. Error: {e} - Key: {:?}", self.k);
		}
	}

	async fn new_write_tx(&self) -> Result<Transaction> {
		self.tf.transaction(Write, self.sequences.clone()).await
	}

	async fn run_event(&mut self, ctx: Context, mut ev: AsyncEventRecord) -> Result<()> {
		// Decode the key before opening the transaction. A decode failure after the
		// writeable transaction is open would return with it still open, tripping
		// `Transactor::drop`'s "a transaction was dropped without being committed
		// or cancelled" error. The queue entry is only removed once it has run, so
		// that failure would repeat on every batch while the entry stays queued.
		// `decode_queued` skips such an entry first, so this check is a second
		// safeguard.
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
			// A queued event body is a statement of its own, run outside the
			// statement that queued it: every clock read in it answers the
			// instant this attempt started at.
			ctx.set_statement_clock(crate::val::Datetime::now());
			let ctx = ctx.freeze();
			let Err(e) = self.run_event_once(&ctx, &eq, &ev).await else {
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
		if let Err(e) = Self::process_event(ctx, &self.opt, eq, ev).await {
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
		ctx: &FrozenContext,
		opt: &Options,
		eq: &EventQueueKey<'_>,
		ev: &AsyncEventRecord,
	) -> Result<()> {
		let ctx = build_event_context(ev, ctx);
		let opt = build_event_options(ev, &ctx.tx(), opt, eq).await?;
		let doc = build_event_cursor_doc(ev);
		// The queued payload persists the stored (text-form) definition;
		// compile it once per dequeued event before execution.
		let compiled = EventDefinition::from_stored(&ev.event_definition)?;
		run_event_bodies(&ctx, &opt, &compiled, &doc).await
	}
}

/// Run a sync event's `THEN` clauses in order, for their effects, against the
/// record image the event was queued with.
///
/// The frame is rebuilt from the queued context and options, so the clauses
/// see the `$before`, `$after`, `$event`, `$value` and `$input` bound when the
/// event fired, and run under the authority the definition recorded. A
/// clause's value is discarded; a failure is the event's, named by it. A
/// `BREAK` or `CONTINUE` in a clause has no loop to target: `entry::scalar`
/// answers it with the misplaced-control-flow error, which the event's name
/// then prefixes, as for a synchronous event.
async fn run_event_bodies(
	ctx: &FrozenContext,
	opt: &Options,
	ev: &EventDefinition,
	doc: &CursorDoc,
) -> Result<()> {
	let exec = crate::exec::entry::frame(ctx, opt, Some(doc)).await;
	for then in ev.then.iter() {
		crate::exec::entry::scalar(&exec, then)
			.await
			.map_err(|e| anyhow::anyhow!("Error while processing event {}: {e}", ev.name))?;
	}
	Ok(())
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

	/// Guards the key check in [`decode_queued`] against rejecting well-formed
	/// keys. If this regressed, every async event would be silently skipped
	/// rather than processed, which is far worse than the leak the check
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

	/// An entry whose key cannot be decoded must be skipped here, before a
	/// transaction is opened for it.
	///
	/// A decode failure inside `run_event`, after its writeable transaction has
	/// opened, returns with that transaction still open. That trips
	/// `Transactor::drop`'s "a transaction was dropped without being committed
	/// or cancelled" error. The entry is never removed on that path, so the
	/// failure would repeat on every later batch.
	#[test]
	fn an_undecodable_key_is_skipped() {
		assert!(
			decode_queued(b"/!eq\xff-not-a-valid-entry", &[]).is_none(),
			"an undecodable key must be skipped, not passed on to run_event"
		);
	}

	/// The value check must still reject a bad payload behind a good key, so
	/// the key check does not short-circuit it.
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
