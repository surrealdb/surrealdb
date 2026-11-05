use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use common::time::sleep;
pub(crate) use surrealdb_datastore::values::event_queue::{AsyncEventRecord, EventClaim};
use surrealdb_kvs::TransactionType::Write;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use uuid::Uuid;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, NodeProvider};
use crate::catalog::{DatabaseId, EventDefinition, FromStored, NamespaceId};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Session};
use crate::doc::{CursorDoc, Error};
use crate::key::schema::{EventQueueKey, NodeDrainHeartbeatKey, NodeEventQueueKey};
use crate::key::{KVKeyDecode, KVValue, Key, TypedRange};
use crate::kvs::sequences::Sequences;
use crate::kvs::tasklease::LeaseHandler;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, PanicSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_panic, maybe_inject_retryable_conflict,
};
use crate::kvs::{
	Datastore, EVENT_CLAIM_TTL, Transaction, TransactionFactory, TransactionType,
	is_conditional_write_conflict, is_indeterminate_commit, is_retryable_transaction_conflict,
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
	eq: &QueuedKey,
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

/// One batch of a queue drain: what it fetched, what it could run, how long
/// the oldest entry had waited, and where the batch ended.
#[derive(Clone, Debug, Default)]
pub struct EventBatch {
	/// Entries fetched, whether or not each could be run.
	pub fetched: usize,
	/// Entries claimed and run. An entry this binary cannot decode, or one
	/// another processor holds, is fetched but never run, and stays in place.
	pub ran: usize,
	/// Entries fetched but left to another processor holding them: being run
	/// by a worker of this process — the event task's, or a caller of
	/// `Datastore::drain_async_events` — claimed within [`EVENT_CLAIM_TTL`]
	/// by another node's, or claimed or settled by one between this batch's
	/// fetch and its worker reaching them. They stay in place for their
	/// holder.
	pub held: usize,
	/// Entries run, failed with retries left, and written back in place for
	/// another attempt. The queue is not at rest while this is non-zero,
	/// however short the batch: the next attempt is owed now, not at the next
	/// tick.
	pub requeued: usize,
	/// Age of the oldest entry run, measured from its enqueue timestamp.
	pub oldest_age: Option<Duration>,
	/// The key of the last entry fetched, so a further fetch of the same range
	/// can resume after it rather than read the same entries again. `None`
	/// when the range held nothing.
	pub last_key: Option<Vec<u8>>,
	/// Whether the lease the batch ran under passed to another node, which
	/// ended the batch: no entry was handed out after the lease check that
	/// found it so, and those fetched but not handed out are still queued, so
	/// a caller must not resume past them on this batch's `last_key`. The
	/// entries handed out before were run and settled as the counts report.
	pub lease_lost: bool,
}

impl EventBatch {
	/// Whether the fetch filled the batch, so the range may hold entries beyond
	/// the last one fetched. Whether anything in it ran is a separate question:
	/// a full batch none of whose entries this binary can run is still full,
	/// and the entries behind it are only reached by resuming past it.
	pub fn is_full(&self, size: u32) -> bool {
		self.fetched >= size as usize
	}
}

/// The report of a batch that failed, carried as `anyhow` context by the
/// error that failed it; see [`process_events_batch`].
///
/// The counts stand: the entries the batch reports as run are settled, and
/// the failure left the rest in place, still queued. A drain that accounts
/// batches to a pass recovers the report with `downcast_ref` and accounts it
/// before propagating the error, so a pass that fails still reports what it
/// did. The error underneath — a lease check's, a cancellation's — still
/// reads as itself.
#[derive(Debug)]
pub struct FailedBatch(pub EventBatch);

impl std::fmt::Display for FailedBatch {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "the batch ran {} of the {} entries it fetched", self.0.ran, self.0.fetched)
	}
}

/// What a queue key says about its entry, whichever layout wrote it.
///
/// Entries are addressed by their raw bytes when deleted or requeued, so the
/// decoded form carries only what running the event needs: the ids the
/// record's names are checked against, the name for the log line, and the
/// enqueue timestamp the queue's lag is measured from.
#[derive(Clone, Debug)]
struct QueuedKey {
	ns: NamespaceId,
	db: DatabaseId,
	ev: String,
	ts: u64,
	/// The node that queued a root-level entry, or `None` for a per-node entry.
	owner: Option<Uuid>,
}

impl QueuedKey {
	/// Decodes a per-node entry, else a root-level one as written by a node on
	/// a release that queues at the root. The two share a tag but not a
	/// prefix, so a key decodes as at most one of them.
	fn decode(k: &[u8]) -> Result<Self> {
		if let Ok(key) = NodeEventQueueKey::decode_key(k) {
			return Ok(Self {
				ns: key.ns,
				db: key.db,
				ev: key.ev.into_owned(),
				ts: key.ts,
				owner: None,
			});
		}
		let key = EventQueueKey::decode_key(k)?;
		Ok(Self {
			ns: key.ns,
			db: key.db,
			ev: key.ev.into_owned(),
			ts: key.ts,
			owner: Some(key.node_id),
		})
	}

	/// How long ago the entry was enqueued. The timestamp is an HLC value
	/// whose upper bits are unix milliseconds.
	fn age(&self) -> Duration {
		let enqueued_ms = self.ts >> 16;
		Duration::from_millis(now_ms().saturating_sub(enqueued_ms))
	}
}

/// Now, in unix milliseconds on the wall clock the queue's timestamps and the
/// claims on its entries are stamped with.
fn now_ms() -> u64 {
	web_time::SystemTime::now()
		.duration_since(web_time::SystemTime::UNIX_EPOCH)
		.map(|d| d.as_millis() as u64)
		.unwrap_or(0)
}

/// Runs one batch of the queued async events in `range`.
///
/// `range` is one node's queue or the root-level queue, or the part of one
/// past the caller's last batch; the caller decides which queues this node
/// drains and under what lease. Each entry is claimed by the worker that
/// runs it, immediately before its body is evaluated, and an entry another
/// processor holds is left to it. Reports what was fetched — not what ran,
/// let alone successfully — and where the batch ended, so the caller can
/// tell a full batch, which may leave more behind, from a short one, and
/// resume after it. A lease found passed to another node before the fetch
/// ends the batch before it reads anything, reported as
/// [`EventBatch::lease_lost`]. Fails as [`process_events_batch`] fails, with
/// the batch's report attached as [`FailedBatch`].
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", target = "surrealdb::core::doc::event", skip_all)
)]
pub async fn process_next_events_batch(
	ds: &Datastore,
	range: &TypedRange<AsyncEventRecord>,
	lh: Option<&LeaseHandler>,
) -> Result<EventBatch> {
	if let Some(lh) = lh
		&& !lh.try_maintain_lease().await?
	{
		return Ok(EventBatch {
			lease_lost: true,
			..EventBatch::default()
		});
	}
	let res = fetch_batch(ds, range).await?;
	process_events_batch(ds, res, lh).await
}

/// Reads the next batch of `range` without holding a write transaction. The
/// values stay encoded so that an entry this binary cannot decode — one
/// written by a newer node, or a partial write — is skipped per entry rather
/// than failing the batch. Nothing but a successful run deletes a queue
/// entry, so failing the batch would stall the queue for good.
async fn fetch_batch(
	ds: &Datastore,
	range: &TypedRange<AsyncEventRecord>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let tx = ds.transaction(TransactionType::Read).await?;
	let res = catch!(tx, tx.scan_raw(range.clone(), ds.event_batch_size(), 0, None).await);
	tx.cancel().await?;
	Ok(res)
}

/// Whether `claim` keeps the processor `node` off the entry it is on, given
/// that no worker of `node`'s process is running the entry; the mark a
/// running worker leaves is consulted first, see [`RunningGuard`].
///
/// A claim this node wrote holds only while a worker of this process runs
/// the entry, which the process knows; one left by a worker that is gone —
/// the process before its last start, or a run whose release failed — is
/// void at once, so a restarted node runs the batch it was in the middle of
/// rather than waiting out a window on it. A claim another node wrote holds
/// for [`EVENT_CLAIM_TTL`] from when it was written, on the clocks the
/// cluster's nodes already keep close enough to expire one another's
/// heartbeats by: that node's worker cannot be asked, and a node that has
/// stopped heartbeating is presumed dead with its claims after the window.
fn claim_holds(node: Uuid, claim: &EventClaim, now: u64) -> bool {
	claim.node != node && now.saturating_sub(claim.at) < EVENT_CLAIM_TTL.as_millis() as u64
}

/// The queue entries a worker of this process is running, by key, shared by
/// every worker the datastore spawns; see [`RunningGuard`].
pub(crate) type RunningEvents = Arc<parking_lot::Mutex<HashSet<Vec<u8>>>>;

/// Marks an entry as being run by a worker of this process, from its claim to
/// its settlement. A second worker of the process — the event task's and an
/// explicit drain's, or two laps of the event task over one entry — is
/// refused the entry while the mark stands, before it reads the entry, and
/// the mark is what makes a claim this node wrote hold, for as long as the
/// body runs; see [`claim_holds`]. The mark goes when the guard drops, so a
/// worker that ends any way at all, a panic included, leaves nothing marked.
struct RunningGuard {
	running: RunningEvents,
	k: Vec<u8>,
}

impl RunningGuard {
	/// Marks `k`, or returns `None` if a worker of this process already has.
	fn enter(running: &RunningEvents, k: &[u8]) -> Option<Self> {
		if !running.lock().insert(k.to_vec()) {
			return None;
		}
		Some(Self {
			running: Arc::clone(running),
			k: k.to_vec(),
		})
	}
}

impl Drop for RunningGuard {
	fn drop(&mut self) {
		self.running.lock().remove(&self.k);
	}
}

/// Renews [`NodeDrainHeartbeatKey`] while a worker is evaluating a claimed
/// event body, so a body that runs past [`EVENT_CLAIM_TTL`] — including on an
/// owner whose node row was archived while the body was in flight — keeps its
/// claim alive against adopters without rewriting the queue entry itself.
#[cfg(not(target_family = "wasm"))]
struct ClaimRenewalGuard {
	handle: tokio::task::JoinHandle<()>,
}

#[cfg(not(target_family = "wasm"))]
impl ClaimRenewalGuard {
	fn start(tf: TransactionFactory, sequences: Sequences, nd: Uuid) -> Self {
		#[cfg(not(test))]
		let interval = (EVENT_CLAIM_TTL / 3).max(Duration::from_millis(1));
		#[cfg(test)]
		let interval = Duration::from_millis(100);
		let handle = spawn(async move {
			loop {
				sleep(interval).await;
				let Ok(tx) = tf.transaction(Write, sequences.clone()).await else {
					continue;
				};
				if tx
					.set_key(
						&NodeDrainHeartbeatKey {
							nd,
						},
						&now_ms(),
					)
					.await
					.is_ok()
				{
					let _ = tx.commit().await;
				} else {
					let _ = tx.cancel().await;
				}
			}
		});
		Self {
			handle,
		}
	}
}

#[cfg(not(target_family = "wasm"))]
impl Drop for ClaimRenewalGuard {
	fn drop(&mut self) {
		self.handle.abort();
	}
}

/// What a worker did with one entry of a batch.
#[derive(Debug)]
enum EntryOutcome {
	/// Another processor holds the entry, or claimed or settled it first, so
	/// its body was not evaluated; the entry is that processor's.
	Held,
	/// The body ran; whether the entry was then requeued for another attempt.
	Ran {
		requeued: bool,
	},
	/// An error other than the event's own — the claim's or the settlement's
	/// transaction failing — left the entry in place for the next pass, its
	/// claim released if one had landed. The batch fails with it once every
	/// worker has finished, so a drain does not report the queue flushed past
	/// the entry.
	Errored(anyhow::Error),
}

/// What became of the entries one worker was handed.
#[derive(Debug, Default)]
struct WorkerTally {
	ran: usize,
	requeued: usize,
	held: usize,
	/// Entries left in place by an error, and the first of those errors.
	errored: usize,
	error: Option<anyhow::Error>,
}

impl WorkerTally {
	fn record(&mut self, outcome: EntryOutcome) {
		match outcome {
			EntryOutcome::Held => self.held += 1,
			EntryOutcome::Ran {
				requeued,
			} => {
				self.ran += 1;
				self.requeued += usize::from(requeued);
			}
			EntryOutcome::Errored(e) => {
				self.errored += 1;
				self.error.get_or_insert(e);
			}
		}
	}

	/// Folds another worker's tally into this one.
	#[cfg(not(target_family = "wasm"))]
	fn merge(&mut self, other: Self) {
		self.ran += other.ran;
		self.requeued += other.requeued;
		self.held += other.held;
		self.errored += other.errored;
		if self.error.is_none() {
			self.error = other.error;
		}
	}

	/// Adds the tally to the batch's report, or fails the batch with the first
	/// error that left an entry in place. The entries that did run are settled
	/// and stay so; the pass that fails is retried from the head, and a drain
	/// does not report the queue flushed past the entry left behind.
	fn add_to(self, batch: &mut EventBatch) -> Result<()> {
		batch.ran += self.ran;
		batch.requeued += self.requeued;
		batch.held += self.held;
		match self.error {
			Some(e) => {
				Err(e.context(format!("{} queued async events were left in place", self.errored)))
			}
			None => Ok(()),
		}
	}
}

/// Runs the entries of one fetched batch. Reports how many were fetched and
/// where the batch ended, how many were run, how many of those were requeued
/// for another attempt, how many were left to another processor holding
/// them, and the age of the oldest among them. Fails, once every worker has
/// finished, with the first error that left an entry in place — the claim's
/// or the settlement's transaction failing, a worker ending without settling
/// what it held, or the lease check failing before an entry — so the pass
/// is retried from the head and a drain does not report the queue flushed
/// past it; what the other workers ran stands, and the error carries the
/// batch's report as [`FailedBatch`] so the pass still accounts it. The
/// lease is checked before each entry is handed out, and one found passed to
/// another node is not a failure: no further entry is handed out, and the
/// batch reports [`EventBatch::lease_lost`] once its workers have finished
/// the entries they hold, so the two nodes do not both run the queue.
///
/// Returns only once every worker it spawned has: the entries they hold are
/// user-defined SurrealQL of unbounded duration, each settled in its own
/// transaction, and `Datastore::shutdown` awaits the pass and then closes the
/// storage engine. That holds whatever ends the batch, including a lease that
/// can no longer be maintained because the pass was cancelled.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn process_events_batch(
	ds: &Datastore,
	res: Vec<(Vec<u8>, Vec<u8>)>,
	lh: Option<&LeaseHandler>,
) -> Result<EventBatch> {
	if lh.is_none()
		&& let Some(limit) = ds.joined_event_workers()
	{
		return process_events_joined(ds, res, limit).await;
	}
	let mut batch = EventBatch {
		fetched: res.len(),
		last_key: res.last().map(|(k, _)| k.clone()),
		..EventBatch::default()
	};
	if res.is_empty() {
		return Ok(batch);
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
		// Spawn a worker, which reports what became of its entries.
		let jh = spawn(async move {
			let mut tally = WorkerTally::default();
			while let Ok(event_context) = receiver.recv().await {
				tally.record(event_context.run_event_checked().await);
			}
			tally
		});
		join_handles.push(jh);
	}

	// Producer: hands out entries until the batch is exhausted, the lease
	// passes to another node, or it can no longer be maintained. The lease
	// is checked before each entry is handed out, so no worker is handed one
	// after the check that found the lease gone: the fetch the batch read
	// first may have outlasted it.
	let produced: Result<()> = async {
		for (k, v) in res {
			if let Some(lh) = lh
				&& !lh.try_maintain_lease().await?
			{
				batch.lease_lost = true;
				break;
			}
			let Some((key, ev)) = decode_queued(&k, &v) else {
				continue;
			};
			batch.oldest_age = batch.oldest_age.max(Some(key.age()));
			match AsyncEventContext::new(ds, k, v, key, ev) {
				Ok(event_context) => sender.send(event_context).await?,
				Err(e) => {
					// Log and skip this entry so other events can still be processed.
					error!("Unexpected Error while processing event: {e}");
				}
			};
		}
		Ok(())
	}
	.await;

	// Whatever ended the producer, the workers finish the entries they hold
	// before the batch returns; see the contract above.
	sender.close();
	let mut tally = WorkerTally::default();
	for jh in join_handles {
		match jh.await {
			Ok(worker) => tally.merge(worker),
			// A worker that panicked settled nothing it held: its entry stays
			// in place, claimed but unmarked, for the next pass to take over,
			// and its count went with it. The batch fails rather than report
			// the queue drained past the entry.
			Err(e) => tally.record(EntryOutcome::Errored(
				anyhow::Error::new(e).context("an async event worker ended without settling"),
			)),
		}
	}
	// The counts stand whatever ended the batch. A lease the producer could
	// not maintain is reported with the entries the workers had settled by
	// then, and reads as the lease's failure rather than as a worker's.
	let tallied = tally.add_to(&mut batch);
	match produced.and(tallied) {
		Ok(()) => Ok(batch),
		Err(e) => Err(e.context(FailedBatch(batch))),
	}
}

/// Runs the entries of one fetched batch, sequentially: `tokio::spawn` is
/// unavailable on wasm. Reports the same as the parallel runner.
#[cfg(target_family = "wasm")]
pub(crate) async fn process_events_batch(
	ds: &Datastore,
	res: Vec<(Vec<u8>, Vec<u8>)>,
	lh: Option<&LeaseHandler>,
) -> Result<EventBatch> {
	if lh.is_none()
		&& let Some(limit) = ds.joined_event_workers()
	{
		return process_events_joined(ds, res, limit).await;
	}
	let mut batch = EventBatch {
		fetched: res.len(),
		last_key: res.last().map(|(k, _)| k.clone()),
		..EventBatch::default()
	};
	let mut tally = WorkerTally::default();
	let ran: Result<()> = async {
		for (k, v) in res {
			if let Some(lh) = lh
				&& !lh.try_maintain_lease().await?
			{
				batch.lease_lost = true;
				break;
			}
			let Some((key, ev)) = decode_queued(&k, &v) else {
				continue;
			};
			batch.oldest_age = batch.oldest_age.max(Some(key.age()));
			let event_context = AsyncEventContext::new(ds, k, v, key, ev)?;
			tally.record(event_context.run_event_checked().await);
		}
		Ok(())
	}
	.await;
	let tallied = tally.add_to(&mut batch);
	match ran.and(tallied) {
		Ok(()) => Ok(batch),
		Err(e) => Err(e.context(FailedBatch(batch))),
	}
}

/// Runs the entries of one fetched batch, at most `limit` at once (at least one), all polled on the
/// calling task: nothing is spawned, so whatever the caller set up for its task reaches every
/// event's transactions, on a target without a task spawner too. Reports and fails as
/// [`process_events_batch`] does, once every entry it started has finished. Each entry is claimed
/// by the future about to run it, so no more than `limit` entries are claimed at a time.
async fn process_events_joined(
	ds: &Datastore,
	res: Vec<(Vec<u8>, Vec<u8>)>,
	limit: usize,
) -> Result<EventBatch> {
	use futures::StreamExt;
	let mut batch = EventBatch {
		fetched: res.len(),
		last_key: res.last().map(|(k, _)| k.clone()),
		..EventBatch::default()
	};
	let mut events = Vec::with_capacity(res.len());
	let mut built: Result<()> = Ok(());
	for (k, v) in res {
		let Some((key, ev)) = decode_queued(&k, &v) else {
			continue;
		};
		batch.oldest_age = batch.oldest_age.max(Some(key.age()));
		match AsyncEventContext::new(ds, k, v, key, ev) {
			Ok(event_context) => events.push(event_context),
			Err(e) => {
				built = Err(e);
				break;
			}
		}
	}
	let mut tally = WorkerTally::default();
	let mut running = futures::stream::iter(events)
		.map(AsyncEventContext::run_event_checked)
		.buffer_unordered(limit.max(1));
	while let Some(outcome) = running.next().await {
		tally.record(outcome);
	}
	let tallied = tally.add_to(&mut batch);
	match built.and(tallied) {
		Ok(()) => Ok(batch),
		Err(e) => Err(e.context(FailedBatch(batch))),
	}
}

/// Decode one queued event, reporting and discarding an entry this binary
/// cannot read.
///
/// A queue entry is only removed once it has run, so an undecodable entry has
/// to be stepped over rather than propagated: returning an error here would
/// leave the entry in place and fail every later batch the same way.
///
/// Both halves are decoded here, before any transaction is opened for the
/// entry. An entry whose *key* cannot be decoded is as unprocessable as one
/// whose value cannot: the key names the namespace and database the record is
/// checked against, so it could never be run. Neither half is deleted,
/// matching how the other queue drains treat entries written by a newer node
/// during a rolling upgrade.
fn decode_queued(k: &[u8], v: &[u8]) -> Option<(QueuedKey, AsyncEventRecord)> {
	let key = match QueuedKey::decode(k) {
		Ok(key) => key,
		Err(e) => {
			error!("Skipping async event queue entry with an undecodable key: {e} - Key: {k:?}");
			return None;
		}
	};
	match KVValue::kv_decode_value(v, ()) {
		Ok(ev) => Some((key, ev)),
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
	/// This node, which is who the claim names.
	node: Uuid,
	/// The entries this process's workers are running; see [`RunningGuard`].
	running: RunningEvents,
	/// This worker's mark on its entry, taken by the claim and held to the
	/// end of the run.
	running_guard: Option<RunningGuard>,
	/// Delivers the live-query notifications an attempt's writes raise, once
	/// that attempt commits. `None` when the datastore delivers none inline.
	broker: Option<Arc<crate::dbs::OrderedBroker>>,
	/// The entry's key as stored, which is how it is deleted or requeued.
	k: Vec<u8>,
	/// The entry's value as this processor last read or wrote it: as fetched
	/// until the claim lands, then as the claim wrote it. Every write to the
	/// entry — the claim, the delete, the requeue — is conditional on it still
	/// reading as this, so a processor cannot claim over another's claim, nor
	/// settle over a run that took the entry from it once its claim was void.
	stored: Vec<u8>,
	/// The same key decoded.
	key: QueuedKey,
	v: Option<AsyncEventRecord>,
}

impl AsyncEventContext {
	fn new(
		ds: &Datastore,
		k: Vec<u8>,
		stored: Vec<u8>,
		key: QueuedKey,
		v: AsyncEventRecord,
	) -> Result<Self> {
		Ok(Self {
			ctx: Some(ds.setup_ctx()?),
			opt: ds.setup_options(&Session::default()),
			tf: ds.transaction_factory().clone(),
			sequences: ds.sequences().clone(),
			node: ds.id(),
			running: Arc::clone(ds.running_events()),
			running_guard: None,
			broker: ds.live_query_broker(),
			k,
			stored,
			key,
			v: Some(v),
		})
	}

	/// Claims the entry, then runs the event. An error of the event's own is
	/// settled by the run; one of the claim's or the settlement's transaction
	/// is reported, and leaves the entry in place for the next pass.
	async fn run_event_checked(mut self) -> EntryOutcome {
		match self.claim().await {
			Ok(true) => {}
			Ok(false) => return EntryOutcome::Held,
			Err(e) => {
				error!("Unexpected error while claiming an event. Error: {e} - Key: {:?}", self.k);
				return EntryOutcome::Errored(e);
			}
		}
		// Stands in for a bug in what runs from here: the worker ends without
		// settling the entry it has claimed.
		#[cfg(test)]
		maybe_inject_panic(PanicSite::AsyncEventWorker, self.node);
		let (Some(ctx), Some(v)) = (self.ctx.take(), self.v.take()) else {
			return EntryOutcome::Errored(anyhow::anyhow!("the event context has already run"));
		};
		match self.run_event(ctx, v).await {
			Ok(requeued) => EntryOutcome::Ran {
				requeued,
			},
			Err(e) => {
				error!(
					"Unexpected error while processing an event. Error: {e} - Key: {:?}",
					self.k
				);
				self.release().await;
				EntryOutcome::Errored(e)
			}
		}
	}

	async fn new_write_tx(&self) -> Result<Transaction> {
		self.tf.transaction(Write, self.sequences.clone()).await
	}

	async fn new_read_tx(&self) -> Result<Transaction> {
		self.tf.transaction(TransactionType::Read, self.sequences.clone()).await
	}

	/// Whether a root-level queue entry is still owned by the live legacy node
	/// that queued it.
	///
	/// A node on a release that predates per-node queues and [`EventClaim`]
	/// writes root-level `!eq` entries carrying its `node_id`, refreshes its
	/// `!nd` row without stamping [`NodeDrainHeartbeatKey`], and drains its own
	/// root entries without claiming them. Leaving those entries to it while
	/// its row is active and ahead of [`NodeDrainHeartbeatKey`] keeps an
	/// upgraded adopter from evaluating them alongside that owner.
	async fn root_owner_holds(&self, owner: Uuid) -> Result<bool> {
		let tx = self.new_read_tx().await?;
		let res: Result<bool> = async {
			let Some(node) = tx.get_node(owner).await?.filter(|node| !node.is_archived()) else {
				return Ok(false);
			};
			let stamp = NodeDrainHeartbeatKey {
				nd: owner,
			};
			Ok(tx.get_key(&stamp, None).await? < Some(node.heartbeat.value))
		}
		.await;
		let _ = tx.cancel().await;
		res
	}

	/// Whether the processor named by `held` has refreshed its
	/// [`NodeDrainHeartbeatKey`] since writing the claim and within
	/// [`EVENT_CLAIM_TTL`] of `now`.
	///
	/// A worker running an event body renews its node's drain heartbeat without
	/// rewriting the queue entry itself (which would conflict with the event's
	/// own settlement commit), so a body that runs past [`EVENT_CLAIM_TTL`]
	/// keeps its claim alive against adopters for as long as the worker runs.
	async fn holder_drain_heartbeat_holds(&self, held: &EventClaim, now: u64) -> Result<bool> {
		let tx = self.new_read_tx().await?;
		let stamp = NodeDrainHeartbeatKey {
			nd: held.node,
		};
		let beat = tx.get_key(&stamp, None).await;
		let _ = tx.cancel().await;
		Ok(beat?.is_some_and(|beat| {
			beat > held.at && now.saturating_sub(beat) < EVENT_CLAIM_TTL.as_millis() as u64
		}))
	}

	/// Releases the claim after a run that ended in an error of its
	/// transactions' rather than the event's, so that an adopter's next pass
	/// retries the entry at once rather than once the window passes; this
	/// node's own next pass would either way, the mark going with this
	/// worker. Best effort: what ended the run may end this too, and the
	/// claim then stands to other nodes until its window passes, which is
	/// what the window is for.
	async fn release(&self) {
		let released: Result<bool> = async {
			let mut ev = AsyncEventRecord::kv_decode_value(&self.stored, ())?;
			ev.claim = None;
			let bytes = ev.kv_encode_value()?;
			let tx = self.new_write_tx().await?;
			let write = tx
				.put_compare(Key::from(self.k.as_slice()), bytes, Some(self.stored.clone()))
				.await;
			self.settle(tx, write).await
		}
		.await;
		if let Err(e) = released {
			debug!(
				"Async event `{}` keeps its claim after a failed run: {e} - Key: {:?}",
				self.key.ev, self.k
			);
		}
	}

	/// Claims the entry for this processor, immediately before its body is
	/// evaluated and in a transaction of its own. Returns whether the claim
	/// landed.
	///
	/// The claim is what keeps one queued event to one evaluation. The
	/// entry's own writes are transactional, and the settlement being
	/// conditional on the entry discards a second processor's, but an effect
	/// the body has outside its transaction — an HTTP request, say — is not
	/// undone by a discarded commit. So the body runs only once the entry
	/// reads as this processor's, and it does not when another processor's
	/// claim on the entry as fetched holds (see [`claim_holds`]), or when the
	/// entry no longer reads as fetched — claimed, requeued or removed by
	/// another processor since — in which case what it reads as now is the
	/// next fetch's to judge. A claim that no longer holds is overwritten: its
	/// holder is presumed to have died with it, and if it has not, its
	/// settlement finds the entry no longer as it wrote it and is discarded.
	///
	/// The claim is written here, by the worker, and not for the whole batch
	/// at the fetch: it fences nothing until it is committed, and its age is
	/// what another node judges it by, so a claim written at the fetch
	/// could be older than [`EVENT_CLAIM_TTL`] by the time a worker reached
	/// the entry behind a long batch, and the entry taken over before its
	/// body ran.
	async fn claim(&mut self) -> Result<bool> {
		// One worker per entry per process: a second is refused here, before
		// the entry is read, for as long as the first is running it.
		let Some(running) = RunningGuard::enter(&self.running, &self.k) else {
			debug!(
				"Async event `{}` is being run by a worker of this node - Key: {:?}",
				self.key.ev, self.k
			);
			return Ok(false);
		};
		let now = now_ms();
		let node = self.node;
		if let Some(owner) = self.key.owner
			&& owner != node
			&& self.root_owner_holds(owner).await?
		{
			debug!(
				"Async event `{}` in the root queue is held by its live owner - Key: {:?}",
				self.key.ev, self.k
			);
			return Ok(false);
		}
		let held = self.v.as_ref().and_then(|ev| ev.claim);
		if let Some(held) = held
			&& (claim_holds(node, &held, now)
				|| (held.node != node && self.holder_drain_heartbeat_holds(&held, now).await?))
		{
			debug!(
				"Async event `{}` is held by another processor - Key: {:?}",
				self.key.ev, self.k
			);
			return Ok(false);
		}
		let Some(ev) = self.v.as_mut() else {
			return Ok(false);
		};
		ev.claim = Some(EventClaim {
			node,
			at: now,
		});
		let claimed = ev.kv_encode_value()?;
		let tx = self.new_write_tx().await?;
		let write = tx
			.put_compare(Key::from(self.k.as_slice()), claimed.clone(), Some(self.stored.clone()))
			.await;
		let landed = match Self::entry_as_read(write) {
			Ok(true) => {
				// Stands in for a commit that failed for a reason other than
				// a conflict. A real one leaves the transaction finished, so
				// the injected one has to as well.
				#[cfg(test)]
				if let Err(e) =
					maybe_inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, node)
				{
					let _ = tx.cancel().await;
					return Err(e);
				}
				// A claim another processor committed meanwhile, on a backend
				// that detects the conflict at commit, reports it from here:
				// the entry is that processor's, and nothing here is an error.
				match tx.commit().await {
					Ok(()) => true,
					Err(e)
						if is_conditional_write_conflict(&e)
							|| is_retryable_transaction_conflict(&e) =>
					{
						false
					}
					Err(e) => return Err(e),
				}
			}
			Ok(false) => {
				let _ = tx.cancel().await;
				false
			}
			Err(e) => {
				let _ = tx.cancel().await;
				return Err(e);
			}
		};
		if !landed {
			self.taken();
			return Ok(false);
		}
		self.stored = claimed;
		self.running_guard = Some(running);
		Ok(true)
	}

	/// Runs the event and settles its queue entry.
	///
	/// The entry was claimed by this worker immediately before, which is what
	/// keeps a second processor that fetched it — an adopter and an owner
	/// archived while still running, or a caller of
	/// `Datastore::drain_async_events` racing the event task — from evaluating
	/// the body as well; see [`Self::claim`]. Every write to the entry — the
	/// delete on success, the requeue or delete on failure — is conditional on
	/// the entry still reading as the claim wrote it, so a run that outlived
	/// its claim and was taken over has its settlement discarded whole rather
	/// than landing on top of the other's.
	///
	/// A transaction conflict is contention, not a failure of the event, so
	/// the attempt is re-run in place up to [`EVENT_CONFLICT_RETRIES`] times
	/// before the definition's `RETRY` budget is charged.
	///
	/// Returns whether the entry was requeued for another attempt, which the
	/// drain owes it now: retries are immediate, without backoff.
	async fn run_event(&mut self, ctx: Context, mut ev: AsyncEventRecord) -> Result<bool> {
		#[cfg(not(target_family = "wasm"))]
		let _renewal = ClaimRenewalGuard::start(self.tf.clone(), self.sequences.clone(), self.node);
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
			// The body's writes are the event's own: their live-query
			// notifications are buffered and released with this attempt's
			// commit, so a rolled-back or re-run attempt notifies nobody.
			let ctx = {
				let mut ctx = ctx;
				if let Some(prepared) = self.broker.clone().and_then(|delivery| {
					crate::dbs::executor::install_broker(
						&mut ctx,
						delivery,
						crate::dbs::live_recheck::RecheckSource {
							tf: self.tf.clone(),
							sequences: self.sequences.clone(),
						},
					)
				}) {
					ctx.tx()
						.on_commit(Box::new(crate::dbs::executor::FlushOnCommit::new(prepared)))
						.await;
				}
				ctx
			};
			let e = match self.run_event_once(&ctx, &ev).await {
				Ok(true) => return Ok(false),
				Ok(false) => {
					self.taken();
					return Ok(false);
				}
				Err(e) => e,
			};
			// Contention or an unknown commit outcome, not a failure of the event:
			// re-run it rather than charging an attempt against the definition's
			// `RETRY` budget. See [`EVENT_CONFLICT_RETRIES`].
			let transient = is_retryable_transaction_conflict(&e) || is_indeterminate_commit(&e);
			if transient && conflicts < EVENT_CONFLICT_RETRIES {
				conflicts += 1;
				debug!("Re-running the event `{}` in place: {e}", self.key.ev);
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
		let tx = self.new_write_tx().await?;
		if let Some(final_error) = Self::is_final_error(&err).await? {
			self.final_error(tx, final_error).await?;
			return Ok(false);
		}
		self.retry_attempt(tx, err, &mut ev).await
	}

	/// One attempt at a queued event: run it and remove its queue entry in a
	/// single transaction, so the event is neither run twice nor lost. Returns
	/// `Ok(false)` when the entry no longer reads as claimed: another processor
	/// took it over and this attempt's writes were discarded.
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
	/// The entry has to read as claimed in this attempt's own transaction
	/// before the body runs. Only a run removes an entry, and an attempt can
	/// follow one that already ran it: a re-run after a conflict, or after a
	/// commit that applied while reporting an unknown outcome, would find the
	/// entry gone with the event's writes, and another node can take the entry
	/// over once the claim's window passes under a long body. Either way the
	/// body is not evaluated
	/// again: the attempt ends here with `Ok(false)`. One that started before
	/// the removal loses the conflict on the entry's key at commit instead,
	/// and the re-run that follows ends here.
	async fn run_event_once(&self, ctx: &FrozenContext, ev: &AsyncEventRecord) -> Result<bool> {
		let tx = ctx.tx();
		match tx.get(Key::from(self.k.as_slice()), None).await {
			Ok(Some(stored)) if stored == self.stored => {}
			Ok(_) => {
				let _ = tx.cancel().await;
				return Ok(false);
			}
			Err(e) => {
				let _ = tx.cancel().await;
				return Err(e);
			}
		}
		if let Err(e) = Self::process_event(ctx, &self.opt, &self.key, ev).await {
			// Roll back the event's partial side effects. The cancel outcome is
			// discarded so it cannot displace the error being reported.
			let _ = tx.cancel().await;
			return Err(e);
		}
		// The delete is in the same transaction as the event's own writes, so
		// either both land or neither does, and only while the entry still
		// reads as claimed.
		match Self::entry_as_read(self.delete_as_read(&tx).await) {
			Ok(true) => {}
			Ok(false) => {
				let _ = tx.cancel().await;
				return Ok(false);
			}
			Err(e) => {
				let _ = tx.cancel().await;
				return Err(e);
			}
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
			self.node,
		) {
			let _ = tx.cancel().await;
			return Err(e);
		}
		// A backend that validates the condition at commit reports the entry
		// taken from here rather than from the delete.
		let res = Self::entry_as_read(tx.commit().await);
		#[cfg(test)]
		if matches!(res, Ok(true)) {
			maybe_inject_non_retryable_error(
				NonRetryableErrorSite::AsyncEventCommitAppliedUnknown,
				self.node,
			)?;
		}
		res
	}

	/// Deletes the entry if it still reads as claimed.
	async fn delete_as_read(&self, tx: &Transaction) -> Result<()> {
		tx.del_compare(Key::from(self.k.as_slice()), Some(self.stored.as_slice())).await
	}

	/// Interprets the result of a write conditional on the entry as claimed:
	/// `Ok(false)` when the condition failed because another processor has
	/// since taken over, deleted or requeued the entry. Any other error
	/// propagates.
	fn entry_as_read(res: Result<()>) -> Result<bool> {
		match res {
			Ok(()) => Ok(true),
			Err(e) if is_conditional_write_conflict(&e) => Ok(false),
			Err(e) => Err(e),
		}
	}

	/// Commits `tx`, whose only write is `write`, conditional on the entry as
	/// claimed. A backend that validates the condition at commit reports a
	/// failed condition from the commit rather than from the write, so both
	/// are read the same way: the entry was settled by another processor, and
	/// nothing here is an error. Returns whether this processor's write is the
	/// one that landed.
	async fn settle(&self, tx: Transaction, write: Result<()>) -> Result<bool> {
		let as_read = match Self::entry_as_read(write) {
			Ok(true) => {
				// Stands in for a commit that failed for a reason other than
				// a conflict; see the claim's counterpart.
				#[cfg(test)]
				if let Err(e) = maybe_inject_non_retryable_error(
					NonRetryableErrorSite::AsyncEventSettle,
					self.node,
				) {
					let _ = tx.cancel().await;
					return Err(e);
				}
				// `commit` finishes the transaction whether or not it
				// succeeds, so there is nothing to cancel after a failed one.
				Self::entry_as_read(tx.commit().await)?
			}
			Ok(false) => {
				let _ = tx.cancel().await;
				false
			}
			Err(e) => {
				let _ = tx.cancel().await;
				return Err(e);
			}
		};
		if !as_read {
			self.taken();
		}
		Ok(as_read)
	}

	/// Notes that the entry no longer reads as this processor last saw it —
	/// another processor claimed, requeued or removed it first, or took it
	/// over once this processor's claim no longer held — so this processor's
	/// write is discarded. Not an error: it is how two processors holding one
	/// entry converge on one evaluation and one settlement.
	fn taken(&self) {
		debug!(
			"Async event `{}` was claimed or settled by another processor first - Key: {:?}",
			self.key.ev, self.k
		);
	}

	/// Requeues the entry for another attempt, releasing the claim so that
	/// whichever processor fetches it next takes it, or removes it once the
	/// retries are spent, in either case only if it still reads as claimed.
	/// Returns whether the entry was requeued.
	async fn retry_attempt(
		&self,
		tx: Transaction,
		e: anyhow::Error,
		ev: &mut AsyncEventRecord,
	) -> Result<bool> {
		// `attempt` is incremented when requeuing; `retry` counts retries, so requeue while
		// attempt <= retry.
		ev.attempt += 1;
		if ev.attempt <= ev.event_definition.retry() {
			// Requeue under the same key so the event keeps its original queue
			// position; retries are bounded here and no backoff is applied.
			ev.claim = None;
			let bytes = catch!(tx, ev.kv_encode_value());
			let write = tx
				.put_compare(Key::from(self.k.as_slice()), bytes, Some(self.stored.clone()))
				.await;
			self.settle(tx, write).await
		} else {
			warn!(
				"Final error after processing the event `{}` on table {} {} times: {e}",
				self.key.ev, ev.event_definition.target_table, ev.attempt
			);
			let write = self.delete_as_read(&tx).await;
			self.settle(tx, write).await?;
			Ok(false)
		}
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

	/// Removes an entry whose error is final, if it still reads as claimed.
	async fn final_error(&self, tx: Transaction, e: &Error) -> Result<()> {
		warn!("Event processing failed: {:?}", e);
		let write = self.delete_as_read(&tx).await;
		self.settle(tx, write).await?;
		Ok(())
	}

	/// Execute a queued event.
	async fn process_event(
		ctx: &FrozenContext,
		opt: &Options,
		eq: &QueuedKey,
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
	use std::sync::Arc;

	use anyhow::Result;
	use uuid::Uuid;

	use super::{
		AsyncEventContext, AsyncEventRecord, EventClaim, QueuedKey, claim_holds, decode_queued,
	};
	#[cfg(not(target_family = "wasm"))]
	use super::{Duration, FailedBatch, process_events_batch, sleep};
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::dbs::Session;
	use crate::key::schema::{EventQueueKey, NodeEventQueueKey, NodeEventQueuePrefix};
	use crate::key::{KVKey, KVValue, Key};
	#[cfg(not(target_family = "wasm"))]
	use crate::kvs::testing::{NonRetryableErrorSite, inject_non_retryable_error};
	use crate::kvs::{Datastore, EVENT_CLAIM_TTL, NORMAL_BATCH_SIZE, TransactionType};
	use crate::types::PublicValue;

	/// A root-level entry, as written by a node on a release that queues at the
	/// root.
	fn legacy_key() -> Vec<u8> {
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

	/// A per-node entry, as the write path enqueues them.
	fn node_key() -> Vec<u8> {
		NodeEventQueueKey {
			nd: Uuid::from_u128(7),
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned("tb".into()),
			ev: Cow::Owned("ev".into()),
			ts: 42,
		}
		.encode_key()
		.expect("a well-formed queue key must encode")
		.to_vec()
	}

	/// Both layouts decode to the same view of the entry, so a drain reads a
	/// root-level entry left by an older node exactly as it reads its own. If
	/// either regressed, every event under that layout would be silently
	/// skipped rather than processed.
	#[test]
	fn either_queue_layout_decodes() {
		for encoded in [legacy_key(), node_key()] {
			let decoded = QueuedKey::decode(&encoded).expect("valid key must decode");
			assert_eq!(decoded.ns, NamespaceId(1));
			assert_eq!(decoded.db, DatabaseId(2));
			assert_eq!(decoded.ev, "ev");
			assert_eq!(decoded.ts, 42);
		}
	}

	/// An entry whose key cannot be decoded must be stepped over here, before a
	/// transaction is opened for it: because a queue entry is only removed once
	/// it has run, an entry that reached `run_event` and failed there would
	/// stay put and fail every subsequent batch the same way.
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
		for encoded in [legacy_key(), node_key()] {
			assert!(
				decode_queued(&encoded, b"not-an-async-event-record").is_none(),
				"an undecodable value must still be skipped"
			);
		}
	}

	/// A datastore with one async event queued on this node, and that entry as
	/// a drain fetches it. The maintenance tasks are off, so nothing but the
	/// test touches the queue.
	async fn one_queued_event() -> Result<(Arc<Datastore>, Session, Vec<u8>, Vec<u8>)> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for result in ds
			.execute(
				"DEFINE NAMESPACE test; DEFINE DATABASE test;
				 DEFINE TABLE person SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT log ON person ASYNC RETRY 3 THEN (CREATE logged SET who = $after.id);
				 CREATE person:1 RETURN NONE;",
				&session,
				None,
			)
			.await?
		{
			result.result?;
		}
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let txn = ds.transaction(TransactionType::Read).await?;
		let mut entries = txn.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await?;
		txn.cancel().await?;
		assert_eq!(entries.len(), 1, "the create queues exactly one event");
		let (k, v) = entries.remove(0);
		Ok((ds, session, k, v))
	}

	/// The entry's decoded halves, as a worker reads them.
	fn decoded(k: &[u8], v: &[u8]) -> (QueuedKey, AsyncEventRecord) {
		decode_queued(k, v).expect("the write path queues a decodable entry")
	}

	/// The entry as a worker is handed it: decoded, with its value as fetched.
	fn context(ds: &Datastore, k: &[u8], v: &[u8]) -> Result<AsyncEventContext> {
		let (key, ev) = decoded(k, v);
		AsyncEventContext::new(ds, k.to_vec(), v.to_vec(), key, ev)
	}

	/// Claims the entry, as a worker does immediately before running it.
	async fn claim(event: &mut AsyncEventContext) -> Result<bool> {
		event.claim().await
	}

	/// Runs the event as a worker would, surfacing the error a worker only
	/// logs. Returns whether the entry was requeued.
	async fn run(event: &mut AsyncEventContext) -> Result<bool> {
		let ctx = event.ctx.take().expect("the context has not run yet");
		let ev = event.v.take().expect("the context has not run yet");
		event.run_event(ctx, ev).await
	}

	/// What another processor does on winning the race: the entry goes.
	async fn delete_entry(ds: &Datastore, k: &[u8]) -> Result<()> {
		let txn = ds.transaction(TransactionType::Write).await?;
		txn.del(Key::from(k)).await?;
		txn.commit().await
	}

	/// Writes `ev` over the entry, as another processor's requeue or claim
	/// does, and returns the bytes now stored.
	async fn write_entry(ds: &Datastore, k: &[u8], ev: &AsyncEventRecord) -> Result<Vec<u8>> {
		let bytes = ev.kv_encode_value()?;
		let txn = ds.transaction(TransactionType::Write).await?;
		txn.set(Key::from(k), bytes.clone()).await?;
		txn.commit().await?;
		Ok(bytes)
	}

	/// The queued entry decoded, or `None` once it is gone.
	async fn queued_record(ds: &Datastore, k: &[u8]) -> Result<Option<AsyncEventRecord>> {
		let txn = ds.transaction(TransactionType::Read).await?;
		let val = txn.get(Key::from(k), None).await?;
		txn.cancel().await?;
		val.map(|v| AsyncEventRecord::kv_decode_value(&v, ())).transpose()
	}

	/// The attempt count of the queued entry, or `None` once it is gone.
	async fn queued_attempt(ds: &Datastore, k: &[u8]) -> Result<Option<u16>> {
		Ok(queued_record(ds, k).await?.map(|ev| ev.attempt))
	}

	async fn logged_count(ds: &Datastore, session: &Session) -> Result<usize> {
		let mut res = ds.execute("SELECT * FROM logged", session, None).await?;
		let value = res.remove(0).result?;
		let PublicValue::Array(a) = &value else {
			panic!("expected an array, got {value:?}");
		};
		Ok(a.len())
	}

	/// The control for the races below: a worker whose claim lands, and that
	/// finds the entry as claimed when it settles, lands the event's writes
	/// and removes the entry.
	#[tokio::test]
	async fn a_run_that_finds_its_entry_as_claimed_lands() -> Result<()> {
		let (ds, session, k, v) = one_queued_event().await?;
		let mut event = context(&ds, &k, &v)?;
		assert!(claim(&mut event).await?, "a free entry is claimed");

		run(&mut event).await?;

		assert_eq!(logged_count(&ds, &session).await?, 1, "the event's writes land");
		assert_eq!(queued_attempt(&ds, &k).await?, None, "the entry is removed");
		Ok(())
	}

	/// Two processors can fetch one entry: an adopter and an owner archived
	/// while still running, or the event task and a caller of
	/// `drain_async_events`. Only the first to claim it runs it. The second's
	/// claim fails whether it fetched before that claim landed or after, so
	/// it never evaluates the body — whose effects outside its transaction,
	/// an HTTP request say, no discarded commit would take back.
	#[tokio::test]
	async fn an_entry_another_processor_holds_is_not_claimed() -> Result<()> {
		let (ds, session, k, v) = one_queued_event().await?;
		let mut first = context(&ds, &k, &v)?;
		assert!(claim(&mut first).await?);

		// Fetched before the claim landed, or after: a worker of this process
		// is running the entry, and a second is refused it either way.
		let mut stale = context(&ds, &k, &v)?;
		assert!(!claim(&mut stale).await?, "an entry this node is running is not claimed again");
		let mut fresh = context(&ds, &k, &first.stored)?;
		assert!(!claim(&mut fresh).await?, "however it was fetched");

		run(&mut first).await?;
		assert_eq!(logged_count(&ds, &session).await?, 1, "the holder's run lands");
		assert_eq!(queued_attempt(&ds, &k).await?, None, "and removes the entry");
		Ok(())
	}

	/// Another node's claim holds for [`EVENT_CLAIM_TTL`] and no longer: its
	/// holder may have died with it, and an adopter that finds a departed
	/// owner's claims waits out the window, then takes the entries. This
	/// node's own claim is not judged by time at all but by whether a worker
	/// of this process is running the entry: one that is not — after a
	/// restart, or a release that failed — is void at once.
	#[tokio::test]
	async fn a_claim_holds_for_another_nodes_window_or_while_this_node_runs_the_entry() -> Result<()>
	{
		let (ds, _session, k, v) = one_queued_event().await?;
		let now = ds.clock_now().value;
		let ttl = EVENT_CLAIM_TTL.as_millis() as u64;
		let other = Uuid::from_u128(9);
		let holds = |node, at| {
			claim_holds(
				ds.id(),
				&EventClaim {
					node,
					at,
				},
				now,
			)
		};
		assert!(holds(other, now), "another node's fresh claim holds");
		assert!(holds(other, now - ttl + 1), "and holds to the end of the window");
		assert!(!holds(other, now - ttl), "then it is void");
		assert!(!holds(ds.id(), now), "this node's own claim is not held by time");

		// As a worker applies the rule: a held entry is left to its holder, a
		// void claim is taken over, and this node's own claim is void once no
		// worker of this process is running the entry.
		let (_, mut ev) = decoded(&k, &v);
		ev.claim = Some(EventClaim {
			node: ds.id(),
			at: now,
		});
		let stored = write_entry(&ds, &k, &ev).await?;
		let mut retaken = context(&ds, &k, &stored)?;
		assert!(claim(&mut retaken).await?, "a claim this node wrote and is not running is void");
		drop(retaken);
		ev.claim = Some(EventClaim {
			node: other,
			at: now,
		});
		let stored = write_entry(&ds, &k, &ev).await?;
		let mut held = context(&ds, &k, &stored)?;
		assert!(!claim(&mut held).await?, "an entry another node holds is left to it");
		ev.claim = Some(EventClaim {
			node: other,
			at: now - ttl,
		});
		let stored = write_entry(&ds, &k, &ev).await?;
		let mut taken = context(&ds, &k, &stored)?;
		assert!(claim(&mut taken).await?, "an entry whose claim expired is taken over");
		assert_eq!(
			queued_record(&ds, &k).await?.and_then(|ev| ev.claim).map(|claim| claim.node),
			Some(ds.id()),
			"and is this node's now"
		);
		Ok(())
	}

	/// Another node honours a claim only for its window, so a run that
	/// outlives it can be taken over and settled by that node. Its own
	/// settlement then finds the entry no longer as it claimed it and is
	/// discarded whole, or the event's writes would land twice.
	#[tokio::test]
	async fn a_run_whose_entry_another_processor_settled_is_discarded() -> Result<()> {
		let (ds, session, k, v) = one_queued_event().await?;
		let mut event = context(&ds, &k, &v)?;
		assert!(claim(&mut event).await?);
		delete_entry(&ds, &k).await?;

		run(&mut event).await?;

		assert_eq!(logged_count(&ds, &session).await?, 0, "the losing run's writes are dropped");
		assert_eq!(queued_attempt(&ds, &k).await?, None, "the entry is not resurrected");
		Ok(())
	}

	/// A batch reports the entries it left to another processor holding them
	/// apart from the ones it ran: a caller of `drain_async_events` waits for
	/// the former, whose bodies may still be running, and would otherwise
	/// return with the queue drained of its own share only.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test]
	async fn a_batch_reports_the_entries_it_left_to_their_holders() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for result in ds
			.execute(
				"DEFINE NAMESPACE test; DEFINE DATABASE test;
				 DEFINE TABLE person SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT log ON person ASYNC THEN (CREATE logged SET who = $after.id);
				 CREATE |person:3| RETURN NONE;",
				&session,
				None,
			)
			.await?
		{
			result.result?;
		}
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let txn = ds.transaction(TransactionType::Read).await?;
		let mut entries = txn.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await?;
		txn.cancel().await?;
		assert_eq!(entries.len(), 3, "one entry per create");
		// Two of them held by another node, as fetched.
		let now = ds.clock_now().value;
		for (k, v) in entries.iter_mut().take(2) {
			let (_, mut ev) = decoded(k.as_slice(), v.as_slice());
			ev.claim = Some(EventClaim {
				node: Uuid::from_u128(9),
				at: now,
			});
			*v = write_entry(&ds, k.as_slice(), &ev).await?;
		}

		let batch = process_events_batch(&ds, entries, None).await?;
		assert_eq!(batch.held, 2, "the held entries are reported");
		assert_eq!(batch.ran, 1, "apart from the one run");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		Ok(())
	}

	/// A lease found passed to another node before an entry is handed out
	/// ends the batch without failing it: no worker is handed an entry after
	/// the check that found it so, and the batch reports the loss, so the
	/// former holder does not run the queue alongside the new one. A batch
	/// begun under a lease another node holds reads nothing.
	#[cfg(not(target_family = "wasm"))]
	#[test_log::test(tokio::test)]
	async fn a_batch_stops_at_a_lease_another_node_holds() -> Result<()> {
		use crate::kvs::tasklease::TaskLeaseType;

		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for result in ds
			.execute(
				"DEFINE NAMESPACE test; DEFINE DATABASE test;
				 DEFINE TABLE person SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT log ON person ASYNC THEN (CREATE logged SET who = $after.id);
				 CREATE |person:3| RETURN NONE;",
				&session,
				None,
			)
			.await?
		{
			result.result?;
		}
		let entries = fetch_queue(&ds).await?;
		assert_eq!(entries.len(), 3, "one entry per create");
		// Another node holds the lease. A handler's first check reads the
		// lease rather than answering from its throttle.
		let lease = |ds: &Datastore| {
			ds.lease_handler(TaskLeaseType::EventProcessing, Duration::from_secs(60), None)
		};
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(9));
		assert!(lease(&other)?.has_lease().await?, "the other node takes the lease");

		let batch = process_events_batch(&ds, entries, Some(&lease(&ds)?)).await?;
		assert!(batch.lease_lost, "the batch reports the lease lost");
		assert_eq!(batch.ran, 0, "no entry is handed out once the lease is found taken");
		assert_eq!(logged_count(&ds, &session).await?, 0);
		assert_eq!(fetch_queue(&ds).await?.len(), 3, "every entry stays queued");

		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let next = super::process_next_events_batch(&ds, &range, Some(&lease(&ds)?)).await?;
		assert!(next.lease_lost, "the batch reports the lease lost");
		assert_eq!(next.fetched, 0, "a batch begun without the lease reads nothing");
		assert_eq!(fetch_queue(&ds).await?.len(), 3);
		Ok(())
	}

	/// A lease that can no longer be maintained — the pass cancelled at
	/// shutdown, or the lease's storage unreachable — ends the batch, but the
	/// entries its workers already hold are user-defined SurrealQL of unbounded
	/// duration that they settle in their own transactions. The batch must not
	/// return until every worker has: `Datastore::shutdown` awaits the pass and
	/// then closes the storage engine, and a worker still running its event
	/// would find the engine closed.
	#[cfg(not(target_family = "wasm"))]
	#[test_log::test(tokio::test)]
	async fn a_batch_ended_by_its_lease_returns_only_once_its_workers_have() -> Result<()> {
		// More entries than the workers and the channel between them hold, so
		// the producer still has entries to hand out when the pass is
		// cancelled, and checks the lease before the next.
		let queued = 2 * num_cpus::get().max(4) + 2;
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for result in ds
			.execute(
				&format!(
					"DEFINE NAMESPACE test; DEFINE DATABASE test;
					 DEFINE TABLE person SCHEMALESS;
					 DEFINE TABLE logged SCHEMALESS;
					 DEFINE EVENT log ON person ASYNC THEN {{
					 	sleep(300ms);
					 	CREATE logged SET who = $after.id;
					 }};
					 CREATE |person:{queued}| RETURN NONE;"
				),
				&session,
				None,
			)
			.await?
		{
			result.result?;
		}
		let entries = fetch_queue(&ds).await?;
		assert_eq!(entries.len(), queued, "one entry per create");

		// The pass is cancelled once a worker has claimed an entry, so the
		// producer's next check fails to maintain the lease.
		let canceller = tokio_util::sync::CancellationToken::new();
		let lh = ds.lease_handler(
			crate::kvs::tasklease::TaskLeaseType::EventProcessing,
			Duration::from_secs(2),
			Some(&canceller),
		)?;
		let cancel_once_claimed = async {
			while !fetch_queue(&ds).await?.iter().any(|(k, v)| decoded(k, v).1.claim.is_some()) {
				sleep(Duration::from_millis(5)).await;
			}
			canceller.cancel();
			Ok::<_, anyhow::Error>(())
		};
		let (res, cancelled) =
			tokio::join!(process_events_batch(&ds, entries, Some(&lh)), cancel_once_claimed);
		cancelled?;
		assert!(res.is_err(), "the lease ends the batch");
		let settled = logged_count(&ds, &session).await?;
		assert!(
			settled > 0 && settled < queued,
			"the workers holding entries finished them before the batch returned, and no \
			 more were handed out: {settled} of {queued}"
		);
		sleep(Duration::from_millis(600)).await;
		assert_eq!(logged_count(&ds, &session).await?, settled, "no worker outlived the batch");
		Ok(())
	}

	/// A datastore with one async event queued whose body fails every time,
	/// and that entry as a drain fetches it.
	#[cfg(not(target_family = "wasm"))]
	async fn one_failing_event() -> Result<(Arc<Datastore>, Vec<u8>, Vec<u8>)> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for result in ds
			.execute(
				"DEFINE NAMESPACE test; DEFINE DATABASE test;
				 DEFINE TABLE person SCHEMALESS;
				 DEFINE EVENT fail ON person ASYNC RETRY 3 THEN { THROW 'every time' };
				 CREATE person:1 RETURN NONE;",
				&session,
				None,
			)
			.await?
		{
			result.result?;
		}
		let (k, v) = fetch_queue(&ds).await?.remove(0);
		Ok((ds, k, v))
	}

	/// This node's queue as a drain fetches it.
	#[cfg(not(target_family = "wasm"))]
	async fn fetch_queue(ds: &Datastore) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let txn = ds.transaction(TransactionType::Read).await?;
		let entries = txn.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await?;
		txn.cancel().await?;
		Ok(entries)
	}

	/// An error of the claim's transaction — a storage fault, not a conflict
	/// — leaves the entry in place, and the batch fails with it once its
	/// workers have finished: a drain must not report the queue flushed past
	/// an entry it could not claim, and the next pass retries it.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test]
	async fn a_claim_that_fails_leaves_the_entry_and_fails_the_batch() -> Result<()> {
		let (ds, session, k, v) = one_queued_event().await?;
		let fault = inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, ds.id());

		let e = process_events_batch(&ds, vec![(k.clone(), v.clone())], None)
			.await
			.expect_err("the batch fails with the claim's error");
		let failed = e.downcast_ref::<FailedBatch>().expect("with its report attached");
		assert_eq!((failed.0.fetched, failed.0.ran), (1, 0), "the entry was fetched, not run");
		assert_eq!(logged_count(&ds, &session).await?, 0, "the body did not run");
		let left = queued_record(&ds, &k).await?.expect("the entry stays in place");
		assert_eq!(left.claim, None, "unclaimed");

		// The fault gone, the next batch runs it.
		drop(fault);
		let batch = process_events_batch(&ds, vec![(k.clone(), v)], None).await?;
		assert_eq!(batch.ran, 1);
		assert_eq!(logged_count(&ds, &session).await?, 1);
		Ok(())
	}

	/// A batch that fails after settling some of its entries reports them
	/// with the error: those entries are gone from the queue, and a pass that
	/// accounts the report before propagating the error counts them as run.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test]
	async fn a_failed_batch_reports_the_entries_it_settled() -> Result<()> {
		let (ds, session, _k, _v) = one_queued_event().await?;
		for result in ds.execute("CREATE person:2 RETURN NONE;", &session, None).await? {
			result.result?;
		}
		let entries = fetch_queue(&ds).await?;
		assert_eq!(entries.len(), 2);
		let _fault = inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, ds.id());

		let e = process_events_batch(&ds, entries, None)
			.await
			.expect_err("the claim that fails fails the batch");
		let failed = e.downcast_ref::<FailedBatch>().expect("with its report attached");
		assert_eq!(failed.0.fetched, 2, "both entries were fetched");
		assert_eq!(failed.0.ran, 1, "the entry whose claim held ran");
		assert!(failed.0.last_key.is_some(), "and the batch's end is reported");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(fetch_queue(&ds).await?.len(), 1, "the entry whose claim failed stays queued");
		Ok(())
	}

	/// A run whose settlement fails — the requeue's transaction, here — is
	/// reported the same way, and releases the claim it had landed, so the
	/// next pass retries the entry at once rather than once the claim
	/// expires.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test]
	async fn a_run_whose_settlement_fails_releases_its_claim() -> Result<()> {
		let (ds, k, v) = one_failing_event().await?;
		let fault = inject_non_retryable_error(NonRetryableErrorSite::AsyncEventSettle, ds.id());

		let res = process_events_batch(&ds, vec![(k.clone(), v)], None).await;
		assert!(res.is_err(), "the batch fails with the settlement's error");
		let left = queued_record(&ds, &k).await?.expect("the entry stays in place");
		assert_eq!(left.attempt, 0, "the requeue did not land");
		assert_eq!(left.claim, None, "and the claim is released");

		drop(fault);
		let batch = process_events_batch(&ds, fetch_queue(&ds).await?, None).await?;
		assert_eq!(batch.requeued, 1, "the next pass retries the entry");
		assert_eq!(queued_attempt(&ds, &k).await?, Some(1));
		Ok(())
	}

	/// A failed run requeues the entry for another attempt — but only the
	/// entry it claimed. Once another processor has taken it over, run and
	/// removed it, a blind requeue would put a finished event back on the
	/// queue.
	#[tokio::test]
	async fn a_requeue_does_not_resurrect_an_entry_another_processor_removed() -> Result<()> {
		let (ds, _session, k, v) = one_queued_event().await?;
		let mut event = context(&ds, &k, &v)?;
		assert!(claim(&mut event).await?);
		let mut ours = event.v.clone().expect("the context has not run yet");
		delete_entry(&ds, &k).await?;

		let tx = event.new_write_tx().await?;
		assert!(
			!event.retry_attempt(tx, anyhow::anyhow!("transient"), &mut ours).await?,
			"a requeue that did not land is not reported as one"
		);

		assert_eq!(queued_attempt(&ds, &k).await?, None, "the removed entry stays removed");
		Ok(())
	}

	/// When both processors fail the same entry, the second requeue must not
	/// overwrite the first: the attempt count would otherwise be reset to what
	/// the second processor fetched plus one, and the retry budget lost.
	#[tokio::test]
	async fn a_requeue_does_not_overwrite_a_competitors_requeue() -> Result<()> {
		let (ds, _session, k, v) = one_queued_event().await?;
		let mut event = context(&ds, &k, &v)?;
		assert!(claim(&mut event).await?);
		let mut ours = event.v.clone().expect("the context has not run yet");
		let (_, mut theirs) = decoded(&k, &v);
		theirs.attempt = 2;
		write_entry(&ds, &k, &theirs).await?;

		let tx = event.new_write_tx().await?;
		assert!(
			!event.retry_attempt(tx, anyhow::anyhow!("transient"), &mut ours).await?,
			"a requeue that did not land is not reported as one"
		);

		assert_eq!(queued_attempt(&ds, &k).await?, Some(2), "the competitor's requeue stands");
		Ok(())
	}

	/// The control for the requeue races: an entry still as claimed is
	/// requeued with its attempt counted and its claim released, so that its
	/// next attempt is claimed afresh by whichever processor fetches it.
	#[tokio::test]
	async fn a_requeue_of_an_entry_as_claimed_counts_the_attempt_and_releases_the_claim()
	-> Result<()> {
		let (ds, _session, k, v) = one_queued_event().await?;
		let mut event = context(&ds, &k, &v)?;
		assert!(claim(&mut event).await?);
		let mut ours = event.v.clone().expect("the context has not run yet");

		let tx = event.new_write_tx().await?;
		assert!(
			event.retry_attempt(tx, anyhow::anyhow!("transient"), &mut ours).await?,
			"the requeue is reported, so the drain owes the entry its next attempt"
		);

		let requeued = queued_record(&ds, &k).await?.expect("the entry is requeued");
		assert_eq!(requeued.attempt, 1, "the attempt is counted");
		assert_eq!(requeued.claim, None, "the claim is released");
		Ok(())
	}
}
