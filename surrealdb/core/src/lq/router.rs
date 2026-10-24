//! Per-node live-query router.
//!
//! The router is the delivery half of the inverted engine. While the write path
//! only captures before/after values into the dedicated `lqe` keyspace
//! ([`crate::lq::event`]), the router tails that keyspace **off** the write path,
//! reconstructs each change, and runs the matching/permission/projection/FETCH
//! pipeline on the subscriber side ([`crate::lq::subscriber`]). Under the Router
//! engine this is the *only* delivery path — the write path's inline
//! notification stage becomes a no-op — so a
//! write's cost no longer scales with the number of live subscribers.
//!
//! The router runs on every node with **no lease**: each node tails the `lqe`
//! entries readable on its local replica and delivers only to the subscriptions
//! it owns — the replay passes over the others', whatever the broker routes
//! to, since a delivery from every node would reach each owner once per node.
//! A pass is driven per tick by the engine's background task
//! ([`Datastore::live_query_router_process`])
//! and delivers at most a slice of the captured events; a backlog is drained a
//! pass at a time.

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use parking_lot::Mutex;
use surrealdb_datastore::tx::refused_by_release;
use surrealdb_kvs::TransactionType::Read;
use surrealdb_observe::TaskLeaseState;
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::dbs::MessageBroker;
use crate::key::schema::{LiveEventsKey, LiveEventsPrefix};
use crate::key::{KVKeyDecode, KVValue, Resumable};
use crate::kvs::tasks::{Pass, Progress};
#[cfg(test)]
use crate::kvs::testing::{NonRetryableErrorSite, maybe_inject_non_retryable_error};
use crate::kvs::walk::{CatalogWalk, Step};
use crate::kvs::{
	Datastore, Direction, LIVE_QUERY_ROUTER_SLICE_BATCHES, NORMAL_BATCH_SIZE, Transaction,
};
use crate::lq::event::{LiveEvent, LiveEvents};
use crate::lq::subscriber::{Replayed, TableReplay};

/// Per-node live-query router state: the tail cursor over the global
/// versionstamp space of the `lqe` keyspace, and the lap in progress.
///
/// The cursor is **in-memory** and is initialised to "now" on the first pass so
/// a freshly started node does not replay retained history to its
/// currently-connected subscribers — matching the inline engine, which only
/// notifies writes that occur after a subscription is registered. Durable,
/// resumable cursors (which let a reconnecting client replay a gap) are the
/// subject of the at-least-once delivery phase.
#[derive(Debug, Default)]
pub(crate) struct LiveQueryRouter {
	/// Held for the whole of a pass, so passes never overlap: two passes that
	/// began from the same cursor would both deliver what lies beyond it.
	pass: tokio::sync::Mutex<()>,
	/// Highest versionstamp already delivered; the next lap scans strictly
	/// after it. `None` until the first pass establishes the baseline.
	cursor: Mutex<Option<u128>>,
	/// The lap a pass stopped in with entries still to deliver, kept for the
	/// pass that continues it; `None` between laps. See [`RouterLap`].
	lap: Mutex<Option<RouterLap>>,
	/// Laps found behind the event retention window, each counted once; see
	/// [`report_if_behind_window`].
	behind_window: AtomicU64,
}

/// A lap of the router over the databases of the catalog, delivering every
/// captured event in `(cursor, safe_vs]`.
///
/// A pass spends at most its budget. An entry holds every change one commit
/// made to a table; an event costs the subscriptions it is replayed to,
/// evaluated or passed over, at least one, and a pass that reaches an event
/// whose subscriptions outnumber what its budget has left replays it to
/// that many and stops inside the event, keeping its place there; a
/// database with nothing to deliver, a
/// database removed since the lap began and a namespace entered each cost
/// one. So a lap spans as many passes as the backlog, the subscriptions and
/// the catalog take, however the backlog is divided into entries. The cursor
/// advances to the lap's watermark only once every database is delivered,
/// which is what keeps a lap that stops between passes from re-delivering
/// the databases it finished or skipping the ones it had not reached. A pass
/// that fails keeps the lap too, at the place it reached, so a transient
/// failure neither restarts the lap nor delivers again what it delivered.
#[derive(Debug)]
struct RouterLap {
	/// The cursor the lap began after: entries at or below it were delivered by
	/// an earlier lap.
	cursor: u128,
	/// The safe watermark the lap delivers up to, read when it began. An event
	/// committed past it while the lap runs is the next lap's, so a lap ends
	/// however fast events are captured.
	safe_vs: u128,
	/// The walk that reaches the lap's databases, a database at a time in
	/// catalog order, as the lap needs them; see [`CatalogWalk`]. A database
	/// defined since the lap began has no event at or below its watermark,
	/// so its scan delivers nothing.
	walk: CatalogWalk,
	/// The database in progress, kept between the passes of a lap that
	/// stopped in it; `None` when the next is the walk's to reach.
	current: Option<LapDatabase>,
	/// The last entry read whole in the database in progress, which the next
	/// read resumes after; `None` at its head.
	after: Option<Vec<u8>>,
	/// The entry the pass stopped inside, when it did, kept decoded with the
	/// place reached in it: the next pass continues it before it reads the
	/// database's entries again, and the read then resumes after it. `None`
	/// when the pass stopped between entries. See [`OpenEntry`].
	partial: Option<OpenEntry>,
	/// Whether a pass has found the lap behind the event retention window,
	/// which is reported once per lap; see [`report_if_behind_window`].
	behind_window: bool,
}

/// A database a lap is delivering: the names the subscriber replay resolves
/// it by, and the ids its events are keyed under.
#[derive(Debug)]
struct LapDatabase {
	ns: Arc<str>,
	db: String,
	ns_id: NamespaceId,
	db_id: DatabaseId,
}

/// An entry a pass is delivering, decoded, with the place reached in it.
///
/// A lap that stops inside an entry keeps it, so the passes that continue it
/// read and decode it once however many passes its events span: a bulk write
/// costs each pass what the pass delivers of it, not the whole entry again.
/// It holds no more than the one entry, which was captured at or below the
/// lap's watermark, and the passes that continue it deliver from this copy
/// whatever has since become of the stored entry.
struct OpenEntry {
	/// The entry's key, which the database's read resumes after once the
	/// entry is delivered.
	key: Vec<u8>,
	/// The table the entry's events changed.
	tb: TableName,
	/// The versionstamp the entry was captured at.
	vs: u128,
	/// The changes the entry's transaction made to the table, in order.
	events: Vec<LiveEvent>,
	/// The subscriptions the transaction registered among its events, each
	/// with the number of them it was registered after, so that each
	/// subscription an event is replayed to is looked up in constant time
	/// however many there are.
	registered: HashMap<Uuid, u32>,
	/// The events delivered whole.
	done: usize,
	/// The last subscription the next event was replayed to or passed, `None`
	/// for none yet.
	cursor: Option<Uuid>,
	/// The [`TableReplay`] (and its read transaction) pinned when an earlier
	/// pass stopped inside `events[done]`, so that every subscription reached
	/// by that event evaluates it against one catalog and data snapshot.
	replay: Option<TableReplay>,
}

impl OpenEntry {
	/// Releases any [`TableReplay`] pinned across passes, cancelling its read
	/// transaction if it is not the current pass's `current_txn`.
	async fn release_pinned(&mut self, current_txn: &Arc<Transaction>) {
		if let Some(replay) = self.replay.take()
			&& !Arc::ptr_eq(replay.txn(), current_txn)
		{
			let _ = replay.txn().cancel().await;
		}
	}
}

impl fmt::Debug for OpenEntry {
	/// The events hold the changed records' values, which a debug rendering
	/// of the router leaves out.
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("OpenEntry")
			.field("tb", &self.tb)
			.field("vs", &self.vs)
			.field("events", &self.events.len())
			.field("done", &self.done)
			.field("cursor", &self.cursor)
			.finish_non_exhaustive()
	}
}

impl LiveQueryRouter {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Begin a lap against the safe watermark `safe_vs`: returns the cursor
	/// (highest versionstamp already delivered) to scan strictly after, or `None`
	/// when this is the first (baseline) pass — which sets the cursor to the
	/// watermark and delivers nothing, so a freshly started node does not replay
	/// history to its currently-connected subscribers.
	///
	/// Synchronous on purpose: the [`MutexGuard`](parking_lot::MutexGuard) is
	/// `!Send` and must never be held across an `await` in [`process`].
	fn begin_pass(&self, safe_vs: u128) -> Option<u128> {
		let mut guard = self.cursor.lock();
		match *guard {
			None => {
				*guard = Some(safe_vs);
				None
			}
			Some(c) => Some(c),
		}
	}

	/// Advance the cursor to the safe watermark the lap delivered up to.
	/// Synchronous for the same `!Send` reason as [`Self::begin_pass`].
	fn advance(&self, safe_vs: u128) {
		*self.cursor.lock() = Some(safe_vs);
	}

	/// The lap the previous pass stopped in, if any, taken for this pass to
	/// continue. Synchronous for the same `!Send` reason as
	/// [`Self::begin_pass`].
	fn take_lap(&self) -> Option<RouterLap> {
		self.lap.lock().take()
	}

	/// Keep `lap` for the next pass to continue. Synchronous for the same
	/// `!Send` reason as [`Self::begin_pass`].
	fn keep_lap(&self, lap: RouterLap) {
		*self.lap.lock() = Some(lap);
	}

	/// Laps found behind the event retention window since the datastore
	/// started.
	#[cfg(test)]
	pub(crate) fn laps_behind_retention(&self) -> u64 {
		self.behind_window.load(Ordering::Relaxed)
	}

	/// Seed the baseline cursor eagerly, before the datastore can accept any
	/// connection. Establishing the baseline at construction (rather than lazily
	/// on the first pass) guarantees that every subscription — and therefore
	/// every write a subscriber expects to observe — happens *after* the
	/// baseline, so no captured event is ever skipped as "history" while a
	/// subscriber was already waiting for it.
	pub(crate) fn set_baseline(&self, safe_vs: u128) {
		*self.cursor.lock() = Some(safe_vs);
	}

	/// Whether the baseline cursor is established.
	pub(crate) fn has_baseline(&self) -> bool {
		self.cursor.lock().is_some()
	}
}

/// Run one tail-and-deliver pass for the Router engine.
///
/// A lap delivers every `lqe` event with a versionstamp in `(cursor, W]`, where
/// `W` is the backend's **safe watermark** (`Transaction::safe_timestamp`) — the
/// versionstamp at or below which every commit is final and visible. Bounding
/// delivery and cursor advancement by `W` (rather than by the highest event
/// seen) is what makes this correct on a backend whose commit log is non-linear:
/// the router never advances past a commit that could still become visible later
/// with a lower versionstamp. On a single-oracle backend `W` is simply "now", so
/// every committed event is delivered immediately.
///
/// A pass spends at most [`LIVE_QUERY_ROUTER_SLICE_BATCHES`] pages' worth of
/// budget, [`NORMAL_BATCH_SIZE`] a page. An entry holds every change one
/// commit made to a table; an event costs the subscriptions it is replayed
/// to, evaluated or passed over, at least one, and a pass that reaches an
/// event whose subscriptions outnumber what its budget has left replays it
/// to that many and stops inside the event, so neither a bulk write nor a
/// table with many subscriptions makes a pass do more than its budget, and
/// a table's subscriptions are read a page at a time as the replay reaches
/// them, so a pass reads no more of them than its budget; a database with
/// nothing to deliver costs one. A pass reports [`Progress::More`] when it
/// stops with events or databases still to deliver: the lap is kept, with its
/// place in the catalog, in the entry it stopped inside and among the
/// subscriptions of the event it stopped in, and the next pass continues it
/// there. The databases are reached a database at a time as the
/// lap needs them, so a pass reads no more of the catalog than it scans. The
/// cursor advances to `W` only once the lap has delivered every database, so
/// a lap that stops between passes neither re-delivers what it finished nor
/// skips what it had not reached, and an event captured past `W` while the
/// lap runs is the next lap's.
///
/// The collector keeps a captured event for `live_query_retention` from its
/// capture, on every node, and a lap's events are protected from it only by
/// the snapshot of the pass that reads them: a lap begun behind the window,
/// or suspended between passes while the window passed the cursor it began
/// after, has events it can no longer reach in the databases it has not
/// finished, and they are collected before delivery. The router reports such
/// a lap once, from the pass that finds it so; see
/// [`report_if_behind_window`].
///
/// Events are replayed in scan order through the subscriber-side compute using
/// the datastore's broker, against each table's subscriptions resolved once
/// per pass, and delivers to the subscriptions this node owns alone. A
/// subscription registered after an event was captured, before the lap began
/// or between its passes, is left out of that event's replay: it is stored
/// with the watermark it was registered against; see [`TableReplay`].
pub(crate) async fn process(ds: &Datastore, router: &LiveQueryRouter) -> Result<Pass> {
	process_bounded(
		ds,
		router,
		LIVE_QUERY_ROUTER_SLICE_BATCHES as u64 * u64::from(NORMAL_BATCH_SIZE),
	)
	.await
}

/// [`process`], spending at most `budget`.
pub(crate) async fn process_bounded(
	ds: &Datastore,
	router: &LiveQueryRouter,
	budget: u64,
) -> Result<Pass> {
	// Without a delivery broker there are no subscribers to notify.
	let Some(broker) = ds.live_query_broker().map(|broker| broker as Arc<dyn MessageBroker>) else {
		return Ok(Pass::done(TaskLeaseState::NotLeased));
	};
	let _pass = router.pass.lock().await;
	let txn = Arc::new(ds.transaction(Read).await?);
	// The safe watermark now: what a lap begun by this pass delivers up to,
	// and the clock every pass measures the retention window against.
	let safe_vs = txn.safe_timestamp().await?.as_versionstamp();
	// Continue the lap the previous pass stopped in, or begin one against the
	// safe watermark: deliver and advance only up to there.
	let mut lap = match router.take_lap() {
		Some(lap) => lap,
		None => {
			// On the first pass, set the baseline to the watermark and deliver
			// nothing: everything already committed (vs <= watermark) is treated
			// as history.
			let Some(cursor) = router.begin_pass(safe_vs) else {
				txn.cancel().await?;
				return Ok(Pass::done(TaskLeaseState::NotLeased));
			};
			// Nothing new is safe to deliver yet.
			if safe_vs <= cursor {
				txn.cancel().await?;
				return Ok(Pass::done(TaskLeaseState::NotLeased));
			}
			// The databases are reached as the lap needs them; see
			// [`CatalogWalk`].
			RouterLap {
				cursor,
				safe_vs,
				walk: CatalogWalk::default(),
				current: None,
				after: None,
				partial: None,
				behind_window: false,
			}
		}
	};
	report_if_behind_window(ds, router, &txn, &mut lap, safe_vs);
	// The lap is kept however the pass ends. A pass that fails keeps it at
	// the place it reached — the entry, the event and the subscription — so
	// the next pass continues there: a lap begun again at the cursor would
	// deliver once more what the passes before the failure delivered. It
	// attaches the events it delivered before failing to its error, with the
	// backlog the kept lap holds; see [`Pass::failed`].
	let mut delivered = 0;
	let outcome = run_pass(ds, &txn, &mut lap, budget, &broker, &mut delivered).await;
	let txn_pinned = lap
		.partial
		.as_ref()
		.and_then(|entry| entry.replay.as_ref())
		.is_some_and(|replay| Arc::ptr_eq(replay.txn(), &txn));
	let cancelled = if txn_pinned {
		Ok(())
	} else {
		txn.cancel().await
	};
	let report = |progress| Pass {
		progress,
		items: delivered,
		oldest_age: None,
		held: 0,
		ran: delivered,
		lease: TaskLeaseState::NotLeased,
	};
	if let Err(e) = outcome.and(cancelled) {
		router.keep_lap(lap);
		return Err(report(Progress::More).failed(e));
	}
	let progress = if lap.current.is_none() && !lap.walk.has_more() {
		// Advance the cursor to the watermark: everything at or below it is now
		// considered (delivered or pre-baseline), and nothing below it can
		// appear later, so the next lap resumes from here without re-delivering
		// or skipping.
		router.advance(lap.safe_vs);
		Progress::Done
	} else {
		router.keep_lap(lap);
		Progress::More
	};
	Ok(report(progress))
}

/// Reports `lap` once if the event retention window has passed the cursor it
/// began after, as of the safe watermark `now` the pass read.
///
/// The collector keeps an event for `live_query_retention` from its capture,
/// on every node, and nothing holds it for a lap: the lap's unreached events
/// are protected only by the snapshot of the pass that reads them. A lap
/// begun behind the window, or suspended between passes while the window
/// passed its cursor, therefore has events it can no longer reach in the
/// databases it has not finished — every one captured between the cursor and
/// the window's start — and they are collected before delivery, however the
/// lap continues. Checked on every pass, since a lap crosses the window
/// while it waits between passes as readily as it begins behind it; the
/// comparison is in versionstamps, which is how the collector's cutoff and
/// the lap's cursor are both minted. A zero retention disables the collector
/// and so the report.
fn report_if_behind_window(
	ds: &Datastore,
	router: &LiveQueryRouter,
	txn: &Transaction,
	lap: &mut RouterLap,
	now: u128,
) {
	if lap.behind_window {
		return;
	}
	let retention = ds.config().datastore.live_query_retention;
	if retention.is_zero() {
		return;
	}
	let ts_impl = txn.timestamp_impl();
	let Some(cutoff) =
		ts_impl.create_from_versionstamp(now).and_then(|now| now.sub_checked(retention))
	else {
		return;
	};
	if lap.cursor < cutoff.as_versionstamp() {
		lap.behind_window = true;
		router.behind_window.fetch_add(1, Ordering::Relaxed);
		warn!(
			"The live-query router on this node is behind the {retention:?} event retention window: \
			 events captured before the window that its lap has yet to deliver are collected before delivery"
		);
	}
}

/// One pass over `lap`: delivers until `budget` is spent or the lap ends.
/// Records in `lap`, as it goes, the database in progress, the last entry
/// delivered whole and the event and subscription a stop leaves it inside,
/// so `lap` holds the place reached whether the pass returns or fails.
/// Counts in `delivered` the events delivered whole, as they are, so the
/// count holds what the pass delivered whether it returns or fails.
async fn run_pass(
	ds: &Datastore,
	txn: &Arc<Transaction>,
	lap: &mut RouterLap,
	mut budget: u64,
	broker: &Arc<dyn MessageBroker>,
	delivered: &mut u64,
) -> Result<()> {
	let ts_impl = txn.timestamp_impl();
	// Encode the lap's bounds for the range scans. The lower bound is inclusive,
	// so events at exactly `cursor` are scanned; those were already delivered,
	// so the page loop skips `vs <= cursor`. The upper bound is inclusive at the
	// watermark where the timestamp can encode it; otherwise the page loop ends
	// a database at the first entry past the watermark.
	let cursor_ts =
		ts_impl.create_from_versionstamp(lap.cursor).unwrap_or_else(|| ts_impl.earliest());
	let mut cursor_buf = [0u8; _];
	let cursor_bytes = cursor_ts.encode(&mut cursor_buf);
	let safe_ts = ts_impl.create_from_versionstamp(lap.safe_vs);
	let mut safe_buf = [0u8; _];
	let safe_bytes = safe_ts.as_ref().map(|ts| ts.encode(&mut safe_buf));

	loop {
		if budget == 0 {
			// Spent with entries, or databases, still to deliver: the next pass
			// continues here.
			break;
		}
		// The database in progress, or the next the walk reaches, which the
		// lap holds while its entries are scanned and lets go of once they
		// are delivered.
		if lap.current.is_none() {
			match lap.walk.next(txn).await? {
				Some(Step::Reached((ns, db))) => {
					lap.current = Some(LapDatabase {
						ns,
						db: db.name.to_string(),
						ns_id: db.namespace_id,
						db_id: db.database_id,
					});
				}
				// A namespace entered costs an entry, as an empty database
				// does: the walk read a page of the catalog to enter it, so a
				// lap over a catalog of namespaces with nothing to deliver
				// spans passes as one over such databases does.
				Some(Step::Passed) => {
					budget -= 1;
					continue;
				}
				// Every database is delivered up to the watermark.
				None => break,
			}
		}
		let Some(front) = &lap.current else {
			continue;
		};
		// A database removed since the lap began has nothing left to deliver,
		// and its names no longer resolve for the replay. One removed and
		// defined again under the same names is another database: the replay
		// resolves its subscriptions by name, and the entries retained under
		// the removed database's ids must not reach them, so the ids are
		// matched as well as the names.
		// One a newer release has defined since is skipped as the walk skips
		// one so written: the replay could not resolve its tables, and a lap
		// kept on it would fail every pass at the same place.
		let same = match txn.get_db_by_name(&front.ns, &front.db, None).await {
			Ok(current) => current
				.as_ref()
				.is_some_and(|db| db.namespace_id == front.ns_id && db.database_id == front.db_id),
			Err(e) if refused_by_release(&e) => {
				warn!(
					"Live-query router skipped database `{}`: its definition needs a newer \
					 release than this build",
					front.db
				);
				false
			}
			Err(e) => return Err(e),
		};
		if !same {
			// Costs one as an empty database does: the lookup read the catalog
			// for it.
			if let Some(mut entry) = lap.partial.take() {
				entry.release_pinned(txn).await;
			}
			lap.after = None;
			lap.current = None;
			budget -= 1;
			continue;
		}
		// The tables' replays, one per table per page, each reading the
		// table's subscriptions as its events reach them.
		let mut replays: HashMap<TableName, Option<TableReplay>> = HashMap::new();
		// The entry an earlier pass stopped inside is continued first, from
		// the copy the lap keeps: after the events it delivered whole, and in
		// the next after the last subscription it evaluated. The read of the
		// database's entries resumes after it once it is delivered. A failure
		// leaves it with the lap, at the place it reached.
		if let Some(entry) = &mut lap.partial {
			let before = entry.done;
			let replayed =
				replay_entry(ds, txn, front, &mut replays, entry, &mut budget, broker).await;
			*delivered += (entry.done - before) as u64;
			if !replayed? {
				// Spent inside the entry: the next pass continues it.
				break;
			}
			if let Some(mut finished_entry) = lap.partial.take() {
				finished_entry.release_pinned(txn).await;
				lap.after = Some(finished_entry.key);
			}
			if budget == 0 {
				break;
			}
		}
		let prefix = LiveEventsPrefix {
			ns: front.ns_id,
			db: front.db_id,
		};
		let mut range = match safe_bytes {
			Some(end) => prefix.range_where(Cow::Borrowed(cursor_bytes)..=Cow::Borrowed(end))?,
			None => prefix.range_where(Cow::Borrowed(cursor_bytes)..)?,
		};
		if let Some(after) = &lap.after {
			range = range.resume_after(after, Direction::Forward);
		}
		// One page of the database's entries, no more than the budget has
		// left. The values stay encoded until an entry is known to be inside
		// the delivery window.
		let limit = budget.min(u64::from(NORMAL_BATCH_SIZE)) as u32;
		let page = txn.scan_raw(range, limit, 0, None).await?;
		let full = page.len() as u32 == limit;
		// An empty scan costs one, so the databases of a lap are paid for as
		// its events are, and a lap over a catalog of databases with nothing
		// to deliver spans passes as a backlog does.
		if page.is_empty() {
			budget -= 1;
		}
		// Whether the budget ran out with events of the page still to
		// deliver, which keeps the database in progress.
		let mut stopped = false;
		// Whether the page reached past the watermark, which ends the database:
		// keys are in versionstamp order, so nothing after is at or below it.
		let mut past = false;
		for (k, v) in page {
			if budget == 0 {
				// Spent before this entry: it is the next pass's.
				stopped = true;
				break;
			}
			let (tb, vs) = {
				let key = LiveEventsKey::decode_key(&k)?;
				(key.tb.into_owned(), ts_impl.decode(key.ts.as_ref())?.as_versionstamp())
			};
			if vs > lap.safe_vs {
				// Read, so it costs one; the database ends here.
				budget -= 1;
				past = true;
				break;
			}
			// Already delivered by the lap before this one; read, so it costs
			// one.
			if vs <= lap.cursor {
				budget -= 1;
				lap.after = Some(k);
				continue;
			}
			let LiveEvents {
				events,
				registered,
			} = match LiveEvents::kv_decode_value(&v, ()) {
				Ok(entry) => entry,
				// No later pass can decode it either, so failing the pass on
				// it would stop the table's delivery for as long as the entry
				// is retained. Read, so it costs one.
				Err(e) => {
					error!(
						"Live-query router skipped an undecodable event for table `{tb}` in database `{}`: {e}",
						front.db
					);
					budget -= 1;
					lap.after = Some(k);
					continue;
				}
			};
			let mut entry = OpenEntry {
				key: k,
				tb,
				vs,
				events,
				registered: registered
					.iter()
					.map(|registration| (registration.id, registration.after))
					.collect(),
				done: 0,
				cursor: None,
				replay: None,
			};
			let replayed =
				replay_entry(ds, txn, front, &mut replays, &mut entry, &mut budget, broker).await;
			*delivered += entry.done as u64;
			let finished = match replayed {
				Ok(finished) => finished,
				Err(e) => {
					lap.partial = Some(entry);
					return Err(e);
				}
			};
			if !finished {
				lap.partial = Some(entry);
				stopped = true;
				break;
			}
			lap.after = Some(entry.key);
		}
		if past || !(stopped || full) {
			// The database is delivered up to the watermark.
			if let Some(mut entry) = lap.partial.take() {
				entry.release_pinned(txn).await;
			}
			lap.after = None;
			lap.current = None;
		}
		// Otherwise the lap holds the continuation: the database, the last
		// entry delivered whole, which the next read resumes after, and the
		// place inside the entry it stopped in.
	}
	Ok(())
}

/// Replays the events of `entry`, captured in the database `front`, from the
/// place it holds until every one is delivered or `budget` is spent, and
/// returns whether the entry is finished.
///
/// An event costs the subscriptions it is replayed to, evaluated or passed
/// over, at least one, and an event whose subscriptions outnumber what the
/// budget has left is replayed to that many, the entry keeping its place
/// among them. An entry with nothing left to deliver, or captured for a
/// table since removed, whose subscriptions were killed with it, was read,
/// so it costs one and is finished. `entry` holds the place reached whether
/// the call returns or fails. `replays` holds the tables' replays the pass
/// has resolved in `front`, and gains this entry's table's. Called with a
/// budget of at least one.
async fn replay_entry(
	ds: &Datastore,
	txn: &Arc<Transaction>,
	front: &LapDatabase,
	replays: &mut HashMap<TableName, Option<TableReplay>>,
	entry: &mut OpenEntry,
	budget: &mut u64,
	broker: &Arc<dyn MessageBroker>,
) -> Result<bool> {
	if entry.events.len() <= entry.done {
		entry.release_pinned(txn).await;
		*budget -= 1;
		return Ok(true);
	}
	if let Some(mut pinned) = entry.replay.take() {
		if !pinned.table_matches(txn).await? {
			if !Arc::ptr_eq(pinned.txn(), txn) {
				let _ = pinned.txn().cancel().await;
			}
			replays.insert(entry.tb.clone(), None);
			*budget -= 1;
			return Ok(true);
		}
		if *budget == 0 {
			entry.replay = Some(pinned);
			return Ok(false);
		}
		match pinned
			.replay(
				entry.vs,
				entry.done,
				&entry.events[entry.done],
				&entry.registered,
				&mut entry.cursor,
				budget,
			)
			.await
		{
			Ok(Replayed::Complete) => {
				if !Arc::ptr_eq(pinned.txn(), txn) {
					let _ = pinned.txn().cancel().await;
				}
				entry.done += 1;
				entry.cursor = None;
				#[cfg(test)]
				maybe_inject_non_retryable_error(
					NonRetryableErrorSite::LiveQueryRouterAfterEvent,
					ds.id(),
				)?;
				if entry.done == entry.events.len() {
					return Ok(true);
				}
			}
			Ok(Replayed::Stopped) => {
				pinned.park();
				entry.replay = Some(pinned);
				return Ok(false);
			}
			Err(e) => {
				pinned.park();
				entry.replay = Some(pinned);
				return Err(e);
			}
		}
	}
	let replay = match replays.entry(entry.tb.clone()) {
		Entry::Occupied(slot) => slot.into_mut(),
		Entry::Vacant(slot) => {
			let replay = TableReplay::resolve(
				ds,
				Arc::clone(txn),
				&front.ns,
				&front.db,
				slot.key(),
				Arc::clone(broker),
			)
			.await?;
			slot.insert(replay)
		}
	};
	let Some(replay) = replay else {
		*budget -= 1;
		return Ok(true);
	};
	while entry.done < entry.events.len() {
		if *budget == 0 {
			return Ok(false);
		}
		match replay
			.replay(
				entry.vs,
				entry.done,
				&entry.events[entry.done],
				&entry.registered,
				&mut entry.cursor,
				budget,
			)
			.await
		{
			Ok(Replayed::Complete) => {
				entry.done += 1;
				entry.cursor = None;
			}
			Ok(Replayed::Stopped) => {
				if let Some(mut pinned) = replays.remove(&entry.tb).flatten() {
					pinned.park();
					entry.replay = Some(pinned);
				}
				return Ok(false);
			}
			Err(e) => {
				if entry.cursor.is_some()
					&& let Some(mut pinned) = replays.remove(&entry.tb).flatten()
				{
					pinned.park();
					entry.replay = Some(pinned);
				}
				return Err(e);
			}
		}
		#[cfg(test)]
		maybe_inject_non_retryable_error(
			NonRetryableErrorSite::LiveQueryRouterAfterEvent,
			ds.id(),
		)?;
	}
	Ok(true)
}
