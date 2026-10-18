//! The state one DiskANN compaction frame shares with the tasks its batched insert spawns.
//!
//! A frame is one compaction plan applied in one write transaction under the graph write lock
//! (`DiskAnnIndex::apply_compaction`). Its fresh vectors are inserted through upstream
//! `multi_insert`, which spawns tasks of its own and, by design, swallows their failures: a task
//! error is logged and its partial result kept, and a panicking task's `JoinError` is discarded.
//! Every one of those tasks reaches this frame through the provider context it was handed, so the
//! frame is where they report back:
//!
//! * **The first typed error** of any provider or accessor call
//!   ([`DiskAnnBuildFrame::record_error`]) is kept with its type intact, so the frame fails with it
//!   even though `multi_insert` answered `Ok(())`, and a shutdown or memory-threshold error keeps
//!   its classification.
//! * **A panic in a spawned task** is observed by that task's [`TaskGuard`], dropped inside the
//!   task during the unwind, and fails the frame.
//! * **Every element the batch inserted has an adjacency list** written by this frame, or the frame
//!   fails ([`DiskAnnBuildFrame::verify_nodes_written`]). That catches every failure in upstream's
//!   candidate generation, whatever its kind, because an item whose search failed contributes no
//!   node.
//!
//! What stays uncovered is an error raised inside upstream after a successful provider read and
//! returned through `?` in a back-edge task: it never reaches this code, does not unwind, and
//! leaves a graph that is merely missing some back-edges. The recall floors are its backstop.
//!
//! # Lifetime
//!
//! A frame is created after the frame's `!dg` bump and conditional deletes are buffered, the graph
//! write lock taken and the continuity span opened, and it is closed
//! ([`DiskAnnBuildFrame::close_now`]) on every path — commit, error, cancellation or panic — while
//! that lock is still held. Closing it sets `cancelled`, so every task still running fails at its
//! next KV operation, and sets the overlay's `closed` flag, so nothing is written through into the
//! shared cache afterwards.
//!
//! # Places
//!
//! The frame takes at most one up-front grant from the node's place pool
//! ([`DiskAnnBuildFrame::take_places`]), without waiting, and sizes its batch to it: upstream
//! spawns at most `granted` tasks in any phase, and none when nothing is granted. The grant is
//! shared, as an `Arc`, by the frame and by the [`TaskGuard`] of every task the batch spawned. A
//! task's share is taken synchronously when its future is built, so the places stay held until
//! every task the frame spawned has exited or been dropped unpolled, even when the frame is closed
//! first.
//!
//! # Reads
//!
//! The overlay holds the current value of every `!de`/`!dn`/`!ds` key the frame has written, and a
//! read of such a key is answered from it. Every other key is clean: the frame has not changed it,
//! so its committed value is the value the frame's transaction holds.
//!
//! Where the engine gives a read-only transaction a view (RocksDB), the frame opens one
//! ([`crate::parallel::ReadView`]) once its write lock is held, and every clean `!de`/`!dn` point
//! read goes through that view's batched read, which does not queue behind the write transaction.
//! What the view reads is committed, so it is published into the shared cache under the view's own
//! generation — but only under the overlay's write lock and only for a key the overlay does not
//! hold ([`DiskAnnBuildFrame::publish_clean`]). Every write-through takes the same lock after it
//! records the key, so a value read through the view never lands in the cache after a value the
//! frame wrote for the same key.
//!
//! Where the engine gives none (mem, SurrealKV, TiKV), the frame reads clean keys through its write
//! transaction, through [`FrameKv`], and publishes nothing it read. Such a frame takes no places,
//! so it runs on its calling task alone.
//!
//! The graph state (`!ds`) is never read through the view: it is read from the overlay, the cache,
//! or the write transaction, and a read-modify-write of it from the overlay or the transaction
//! alone.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use common::EngineError;
use surrealdb_datastore::Transaction;
use surrealdb_datastore::sequences::Sequences;
use surrealdb_datastore::tx::MeteredValsCursor;
use surrealdb_kvs::Direction;
use surrealdb_kvs::api::ValsBatch;

use crate::catalog::providers::{BoxProviderFut, CancellationProbe};
use crate::config::IdxConfig;
use crate::env::{BoxEnvFut, IndexEnv};
use crate::key::{KVKey, KVValue, TypedRange};
use crate::parallel::ReadView;
use crate::places::{PlaceGrant, Ticket};
use crate::trees::diskann::cache::Stamp;
use crate::trees::diskann::{DiskAnnElement, DiskAnnNode, DiskAnnState, ElementId};
use crate::trees::store::IndexStores;

/// A place grant shared by a frame and the tasks it spawned. The mutex is never locked: it makes
/// the grant, whose places need only be `Send`, shareable across those tasks.
type SharedGrant = Mutex<PlaceGrant>;

/// Why a frame failed when nothing typed reached its error side-channel.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DiskAnnFrameError {
	/// A task the frame's batched insert spawned unwound.
	#[error("a DiskANN build task panicked")]
	TaskPanicked,
	/// An element the frame's batched insert placed in the graph has no adjacency list written by
	/// the frame: its candidate generation failed, and upstream dropped the failure.
	#[error("DiskANN element {element_id} was inserted without an adjacency list")]
	NodeNotWritten {
		element_id: ElementId,
	},
}

/// Where a frame reads keys it has not written.
enum ViewSlot {
	/// The engine gives no view: the frame reads them through its write transaction.
	Absent,
	/// The frame's view, until [`DiskAnnBuildFrame::close_now`] takes it.
	Open(FrameView),
	/// The frame is closed: no read falls back to the write transaction, and every read fails as
	/// cancelled.
	Closed,
}

/// One read view shared by every task of a frame.
#[derive(Clone)]
pub(crate) struct FrameView {
	/// The read-only transaction and its view. Shared, so a task holds its own handle for the
	/// length of one read, outside the slot's lock.
	read: Arc<ReadView>,
	/// The stamp a value read through the view is published under: this wrapper's owner and the
	/// `!dg` generation the view holds. `None` when the view must not publish: it is inside the
	/// wrapper's span but behind its head.
	publication: Option<Stamp>,
}

impl FrameView {
	/// The stamp what this view reads is published under, or `None` when it publishes nothing.
	pub(super) fn publication(&self) -> Option<Stamp> {
		self.publication
	}
}

/// What opening a frame's view found: the `!dg` generation the view holds, or that the engine
/// gives no view.
pub(crate) enum ViewGeneration {
	/// The engine gives no view; the frame reads through its write transaction.
	Absent,
	/// The view holds `!dg` at this generation.
	Holds(Option<u64>),
}

/// A test fault injected while a frame opens its view.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ViewOpenFault {
	/// Fails before the read transaction is opened.
	BeforeOpen,
	/// Fails once the read transaction is open and its view taken.
	AfterOpen,
}

/// What a frame has written to the graph keys, by key.
#[derive(Default)]
struct FrameOverlay {
	/// `None` records a hard delete (`release`) in this frame.
	elements: HashMap<ElementId, Option<Arc<DiskAnnElement>>>,
	/// Every adjacency list this frame wrote.
	nodes: HashMap<ElementId, Arc<DiskAnnNode>>,
	/// The elements whose own adjacency list this frame set: the list an insert chose for the
	/// element it inserted, rather than a back-edge another insert added to it.
	own_lists: HashSet<ElementId>,
	/// The graph state this frame last wrote.
	state: Option<DiskAnnState>,
	/// Set when the frame is closed; nothing is recorded or written through after it.
	closed: bool,
}

/// What a frame did, counted. Tests pin the frame's structure with exact counts on these.
#[derive(Default)]
pub(crate) struct FrameStats {
	/// Point reads of graph keys the frame sent to its write transaction.
	pub(crate) reads_tx: AtomicU64,
	/// Graph keys answered from the frame's own writes.
	pub(crate) reads_overlay: AtomicU64,
	/// Batched point reads of graph keys the frame sent to its read view.
	pub(crate) reads_view: AtomicU64,
	/// Values read through the view that the frame published into the shared cache.
	pub(crate) cache_publishes: AtomicU64,
	/// Read transactions the frame opened for its view: 1 with a view, 0 without.
	pub(crate) read_txs_opened: AtomicU64,
	/// Writes of the graph state (`!ds`).
	pub(crate) state_writes: AtomicU64,
	/// Futures the frame's batched insert built through `wrap_spawn`.
	pub(crate) spawns: AtomicU64,
	/// The most of those tasks alive at once.
	pub(crate) peak_tasks: AtomicUsize,
	/// The places the frame was granted.
	pub(crate) granted: AtomicUsize,
	/// Fresh vectors inserted through upstream's batched insert, and the calls that inserted
	/// them.
	pub(crate) batched_vectors: AtomicU64,
	pub(crate) batch_calls: AtomicU64,
	/// The most fresh vectors one batched call inserted.
	pub(crate) largest_batch: AtomicUsize,
	/// Fresh vectors inserted one at a time on the calling task.
	pub(crate) sequential_vectors: AtomicU64,
	/// Batches read through a cursor opened by the frame.
	pub(crate) scan_batches: AtomicU64,
	/// The tasks alive each time one of the frame's transactions ended.
	#[cfg(test)]
	pub(crate) live_at_tx_end: Mutex<Vec<usize>>,
}

impl FrameStats {
	/// Records that one of the frame's transactions ends now, with `live` of its tasks alive.
	pub(crate) fn tx_ended(&self, live: usize) {
		#[cfg(test)]
		lock(&self.live_at_tx_end).push(live);
		#[cfg(not(test))]
		let _ = live;
	}
}

/// State shared by every task of one compaction frame.
///
/// Invariants:
///  * `overlay` holds the current value of every `!de`/`!dn`/`!ds` key this frame's transaction has
///    written, so a read of a key the frame wrote never needs the transaction or the view.
///  * The view, when there is one, was taken after the frame's write transaction buffered its `!dg`
///    bump, and serves only keys absent from `overlay`.
///  * A value read through the view is published into the shared cache only while holding
///    `overlay`'s write lock, only while `overlay.closed` is unset, and only for a key absent from
///    `overlay`. Every write-through holds that lock too, after it records the key, so a value read
///    through the view never overwrites one the frame wrote.
///  * The frame holds at most one place grant, taken without waiting, and never more places than it
///    was granted. Its batch spawns at most `granted` tasks per phase, and none at a grant of 0. A
///    frame without a view asks for none.
///  * The grant returns to the pool only when the frame and every task it spawned have dropped
///    their share of it.
pub(crate) struct DiskAnnBuildFrame {
	/// Where the frame reads keys it has not written. Held only to read or swap it, never across
	/// an `.await`.
	view: Mutex<ViewSlot>,
	/// The ticket the frame's grant is taken through.
	ticket: Ticket,
	/// Whether the frame has asked for its grant.
	asked: AtomicBool,
	/// The frame's share of its grant, until [`Self::close_now`] drops it.
	places: Mutex<Option<Arc<SharedGrant>>>,
	overlay: RwLock<FrameOverlay>,
	/// The first failure any task of the frame reported, with its type intact.
	first_error: Mutex<Option<anyhow::Error>>,
	/// Set when the frame is cancelled or closed; every KV operation of the frame checks it first.
	cancelled: AtomicBool,
	/// Tasks built through `wrap_spawn` that have not exited, counted by [`TaskGuard`].
	live_tasks: AtomicUsize,
	/// Notified when `live_tasks` reaches 0.
	tasks_done: tokio::sync::Notify,
	pub(crate) stats: FrameStats,
	/// Faults a test injects into this frame.
	#[cfg(test)]
	pub(crate) faults: Option<Arc<FrameFaults>>,
	/// Lets a frame without a view still take places, so a test runs the batched path on an
	/// engine that gives no view.
	#[cfg(test)]
	batch_without_view: bool,
}

impl DiskAnnBuildFrame {
	fn new(ticket: Ticket, view: ViewSlot) -> Self {
		Self {
			view: Mutex::new(view),
			ticket,
			asked: AtomicBool::new(false),
			places: Mutex::new(None),
			overlay: RwLock::new(FrameOverlay::default()),
			first_error: Mutex::new(None),
			cancelled: AtomicBool::new(false),
			live_tasks: AtomicUsize::new(0),
			tasks_done: tokio::sync::Notify::new(),
			stats: FrameStats::default(),
			#[cfg(test)]
			faults: None,
			#[cfg(test)]
			batch_without_view: false,
		}
	}

	/// Opens a frame alongside `tx`, its write transaction, whose batch takes its places through
	/// `ticket`.
	///
	/// Where the engine gives a read-only transaction a view, it opens one and reads the `!dg`
	/// generation (`generation_key`) through it; `publication` maps that generation to the stamp
	/// what the view reads is published under. The answer says which generation the view holds, or
	/// that the engine gives none, in which case the frame reads through `tx` and opens nothing.
	///
	/// On failure, a read transaction already opened is cancelled before the error returns. `tx`
	/// is the caller's to cancel.
	pub(super) async fn open<K>(
		tx: &Transaction,
		ticket: Ticket,
		generation_key: &K,
		publication: impl FnOnce(Option<u64>) -> Option<Stamp>,
		#[cfg(test)] hooks: FrameHooks,
	) -> Result<(Arc<Self>, ViewGeneration)>
	where
		K: KVKey<Value = u64>,
	{
		#[cfg(test)]
		let fault = hooks.faults.as_ref().and_then(|f| f.view_open);
		#[cfg(test)]
		if fault == Some(ViewOpenFault::BeforeOpen) {
			anyhow::bail!("injected failure opening a DiskANN frame's read view");
		}
		#[cfg(test)]
		let read = match hooks.faults.as_ref().is_some_and(|f| f.no_view) {
			true => None,
			false => ReadView::open(tx).await?,
		};
		#[cfg(not(test))]
		let read = ReadView::open(tx).await?;
		let (slot, generation, opened) = match read {
			None => (ViewSlot::Absent, ViewGeneration::Absent, 0),
			Some(read) => {
				let read = Arc::new(read);
				#[cfg(test)]
				if let Some(faults) = &hooks.faults {
					lock(&faults.opened_views).push(Arc::clone(&read));
				}
				let held = async {
					#[cfg(test)]
					if fault == Some(ViewOpenFault::AfterOpen) {
						anyhow::bail!("injected failure after opening a DiskANN frame's read view");
					}
					let key = generation_key.encode_key()?;
					let fetched = read.get_many(std::slice::from_ref(&key)).await?;
					fetched
						.into_iter()
						.next()
						.flatten()
						.map(|value| u64::kv_decode_value(&value, ()))
						.transpose()
				}
				.await;
				let held = match held {
					Ok(held) => held,
					Err(e) => {
						read.close().await;
						return Err(e);
					}
				};
				let view = FrameView {
					read,
					publication: publication(held),
				};
				(ViewSlot::Open(view), ViewGeneration::Holds(held), 1)
			}
		};
		#[allow(unused_mut)]
		let mut frame = Self::new(ticket, slot);
		frame.stats.read_txs_opened.store(opened, Ordering::Relaxed);
		#[cfg(test)]
		{
			frame.faults = hooks.faults;
			frame.batch_without_view = hooks.batch_without_view;
		}
		Ok((Arc::new(frame), generation))
	}

	/// A frame with no view, for the tests that drive a frame without a transaction.
	#[cfg(test)]
	pub(crate) fn without_view(ticket: Ticket, batch_without_view: bool) -> Arc<Self> {
		let mut frame = Self::new(ticket, ViewSlot::Absent);
		frame.batch_without_view = batch_without_view;
		Arc::new(frame)
	}

	/// Takes the frame's up-front grant, and returns how many places it holds: between none and
	/// `max`, without waiting. Called at most once per frame; a later call answers `None`.
	///
	/// A frame without a view asks for nothing: its tasks would all read through the one write
	/// transaction, so it runs on its calling task. Without a runtime to spawn onto, nothing is
	/// asked for either, since the batch could spawn nothing.
	pub(crate) fn take_places(&self, max: usize) -> Option<NonZeroUsize> {
		if self.asked.swap(true, Ordering::AcqRel) || self.is_cancelled() {
			return None;
		}
		#[cfg(test)]
		let without_view = self.batch_without_view;
		#[cfg(not(test))]
		let without_view = false;
		if matches!(*lock(&self.view), ViewSlot::Absent) && !without_view {
			return None;
		}
		if tokio::runtime::Handle::try_current().is_err() {
			return None;
		}
		let grant = self.ticket.acquire_places(max);
		let granted = NonZeroUsize::new(grant.len())?;
		self.stats.granted.store(granted.get(), Ordering::Release);
		*lock(&self.places) = Some(Arc::new(Mutex::new(grant)));
		Some(granted)
	}

	/// A share of the frame's grant, for a task about to be spawned. `None` once the frame has
	/// dropped its own share, or when it holds none.
	fn places_share(&self) -> Option<Arc<SharedGrant>> {
		lock(&self.places).clone()
	}

	/// The view to read clean keys through, as a handle cloned under the slot's short lock:
	/// `Some` while it is open, `None` when the frame has none, and an error once the frame is
	/// closed — never a fallback to the write transaction.
	pub(crate) fn view(&self) -> Result<Option<FrameView>> {
		match &*lock(&self.view) {
			ViewSlot::Absent => Ok(None),
			ViewSlot::Open(view) => Ok(Some(view.clone())),
			ViewSlot::Closed => Err(EngineError::QueryCancelled.into()),
		}
	}

	/// Reads `keys` through `view` in one batch, checked against the frame first. One answer per
	/// key, in input order.
	pub(crate) async fn read_through<K>(
		&self,
		view: &FrameView,
		keys: &[K],
	) -> Result<Vec<Option<K::Value>>>
	where
		K: KVKey,
		K::Value: KVValue<KeyContext = ()>,
	{
		self.check()?;
		let encoded = keys.iter().map(KVKey::encode_key).collect::<Result<Vec<_>>>()?;
		self.stats.reads_view.fetch_add(1, Ordering::Relaxed);
		let fetched = view.read.get_many(&encoded).await?;
		// A short answer would leave the trailing keys unread, silently.
		anyhow::ensure!(
			fetched.len() == keys.len(),
			"{} values read through the view for {} keys",
			fetched.len(),
			keys.len()
		);
		fetched
			.into_iter()
			.map(|value| value.map(|value| K::Value::kv_decode_value(&value, ())).transpose())
			.collect()
	}

	/// Closes the frame synchronously: sets `cancelled`, sets `closed` under the overlay's write
	/// lock, drops the frame's share of its places and swaps the view slot to closed, returning the
	/// view it held for the caller to close, which cancels its read transaction. Idempotent.
	///
	/// The places return to the pool once the last task the frame spawned has exited: at once after
	/// a batch that ran to an end, whose tasks upstream joined; later after an aborted one. A task
	/// still holding a handle to the view reads safely after this: a read that starts after the
	/// read transaction is cancelled fails, and the cancel waits for one in flight.
	pub(crate) fn close_now(&self) -> Option<Arc<ReadView>> {
		self.cancelled.store(true, Ordering::Release);
		write(&self.overlay).closed = true;
		drop(lock(&self.places).take());
		match std::mem::replace(&mut *lock(&self.view), ViewSlot::Closed) {
			ViewSlot::Open(view) => Some(view.read),
			ViewSlot::Absent | ViewSlot::Closed => None,
		}
	}

	/// Whether the frame has been closed.
	#[cfg(test)]
	pub(crate) fn is_closed(&self) -> bool {
		matches!(*lock(&self.view), ViewSlot::Closed)
	}

	/// Records `e` as the frame's failure, unless one is already recorded.
	pub(crate) fn record_error(&self, e: anyhow::Error) {
		let mut first = lock(&self.first_error);
		if first.is_none() {
			*first = Some(e);
		}
	}

	/// Records that a spawned task unwound. Called from [`TaskGuard::drop`], the only place a panic
	/// of such a task is observable: upstream joins those tasks and discards the `JoinError`.
	pub(crate) fn record_task_panic(&self) {
		self.record_error(DiskAnnFrameError::TaskPanicked.into());
	}

	/// Takes the frame's recorded failure.
	pub(crate) fn take_error(&self) -> Option<anyhow::Error> {
		lock(&self.first_error).take()
	}

	/// Cancels the frame: every KV operation the frame makes after this fails.
	pub(crate) fn cancel(&self) {
		self.cancelled.store(true, Ordering::Release);
	}

	/// Whether the frame has been cancelled or closed.
	pub(crate) fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::Acquire)
	}

	/// Fails once the frame has been cancelled or closed.
	pub(crate) fn check(&self) -> Result<()> {
		if self.is_cancelled() {
			return Err(EngineError::QueryCancelled.into());
		}
		Ok(())
	}

	/// The tasks built through `wrap_spawn` that have not exited.
	pub(crate) fn live_tasks(&self) -> usize {
		self.live_tasks.load(Ordering::Acquire)
	}

	/// Waits until every task this frame spawned has exited — joined by upstream or detached by
	/// it. Registers for the notification before it reads the count, so no exit is missed.
	/// Terminates once `cancelled` is set, because every KV operation in the frame checks it first;
	/// on a frame whose batch ran to an end it returns at once.
	pub(crate) async fn drain_tasks(&self) {
		loop {
			let notified = self.tasks_done.notified();
			let mut notified = std::pin::pin!(notified);
			notified.as_mut().enable();
			if self.live_tasks() == 0 {
				return;
			}
			notified.await;
		}
	}

	/// The structural post-condition: fails naming the first of `ids` whose own adjacency list
	/// this frame did not set. A back-edge another element added to it does not count: an element
	/// whose candidate generation failed can still receive one, and is then left with only the
	/// edges others chose for it.
	pub(crate) fn verify_nodes_written(&self, ids: &[ElementId]) -> Result<()> {
		let overlay = read(&self.overlay);
		match ids.iter().find(|id| !overlay.own_lists.contains(id)) {
			Some(&element_id) => Err(DiskAnnFrameError::NodeNotWritten {
				element_id,
			}
			.into()),
			None => Ok(()),
		}
	}

	/// Records an element this frame wrote (`None` for a hard delete) and runs `through`, which
	/// writes it through into the shared cache, in one critical section. Neither happens once the
	/// frame is closed.
	pub(crate) fn wrote_element(
		&self,
		id: ElementId,
		element: Option<Arc<DiskAnnElement>>,
		through: impl FnOnce(),
	) {
		let mut overlay = write(&self.overlay);
		if overlay.closed {
			return;
		}
		overlay.elements.insert(id, element);
		through();
	}

	/// Records an adjacency list this frame wrote, as [`Self::wrote_element`] records an element.
	/// `own` says the list is the one an insert chose for the element it inserted.
	pub(crate) fn wrote_node(
		&self,
		id: ElementId,
		node: Arc<DiskAnnNode>,
		own: bool,
		through: impl FnOnce(),
	) {
		let mut overlay = write(&self.overlay);
		if overlay.closed {
			return;
		}
		overlay.nodes.insert(id, node);
		if own {
			overlay.own_lists.insert(id);
		}
		through();
	}

	/// Records the graph state this frame wrote, as [`Self::wrote_element`] records an element.
	pub(crate) fn wrote_state(&self, state: DiskAnnState, through: impl FnOnce()) {
		self.stats.state_writes.fetch_add(1, Ordering::Relaxed);
		let mut overlay = write(&self.overlay);
		if overlay.closed {
			return;
		}
		overlay.state = Some(state);
		through();
	}

	/// The frame's own value of each of `ids`' elements, in input order: `Some(None)` for one it
	/// hard-deleted, `None` for one it has not written.
	pub(crate) fn written_elements_of(
		&self,
		ids: &[ElementId],
	) -> Vec<Option<Option<Arc<DiskAnnElement>>>> {
		let overlay = read(&self.overlay);
		ids.iter().map(|id| overlay.elements.get(id).cloned()).collect()
	}

	/// The frame's own adjacency list of each of `ids`, in input order: `None` for one it has not
	/// written.
	pub(crate) fn written_nodes_of(&self, ids: &[ElementId]) -> Vec<Option<Arc<DiskAnnNode>>> {
		let overlay = read(&self.overlay);
		ids.iter().map(|id| overlay.nodes.get(id).cloned()).collect()
	}

	/// The graph state this frame last wrote, if it wrote one.
	pub(crate) fn written_state(&self) -> Option<DiskAnnState> {
		read(&self.overlay).state.clone()
	}

	/// Publishes elements read through the view: runs `publish` for each one the overlay does not
	/// hold, all under the overlay's write lock, and none once the frame is closed.
	pub(crate) fn publish_clean_elements<'v>(
		&self,
		values: impl IntoIterator<Item = (ElementId, &'v Arc<DiskAnnElement>)>,
		mut publish: impl FnMut(ElementId, &'v Arc<DiskAnnElement>),
	) {
		let overlay = write(&self.overlay);
		if overlay.closed {
			return;
		}
		for (id, value) in values {
			if !overlay.elements.contains_key(&id) {
				publish(id, value);
				self.stats.cache_publishes.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	/// Publishes adjacency lists read through the view, as [`Self::publish_clean_elements`]
	/// publishes elements.
	pub(crate) fn publish_clean_nodes<'v>(
		&self,
		values: impl IntoIterator<Item = (ElementId, &'v Arc<DiskAnnNode>)>,
		mut publish: impl FnMut(ElementId, &'v Arc<DiskAnnNode>),
	) {
		let overlay = write(&self.overlay);
		if overlay.closed {
			return;
		}
		for (id, value) in values {
			if !overlay.nodes.contains_key(&id) {
				publish(id, value);
				self.stats.cache_publishes.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	/// The elements this frame has written an adjacency list for, its own or a back-edge.
	#[cfg(test)]
	pub(crate) fn written_nodes(&self) -> Vec<ElementId> {
		read(&self.overlay).nodes.keys().copied().collect()
	}

	/// The elements this frame has written, with `None` for a hard delete.
	#[cfg(test)]
	pub(crate) fn written_elements(&self) -> Vec<(ElementId, Option<Arc<DiskAnnElement>>)> {
		read(&self.overlay).elements.iter().map(|(id, e)| (*id, e.clone())).collect()
	}
}

/// Lives with one task a frame's batched insert spawns: built by `wrap_spawn` before the task's
/// future is returned, moved into that future, and dropped with it — when the task finishes,
/// unwinds, or is dropped without ever having been polled.
///
/// It records a panic escaping the task: upstream joins those tasks and discards the `JoinError`,
/// so a panicking task would otherwise leave the frame committing a graph missing its work. Dropped
/// inside the wrapped future, it observes the unwind before upstream swallows it.
///
/// It holds a share of the frame's place grant, so a task that outlives its frame keeps its place
/// until it exits; and it counts the task as live on the frame from [`Self::new`] to `Drop`, which
/// is how the frame waits for every task it spawned, joined by upstream or not.
pub(crate) struct TaskGuard {
	frame: Arc<DiskAnnBuildFrame>,
	places: Option<Arc<SharedGrant>>,
}

impl TaskGuard {
	/// Takes a share of the frame's grant and counts the task. Built synchronously in `wrap_spawn`.
	pub(crate) fn new(frame: &Arc<DiskAnnBuildFrame>) -> Self {
		let live = frame.live_tasks.fetch_add(1, Ordering::AcqRel) + 1;
		frame.stats.spawns.fetch_add(1, Ordering::Relaxed);
		frame.stats.peak_tasks.fetch_max(live, Ordering::Relaxed);
		Self {
			frame: Arc::clone(frame),
			places: frame.places_share(),
		}
	}
}

impl Drop for TaskGuard {
	fn drop(&mut self) {
		if std::thread::panicking() {
			self.frame.record_task_panic();
		}
		// The place share goes first, so a drained frame holds no place of its tasks.
		drop(self.places.take());
		if self.frame.live_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
			self.frame.tasks_done.notify_waiters();
		}
	}
}

/// How a frame's code reaches its write transaction: checked against the frame's cancellation
/// before every operation it forwards. The frame's other route to KV, its read view, is checked
/// the same way ([`DiskAnnBuildFrame::read_through`]).
///
/// Outside a frame — a KNN search, or the pending-write path — there is no frame to check, and
/// every operation goes straight to the transaction.
#[derive(Clone, Copy)]
pub(crate) struct FrameKv<'a> {
	tx: &'a Transaction,
	frame: Option<&'a DiskAnnBuildFrame>,
}

impl<'a> FrameKv<'a> {
	pub(crate) fn new(tx: &'a Transaction, frame: Option<&'a DiskAnnBuildFrame>) -> Self {
		Self {
			tx,
			frame,
		}
	}

	/// Fails once the frame has been cancelled or closed.
	pub(crate) fn check(&self) -> Result<()> {
		match self.frame {
			Some(frame) => frame.check(),
			None => Ok(()),
		}
	}

	pub(crate) async fn get_key<K>(&self, key: &K) -> Result<Option<K::Value>>
	where
		K: KVKey + std::fmt::Debug,
	{
		self.check()?;
		self.tx.get_key(key, None).await
	}

	pub(crate) async fn get_many_key<K>(&self, keys: Vec<K>) -> Result<Vec<Option<K::Value>>>
	where
		K: KVKey + std::fmt::Debug,
	{
		self.check()?;
		self.tx.get_many_key(keys, None).await
	}

	pub(crate) async fn set_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + std::fmt::Debug,
	{
		self.check()?;
		self.tx.set_key(key, val).await
	}

	pub(crate) async fn del_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + std::fmt::Debug,
	{
		self.check()?;
		self.tx.del_key(key).await
	}

	/// Opens a forward key+value cursor over `rng`, which checks the frame before each batch.
	pub(crate) async fn open_vals_cursor<V>(&self, rng: TypedRange<V>) -> Result<FrameCursor<'a>> {
		self.check()?;
		Ok(FrameCursor {
			inner: self.tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?,
			frame: self.frame,
		})
	}
}

/// A cursor opened through [`FrameKv`]: it checks the frame before each batch it reads.
pub(crate) struct FrameCursor<'a> {
	inner: MeteredValsCursor<'a>,
	frame: Option<&'a DiskAnnBuildFrame>,
}

impl FrameCursor<'_> {
	pub(crate) async fn next_batch(&mut self, limit: u32) -> Result<ValsBatch<'_>> {
		if let Some(frame) = self.frame {
			frame.check()?;
			frame.stats.reads_tx.fetch_add(1, Ordering::Relaxed);
			let read = frame.stats.scan_batches.fetch_add(1, Ordering::Relaxed) + 1;
			#[cfg(not(test))]
			let _ = read;
			#[cfg(test)]
			if let Some(faults) = &frame.faults
				&& faults.cancel_after_scan_batches.load(Ordering::Acquire) == read as usize
			{
				let batch = self.inner.next_batch(limit).await;
				frame.cancel();
				return batch;
			}
		}
		self.inner.next_batch(limit).await
	}
}

/// The environment a frame resolves doc-ids in: the statement's, which also reports done, and
/// whose cancellation probe also fails, once the frame is cancelled. The doc-ID sequence allocator
/// polls that probe before every window claim it attempts.
pub(crate) struct FrameEnv<'a> {
	pub(crate) inner: &'a dyn IndexEnv,
	pub(crate) frame: &'a DiskAnnBuildFrame,
}

impl CancellationProbe for FrameEnv<'_> {
	fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
		Box::pin(async move {
			self.frame.check()?;
			self.inner.expect_not_timedout().await
		})
	}
}

impl IndexEnv for FrameEnv<'_> {
	fn tx(&self) -> Arc<Transaction> {
		self.inner.tx()
	}

	fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
		if self.frame.is_cancelled() {
			return Box::pin(async { Ok(true) });
		}
		self.inner.is_done(count)
	}

	fn index_stores(&self) -> &IndexStores {
		self.inner.index_stores()
	}

	fn node_id(&self) -> uuid::Uuid {
		self.inner.node_id()
	}

	fn sequences(&self) -> Result<&Sequences> {
		self.inner.sequences()
	}

	fn config(&self) -> &IdxConfig {
		self.inner.config()
	}

	fn places(&self) -> Ticket {
		self.inner.places()
	}
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read<T>(m: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
	m.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write<T>(m: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
	m.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Faults a test injects into a frame, through the index the frame runs on.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct FrameFaults {
	/// Fails the next `append_vector` with the error this builds, through the side-channel.
	pub(crate) fail_append: Mutex<Option<Box<dyn FnOnce() -> anyhow::Error + Send>>>,
	/// Panics the next `append_vector` call.
	pub(crate) panic_append: AtomicBool,
	/// Fails the search for this vector without recording it, standing in for an
	/// upstream-internal failure of that item's candidate generation.
	pub(crate) fail_search_for: Mutex<Option<crate::trees::vector::SerializedVector>>,
	/// Fails `set_element` for this id, through the side-channel, without parking.
	pub(crate) fail_set_element: Mutex<Option<ElementId>>,
	/// Cancels the frame once a cursor it opened has read this many batches.
	pub(crate) cancel_after_scan_batches: AtomicUsize,
	/// Parks every `set_element` until released; see [`Park`].
	pub(crate) park_set_element: Option<Arc<Park>>,
	/// Panics the frame's own task once this many tasks are parked in `park_set_element`.
	pub(crate) panic_frame_when_parked: Option<usize>,
	/// Fails the frame while it opens its read view.
	pub(crate) view_open: Option<ViewOpenFault>,
	/// Every read view a frame opened, so a test can check how each one ended.
	pub(crate) opened_views: Mutex<Vec<Arc<ReadView>>>,
	/// Opens every frame without a view, on an engine that gives one: the no-view path.
	pub(crate) no_view: bool,
}

/// What a test hands a frame it opens.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct FrameHooks {
	pub(crate) faults: Option<Arc<FrameFaults>>,
	/// Lets the frame take places without a view.
	pub(crate) batch_without_view: bool,
}

/// A place a test parks a frame's tasks in, and releases them from.
#[cfg(test)]
pub(crate) struct Park {
	parked: AtomicUsize,
	arrived: tokio::sync::Notify,
	released: tokio::sync::watch::Sender<bool>,
}

#[cfg(test)]
impl Park {
	pub(crate) fn new() -> Arc<Self> {
		Arc::new(Self {
			parked: AtomicUsize::new(0),
			arrived: tokio::sync::Notify::new(),
			released: tokio::sync::watch::Sender::new(false),
		})
	}

	/// Parks the caller until [`Self::release`].
	pub(crate) async fn park(&self) {
		let mut released = self.released.subscribe();
		self.parked.fetch_add(1, Ordering::AcqRel);
		self.arrived.notify_waiters();
		let _ = released.wait_for(|released| *released).await;
	}

	/// Returns once `n` callers have parked.
	pub(crate) async fn wait_for(&self, n: usize) {
		loop {
			let arrived = self.arrived.notified();
			let mut arrived = std::pin::pin!(arrived);
			arrived.as_mut().enable();
			if self.parked.load(Ordering::Acquire) >= n {
				return;
			}
			arrived.await;
		}
	}

	pub(crate) fn release(&self) {
		self.released.send_replace(true);
	}
}

impl crate::trees::doc_elements::ElementsKv for FrameKv<'_> {
	async fn get_elements<K>(&self, key: &K) -> Result<Option<crate::trees::knn::Ids64>>
	where
		K: KVKey<Value = crate::trees::knn::Ids64> + std::fmt::Debug + Sync,
	{
		self.get_key(key).await
	}

	async fn set_elements<K>(&self, key: &K, elements: &crate::trees::knn::Ids64) -> Result<()>
	where
		K: KVKey<Value = crate::trees::knn::Ids64> + std::fmt::Debug + Sync,
	{
		self.set_key(key, elements).await
	}

	async fn del_elements<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey<Value = crate::trees::knn::Ids64> + std::fmt::Debug + Sync,
	{
		self.del_key(key).await
	}
}
