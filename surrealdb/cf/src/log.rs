//! A storage backend over a sequenced log and shale's tier in R2.
//!
//! The data has four layers, read newest first:
//!
//! - a transaction's own writes;
//! - the unsealed suffix: the log applied past the last sealed position, in memory, deletes kept
//!   as tombstones (they must hide what the layers below hold). Bounded by how often the log is
//!   sealed, not by the size of the database;
//! - the sealed segments: shale segments in R2, read by key through their indexes (`edge_tier`);
//! - the tree: shale's copy-on-write B+tree in R2 at the manifest root.
//!
//! The last two are the tier ([`Tier`]), read as of a [`View`] the node has adopted. A
//! transaction reads the suffix as of the position applied when it began and the view adopted
//! then; commits hand the write set and that position to the host, which sends them to the
//! sequencer; accepted writes reach the suffix the same way every other writer's do, by applying
//! the log, and the host resolves the commit once they have.
//!
//! When log maintenance publishes a manifest (new segments, or a fold), a node adopts it once it
//! has applied every position it reflects, and drops the suffix entries it now holds.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use edge_tier::read::{Published, Tier, View};
use futures::FutureExt;
use shale::format::Direction as TierDirection;
use surrealdb_kvs::api::BoxFut;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{
	Direction, Error, Key, KeyRange, KeysResult, Metrics, Result, ScanResult, Transactable,
	TransactionBuilder, TransactionType, Val,
};
#[cfg(feature = "js")]
use wasm_bindgen::{JsCast, JsValue};
#[cfg(feature = "js")]
use wasm_bindgen_futures::JsFuture;

/// The length that marks a deletion in a packed write set.
const DELETE: u32 = u32::MAX;

/// A future asserted `Send`.
///
/// `wasm32-unknown-unknown` without the atomics feature has one thread, so
/// nothing is ever sent anywhere; the storage traits require `Send` only
/// because native backends run on a thread pool.
struct AssertSend<F>(F);

// SAFETY: see the type's documentation; this crate only builds for a
// single-threaded wasm host.
unsafe impl<F> Send for AssertSend<F> {}

impl<F: Future> Future for AssertSend<F> {
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
		// SAFETY: structural pinning of the only field.
		unsafe { self.map_unchecked_mut(|s| &mut s.0) }.poll(cx)
	}
}

/// Why a commit failed, from the host's commit function.
#[derive(Debug)]
pub enum CommitError {
	/// The sequencer refused the commit: another write to a key it wrote or read came after its
	/// snapshot.
	Conflict(String),
	/// Anything else.
	Other(String),
}

/// What a host's commit function answers: once the commit's position has been applied to the
/// replica, `Ok`.
pub type CommitFuture = Pin<Box<dyn Future<Output = std::result::Result<(), CommitError>>>>;

/// The host's commit function: `(snapshot, section, key hashes)`, answering once the replica has
/// applied the commit, or with a [`CommitError`]. The section (`surrealds-section`: the packed
/// write set, sorted, then its bloom and key range) and the hash of each key (what the sequencer
/// screens the commit with) are made in the loop that packs the write set. They are handed over,
/// so the host holds the only copy across the commit's round trip. A host that wants the plain
/// write set reads the section's prefix: it is byte for byte a packed write set.
///
/// For a JavaScript host it is `(snapshot: number, writes: Uint8Array) => Promise<number>`
/// (see [`CommitHook::from_js`]).
pub struct CommitHook(Box<dyn Fn(u64, Vec<u8>, Vec<u64>) -> CommitFuture>);

// SAFETY: single-threaded host, as for `AssertSend`.
unsafe impl Send for CommitHook {}
unsafe impl Sync for CommitHook {}

impl CommitHook {
	pub fn new(f: impl Fn(u64, Vec<u8>, Vec<u64>) -> CommitFuture + 'static) -> Self {
		Self(Box::new(f))
	}

	/// A hook calling a JavaScript function `(snapshot: number, writes: Uint8Array) =>
	/// Promise<number>`, which resolves to the commit's log position once the replica has
	/// applied it, or rejects with an `Error` whose message starts with `conflict` when the
	/// sequencer refused it.
	#[cfg(feature = "js")]
	pub fn from_js(f: js_sys::Function) -> Self {
		Self::new(move |snap, packed, _hashes| {
			// The writes as a JavaScript host takes them: exactly a packed write set (the section's
			// prefix; its meta, which a TypeScript host sizes messages and objects by `length`, is
			// not part of it).
			let bytes = js_sys::Uint8Array::from(packed_prefix(&packed));
			// Only the host's copy stays alive across the commit's round trip.
			drop(packed);
			let promise = f.call2(&JsValue::NULL, &JsValue::from_f64(snap as f64), &bytes);
			Box::pin(async move {
				let promise = promise.map_err(|e| CommitError::Other(js_message(&e)))?;
				match JsFuture::from(js_sys::Promise::from(promise)).await {
					Ok(_) => Ok(()),
					Err(e) => {
						let msg = js_message(&e);
						if msg.starts_with("conflict") {
							Err(CommitError::Conflict(msg))
						} else {
							Err(CommitError::Other(msg))
						}
					}
				}
			})
		})
	}
}

fn tier_err(e: impl std::fmt::Display) -> Error {
	Error::Transaction(format!("tier: {e}"))
}

// ---- suffix encoding ----------------------------------------------------------

/// A suffix value: `1 pos:u64 value` for a write, `0 pos:u64` for a delete. The position lets an
/// adoption drop exactly the entries the tier now holds.
fn sfx_set(pos: u64, v: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(9 + v.len());
	out.push(1);
	out.extend_from_slice(&pos.to_le_bytes());
	out.extend_from_slice(v);
	out
}

fn sfx_del(pos: u64) -> Vec<u8> {
	let mut out = Vec::with_capacity(9);
	out.push(0);
	out.extend_from_slice(&pos.to_le_bytes());
	out
}

/// `Some(Some(value))`, `Some(None)` for a delete, `None` for a malformed entry.
fn sfx_decode(e: &[u8]) -> Option<Option<&[u8]>> {
	match e.first()? {
		1 => Some(Some(e.get(9..)?)),
		0 => Some(None),
		_ => None,
	}
}

fn sfx_pos(e: &[u8]) -> u64 {
	e.get(1..9).and_then(|b| b.try_into().ok()).map_or(0, u64::from_le_bytes)
}

/// A transaction's own writes carry no log position yet.
const UNSEQUENCED: u64 = u64::MAX;

// ---- the replica -------------------------------------------------------------

/// The unsealed suffix, the tier under it, and the snapshots pinned queries read.
pub struct Replica {
	/// The unsealed suffix's store. Replaced (not trimmed) when entries leave it in bulk, see
	/// [`Replica::rebuild_suffix`]. Read it with [`Replica::suffix`].
	suffix: Mutex<Arc<surrealdb_kvs_mem::Datastore>>,
	pub tier: Tier,
	applied: AtomicU64,
	/// The tier view new transactions read.
	view: Mutex<View>,
	/// Read snapshots of recent positions, oldest first: the suffix as of the position, and the
	/// view adopted then. Each keeps seeing exactly its position for as long as it is held, so a
	/// query pinned to one reads that position and no other (see [`Pinned`]).
	snapshots: Mutex<std::collections::VecDeque<(u64, Arc<dyn Transactable>, View, IngestedSet, u64)>>,
	/// The most bytes of applied log the held snapshots may pin together (0: no bound). A snapshot
	/// keeps every suffix entry applied before it, so entries a roll swapped for layers stay in memory
	/// until the snapshots taken since they were applied are dropped.
	snapshot_bytes: AtomicU64,
	keep: std::sync::atomic::AtomicUsize,
	/// Layers applied and not yet covered by the tier's sealed position, oldest first: ingested
	/// objects, and the sections of rolled runs, one layer per run (see `ingest`). Read in place,
	/// never applied into the suffix. They cover disjoint, ascending positions.
	ingested: Mutex<IngestedSet>,
	/// The meta of each section commit applied into the suffix and not yet swapped for a layer
	/// (see [`Replica::roll_swap`]), by position: what a layer over the section is made from, so a
	/// `rolled` notice needs no read.
	live: Mutex<BTreeMap<u64, LiveMeta>>,
	/// Bytes the suffix's store holds, about: what applied commits added, less what a rebuild left
	/// out. What [`Replica::rebuild_wanted`] compares the garbage with.
	suffix_bytes: AtomicU64,
	/// Commits swapped for layers whose entries are still in the suffix's store, by position, and
	/// their bytes: dropped by the next rebuild. A swap does not rebuild at once (that copies the
	/// whole unrolled tail), see [`Replica::roll_swap`].
	garbage: Mutex<(std::collections::HashSet<u64>, u64)>,
	/// The sealed position the suffix's store may still hold entries at or below (a trim that was
	/// not done), dropped by the next rebuild.
	trim_through: AtomicU64,
	/// The garbage, in bytes, below which no rebuild is wanted whatever the tail (1 MiB).
	rebuild_floor: AtomicU64,
	/// Rebuilds done, and wanted but refused for lack of heap (see [`rebuild_allowed`]).
	rebuilds: AtomicU64,
	rebuilds_refused: AtomicU64,
}

/// The garbage a swap may leave in the suffix's store before it is rebuilt, whatever the tail:
/// below this a rebuild's copy is not worth what it frees.
pub const REBUILD_FLOOR_BYTES: u64 = 1 << 20;
/// A rebuild is wanted when the garbage is at least this fraction (1/8) of what stays: the copy is
/// then at most 8 bytes for each byte freed, and the store is at most 1/8 larger than the tail.
pub const REBUILD_FRACTION: u64 = 8;

/// Whether a rebuild's copy fits: the new store is built beside the old, so the heap must have room
/// for the entries that stay (`copy`, at 5/4 for the store's own cost) within `budget`.
pub fn rebuild_allowed(heap_live: u64, copy: u64, budget: u64) -> bool {
	heap_live.saturating_add(copy / 4 * 5) <= budget
}

/// A section commit applied into memory and not yet swapped: its length, and its meta's bytes as
/// the section carries them (parsed into a run's directory at the swap, so a commit held in
/// memory costs the meta's length and little else).
type LiveMeta = (u32, Box<[u8]>);

/// The ingested objects a reader sees: shared, replaced (never changed) when one is added or
/// retired, so a snapshot or transaction keeps the set it began with.
pub type IngestedSet = Arc<Vec<Arc<crate::ingest::Layer>>>;

// SAFETY: single-threaded host, as for `AssertSend`.
unsafe impl Send for Replica {}
unsafe impl Sync for Replica {}

thread_local! {
	/// The position the transactions being created should read, while a pinned
	/// query is being polled.
	static PIN: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// A future whose read transactions read log position `pos`: the pin is in
/// force only while this future is polled, so queries interleaving with it on
/// the one thread are unaffected.
pub struct Pinned<F> {
	pub pos: u64,
	pub inner: Pin<Box<F>>,
}

impl<F: Future> Future for Pinned<F> {
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
		let this = self.get_mut();
		let prev = PIN.with(|p| p.replace(Some(this.pos)));
		let poll = this.inner.as_mut().poll(cx);
		PIN.with(|p| p.set(prev));
		poll
	}
}

/// `fut`'s output if it finishes on its first poll, else `None`. On emscripten the poll runs inside a
/// Tokio task, whose cooperative budget (128 operations a poll) would turn the in-memory store's
/// lock operations into `Pending` past the 128th though the store never waits, so the budget is lifted
/// for this poll. Elsewhere there is no budget to lift.
pub(crate) fn now<F: Future>(fut: F) -> Option<F::Output> {
	#[cfg(target_os = "emscripten")]
	let fut = tokio::task::unconstrained(fut);
	fut.now_or_never()
}

fn sync<T>(fut: impl Future<Output = Result<T>>, what: &str) -> Result<T> {
	// The in-memory store never waits on anything, so its futures finish on their first poll.
	now(fut).ok_or_else(|| Error::Transaction(format!("{what} yielded")))?
}

impl Replica {
	pub async fn new(tier: Tier) -> Result<Self> {
		Ok(Self {
			suffix: Mutex::new(Arc::new(surrealdb_kvs_mem::Datastore::new(Default::default()).await?)),
			tier,
			applied: AtomicU64::new(0),
			view: Mutex::new(View::default()),
			snapshots: Mutex::new(Default::default()),
			snapshot_bytes: AtomicU64::new(0),
			keep: std::sync::atomic::AtomicUsize::new(0),
			ingested: Mutex::new(Arc::new(Vec::new())),
			live: Mutex::new(BTreeMap::new()),
			suffix_bytes: AtomicU64::new(0),
			garbage: Mutex::new(Default::default()),
			trim_through: AtomicU64::new(0),
			rebuild_floor: AtomicU64::new(REBUILD_FLOOR_BYTES),
			rebuilds: AtomicU64::new(0),
			rebuilds_refused: AtomicU64::new(0),
		})
	}

	/// The last log position applied.
	pub fn applied(&self) -> u64 {
		self.applied.load(Ordering::Acquire)
	}

	pub fn view(&self) -> View {
		*self.view.lock().expect("view")
	}

	/// Start from the published tier: adopt it, and read the log from what it reflects on.
	/// Only for an empty replica. Returns the position the replica now stands at.
	pub async fn open_tier(&self) -> Result<u64> {
		if self.applied() != 0 {
			return Err(Error::Transaction("replica is not empty".into()));
		}
		let Some(p) = self.tier.published().await.map_err(tier_err)? else {
			return Ok(0);
		};
		let v = self.tier.adopt(&p).await.map_err(tier_err)?;
		*self.view.lock().expect("view") = v;
		let at = p.reflects();
		self.applied.store(at, Ordering::Release);
		Ok(at)
	}

	/// Adopt the published manifest if it is newer than the view read and this replica has
	/// applied everything it reflects; then drop the suffix entries it now holds. Returns the
	/// sealed position read after.
	pub async fn adopt_published(&self) -> Result<u64> {
		let Some(p) = self.tier.published().await.map_err(tier_err)? else {
			return Ok(self.view().sealed_through);
		};
		self.adopt(&p).await
	}

	async fn adopt(&self, p: &Published) -> Result<u64> {
		let cur = self.view();
		if (p.sealed_through <= cur.sealed_through && p.watermark <= cur.watermark) || p.reflects() > self.applied() {
			return Ok(cur.sealed_through);
		}
		// Segments stay installed while any held snapshot's view still needs them, however many
		// folds have landed since the snapshot was taken.
		let keep_from = {
			let snaps = self.snapshots.lock().expect("snapshots lock");
			snaps.iter().map(|(_, _, v, _, _)| v.watermark).min().unwrap_or(u64::MAX)
		};
		let v = self.tier.adopt_keeping(p, keep_from).await.map_err(tier_err)?;
		*self.view.lock().expect("view") = v;
		// The view first: until the trim, a suffix entry the segments also hold says the same.
		let through = v.sealed_through.max(v.watermark);
		self.trim_through.fetch_max(through, Ordering::AcqRel);
		self.rebuild_if_allowed(false)?;
		self.retire_layers(through);
		// A section the tier holds is never swapped: its meta is not kept.
		let mut live = self.live.lock().expect("live");
		*live = live.split_off(&(through + 1));
		drop(live);
		Ok(v.sealed_through)
	}

	/// Drop the layers the sealed tier holds (positions up to `through`) in favour of reading them
	/// from it: a layer wholly below goes, and a run the seal ends inside of keeps the sections
	/// above, as a new directory (the old one is held by whoever began on it).
	fn retire_layers(&self, through: u64) {
		let mut ing = self.ingested.lock().expect("ingested");
		if !ing.iter().any(|l| l.first() <= through) {
			return;
		}
		*ing = Arc::new(
			ing.iter()
				.filter_map(|l| match (l.last() <= through, &**l) {
					(true, _) => None,
					(false, crate::ingest::Layer::Run(r)) if l.first() <= through => r.after(through).map(|d| Arc::new(crate::ingest::Layer::Run(d))),
					(false, _) => Some(Arc::clone(l)),
				})
				.collect(),
		);
	}

	/// Apply the packed write set of log position `pos`, atomically and without yielding, so
	/// that a transaction begun on either side of it sees all of it or none. Positions at or
	/// below [`Self::applied`] are skipped, so a replay that overlaps is harmless. A position past
	/// the next one is a gap and fails without applying: advancing over it would make a later
	/// replay of the missing position look already applied.
	pub fn apply(&self, pos: u64, packed: &[u8]) -> Result<bool> {
		let applied = self.applied();
		if pos <= applied {
			return Ok(false);
		}
		if pos > applied + 1 {
			return Err(Error::Transaction(format!("log gap: position {pos} arrived after {applied}")));
		}
		sync(
			async {
				let tx = self.suffix().transaction(TransactionType::Write).await?;
				for (k, v) in unpack(packed)? {
					match v {
						Some(v) => tx.set(Key::from(k), sfx_set(pos, v)).await?,
						None => tx.set(Key::from(k), sfx_del(pos)).await?,
					}
				}
				tx.commit().await
			},
			"replica apply",
		)?;
		self.applied.store(pos, Ordering::Release);
		self.suffix_bytes.fetch_add(packed.len() as u64, Ordering::Relaxed);
		self.snap_after_apply(pos, packed.len() as u64)?;
		Ok(true)
	}

	/// Apply a whole batch frame (shale's format, as the log carries it) in one transaction: every
	/// record above [`Self::applied`], the last write of a key in the batch winning, and one read
	/// snapshot at the batch's last position. A transaction begun on either side sees all of the
	/// batch or none of it; positions inside a batch are never a snapshot, which is safe because
	/// every node applies the same frames, so every node's batch boundaries are the same.
	pub fn apply_frame(&self, frame: &[u8], cutoff: u64) -> Result<FrameApplied> {
		self.apply_frame_resolved(frame, cutoff, &[])
	}

	/// The ingested objects new readers see.
	pub fn ingested(&self) -> IngestedSet {
		Arc::clone(&self.ingested.lock().expect("ingested"))
	}

	/// How many layers are held: ingested objects, and rolled runs.
	pub fn ingested_len(&self) -> usize {
		self.ingested.lock().expect("ingested").len()
	}

	/// How many commits the layers hold in place.
	pub fn layer_sections(&self) -> usize {
		self.ingested.lock().expect("ingested").iter().map(|l| l.sections()).sum()
	}

	/// Sections applied into memory and awaiting their run's `rolled` notice.
	pub fn live_sections(&self) -> usize {
		self.live.lock().expect("live").len()
	}

	/// Bytes of the section commits applied into memory and not yet swapped for layers (the
	/// sections' own lengths: what the commits are, not what the engine's store spends on them).
	pub fn live_bytes(&self) -> u64 {
		self.live.lock().expect("live").values().map(|(len, _)| u64::from(*len)).sum()
	}

	/// Bytes the layers' metas take (ranges, blooms, fences, indexes).
	pub fn layer_bytes(&self) -> usize {
		self.ingested.lock().expect("ingested").iter().map(|i| i.footprint()).sum()
	}

	/// Add layers to the set readers see, which stays in position order (a layer is found by
	/// position, newest first).
	fn add_layers(&self, layers: Vec<Arc<crate::ingest::Layer>>) {
		let mut ing = self.ingested.lock().expect("ingested");
		let mut next: Vec<_> = ing.iter().cloned().collect();
		next.extend(layers);
		next.sort_by_key(|l| l.first());
		*ing = Arc::new(next);
	}

	/// The suffix's store now. A transaction or a snapshot keeps the store it began on, so a
	/// replacement never changes what one of them reads.
	fn suffix(&self) -> Arc<surrealdb_kvs_mem::Datastore> {
		Arc::clone(&self.suffix.lock().expect("suffix"))
	}

	/// Replace the suffix's store with one holding every entry whose position is not `gone`, and
	/// return how many entries were dropped. The in-memory store keeps what is deleted from it
	/// (versions, tombstones, and the write sets of recent commits) until its own sweeps reach it, and
	/// on wasm it has no background sweep, so deleting the commits a roll swapped for layers frees none
	/// of their memory, where a new store holds only the unrolled tail. One step that
	/// does not yield, so a transaction begun on either side of it sees the entries in the suffix or
	/// in the layers that replace them, never neither. The old store goes when the last transaction
	/// or snapshot on it does.
	fn rebuild_suffix(&self, gone: impl Fn(u64) -> bool) -> Result<usize> {
		sync(
			async {
				let old = self.suffix();
				// Nothing to drop (a seal announcement that covers no entry held, a notice for what was
				// swapped already): the store stays, and nothing is copied.
				let any = {
					let probe = old.transaction(TransactionType::Read).await?;
					let mut found = false;
					let mut next = Some(KeyRange::from(vec![]..vec![0xff; 512]));
					while let (Some(rng), false) = (next, found) {
						let batch = probe.batch_keys_vals(rng, 10_000, None).await?;
						next = batch.next;
						found = batch.result.iter().any(|(_, e)| gone(sfx_pos(e)));
					}
					probe.cancel().await?;
					found
				};
				if !any {
					return Ok(0);
				}
				let fresh = surrealdb_kvs_mem::Datastore::new(Default::default()).await?;
				let read = old.transaction(TransactionType::Read).await?;
				let write = fresh.transaction(TransactionType::Write).await?;
				let mut dropped = 0;
				let mut kept_bytes = 0u64;
				let mut next = Some(KeyRange::from(vec![]..vec![0xff; 512]));
				while let Some(rng) = next {
					let batch = read.batch_keys_vals(rng, 10_000, None).await?;
					next = batch.next;
					for (k, e) in batch.result {
						if gone(sfx_pos(&e)) {
							dropped += 1;
						} else {
							kept_bytes += (k.len() + e.len()) as u64;
							write.set(Key::from(k), e).await?;
						}
					}
				}
				write.commit().await?;
				read.cancel().await?;
				*self.suffix.lock().expect("suffix") = Arc::new(fresh);
				self.suffix_bytes.store(kept_bytes, Ordering::Relaxed);
				Ok(dropped)
			},
			"suffix rebuild",
		)
	}

	/// Whether the suffix's store has enough garbage (commits swapped for layers, or trimmed by a
	/// seal) that rebuilding it is worth its copy: at least [`REBUILD_FLOOR_BYTES`] and at least
	/// [`REBUILD_FRACTION`]th of what stays, so the copy is bounded by a multiple of what it frees
	/// and a swap per second does not copy the unrolled tail per second.
	fn rebuild_wanted(&self) -> bool {
		let garbage = self.garbage.lock().expect("garbage").1;
		let total = self.suffix_bytes.load(Ordering::Relaxed);
		let stay = total.saturating_sub(garbage);
		garbage > 0 && garbage >= self.rebuild_floor.load(Ordering::Relaxed).max(stay / REBUILD_FRACTION)
	}

	/// Rebuild the suffix's store without what a swap and a seal made garbage of, if the heap has
	/// room for the copy ([`rebuild_allowed`]; refused, counted, and tried again at the next swap
	/// otherwise: the entries left are stale but say what the layers say). `refresh`: a swap, after
	/// which the newest snapshot is moved to the new store (the layers it needs are in place); a
	/// seal's view could not be given to a snapshot, which keeps the store it began on.
	fn rebuild_if_allowed(&self, refresh: bool) -> Result<usize> {
		let through = self.trim_through.load(Ordering::Acquire);
		let garbage: std::collections::HashSet<u64> = self.garbage.lock().expect("garbage").0.clone();
		if through == 0 && garbage.is_empty() {
			return Ok(0);
		}
		let old_bytes = self.suffix_bytes.load(Ordering::Relaxed);
		if !rebuild_allowed(crate::heap_live(), old_bytes, crate::heap_budget()) {
			self.rebuilds_refused.fetch_add(1, Ordering::Relaxed);
			return Ok(0);
		}
		let dropped = self.rebuild_suffix(|pos| pos <= through || garbage.contains(&pos))?;
		*self.garbage.lock().expect("garbage") = Default::default();
		self.rebuilds.fetch_add(1, Ordering::Relaxed);
		self.after_rebuild(old_bytes, refresh);
		Ok(dropped)
	}

	/// The snapshots after a rebuild: the newest moves to the new store when it can, and the older
	/// ones, which keep the old store (all of it, not the log they were charged for) in memory, go
	/// when that is more than the snapshot budget.
	fn after_rebuild(&self, old_bytes: u64, refresh: bool) {
		let budget = self.snapshot_bytes.load(Ordering::Relaxed);
		let applied = self.applied();
		let mut s = self.snapshots.lock().expect("snapshots lock");
		if refresh {
			if let Some(last) = s.back_mut().filter(|l| l.0 == applied) {
				if let Ok(tx) = sync(self.suffix().transaction(TransactionType::Read), "snapshot reopen") {
					let old = std::mem::replace(&mut last.1, Arc::from(tx));
					let _ = now(old.cancel());
					// The view travels with the store and the layers: all three have dropped what the
					// segments the view names now hold.
					last.2 = self.view();
					last.3 = self.ingested();
				}
			}
		}
		if budget > 0 && old_bytes > budget {
			while s.len() > 1 {
				if let Some((_, tx, ..)) = s.pop_front() {
					let _ = now(tx.cancel());
				}
			}
		}
	}

	/// What rebuilds did: done, refused for heap, and the garbage waiting (commits swapped whose
	/// entries are still in the store, and their bytes).
	pub fn rebuild_stats(&self) -> (u64, u64, usize, u64) {
		let g = self.garbage.lock().expect("garbage");
		(self.rebuilds.load(Ordering::Relaxed), self.rebuilds_refused.load(Ordering::Relaxed), g.0.len(), g.1)
	}

	/// Bytes the suffix's store holds, about (see `suffix_bytes`).
	pub fn suffix_bytes(&self) -> u64 {
		self.suffix_bytes.load(Ordering::Relaxed)
	}

	/// The garbage below which no rebuild is wanted (tests set it to 0).
	pub fn set_rebuild_floor(&self, bytes: u64) {
		self.rebuild_floor.store(bytes, Ordering::Relaxed);
	}

	/// A run holding sections was rolled: for each `(position, offset, length)` of `secs` in the
	/// run `run_key`, replace the commit's entries in memory with a layer over the section, read in
	/// place, and free the memory. Only sections this replica applied live are swapped (their metas
	/// were kept then); the count of the others is returned, for a host to retry once it has
	/// applied them. Reads see the same data before and after: the layers are added before the
	/// entries go, in one step that does not yield.
	pub fn roll_swap(&self, run_key: &str, secs: &[(u64, u32, u32)]) -> Result<Swapped> {
		let through = {
			let v = self.view();
			v.sealed_through.max(v.watermark)
		};
		// Looked at first, taken only once the directory is built: a meta that does not parse must
		// leave what it names in memory, not lose its meta and with it the chance to swap it.
		let mut items: Vec<(u64, u32, LiveMeta)> = Vec::with_capacity(secs.len());
		let mut missing = 0;
		{
			let live = self.live.lock().expect("live");
			for &(pos, off, _len) in secs {
				// What the sealed tier holds is read from it: nothing to swap.
				if pos <= through {
					continue;
				}
				match live.get(&pos) {
					Some(meta) => items.push((pos, off, meta.clone())),
					None => missing += 1,
				}
			}
		}
		if items.is_empty() {
			return Ok(Swapped { layers: 0, missing, freed: 0, bytes: 0 });
		}
		items.sort_by_key(|(pos, ..)| *pos);
		items.dedup_by_key(|(pos, ..)| *pos);
		let n = items.len();
		let mut dir = crate::ingest::RunDir::new(Arc::from(run_key), n);
		for (pos, off, (len, meta)) in &items {
			let m = surrealds_section::Meta::parse(meta, *len).map_err(|e| Error::Transaction(format!("run {run_key}: {e}")))?;
			dir.push(*pos, *off, &m);
		}
		{
			let mut live = self.live.lock().expect("live");
			for (pos, ..) in &items {
				live.remove(pos);
			}
		}
		let positions: std::collections::HashSet<u64> = items.iter().map(|i| i.0).collect();
		// The layer first: until the trim, a suffix entry and its layer say the same.
		self.add_layers(vec![Arc::new(crate::ingest::Layer::Run(dir))]);
		let bytes: u64 = items.iter().map(|(_, _, (len, _))| u64::from(*len)).sum();
		{
			let mut g = self.garbage.lock().expect("garbage");
			g.0.extend(positions);
			g.1 += bytes;
		}
		// A rebuild that fails leaves the swap done (the layers are in place and the entries wait):
		// the garbage is tried again at the next swap.
		let freed = if self.rebuild_wanted() { self.rebuild_if_allowed(true).unwrap_or(0) } else { 0 };
		Ok(Swapped { layers: n, missing, freed, bytes })
	}

	/// Start from a run's footer: register the sections of the run `run_key`, whose records are all
	/// sections and cover `first..=last`, as one layer, applying nothing into memory. Each entry is
	/// `(position, offset, length, meta)`, ascending by position. Positions the sealed tier holds
	/// are not registered. Like applying a frame: a run that starts after the next position needed
	/// is a [`FrameApplied::Gap`].
	pub fn layer_run(&self, run_key: &str, first: u64, last: u64, entries: &[(u64, u32, u32, &[u8])]) -> Result<FrameApplied> {
		let applied = self.applied();
		if last <= applied {
			return Ok(FrameApplied::Applied(applied));
		}
		if first > applied + 1 {
			return Ok(FrameApplied::Gap);
		}
		// The footer must list every position of the run, once and in order: `applied` moves to
		// `last`, so a position the footer omits would never be applied.
		let covers = last.checked_sub(first).is_some_and(|span| span.checked_add(1) == Some(entries.len() as u64))
			&& entries.iter().zip(first..).all(|(e, pos)| e.0 == pos);
		if !covers {
			return Err(Error::Transaction(format!("run {run_key}: the footer does not list exactly positions {first}..={last}")));
		}
		self.register_sections_above(run_key, entries, applied)?;
		self.applied.store(last, Ordering::Release);
		self.snap_after_apply(last, 0)?;
		Ok(FrameApplied::Applied(last))
	}

	/// Lay the sections a footer lists as one layer over the run `run_key`, applying nothing: their
	/// positions are applied already, as the section records a start left out of memory
	/// ([`Self::apply_frame_skipping`]) of a run that mixed them with other records. Positions the
	/// sealed tier holds are not registered. Each entry is `(position, offset, length, meta)`.
	pub fn register_sections(&self, run_key: &str, entries: &[(u64, u32, u32, &[u8])]) -> Result<()> {
		self.register_sections_above(run_key, entries, 0)
	}

	fn register_sections_above(&self, run_key: &str, entries: &[(u64, u32, u32, &[u8])], applied: u64) -> Result<()> {
		let v = self.view();
		let through = v.sealed_through.max(v.watermark).max(applied);
		let mut dir = crate::ingest::RunDir::new(Arc::from(run_key), entries.len());
		let mut prev = 0;
		for &(pos, off, len, meta) in entries {
			if pos <= prev {
				return Err(Error::Transaction(format!("run {run_key}: the footer's positions do not ascend")));
			}
			prev = pos;
			if pos <= through {
				continue;
			}
			let m = surrealds_section::Meta::parse(meta, len).map_err(|e| Error::Transaction(format!("{e}")))?;
			dir.push(pos, off, &m);
		}
		if !dir.is_empty() {
			let first = dir.first();
			let layer = Arc::new(crate::ingest::Layer::Run(dir));
			self.add_layers(vec![Arc::clone(&layer)]);
			// A snapshot taken at a position the layer covers (the frame that left the sections out was
			// applied before the layer was laid) is given the layer on top of its own set: the layer
			// holds nothing above the snapshot's position that it could show (reads filter by
			// position), and the snapshot's set keeps the layers its own view still needs.
			for snap in self.snapshots.lock().expect("snapshots lock").iter_mut().filter(|s| s.0 >= first) {
				let mut set: Vec<_> = snap.3.iter().cloned().collect();
				set.push(Arc::clone(&layer));
				set.sort_by_key(|l| l.first());
				snap.3 = Arc::new(set);
			}
		}
		Ok(())
	}

	/// One block of an ingested object or a run, read from the store.
	async fn ingested_block(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
		self.tier
			.get_range(key, start, len)
			.await
			.map_err(tier_err)?
			.ok_or_else(|| Error::Transaction(format!("ingested object {key} is missing")))
	}

	/// `key`'s write in the block `at` names, if it holds one: `Some(None)` for a delete.
	async fn ingested_get(&self, at: &crate::ingest::BlockAt, key: &[u8]) -> Result<Option<Option<Val>>> {
		let bytes = self.ingested_block(&at.key, at.start, at.len).await?;
		let found = surrealds_section::find_in_block(&bytes, key).map_err(|e| Error::Transaction(e.to_string()))?;
		Ok(found.map(|v| v.map(<[u8]>::to_vec)))
	}

	/// The objects a batch frame's pointer records name (see `pointer_write_set`), in order.
	pub fn frame_pointers(frame: &[u8]) -> Result<Vec<String>> {
		let batch = shale::format::decode_batch(frame)
			.map_err(|e| Error::Transaction(format!("bad batch frame: {e}")))?;
		let mut out = Vec::new();
		for (_, rec) in &batch.records {
			if let Some(name) = pointer_of(rec) {
				out.push(name);
			}
		}
		Ok(out)
	}

	/// [`Self::apply_frame`], with each pointer record's writes read from its object (`objects`,
	/// by name: the packed write set the writer put to R2) instead of the record's units, which
	/// are its keys only. Nothing is decoded into objects or re-encoded on the way.
	pub fn apply_frame_resolved(&self, frame: &[u8], cutoff: u64, objects: &[(String, Vec<u8>)]) -> Result<FrameApplied> {
		self.apply_frame_skipping(frame, cutoff, objects, &std::collections::HashSet::new())
	}

	/// [`Self::apply_frame_resolved`], leaving out of memory the section records at the positions in
	/// `skip`: the footer of the run they came from lists them, and [`Self::register_sections`] lays
	/// them as a layer once the run's other records are applied. (A start that replays a run which
	/// mixes sections with pointer and legacy records applies the others and layers the rest.)
	pub fn apply_frame_skipping(&self, frame: &[u8], cutoff: u64, objects: &[(String, Vec<u8>)], skip: &std::collections::HashSet<u64>) -> Result<FrameApplied> {
		let batch = shale::format::decode_batch(frame)
			.map_err(|e| Error::Transaction(format!("bad batch frame: {e}")))?;
		let Some(&(first, _)) = batch.records.first() else {
			return Ok(FrameApplied::Applied(self.applied()));
		};
		// Formed by an epoch fenced off before it could be acknowledged: not the log.
		if first >= cutoff {
			return Ok(FrameApplied::Fenced);
		}
		// A batch that straddles the cutoff holds the epoch's valid records and then fenced ones;
		// only the valid ones are the log.
		let last = batch.records.iter().rev().map(|(p, _)| *p).find(|p| *p < cutoff).unwrap_or(first);
		let applied = self.applied();
		if last <= applied {
			return Ok(FrameApplied::Applied(applied));
		}
		if first > applied + 1 {
			return Ok(FrameApplied::Gap);
		}
		// Every position above `applied` up to `last` must be present, in order: `applied` moves to
		// `last`, so a position the batch omits would never be applied.
		let mut expected = applied + 1;
		for (pos, _) in batch.records.iter().filter(|(p, _)| *p < cutoff && *p > applied) {
			if *pos != expected {
				return Err(Error::Transaction(format!("batch frame: position {pos} follows {}", expected - 1)));
			}
			expected += 1;
		}
		let mut writes: BTreeMap<&[u8], Vec<u8>> = BTreeMap::new();
		let mut ingest_now: Vec<Arc<crate::ingest::Layer>> = Vec::new();
		let mut live_now: Vec<(u64, LiveMeta)> = Vec::new();
		let mut skipped_bytes = 0usize;
		for (pos, rec) in batch.records.iter().filter(|(p, _)| *p < cutoff) {
			if *pos <= applied {
				continue;
			}
			// Laid as a layer by its run's footer: nothing of it goes into memory.
			if skip.contains(pos) {
				if let Some(section) = section_of(rec) {
					skipped_bytes += section.len();
					continue;
				}
			}
			// A commit that came as a section: applied into memory at once, so it is visible, and
			// its meta kept to swap the memory for a layer when its run is rolled.
			if let Some(section) = section_of(rec) {
				let ml = surrealds_section::meta_len(section).filter(|ml| *ml <= section.len()).ok_or_else(|| Error::Transaction("a section record has no trailer".into()))?;
				for (k, v) in surrealds_section::entries(section).map_err(|e| Error::Transaction(e.to_string()))? {
					match v {
						Some(v) => writes.insert(k, sfx_set(*pos, v)),
						None => writes.insert(k, sfx_del(*pos)),
					};
				}
				live_now.push((*pos, (section.len() as u32, section[section.len() - ml..].into())));
				continue;
			}
			if let Some(name) = pointer_of(rec) {
				let packed = objects
					.iter()
					.find(|(n, _)| *n == name)
					.map(|(_, b)| b.as_slice())
					.ok_or_else(|| Error::Transaction(format!("pointer object {name} was not supplied")))?;
				// An object with metadata (`key \0 meta_start \0 meta_len`): `packed` is its
				// metadata, and the object becomes a layer at this position, read in place.
				if let Some((key, meta_start)) = parse_pointer(&name) {
					let ing = crate::ingest::Ingested::parse(*pos, key, meta_start, packed)
						.map_err(|e| Error::Transaction(format!("{e:#}")))?;
					ingest_now.push(Arc::new(crate::ingest::Layer::Object(ing)));
					continue;
				}
				for (k, v) in unpack(packed)? {
					match v {
						Some(v) => writes.insert(k, sfx_set(*pos, v)),
						None => writes.insert(k, sfx_del(*pos)),
					};
				}
				continue;
			}
			for u in &rec.units {
				match u {
					shale::format::Unit::Set { key, value } => {
						writes.insert(key.0.as_slice(), sfx_set(*pos, &value.0));
					}
					shale::format::Unit::Del { key } => {
						writes.insert(key.0.as_slice(), sfx_del(*pos));
					}
					_ => {}
				}
			}
		}
		sync(
			async {
				let tx = self.suffix().transaction(TransactionType::Write).await?;
				for (k, v) in writes {
					tx.set(Key::from(k.to_vec()), v).await?;
				}
				tx.commit().await
			},
			"replica apply frame",
		)?;
		if !ingest_now.is_empty() {
			self.add_layers(ingest_now);
		}
		if !live_now.is_empty() {
			self.live.lock().expect("live").extend(live_now);
		}
		let held = frame.len().saturating_sub(skipped_bytes) as u64;
		self.suffix_bytes.fetch_add(held, Ordering::Relaxed);
		self.applied.store(last, Ordering::Release);
		self.snap_after_apply(last, held)?;
		Ok(FrameApplied::Applied(last))
	}

	/// How many keys the suffix holds.
	pub fn suffix_len(&self) -> Result<usize> {
		sync(
			async {
				let tx = self.suffix().transaction(TransactionType::Read).await?;
				let mut n = 0;
				let mut next = Some(KeyRange::from(vec![]..vec![0xff; 512]));
				while let Some(rng) = next {
					let batch = tx.batch_keys_vals(rng, 10_000, None).await?;
					next = batch.next;
					n += batch.result.len();
				}
				tx.cancel().await?;
				Ok(n)
			},
			"suffix count",
		)
	}

	/// Keep a snapshot of the last `n` positions applied (0 keeps none).
	pub fn keep_snapshots(&self, n: usize) {
		self.keep.store(n, Ordering::Relaxed);
		self.trim_snapshots(n);
		let empty = self.snapshots.lock().expect("snapshots lock").is_empty();
		if n > 0 && empty {
			let _ = self.snap_after_apply(self.applied(), 0);
		}
	}

	/// [`Self::keep_snapshots`], and the held snapshots together pin at most `bytes` of applied log
	/// (the newest is always kept): a snapshot keeps every suffix entry applied before it, so
	/// without a bound the commits a roll swapped for layers stay in memory until `n` more
	/// positions have been applied, which under bulk writes is most of a hundred batches (the
	/// local smoke's preload ran the engine's heap budget out that way). A pinned query older than
	/// what is kept finds its snapshot gone, as it always has past `n`.
	pub fn keep_snapshots_within(&self, n: usize, bytes: u64) {
		self.snapshot_bytes.store(bytes, Ordering::Relaxed);
		self.keep_snapshots(n);
	}

	fn trim_snapshots(&self, n: usize) {
		let budget = self.snapshot_bytes.load(Ordering::Relaxed);
		let mut s = self.snapshots.lock().expect("snapshots lock");
		let mut pinned: u64 = s.iter().map(|(.., b)| *b).sum();
		while s.len() > n || (budget > 0 && pinned > budget && s.len() > 1) {
			if let Some((_, tx, _, _, b)) = s.pop_front() {
				pinned -= b;
				let _ = now(tx.cancel());
			}
		}
		drop(s);
	}

	/// The oldest and newest positions a pinned query can read.
	pub fn snapshot_range(&self) -> Option<(u64, u64)> {
		let s = self.snapshots.lock().expect("snapshots lock");
		Some((s.front()?.0, s.back()?.0))
	}

	fn snapshot(&self, pos: u64) -> Option<(Arc<dyn Transactable>, View, IngestedSet)> {
		let s = self.snapshots.lock().expect("snapshots lock");
		s.iter().find(|(p, ..)| *p == pos).map(|(_, tx, v, i, _)| (Arc::clone(tx), *v, Arc::clone(i)))
	}

	fn snap_after_apply(&self, pos: u64, bytes: u64) -> Result<()> {
		let keep = self.keep.load(Ordering::Relaxed);
		if keep == 0 {
			return Ok(());
		}
		let tx = sync(self.suffix().transaction(TransactionType::Read), "snapshot open")?;
		let view = self.view();
		let ingested = self.ingested();
		self.snapshots.lock().expect("snapshots lock").push_back((pos, Arc::from(tx), view, ingested, bytes));
		self.trim_snapshots(keep);
		Ok(())
	}
}

/// What [`Replica::apply_frame`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameApplied {
	/// Applied (or already applied) through this position.
	Applied(u64),
	/// The batch starts after the next position needed: nothing applied.
	Gap,
	/// The batch belongs to a fenced epoch: not the log.
	Fenced,
}

/// What [`Replica::roll_swap`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Swapped {
	/// Sections now read in place.
	pub layers: usize,
	/// Sections named that this replica holds no meta for: already swapped (a notice that came
	/// twice), applied without one, or sealed meanwhile. Nothing to do for them: they are in memory
	/// until a seal or already read in place.
	pub missing: usize,
	/// Entries freed from memory now (a swap that leaves its garbage for a later rebuild frees none).
	pub freed: usize,
	/// The bytes of the commits swapped for layers: what a node stops counting as held in memory.
	pub bytes: u64,
}

/// The section a record carries, if it is a section record: one reserved unit (`ff ff "sec" 00`)
/// whose value is the section.
fn section_of(rec: &shale::format::Record) -> Option<&[u8]> {
	rec.units.iter().find_map(|u| match u {
		shale::format::Unit::Set { key, value } if key.0 == surrealds_section::SEC_KEY => Some(value.0.as_slice()),
		_ => None,
	})
}

/// Append one write to a packed write set: `klen:u32 key vlen:u32 value`,
/// `vlen = u32::MAX` for a deletion.
#[cfg(test)]
fn push(out: &mut Vec<u8>, k: &[u8], v: Option<&[u8]>) {
	out.extend_from_slice(&(k.len() as u32).to_le_bytes());
	out.extend_from_slice(k);
	match v {
		Some(v) => {
			out.extend_from_slice(&(v.len() as u32).to_le_bytes());
			out.extend_from_slice(v);
		}
		None => out.extend_from_slice(&DELETE.to_le_bytes()),
	}
}

/// A section's packed write set: its entries, without the meta after them. A host that handles
/// a commit as a plain packed write set (the TypeScript node: messages and objects sized by length)
/// takes this.
pub fn packed_prefix(section: &[u8]) -> &[u8] {
	match surrealds_section::meta_len(section) {
		Some(m) if m <= section.len() => &section[..section.len() - m],
		_ => section,
	}
}

/// The reserved key prefix of a pointer unit: `ff ff "ptr" 00` (no SurrealDB key starts with
/// 0xff), the object's name after it. The same convention as cf-experiments' `src/pointer.ts`.
const PTR_PREFIX: &[u8] = &[0xff, 0xff, b'p', b't', b'r', 0];

/// The object a record points at, if it is a pointer record.
fn pointer_of(rec: &shale::format::Record) -> Option<String> {
	rec.units.iter().find_map(|u| match u {
		shale::format::Unit::Set { key, value } if key.0.starts_with(PTR_PREFIX) => {
			std::str::from_utf8(&value.0).ok().map(str::to_owned)
		}
		_ => None,
	})
}

/// A pointer's object key and the offset of its metadata, when it names an object with
/// metadata (`key \0 meta_start \0 meta_len`); `None` for an object without (read whole).
pub fn parse_pointer(name: &str) -> Option<(String, u32)> {
	let mut parts = name.split('\0');
	let key = parts.next()?;
	let start = parts.next()?.parse().ok()?;
	Some((key.to_owned(), start))
}

/// The write set a pointer commit sends for `packed` (put to R2 as `object`): its keys with
/// empty values, which the sequencer checks for conflicts, and one reserved unit naming the
/// object. Built here rather than in the host, which made an object per write.
pub fn pointer_write_set(packed: &[u8], object: &str) -> Result<Vec<u8>> {
	let writes = unpack(packed)?;
	let mut ptr = PTR_PREFIX.to_vec();
	ptr.extend_from_slice(object.as_bytes());
	let mut out = Vec::with_capacity(4 + writes.iter().map(|(k, _)| 8 + k.len()).sum::<usize>() + 8 + ptr.len() * 2);
	out.extend_from_slice(&((writes.len() + 1) as u32).to_le_bytes());
	for (k, _) in &writes {
		out.extend_from_slice(&(k.len() as u32).to_le_bytes());
		out.extend_from_slice(k);
		out.extend_from_slice(&0u32.to_le_bytes());
	}
	out.extend_from_slice(&(ptr.len() as u32).to_le_bytes());
	out.extend_from_slice(&ptr);
	out.extend_from_slice(&(object.len() as u32).to_le_bytes());
	out.extend_from_slice(object.as_bytes());
	Ok(out)
}

/// Read a packed write set: `n:u32` then `n` writes as [`push`] writes them.
pub fn unpack(b: &[u8]) -> Result<Vec<(&[u8], Option<&[u8]>)>> {
	let bad = || Error::Transaction("malformed write set".into());
	let mut off = 0usize;
	let u32_at = |off: &mut usize| -> Result<u32> {
		let end = off.checked_add(4).ok_or_else(bad)?;
		let s = b.get(*off..end).ok_or_else(bad)?;
		*off = end;
		Ok(u32::from_le_bytes(s.try_into().map_err(|_| bad())?))
	};
	let n = u32_at(&mut off)? as usize;
	// A write holds at least its two length words, so a count the input cannot hold is malformed.
	if n > (b.len() - off) / 8 {
		return Err(bad());
	}
	let mut out = Vec::with_capacity(n);
	for _ in 0..n {
		let kl = u32_at(&mut off)? as usize;
		let end = off.checked_add(kl).ok_or_else(bad)?;
		let k = b.get(off..end).ok_or_else(bad)?;
		off = end;
		let vl = u32_at(&mut off)?;
		let v = if vl == DELETE {
			None
		} else {
			let end = off.checked_add(vl as usize).ok_or_else(bad)?;
			let v = b.get(off..end).ok_or_else(bad)?;
			off = end;
			Some(v)
		};
		out.push((k, v));
	}
	Ok(out)
}

// ---- merged reads --------------------------------------------------------------

/// What a read sees: a suffix layer (a transaction's own, or a held snapshot) at position `at`,
/// over the tier as of `view`.
struct Layers<'a> {
	suffix: &'a dyn Transactable,
	view: View,
	at: u64,
	replica: &'a Replica,
	/// Layers visible here (those with positions at or below `at`), oldest first.
	ingested: &'a [Arc<crate::ingest::Layer>],
}

/// Suffix entries a merged scan reads at a time, and tier entries it asks for at a time.
const SUFFIX_BATCH: u32 = 1000;
const TIER_BATCH: usize = 512;

impl Layers<'_> {
	async fn get(&self, key: &[u8]) -> Result<Option<Val>> {
		let entry = self.suffix.get(Key::from(key), None).await?;
		// A layer's commit newer than the suffix's write wins, the newest of them: layers are ordered
		// by their first position but a run's layer can span a pointer commit's own layer, so the
		// candidates of every visible layer are put in position order before any is read.
		let newer_than = entry.as_deref().map_or(0, sfx_pos);
		let h = surrealds_section::hash(key);
		let mut blocks = Vec::new();
		for layer in self.ingested.iter().rev() {
			if layer.first() > self.at || layer.last() <= newer_than {
				continue;
			}
			layer.probe(key, h, newer_than, self.at, &mut blocks);
		}
		blocks.sort_by(|a, b| b.pos.cmp(&a.pos));
		for at in blocks {
			if let Some(w) = self.replica.ingested_get(&at, key).await? {
				return Ok(w);
			}
		}
		if let Some(e) = entry {
			return Ok(sfx_decode(&e).flatten().map(<[u8]>::to_vec));
		}
		self.replica.tier.get(&self.view, self.at, key).await.map_err(tier_err)
	}

	/// Live entries of `rng` in `dir` order, newest layer winning: at most `limit` (0 for no
	/// limit) after skipping `skip`.
	async fn scan(&self, rng: KeyRange<'_>, limit: u32, skip: u32, dir: Direction) -> Result<Vec<(Vec<u8>, Val)>> {
		let mut m = Merge::new(rng, dir, skip, TIER_BATCH, TIER_BATCH);
		let mut out = Vec::new();
		while limit == 0 || out.len() < limit as usize {
			match m.next(self).await? {
				Some(row) => out.push(row),
				None => break,
			}
		}
		Ok(out)
	}
}

/// A merged scan of the suffix over the tier, resumable: each layer is read a batch at a time and
/// continues past the last key it gave, the suffix's entry winning where both hold a key. A
/// one-shot scan drives it once; a cursor keeps it across the engine's batches, so a long scan
/// opens far fewer tree cursors (each with its own read-ahead) than one per batch.
struct Merge<'a> {
	forward: bool,
	s_lo: Vec<u8>,
	s_hi: Vec<u8>,
	s_buf: std::collections::VecDeque<(Vec<u8>, Vec<u8>)>,
	s_done: bool,
	t_lo: Vec<u8>,
	t_hi: Vec<u8>,
	t_buf: std::collections::VecDeque<(Vec<u8>, Vec<u8>)>,
	t_done: bool,
	/// Tier rows asked for next, doubled after each read up to `t_max`: a short scan reads little,
	/// a long one reads in chunks large enough for the tree cursor's prefetch to run ahead.
	t_batch: usize,
	t_max: usize,
	skip: u32,
	/// The next tier batch, already started (see [`Merge::start_next`]), and the rows it asks for.
	t_next: Option<(usize, Started<'a>)>,
	/// The merge's whole range, which never moves: `t_lo` and `t_hi` advance with the tier's
	/// batches, and the ingested objects (opened after the first of them) read from the start.
	r_lo: Vec<u8>,
	r_hi: Vec<u8>,
	/// Ingested objects' writes in range, one cursor each (opened on the first read).
	i_cursors: Option<Vec<IngestCursor>>,
	/// The cursors' next keys, in the merge's direction: with a section per commit there can be
	/// thousands of cursors, so the next key is not looked for in each of them.
	i_heap: std::collections::BinaryHeap<Head>,
}

/// A cursor's next key in the heap: the smallest first going forward, the largest going backward.
struct Head {
	key: Vec<u8>,
	idx: usize,
	forward: bool,
}

impl PartialEq for Head {
	fn eq(&self, o: &Self) -> bool {
		self.key == o.key && self.idx == o.idx
	}
}
impl Eq for Head {}
impl PartialOrd for Head {
	fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(o))
	}
}
impl Ord for Head {
	fn cmp(&self, o: &Self) -> std::cmp::Ordering {
		let c = self.key.cmp(&o.key).then(self.idx.cmp(&o.idx));
		if self.forward { c.reverse() } else { c }
	}
}

/// A merge's read of one ingested object: its blocks in range, in the merge's direction, a block
/// at a time.
struct IngestCursor {
	/// The commit's position, and the object its blocks are in.
	pos: u64,
	key: Arc<str>,
	/// The merge's whole range: the tier's bounds move ahead of what has been merged.
	lo: Vec<u8>,
	hi: Vec<u8>,
	/// The blocks left to read: (start, length) in the object.
	blocks: std::collections::VecDeque<(u64, u64)>,
	buf: std::collections::VecDeque<(Vec<u8>, Option<Vec<u8>>)>,
}

/// A started [`TierScan`], held across the engine's batches in a cursor the storage traits want
/// `Send + Sync`: as with [`AssertSend`], there is one thread, and nothing is shared.
struct Started<'a>(TierScan<'a>);

// SAFETY: see `AssertSend`; this crate only builds for a single-threaded wasm host.
unsafe impl Send for Started<'_> {}
// SAFETY: as above; and the future is only ever reached through `&mut`.
unsafe impl Sync for Started<'_> {}

/// A tier batch being read.
type TierScan<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<(Vec<u8>, Vec<u8>)>>> + 'a>>;

/// The tier batch size from which a merge reads the next batch while the engine works through the
/// current one: by then the scan has read thousands of rows and is likely to want more, so the
/// read ahead is rarely wasted (a `LIMIT`ed or point scan never gets this far).
const READ_AHEAD_FROM: usize = 1024;

impl<'a> Merge<'a> {
	fn new(rng: KeyRange<'_>, dir: Direction, skip: u32, t_first: usize, t_max: usize) -> Self {
		let (lo, hi) = (rng.start.as_slice().to_vec(), rng.end.as_slice().to_vec());
		Merge {
			forward: matches!(dir, Direction::Forward),
			r_lo: lo.clone(),
			r_hi: hi.clone(),
			s_lo: lo.clone(),
			s_hi: hi.clone(),
			s_buf: Default::default(),
			s_done: false,
			t_lo: lo,
			t_hi: hi,
			t_buf: Default::default(),
			t_done: false,
			t_batch: t_first,
			t_max,
			skip,
			t_next: None,
			i_cursors: None,
			i_heap: Default::default(),
		}
	}

	/// The tier batch after the one just read, as a future. Polled once here, which issues its
	/// object reads: they proceed on the host while the engine processes the current batch (the
	/// isolate's network I/O does not wait for the wasm to yield), so the next await finds them
	/// done or nearly, rather than starting them then.
	fn start_next(&mut self, layers: &Layers<'a>) {
		let tier = &layers.replica.tier;
		let (view, at, want) = (layers.view, layers.at, self.t_batch);
		let (lo, hi) = (self.t_lo.clone(), self.t_hi.clone());
		let tdir = if self.forward { TierDirection::Forward } else { TierDirection::Reverse };
		let mut fut: TierScan<'a> =
			Box::pin(async move { tier.scan(&view, at, &lo, &hi, want, tdir).await.map_err(tier_err) });
		let waker = futures::task::noop_waker();
		let mut cx = std::task::Context::from_waker(&waker);
		// Ready already (all cached): kept as a finished future, taken at the next read.
		let polled = fut.as_mut().poll(&mut cx);
		let fut: TierScan<'a> = match polled {
			std::task::Poll::Ready(r) => Box::pin(std::future::ready(r)),
			std::task::Poll::Pending => fut,
		};
		self.t_next = Some((want, Started(fut)));
	}

	/// A cursor per commit in the layers visible to `layers` that can hold keys in this merge's
	/// range (a run's directory says which of its sections).
	fn open_ingested(&self, layers: &Layers<'a>) -> Vec<IngestCursor> {
		let mut srcs = Vec::new();
		for layer in layers.ingested.iter().filter(|l| l.first() <= layers.at) {
			layer.scan_srcs(&self.r_lo, &self.r_hi, layers.at, &mut srcs);
		}
		srcs.into_iter()
			.filter(|s| !s.blocks.is_empty())
			.map(|s| {
				let mut blocks: std::collections::VecDeque<(u64, u64)> = s.blocks.into();
				if !self.forward {
					blocks = blocks.into_iter().rev().collect();
				}
				IngestCursor { pos: s.pos, key: s.key, lo: self.r_lo.clone(), hi: self.r_hi.clone(), blocks, buf: Default::default() }
			})
			.collect()
	}

	/// The next live entry, or `None` at the end of the range.
	async fn next(&mut self, layers: &Layers<'a>) -> Result<Option<(Vec<u8>, Val)>> {
		let forward = self.forward;
		let before = |a: &[u8], b: &[u8]| if forward { a < b } else { a > b };
		loop {
			if self.s_buf.is_empty() && !self.s_done {
				let r = KeyRange::from(self.s_lo.clone()..self.s_hi.clone());
				let batch = if forward {
					layers.suffix.scan(r, SUFFIX_BATCH, 0, None).await?.values
				} else {
					layers.suffix.scanr(r, SUFFIX_BATCH, 0, None).await?.values
				};
				self.s_done = (batch.len() as u32) < SUFFIX_BATCH;
				if let Some((last, _)) = batch.last() {
					if forward {
						self.s_lo = last.clone();
						self.s_lo.push(0);
					} else {
						self.s_hi = last.clone();
					}
				}
				self.s_buf.extend(batch);
			}
			if self.t_buf.is_empty() && !self.t_done {
				let (want, batch) = match self.t_next.take() {
					Some((want, Started(started))) => (want, started.await?),
					None => {
						let tdir = if forward { TierDirection::Forward } else { TierDirection::Reverse };
						let want = self.t_batch;
						let batch = layers
							.replica
							.tier
							.scan(&layers.view, layers.at, &self.t_lo, &self.t_hi, want, tdir)
							.await
							.map_err(tier_err)?;
						(want, batch)
					}
				};
				self.t_done = batch.len() < want;
				self.t_batch = (want * 2).min(self.t_max);
				if let Some((last, _)) = batch.last() {
					if forward {
						self.t_lo = last.clone();
						self.t_lo.push(0);
					} else {
						self.t_hi = last.clone();
					}
				}
				self.t_buf.extend(batch);
				if !self.t_done && want >= READ_AHEAD_FROM {
					self.start_next(layers);
				}
			}
			if self.i_cursors.is_none() {
				let mut cursors = self.open_ingested(layers);
				// The cursors' first blocks are independent reads: one round trip, not one per cursor.
				futures::future::try_join_all(cursors.iter_mut().map(|c| c.refill(layers, forward))).await?;
				for (idx, c) in cursors.iter().enumerate() {
					if let Some((k, _)) = c.buf.front() {
						self.i_heap.push(Head { key: k.clone(), idx, forward });
					}
				}
				self.i_cursors = Some(cursors);
			}
			// The next key in order among every layer's head.
			let mut next: Option<&Vec<u8>> = None;
			let heads = self.s_buf.front().map(|(k, _)| k).into_iter().chain(self.t_buf.front().map(|(k, _)| k)).chain(self.i_heap.peek().map(|h| &h.key));
			for k in heads {
				if next.is_none_or(|n| before(k, n)) {
					next = Some(k);
				}
			}
			let Some(k) = next.cloned() else {
				return Ok(None);
			};
			// Every layer's write of that key, the newest winning: the suffix's by its position,
			// an ingested object's at its own, the tier's below both.
			let suffix = if self.s_buf.front().is_some_and(|(sk, _)| *sk == k) {
				self.s_buf.pop_front().map(|(_, e)| e)
			} else {
				None
			};
			let tier = if self.t_buf.front().is_some_and(|(tk, _)| *tk == k) {
				self.t_buf.pop_front().map(|(_, v)| v)
			} else {
				None
			};
			let mut ingested: Option<(u64, Option<Vec<u8>>)> = None;
			let mut taken = Vec::new();
			while self.i_heap.peek().is_some_and(|h| h.key == k) {
				taken.push(self.i_heap.pop().expect("peeked").idx);
			}
			let cursors = self.i_cursors.as_mut().expect("opened");
			for &idx in &taken {
				let c = &mut cursors[idx];
				let (_, v) = c.buf.pop_front().expect("a head");
				if ingested.as_ref().is_none_or(|(p, _)| c.pos > *p) {
					ingested = Some((c.pos, v));
				}
			}
			// Each cursor that gave its head reads on (a block at a time) and offers its next.
			for idx in taken {
				let c = &mut cursors[idx];
				c.refill(layers, forward).await?;
				if let Some((nk, _)) = c.buf.front() {
					self.i_heap.push(Head { key: nk.clone(), idx, forward });
				}
			}
			let entry = match (suffix, ingested) {
				(Some(e), Some((p, _))) if sfx_pos(&e) > p => sfx_decode(&e).flatten().map(|v| (k, v.to_vec())),
				(_, Some((_, v))) => v.map(|v| (k, v)),
				(Some(e), None) => sfx_decode(&e).flatten().map(|v| (k, v.to_vec())),
				(None, None) => tier.map(|v| (k, v)),
			};
			let Some(entry) = entry else { continue };
			if self.skip > 0 {
				self.skip -= 1;
				continue;
			}
			return Ok(Some(entry));
		}
	}
}

impl IngestCursor {
	/// Read blocks until one holds writes in `[lo, hi)` or none are left.
	async fn refill(&mut self, layers: &Layers<'_>, forward: bool) -> Result<()> {
		while self.buf.is_empty() {
			let Some((start, len)) = self.blocks.pop_front() else {
				return Ok(());
			};
			let bytes = layers.replica.ingested_block(&self.key, start, len).await?;
			let mut writes: Vec<(Vec<u8>, Option<Vec<u8>>)> = crate::ingest::block_writes(&bytes)
				.map_err(|e| Error::Transaction(format!("{e:#}")))?
				.into_iter()
				.filter(|(k, _)| *k >= self.lo.as_slice() && *k < self.hi.as_slice())
				.map(|(k, v)| (k.to_vec(), v.map(<[u8]>::to_vec)))
				.collect();
			if !forward {
				writes.reverse();
			}
			self.buf.extend(writes);
		}
		Ok(())
	}
}

/// Tier rows a cursor reads at first and at most per read (see [`Merge::t_batch`]).
const CURSOR_TIER_FIRST: usize = 512;
const CURSOR_TIER_MAX: usize = 8192;

/// The engine's stateful scan cursor over the layers ([`Transactable::open_vals_cursor`] and
/// `open_keys_cursor`): one [`Merge`] kept for the whole walk.
struct LayersCursor<'a> {
	layers: Layers<'a>,
	merge: Merge<'a>,
	key_buf: Vec<u8>,
	val_buf: Vec<u8>,
	vspans: Vec<surrealdb_kvs::api::KeyValSpan>,
	kspans: Vec<surrealdb_kvs::api::KeySpan>,
}

impl<'a> LayersCursor<'a> {
	fn new(layers: Layers<'a>, rng: KeyRange<'_>, dir: Direction, skip: u32) -> Self {
		LayersCursor {
			layers,
			merge: Merge::new(rng, dir, skip, CURSOR_TIER_FIRST, CURSOR_TIER_MAX),
			key_buf: Vec::new(),
			val_buf: Vec::new(),
			vspans: Vec::new(),
			kspans: Vec::new(),
		}
	}

	/// Fill the buffers with up to `limit` rows (values too when `vals`); (key bytes, value bytes).
	async fn fill(&mut self, limit: u32, vals: bool) -> Result<(u64, u64)> {
		self.key_buf.clear();
		self.val_buf.clear();
		self.vspans.clear();
		self.kspans.clear();
		let (mut kb, mut vb) = (0u64, 0u64);
		for _ in 0..limit {
			let Some((k, v)) = self.merge.next(&self.layers).await? else { break };
			let key_offset = self.key_buf.len();
			self.key_buf.extend_from_slice(&k);
			kb += k.len() as u64;
			if vals {
				let val_offset = self.val_buf.len();
				self.val_buf.extend_from_slice(&v);
				vb += v.len() as u64;
				self.vspans.push(surrealdb_kvs::api::KeyValSpan { key_offset, key_len: k.len(), val_offset, val_len: v.len() });
			} else {
				self.kspans.push(surrealdb_kvs::api::KeySpan { offset: key_offset, len: k.len() });
			}
		}
		Ok((kb, vb))
	}
}

impl surrealdb_kvs::api::ScanCursorVals for LayersCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<surrealdb_kvs::api::ValsBatch<'s>>> {
		Box::pin(AssertSend(async move {
			let (kb, vb) = self.fill(limit, true).await?;
			Ok(surrealdb_kvs::api::ValsBatch::from_parts(&self.key_buf, &self.val_buf, &self.vspans, kb, vb))
		}))
	}

	fn for_each<'s>(&'s mut self, limit: u32, f: &'s mut dyn surrealdb_kvs::api::ValVisitor) -> BoxFut<'s, Result<surrealdb_kvs::api::ScanChunkStats>> {
		Box::pin(AssertSend(async move {
			let mut stats = surrealdb_kvs::api::ScanChunkStats::default();
			while stats.rows < u64::from(limit) {
				let Some((k, v)) = self.merge.next(&self.layers).await? else { break };
				let flow = f(&k, &v)?;
				stats.rows += 1;
				stats.key_bytes += k.len() as u64;
				stats.value_bytes += v.len() as u64;
				if flow.is_break() {
					break;
				}
			}
			Ok(stats)
		}))
	}
}

impl surrealdb_kvs::api::ScanCursorKeys for LayersCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<surrealdb_kvs::api::KeysBatch<'s>>> {
		Box::pin(AssertSend(async move {
			let (kb, _) = self.fill(limit, false).await?;
			Ok(surrealdb_kvs::api::KeysBatch::from_parts(&self.key_buf, &self.kspans, kb))
		}))
	}

	fn for_each<'s>(&'s mut self, limit: u32, f: &'s mut dyn surrealdb_kvs::api::KeyVisitor) -> BoxFut<'s, Result<surrealdb_kvs::api::ScanChunkStats>> {
		Box::pin(AssertSend(async move {
			let mut stats = surrealdb_kvs::api::ScanChunkStats::default();
			while stats.rows < u64::from(limit) {
				let Some((k, _)) = self.merge.next(&self.layers).await? else { break };
				let flow = f(&k)?;
				stats.rows += 1;
				stats.key_bytes += k.len() as u64;
				if flow.is_break() {
					break;
				}
			}
			Ok(stats)
		}))
	}
}

fn keys_result(rows: Vec<(Vec<u8>, Val)>) -> KeysResult {
	let key_bytes = rows.iter().map(|(k, _)| k.len() as u64).sum();
	KeysResult {
		keys: rows.into_iter().map(|(k, _)| k).collect(),
		key_bytes,
	}
}

fn scan_result(rows: Vec<(Vec<u8>, Val)>) -> ScanResult {
	let key_bytes = rows.iter().map(|(k, _)| k.len() as u64).sum();
	let value_bytes = rows.iter().map(|(_, v)| v.len() as u64).sum();
	ScanResult {
		values: rows,
		key_bytes,
		value_bytes,
	}
}

// ---- the backend ---------------------------------------------------------------

/// Creates transactions over the replica that commit through the hook.
pub struct LogBackend {
	pub replica: Arc<Replica>,
	pub hook: Arc<CommitHook>,
}

impl TransactionBuilder for LogBackend {
	fn name(&self) -> &'static str {
		"cfedge"
	}

	fn new_transaction(&self, write: TransactionType) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(AssertSend(async move {
			// A read inside a pinned query reads the held snapshot of its position.
			if let Some(pos) = PIN.with(|p| p.get())
				&& matches!(write, TransactionType::Read)
			{
				let (suffix, view, ingested) = self
					.replica
					.snapshot(pos)
					.ok_or_else(|| Error::Transaction(format!("snapshot {pos} is no longer held")))?;
				let tx: Box<dyn Transactable> = Box::new(SnapshotTransaction {
					suffix,
					view,
					at: pos,
					replica: Arc::clone(&self.replica),
					ingested,
				});
				return Ok((tx, true));
			}
			// The snapshot position is read before the suffix's transaction takes its snapshot.
			// An apply landing in between makes the recorded position older than the data read,
			// which can only refuse a commit that would have passed, never accept one that
			// should not.
			let snap = self.replica.applied();
			let view = self.replica.view();
			let ingested = self.replica.ingested();
			// Always a write transaction on the suffix: the transaction's own writes go there as
			// suffix entries, so a delete can hide what the tier holds.
			let inner = self.replica.suffix().transaction(TransactionType::Write).await?;
			let tx: Box<dyn Transactable> = Box::new(LogTransaction {
				inner,
				view,
				snap,
				write: matches!(write, TransactionType::Write),
				hook: Arc::clone(&self.hook),
				replica: Arc::clone(&self.replica),
				writes: Mutex::new(Writes::default()),
				ingested,
			});
			Ok((tx, true))
		}))
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn register_metrics(&self) -> Option<Metrics> {
		None
	}

	fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
		None
	}
}

/// A transaction's writes in the order made, with the length at each open
/// savepoint so a rollback can drop the writes made since.
#[derive(Default)]
struct Writes {
	ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
	savepoints: Vec<usize>,
}

struct LogTransaction {
	/// The suffix as of `snap`, plus this transaction's own writes. Never committed: accepted
	/// writes reach the suffix by applying the log.
	inner: Box<dyn Transactable>,
	view: View,
	snap: u64,
	write: bool,
	hook: Arc<CommitHook>,
	replica: Arc<Replica>,
	writes: Mutex<Writes>,
	ingested: IngestedSet,
}

impl LogTransaction {
	fn layers(&self) -> Layers<'_> {
		Layers {
			suffix: self.inner.as_ref(),
			view: self.view,
			at: self.snap,
			replica: &self.replica,
			ingested: &self.ingested,
		}
	}

	fn check_write(&self) -> Result<()> {
		if self.inner.closed() {
			return Err(Error::TransactionFinished);
		}
		if !self.write {
			return Err(Error::TransactionReadonly);
		}
		Ok(())
	}

	async fn write(&self, key: &[u8], val: Option<Val>) -> Result<()> {
		self.check_write()?;
		// A write set that would run the node out of memory fails here, with a reason, rather
		// than ending the object and every client on it.
		if crate::heap_live() > crate::heap_budget() {
			return Err(Error::Transaction(crate::over_budget_message()));
		}
		let e = match &val {
			Some(v) => sfx_set(UNSEQUENCED, v),
			None => sfx_del(UNSEQUENCED),
		};
		self.inner.set(Key::from(key), e).await?;
		self.writes.lock().expect("writes lock").ops.push((key.to_vec(), val));
		Ok(())
	}
}

impl Transactable for LogTransaction {
	fn kind(&self) -> &'static str {
		"cfedge"
	}

	fn closed(&self) -> bool {
		self.inner.closed()
	}

	fn writeable(&self) -> bool {
		self.write
	}

	fn cancel<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		self.inner.cancel()
	}

	fn commit<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move {
			if self.inner.closed() {
				return Err(Error::TransactionFinished);
			}
			// The last write to each key wins.
			let ops: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
				std::mem::take(&mut self.writes.lock().expect("writes lock").ops).into_iter().collect();
			self.inner.cancel().await?;
			if ops.is_empty() {
				return Ok(());
			}
			// One loop packs the write set (sorted: it is a BTreeMap) and makes it a section, with
			// the hash of each key: no second pass over the writes.
			let bytes: usize = ops.iter().map(|(k, v)| 8 + k.len() + v.as_ref().map_or(0, Vec::len)).sum();
			let mut section = surrealds_section::Builder::with_capacity(ops.len(), bytes);
			for (k, v) in &ops {
				section.push(k, v.as_deref()).map_err(|e| Error::Transaction(e.to_string()))?;
			}
			drop(ops);
			let built = section.finish();
			match (self.hook.0)(self.snap, built.bytes, built.hashes).await {
				Ok(()) => Ok(()),
				Err(CommitError::Conflict(msg)) => Err(Error::TransactionConflict(msg)),
				Err(CommitError::Other(msg)) => Err(Error::Transaction(msg)),
			}
		}))
	}

	fn exists<'a>(&'a self, key: Key<'a>, _version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(AssertSend(async move { Ok(self.layers().get(&key).await?.is_some()) }))
	}

	fn get<'a>(&'a self, key: Key<'a>, _version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(AssertSend(async move { self.layers().get(&key).await }))
	}

	/// A locked read joins the commit's write set with the value it read, which the sequencer
	/// screens like any write: the commit is refused if anyone else wrote the key after our
	/// snapshot, which is what holding the lock would have prevented. Rewriting the value read
	/// leaves the stored state unchanged. A read-only transaction has nothing to commit and reads
	/// plainly.
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(AssertSend(async move {
			let val = self.layers().get(&key).await?;
			if self.check_write().is_ok() {
				self.writes.lock().expect("writes lock").ops.push((key.to_vec(), val.clone()));
			}
			Ok(val)
		}))
	}

	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move { self.write(&key, Some(val)).await }))
	}

	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move { self.write(&key, Some(val)).await }))
	}

	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move {
			self.check_write()?;
			if self.layers().get(&key).await?.is_some() {
				return Err(Error::TransactionKeyAlreadyExists);
			}
			self.write(&key, Some(val)).await
		}))
	}

	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move {
			self.check_write()?;
			if self.layers().get(&key).await? != chk {
				return Err(Error::TransactionConditionNotMet);
			}
			self.write(&key, Some(val)).await
		}))
	}

	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move { self.write(&key, None).await }))
	}

	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move {
			self.check_write()?;
			if self.layers().get(&key).await?.as_deref() != chk {
				return Err(Error::TransactionConditionNotMet);
			}
			self.write(&key, None).await
		}))
	}

	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(AssertSend(async move { self.write(&key, None).await }))
	}

	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		self.delc(key, chk)
	}

	fn keys<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(AssertSend(async move { Ok(keys_result(self.layers().scan(rng, limit, skip, Direction::Forward).await?)) }))
	}

	fn keysr<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(AssertSend(async move { Ok(keys_result(self.layers().scan(rng, limit, skip, Direction::Backward).await?)) }))
	}

	fn scan<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(AssertSend(async move { Ok(scan_result(self.layers().scan(rng, limit, skip, Direction::Forward).await?)) }))
	}

	fn scanr<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(AssertSend(async move { Ok(scan_result(self.layers().scan(rng, limit, skip, Direction::Backward).await?)) }))
	}

	/// Stateful cursors: one merge of the layers kept across the engine's batches, so a long scan
	/// opens few tree cursors (each prefetching its window of leaves in parallel), not one a batch.
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		_v: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn surrealdb_kvs::api::ScanCursorVals + 'a>>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(LayersCursor::new(self.layers(), rng, dir, skip)) as Box<dyn surrealdb_kvs::api::ScanCursorVals + 'a>)
		}))
	}

	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		_v: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn surrealdb_kvs::api::ScanCursorKeys + 'a>>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(LayersCursor::new(self.layers(), rng, dir, skip)) as Box<dyn surrealdb_kvs::api::ScanCursorKeys + 'a>)
		}))
	}

	/// Live keys of `rng`, counted by one merge reading the tier in large chunks.
	fn count<'a>(&'a self, rng: KeyRange<'a>, _v: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let layers = self.layers();
			let mut m = Merge::new(rng, Direction::Forward, 0, CURSOR_TIER_MAX, CURSOR_TIER_MAX);
			let mut n = 0;
			while m.next(&layers).await?.is_some() {
				n += 1;
			}
			Ok(n)
		}))
	}


	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.new_save_point().await?;
			let mut w = self.writes.lock().expect("writes lock");
			let len = w.ops.len();
			w.savepoints.push(len);
			Ok(())
		})
	}

	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.release_last_save_point().await?;
			self.writes.lock().expect("writes lock").savepoints.pop();
			Ok(())
		})
	}

	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.rollback_to_save_point().await?;
			let mut w = self.writes.lock().expect("writes lock");
			if let Some(len) = w.savepoints.pop() {
				w.ops.truncate(len);
			}
			Ok(())
		})
	}

	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.inner.timestamp()
	}

	fn safe_timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.inner.safe_timestamp()
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.inner.timestamp_impl()
	}
}

/// A read of a held snapshot: its suffix and its view. Many queries may read one snapshot at
/// once, so ending this transaction leaves the snapshot open; the replica closes it when it
/// falls out of the window.
struct SnapshotTransaction {
	suffix: Arc<dyn Transactable>,
	view: View,
	at: u64,
	replica: Arc<Replica>,
	ingested: IngestedSet,
}

impl SnapshotTransaction {
	fn layers(&self) -> Layers<'_> {
		Layers {
			suffix: self.suffix.as_ref(),
			view: self.view,
			at: self.at,
			replica: &self.replica,
			ingested: &self.ingested,
		}
	}
}

impl Transactable for SnapshotTransaction {
	fn kind(&self) -> &'static str {
		"cfedge-snapshot"
	}

	fn closed(&self) -> bool {
		false
	}

	fn writeable(&self) -> bool {
		false
	}

	fn cancel<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn commit<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn exists<'a>(&'a self, key: Key<'a>, _version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(AssertSend(async move { Ok(self.layers().get(&key).await?.is_some()) }))
	}

	fn get<'a>(&'a self, key: Key<'a>, _version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(AssertSend(async move { self.layers().get(&key).await }))
	}

	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(AssertSend(async move { self.layers().get(&key).await }))
	}

	fn set<'a>(&'a self, _key: Key<'a>, _val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Err(Error::TransactionReadonly) })
	}

	fn put<'a>(&'a self, _key: Key<'a>, _val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Err(Error::TransactionReadonly) })
	}

	fn putc<'a>(&'a self, _key: Key<'a>, _val: Val, _chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Err(Error::TransactionReadonly) })
	}

	fn del<'a>(&'a self, _key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Err(Error::TransactionReadonly) })
	}

	fn delc<'a>(&'a self, _key: Key<'a>, _chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Err(Error::TransactionReadonly) })
	}

	fn keys<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(AssertSend(async move { Ok(keys_result(self.layers().scan(rng, limit, skip, Direction::Forward).await?)) }))
	}

	fn keysr<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(AssertSend(async move { Ok(keys_result(self.layers().scan(rng, limit, skip, Direction::Backward).await?)) }))
	}

	fn scan<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(AssertSend(async move { Ok(scan_result(self.layers().scan(rng, limit, skip, Direction::Forward).await?)) }))
	}

	fn scanr<'a>(&'a self, rng: KeyRange<'a>, limit: u32, skip: u32, _v: Option<u64>) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(AssertSend(async move { Ok(scan_result(self.layers().scan(rng, limit, skip, Direction::Backward).await?)) }))
	}

	/// Stateful cursors: one merge of the layers kept across the engine's batches, so a long scan
	/// opens few tree cursors (each prefetching its window of leaves in parallel), not one a batch.
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		_v: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn surrealdb_kvs::api::ScanCursorVals + 'a>>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(LayersCursor::new(self.layers(), rng, dir, skip)) as Box<dyn surrealdb_kvs::api::ScanCursorVals + 'a>)
		}))
	}

	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		_v: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn surrealdb_kvs::api::ScanCursorKeys + 'a>>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(LayersCursor::new(self.layers(), rng, dir, skip)) as Box<dyn surrealdb_kvs::api::ScanCursorKeys + 'a>)
		}))
	}

	/// Live keys of `rng`, counted by one merge reading the tier in large chunks.
	fn count<'a>(&'a self, rng: KeyRange<'a>, _v: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(AssertSend(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			let layers = self.layers();
			let mut m = Merge::new(rng, Direction::Forward, 0, CURSOR_TIER_MAX, CURSOR_TIER_MAX);
			let mut n = 0;
			while m.next(&layers).await?.is_some() {
				n += 1;
			}
			Ok(n)
		}))
	}


	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.suffix.timestamp()
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.suffix.timestamp_impl()
	}
}

/// The message of a JavaScript error, or the value itself as a string.
#[cfg(feature = "js")]
pub fn js_message(e: &JsValue) -> String {
	if let Some(e) = e.dyn_ref::<js_sys::Error>() {
		return String::from(e.message());
	}
	e.as_string().unwrap_or_else(|| format!("{e:?}"))
}

#[cfg(test)]
mod tests {
	//! Reads over a sealed base with ingested objects on top (what a pointer commit leaves until
	//! the next seal), checked against a model: point gets, scans in both directions, key scans,
	//! both cursors, and counts must all agree, newest layer winning in every one.

	use std::collections::BTreeMap;
	use std::ops::ControlFlow;

	use shale::format::{Hlc, Key as SKey, Record, TxnId, Unit, Value as SValue, Verdict, encode_batch};
	use shale::object::{MemObjectStore, ObjectStore, PutCondition};

	use super::*;
	use crate::ingest;

	const N: u32 = 20_000;

	fn key(i: u32) -> Vec<u8> {
		format!("/*t*\u{0}{i:010}").into_bytes()
	}

	fn pack(writes: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Vec<u8> {
		let mut out = (writes.len() as u32).to_le_bytes().to_vec();
		for (k, v) in writes {
			push(&mut out, k, v.as_deref());
		}
		out
	}

	/// Everything the reads below compare: the replica over a store holding the base, and the
	/// model of what a reader must see.
	struct Fixture {
		backend: LogBackend,
		model: BTreeMap<Vec<u8>, Vec<u8>>,
	}

	fn val(tag: &str, i: u32) -> Vec<u8> {
		format!("{tag}{i:010}-{}", "x".repeat(40)).into_bytes()
	}

	/// A store holding `N` keys sealed into the tier at positions 1..=20 (a segment), and the model
	/// of what they read as.
	fn base_store() -> (Arc<MemObjectStore>, BTreeMap<Vec<u8>, Vec<u8>>) {
		let store = Arc::new(MemObjectStore::new());
		let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
		// The base: 20 records of 1,000 sets, sealed into one segment.
		let mut records = Vec::new();
		for p in 0..20u32 {
			let units = (p * 1000..(p + 1) * 1000)
				.map(|i| {
					model.insert(key(i), val("base", i));
					Unit::Set { key: SKey(key(i)), value: SValue(val("base", i)) }
				})
				.collect();
			records.push((u64::from(p) + 1, Record { hlc: Hlc::new(1_700_000_000_000 + u64::from(p), 0), txn: TxnId(u128::from(p)), scope: 0, units, verdict: Verdict::ImplicitTrue }));
		}
		let frame = encode_batch(0, 0, &records);
		let mut frames = 1u32.to_le_bytes().to_vec();
		frames.extend_from_slice(&(frame.len() as u32).to_le_bytes());
		frames.extend_from_slice(&frame);
		let sealed = futures::executor::block_on(edge_tier::seal::seal(Arc::clone(&store), "t", &frames, 20, 64 << 20)).expect("seal");
		assert_eq!(sealed.sealed_through, 20);
		(store, model)
	}

	/// A replica over a store that holds the base, standing at its position 20.
	fn replica_over(store: &Arc<MemObjectStore>) -> Replica {
		let replica = futures::executor::block_on(async {
			let replica = Replica::new(Tier::with_store(Arc::clone(store), "t".into(), 8 << 20, 8 << 20)).await?;
			assert_eq!(replica.open_tier().await?, 20);
			Ok::<_, Error>(replica)
		})
		.expect("replica");
		// The base is in the tier, not the suffix: a first tier batch moves the merge's bounds.
		assert_eq!(replica.applied(), 20);
		replica.keep_snapshots(1);
		replica
	}

	/// `N` keys sealed into the tier at positions 1..=20 (a segment), then, newest last:
	/// a suffix write (21), ingested objects that delete every 5th key (22), update every 3rd (23)
	/// and revive some deleted keys while deleting some updated ones (24), and a suffix write (25).
	fn fixture() -> Fixture {
		let (store, mut model) = base_store();
		let replica = replica_over(&store);

		let apply_suffix = |pos: u64, writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>, model: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
			replica.apply(pos, &pack(&writes)).expect("apply");
			for (k, v) in writes {
				match v {
					Some(v) => model.insert(k, v),
					None => model.remove(&k),
				};
			}
		};
		let ingest_object = |pos: u64, writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>, model: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
			let (obj, meta_start, meta_len) = ingest::build(&pack(&writes)).expect("build");
			let name = format!("t/ptr/obj-{pos}");
			futures::executor::block_on(store.put(&name, &obj, PutCondition::None)).expect("put");
			let meta = &obj[meta_start as usize..(meta_start + meta_len) as usize];
			let ing = ingest::Ingested::parse(pos, name, meta_start, meta).expect("parse");
			{
				let mut set = replica.ingested.lock().expect("ingested");
				let mut next: Vec<_> = set.iter().cloned().collect();
				next.push(Arc::new(ingest::Layer::Object(ing)));
				*set = Arc::new(next);
			}
			replica.applied.store(pos, Ordering::Release);
			for (k, v) in writes {
				match v {
					Some(v) => model.insert(k, v),
					None => model.remove(&k),
				};
			}
		};

		// 21: a suffix write under the ingested objects: key 7 changed, key 10 (deleted by 22) set.
		apply_suffix(21, BTreeMap::from([(key(7), Some(val("sfx21-", 7))), (key(10), Some(val("sfx21-", 10)))]), &mut model);
		// 22: every 5th key deleted (the staging DELETE), across the whole range.
		ingest_object(22, (0..N).filter(|i| i % 5 == 0).map(|i| (key(i), None)).collect(), &mut model);
		// 23: every 3rd key updated (the staging UPDATE; some of them are deleted by 22: a write
		// of a key 22 deleted revives it, so only those not deleted are updates).
		ingest_object(23, (0..N).filter(|i| i % 3 == 0 && i % 5 != 0).map(|i| (key(i), Some(val("upd-", i)))).collect(), &mut model);
		// 24: revive every 35th key (deleted by 22) and delete every 21st that is not deleted.
		ingest_object(
			24,
			(0..N)
				.filter_map(|i| {
					if i % 35 == 0 {
						Some((key(i), Some(val("rev-", i))))
					} else if i % 21 == 0 {
						Some((key(i), None))
					} else {
						None
					}
				})
				.collect(),
			&mut model,
		);
		// 25: a suffix write over the newest ingested object: key 21 (deleted by 24) set again,
		// key 35 (revived by 24) deleted.
		apply_suffix(25, BTreeMap::from([(key(21), Some(val("sfx25-", 21))), (key(35), None)]), &mut model);
		assert_eq!(replica.ingested_len(), 3);

		let hook = CommitHook::new(|_, _, _| Box::pin(async { Err(CommitError::Other("read only".into())) }));
		Fixture { backend: LogBackend { replica: Arc::new(replica), hook: Arc::new(hook) }, model }
	}

	fn expect_rows(model: &BTreeMap<Vec<u8>, Vec<u8>>, lo: &[u8], hi: &[u8], forward: bool, skip: usize, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
		let rows = model.range(lo.to_vec()..hi.to_vec()).map(|(k, v)| (k.clone(), v.clone()));
		let rows: Vec<_> = if forward { rows.collect() } else { rows.rev().collect() };
		rows.into_iter().skip(skip).take(if limit == 0 { usize::MAX } else { limit }).collect()
	}

	/// The ranges read: the whole table, one inside the first tier batch, one across it, one
	/// starting past it, and one that ends inside it.
	fn ranges() -> Vec<(Vec<u8>, Vec<u8>)> {
		vec![
			(vec![0u8], vec![0xff]),
			(key(100), key(300)),
			(key(100), key(9_000)),
			(key(3_000), key(12_000)),
			(key(0), key(400)),
			(key(15_000), key(N)),
		]
	}

	/// Compare a read's rows with the model's, naming the range and the first difference.
	fn same(what: &str, got: Vec<(Vec<u8>, Vec<u8>)>, want: Vec<(Vec<u8>, Vec<u8>)>) {
		if got == want {
			return;
		}
		let first = got.iter().zip(&want).position(|(g, w)| g != w);
		let key_of = |rows: &[(Vec<u8>, Vec<u8>)], at: Option<usize>| at.and_then(|i| rows.get(i)).map(|(k, _)| String::from_utf8_lossy(k).into_owned());
		panic!("{what}: {} rows, wanted {}; first difference at {first:?}: got {:?}, wanted {:?}", got.len(), want.len(), key_of(&got, first), key_of(&want, first));
	}

	async fn read_all(fx: &Fixture, tx: &dyn Transactable) {
		let model = &fx.model;
		// Point reads of every key in the table.
		for i in 0..N {
			let got = tx.get(Key::from(key(i)), None).await.expect("get");
			assert_eq!(got.as_ref(), model.get(&key(i)), "get {i}");
		}
		for (n, (lo, hi)) in ranges().into_iter().enumerate() {
			let rng = || KeyRange::from(lo.clone()..hi.clone());
			for forward in [true, false] {
				let dir = if forward { Direction::Forward } else { Direction::Backward };
				let name = |what: &str| format!("range {n} {what} {}", if forward { "forward" } else { "backward" });
				// Whole range, and a window with skip and limit.
				for (skip, limit) in [(0usize, 0usize), (50, 700)] {
					let got = if forward { tx.scan(rng(), limit as u32, skip as u32, None).await } else { tx.scanr(rng(), limit as u32, skip as u32, None).await }.expect("scan").values;
					let want = expect_rows(model, &lo, &hi, forward, skip, limit);
					same(&name(&format!("scan skip {skip} limit {limit}")), got, want.clone());
					let keys = if forward { tx.keys(rng(), limit as u32, skip as u32, None).await } else { tx.keysr(rng(), limit as u32, skip as u32, None).await }.expect("keys").keys;
					assert_eq!(keys, want.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), "{}", name("keys"));
				}
				// The cursors, drained in chunks.
				let want = expect_rows(model, &lo, &hi, forward, 0, 0);
				let mut vals = tx.open_vals_cursor(rng(), dir, 0, None).await.expect("cursor");
				let mut got = Vec::new();
				loop {
					let mut chunk = 0;
					let mut f = |k: &[u8], v: &[u8]| {
						got.push((k.to_vec(), v.to_vec()));
						chunk += 1;
						Ok(ControlFlow::Continue(()))
					};
					vals.for_each(1000, &mut f).await.expect("for_each");
					if chunk == 0 {
						break;
					}
				}
				drop(vals);
				same(&name("vals cursor"), got, want.clone());
				let mut keys = tx.open_keys_cursor(rng(), dir, 0, None).await.expect("cursor");
				let mut got = Vec::new();
				loop {
					let mut chunk = 0;
					let mut f = |k: &[u8]| {
						got.push(k.to_vec());
						chunk += 1;
						Ok(ControlFlow::Continue(()))
					};
					keys.for_each(1000, &mut f).await.expect("for_each");
					if chunk == 0 {
						break;
					}
				}
				drop(keys);
				assert_eq!(got, want.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), "{}", name("keys cursor"));
			}
			assert_eq!(tx.count(rng(), None).await.expect("count"), expect_rows(model, &lo, &hi, true, 0, 0).len(), "range {n} count");
		}
	}

	#[test]
	fn ingested_deletes_and_updates_win_in_every_read() {
		let fx = fixture();
		// The staging numbers: a fifth of the keys deleted (so 16,000 of 20,000 before the
		// other layers touch some), and the count over the whole table matches the model.
		assert!(fx.model.len() < N as usize * 4 / 5 + 400);
		futures::executor::block_on(AssertSend(async {
			// A transaction of the node's own, then the held snapshot a fan-out leg reads (pinned
			// to the last position, where the snapshot holds all three ingested objects).
			let (tx, _) = fx.backend.new_transaction(TransactionType::Read).await.expect("tx");
			assert_eq!(tx.kind(), "cfedge");
			read_all(&fx, tx.as_ref()).await;
			PIN.with(|p| p.set(Some(25)));
			let pinned = fx.backend.new_transaction(TransactionType::Read).await;
			PIN.with(|p| p.set(None));
			let (snap, _) = pinned.expect("snapshot tx");
			assert_eq!(snap.kind(), "cfedge-snapshot");
			read_all(&fx, snap.as_ref()).await;
		}));
	}

	// ---- sections: a run read in place, and memory swapped for layers ----------------------------

	type Writes = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

	/// A deterministic stream of numbers.
	struct Lcg(u64);

	impl Lcg {
		fn next(&mut self) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			self.0 >> 33
		}
	}

	/// `n` commits of 5..60 writes each over the table (a third of them deletes), at positions from
	/// 21: what a burst of small transactions makes.
	fn commits(seed: u64, n: u64) -> Vec<(u64, Writes)> {
		let mut rng = Lcg(seed);
		(0..n)
			.map(|c| {
				let writes = (0..5 + rng.next() % 55)
					.map(|_| {
						let i = (rng.next() % u64::from(N)) as u32;
						(key(i), if rng.next() % 3 == 0 { None } else { Some(val(&format!("c{c}-"), i)) })
					})
					.collect();
				(21 + c, writes)
			})
			.collect()
	}

	fn apply_to_model(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, writes: &Writes) {
		for (k, v) in writes {
			match v {
				Some(v) => model.insert(k.clone(), v.clone()),
				None => model.remove(k),
			};
		}
	}

	fn section(writes: &Writes) -> surrealds_section::Built {
		surrealds_section::build(writes.iter().map(|(k, v)| (k.as_slice(), v.as_deref()))).expect("sorted")
	}

	/// A run: a few bytes of something else, then each commit's section, back to back. Returns the
	/// object and `(position, offset, length, meta)` of each section, as a footer lists them.
	fn run_of(commits: &[(u64, Writes)]) -> (Vec<u8>, Vec<(u64, u32, u32, Vec<u8>)>) {
		let mut run = b"frames?".to_vec();
		let mut entries = Vec::new();
		for (pos, writes) in commits {
			let s = section(writes);
			let ml = surrealds_section::meta_len(&s.bytes).expect("trailer");
			entries.push((*pos, run.len() as u32, s.bytes.len() as u32, s.bytes[s.bytes.len() - ml..].to_vec()));
			run.extend_from_slice(&s.bytes);
		}
		(run, entries)
	}

	/// The same data as a log would carry it: batch frames of section records, 25 to a frame.
	fn frames_of(commits: &[(u64, Writes)], prev: u64) -> Vec<Vec<u8>> {
		let mut prev = prev;
		commits
			.chunks(25)
			.map(|chunk| {
				let records: Vec<(u64, Record)> = chunk
					.iter()
					.map(|(pos, writes)| {
						let units = vec![Unit::Set { key: SKey(surrealds_section::SEC_KEY.to_vec()), value: SValue(section(writes).bytes) }];
						(*pos, Record { hlc: Hlc::new(1_700_000_001_000 + pos, 0), txn: TxnId(u128::from(*pos)), scope: 0, units, verdict: Verdict::ImplicitTrue })
					})
					.collect();
				let frame = encode_batch(0, prev, &records);
				prev = chunk.last().expect("a chunk").0;
				frame
			})
			.collect()
	}

	fn fixture_over(replica: Replica, model: BTreeMap<Vec<u8>, Vec<u8>>) -> Fixture {
		let hook = CommitHook::new(|_, _, _| Box::pin(async { Err(CommitError::Other("read only".into())) }));
		Fixture { backend: LogBackend { replica: Arc::new(replica), hook: Arc::new(hook) }, model }
	}

	async fn check_reads(fx: &Fixture) {
		let (tx, _) = fx.backend.new_transaction(TransactionType::Read).await.expect("tx");
		read_all(fx, tx.as_ref()).await;
		let last = fx.backend.replica.applied();
		PIN.with(|p| p.set(Some(last)));
		let pinned = fx.backend.new_transaction(TransactionType::Read).await;
		PIN.with(|p| p.set(None));
		let (snap, _) = pinned.expect("snapshot tx");
		read_all(fx, snap.as_ref()).await;
	}

	#[test]
	fn a_run_of_many_sections_read_in_place_newest_section_wins() {
		let (store, mut model) = base_store();
		let all = commits(7, 300);
		for (_, w) in &all {
			apply_to_model(&mut model, w);
		}
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		let refs: Vec<(u64, u32, u32, &[u8])> = entries.iter().map(|(p, o, l, m)| (*p, *o, *l, m.as_slice())).collect();
		// A start from the run's footer: layers, and nothing in memory.
		assert_eq!(replica.layer_run("t/log/run-1.b", 21, 320, &refs).expect("layer"), FrameApplied::Applied(320));
		assert_eq!((replica.applied(), replica.ingested_len(), replica.layer_sections(), replica.suffix_len().expect("len")), (320, 1, 300, 0), "one layer for the run");
		// A run that starts after the next position needed is a gap, and one already covered is not
		// applied twice.
		assert_eq!(replica.layer_run("t/log/run-2.b", 400, 410, &refs[..1]).expect("gap"), FrameApplied::Gap);
		assert_eq!(replica.layer_run("t/log/run-1.b", 21, 320, &refs).expect("again"), FrameApplied::Applied(320));
		assert_eq!(replica.ingested_len(), 1);
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
		// Reading as of a position in the middle of the run: only the commits up to it.
		let mut mid_model: BTreeMap<Vec<u8>, Vec<u8>> = base_store().1;
		for (pos, w) in &all {
			if *pos <= 170 {
				apply_to_model(&mut mid_model, w);
			}
		}
		futures::executor::block_on(AssertSend(async {
			let suffix = fx.backend.replica.suffix().transaction(TransactionType::Read).await.expect("suffix");
			let ing = fx.backend.replica.ingested();
			let layers = Layers { suffix: suffix.as_ref(), view: fx.backend.replica.view(), at: 170, replica: &fx.backend.replica, ingested: &ing };
			for i in (0..N).step_by(7) {
				assert_eq!(layers.get(&key(i)).await.expect("get"), mid_model.get(&key(i)).cloned(), "key {i} as of 170");
			}
			let rows = layers.scan(KeyRange::from(key(100)..key(9_000)), 0, 0, Direction::Backward).await.expect("scan");
			assert_eq!(rows, expect_rows(&mid_model, &key(100), &key(9_000), false, 0, 0));
		}));
	}

	#[test]
	fn many_runs_of_many_sections_agree_with_the_model_at_every_position_and_retire_by_run() {
		let (store, mut model) = base_store();
		// 12 runs of 50 commits, positions 21..=620, each run its own object with its own footer.
		let all = commits(13, 600);
		let mut footers = Vec::new();
		for (i, chunk) in all.chunks(50).enumerate() {
			let (run, entries) = run_of(chunk);
			futures::executor::block_on(store.put(&format!("t/log/run-{i}.b"), &run, PutCondition::None)).expect("put");
			footers.push((chunk[0].0, chunk[49].0, entries));
		}
		for (_, w) in &all {
			apply_to_model(&mut model, w);
		}
		let replica = replica_over(&store);
		for (i, (first, last, entries)) in footers.iter().enumerate() {
			let refs: Vec<(u64, u32, u32, &[u8])> = entries.iter().map(|(p, o, l, m)| (*p, *o, *l, m.as_slice())).collect();
			assert_eq!(replica.layer_run(&format!("t/log/run-{i}.b"), *first, *last, &refs).expect("layer"), FrameApplied::Applied(*last));
		}
		assert_eq!((replica.ingested_len(), replica.layer_sections(), replica.suffix_len().expect("len")), (12, 600, 0), "a layer per run, nothing in memory");
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
		// As of positions at the start, the middle and the end of runs, and between them.
		let base = base_store().1;
		futures::executor::block_on(AssertSend(async {
			let suffix = fx.backend.replica.suffix().transaction(TransactionType::Read).await.expect("suffix");
			let ing = fx.backend.replica.ingested();
			for at in [21u64, 45, 70, 71, 120, 121, 333, 370, 371, 619, 620] {
				let mut want = base.clone();
				for (pos, w) in &all {
					if *pos <= at {
						apply_to_model(&mut want, w);
					}
				}
				let layers = Layers { suffix: suffix.as_ref(), view: fx.backend.replica.view(), at, replica: &fx.backend.replica, ingested: &ing };
				for i in (0..N).step_by(97) {
					assert_eq!(layers.get(&key(i)).await.expect("get"), want.get(&key(i)).cloned(), "key {i} as of {at}");
				}
				let rows = layers.scan(KeyRange::from(key(2_000)..key(6_000)), 0, 0, Direction::Forward).await.expect("scan");
				assert_eq!(rows, expect_rows(&want, &key(2_000), &key(6_000), true, 0, 0), "scan as of {at}");
			}
		}));
		// A seal that ends inside a run: the runs below go, the run it ends in keeps the sections
		// above it, and the runs above are the same layers.
		let before = fx.backend.replica.ingested();
		fx.backend.replica.retire_layers(335);
		let after = fx.backend.replica.ingested();
		assert_eq!((after.len(), after.iter().map(|l| l.sections()).sum::<usize>()), (6, 600 - 315));
		assert_eq!((after[0].first(), after[0].last(), after[1].first()), (336, 370, 371));
		assert!(Arc::ptr_eq(&after[1], &before[7]), "a run wholly above is untouched");
		assert_eq!(before.len(), 12, "a snapshot that began before keeps its layers");
		fx.backend.replica.retire_layers(620);
		assert_eq!(fx.backend.replica.ingested_len(), 0);
	}

	#[test]
	fn a_run_that_mixes_sections_with_legacy_records_is_applied_without_its_sections_and_layered() {
		crate::set_heap_budget(u64::MAX);
		let (store, mut model) = base_store();
		let all = commits(31, 80);
		for (_, w) in &all {
			apply_to_model(&mut model, w);
		}
		// Every third commit is a record of plain writes (a legacy commit); the rest are sections.
		let is_section = |pos: u64| pos % 3 != 0;
		let sections: Vec<(u64, Writes)> = all.iter().filter(|(p, _)| is_section(*p)).cloned().collect();
		let (run, entries) = run_of(&sections);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let records: Vec<(u64, Record)> = all
			.iter()
			.map(|(pos, writes)| {
				let units = if is_section(*pos) {
					vec![Unit::Set { key: SKey(surrealds_section::SEC_KEY.to_vec()), value: SValue(section(writes).bytes) }]
				} else {
					writes.iter().map(|(k, v)| match v {
						Some(v) => Unit::Set { key: SKey(k.clone()), value: SValue(v.clone()) },
						None => Unit::Del { key: SKey(k.clone()) },
					}).collect()
				};
				(*pos, Record { hlc: Hlc::new(1_700_000_001_000 + pos, 0), txn: TxnId(u128::from(*pos)), scope: 0, units, verdict: Verdict::ImplicitTrue })
			})
			.collect();
		let frame = encode_batch(0, 20, &records);
		let replica = replica_over(&store);
		let skip: std::collections::HashSet<u64> = sections.iter().map(|(p, _)| *p).collect();
		assert_eq!(replica.apply_frame_skipping(&frame, u64::MAX, &[], &skip).expect("apply"), FrameApplied::Applied(100));
		// Only the legacy records are in memory: no section's entries, no held meta.
		assert_eq!((replica.live_sections(), replica.layer_sections()), (0, 0));
		let legacy_keys: std::collections::BTreeSet<&Vec<u8>> = all.iter().filter(|(p, _)| !is_section(*p)).flat_map(|(_, w)| w.keys()).collect();
		assert!(replica.suffix_len().expect("len") <= legacy_keys.len());
		let refs: Vec<(u64, u32, u32, &[u8])> = entries.iter().map(|(p, o, l, m)| (*p, *o, *l, m.as_slice())).collect();
		replica.register_sections("t/log/run-1.b", &refs).expect("register");
		assert_eq!((replica.ingested_len(), replica.layer_sections(), replica.applied()), (1, sections.len(), 100));
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
	}

	#[test]
	fn a_pointer_commit_inside_a_rolled_run_is_older_than_the_sections_after_it() {
		// Positions 21..=30: sections, except 25, a pointer commit with metadata (an Object layer). The
		// run's layer spans 21..=30 and the Object layer sits inside that span; a read must pick the
		// newest write of a key wherever it is: key 5 written at 22 (section), 25 (pointer) and 27 (section),
		// key 6 at 25 and 23, key 7 at 25 and 29.
		let (store, mut model) = base_store();
		let w = |pairs: &[(u32, Option<&str>)]| -> Writes { pairs.iter().map(|(i, v)| (key(*i), v.map(|v| val(v, *i)))).collect() };
		let mut all: Vec<(u64, Writes)> = Vec::new();
		for pos in 21..=30u64 {
			let writes = match pos {
				22 => w(&[(5, Some("s22-")), (6, Some("s22-"))]),
				23 => w(&[(6, Some("s23-"))]),
				27 => w(&[(5, Some("s27-"))]),
				29 => w(&[(7, Some("s29-"))]),
				_ => w(&[(100 + pos as u32, Some("x-"))]),
			};
			all.push((pos, writes));
		}
		let pointer = w(&[(5, Some("p25-")), (6, Some("p25-")), (7, Some("p25-"))]);
		for (pos, writes) in &all {
			apply_to_model(&mut model, if *pos == 25 { &pointer } else { writes });
		}
		let sections: Vec<(u64, Writes)> = all.iter().filter(|(p, _)| *p != 25).cloned().collect();
		let (run, entries) = run_of(&sections);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let (obj, meta_start, meta_len) = ingest::build(&pack(&pointer)).expect("build");
		futures::executor::block_on(store.put("t/ptr/obj-25", &obj, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		for frame in frames_of(&sections[..4], 20) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		let ing = ingest::Ingested::parse(25, "t/ptr/obj-25".into(), meta_start, &obj[meta_start as usize..(meta_start + meta_len) as usize]).expect("parse");
		replica.add_layers(vec![Arc::new(ingest::Layer::Object(ing))]);
		replica.applied.store(25, Ordering::Release);
		for frame in frames_of(&sections[4..], 25) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		let secs: Vec<(u64, u32, u32)> = entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		let swapped = replica.roll_swap("t/log/run-1.b", &secs).expect("swap");
		assert_eq!((swapped.layers, swapped.missing), (9, 0));
		let layers = replica.ingested();
		assert_eq!(layers.len(), 2, "the run's layer and the pointer's");
		assert!(layers[0].first() < 25 && layers[0].last() > 25, "the run spans the pointer commit");
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(async {
			let (tx, _) = fx.backend.new_transaction(TransactionType::Read).await.expect("tx");
			// The sections written after the pointer win; the pointer wins over those before it.
			assert_eq!(tx.get(Key::from(key(5)), None).await.expect("get"), Some(val("s27-", 5)));
			assert_eq!(tx.get(Key::from(key(6)), None).await.expect("get"), Some(val("p25-", 6)));
			assert_eq!(tx.get(Key::from(key(7)), None).await.expect("get"), Some(val("s29-", 7)));
			// A scan agrees with a get, and both with the model.
			let rows = tx.scan(KeyRange::from(key(0)..key(200)), 0, 0, None).await.expect("scan").values;
			assert_eq!(rows.into_iter().map(|(k, v)| (k.to_vec(), v)).collect::<Vec<_>>(), expect_rows(&model, &key(0), &key(200), true, 0, 0));
		}));
		futures::executor::block_on(AssertSend(check_reads(&fx)));
	}

	#[test]
	fn memory_swapped_for_layers_on_a_rolled_notice_reads_the_same_and_is_freed() {
		crate::set_heap_budget(u64::MAX);
		let (store, mut model) = base_store();
		let all = commits(11, 200);
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		replica.set_rebuild_floor(0);
		// Applied live, as frames of section records: visible at once, held in memory.
		let mut prev = 20;
		for frame in frames_of(&all, 20) {
			let applied = replica.apply_frame(&frame, u64::MAX).expect("apply");
			let FrameApplied::Applied(at) = applied else { panic!("applied") };
			prev = at;
		}
		assert_eq!(prev, 220);
		for (_, w) in &all {
			apply_to_model(&mut model, w);
		}
		assert_eq!((replica.live_sections(), replica.ingested_len()), (200, 0));
		let in_memory = replica.suffix_len().expect("len");
		assert!(in_memory > 1000, "{in_memory} keys in memory");
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
		// The first half is rolled: layers over the run, the entries gone from memory.
		let first: Vec<(u64, u32, u32)> = entries[..100].iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		let swapped = fx.backend.replica.roll_swap("t/log/run-1.b", &first).expect("swap");
		assert_eq!((swapped.layers, swapped.missing), (100, 0));
		assert!(swapped.freed > 0);
		assert_eq!((fx.backend.replica.live_sections(), fx.backend.replica.ingested_len(), fx.backend.replica.layer_sections()), (100, 1, 100));
		let half = fx.backend.replica.suffix_len().expect("len");
		assert!(half < in_memory, "memory fell: {half} of {in_memory}");
		futures::executor::block_on(AssertSend(check_reads(&fx)));
		// A notice for what was swapped already finds nothing to do; one for the rest swaps it, and
		// the suffix is empty.
		assert_eq!(fx.backend.replica.roll_swap("t/log/run-1.b", &first).expect("again"), Swapped { layers: 0, missing: 100, freed: 0, bytes: 0 });
		let rest: Vec<(u64, u32, u32)> = entries[100..].iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		fx.backend.replica.roll_swap("t/log/run-1.b", &rest).expect("swap");
		assert_eq!((fx.backend.replica.suffix_len().expect("len"), fx.backend.replica.live_sections(), fx.backend.replica.layer_sections()), (0, 0, 200));
		assert!(fx.backend.replica.layer_bytes() < 200 * 600, "the layers' metas are small: {}", fx.backend.replica.layer_bytes());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
	}

	#[test]
	fn a_javascript_host_gets_exactly_the_packed_write_set() {
		let writes: Writes = BTreeMap::from([(key(1), Some(b"a".to_vec())), (key(2), None), (key(3), Some(vec![9; 100]))]);
		let built = section(&writes);
		// The same bytes the legacy packer wrote, byte for byte: no trailing meta.
		assert_eq!(packed_prefix(&built.bytes), pack(&writes).as_slice());
		// Bytes that are not a section are left as they are.
		assert_eq!(packed_prefix(b"not a section"), b"not a section");
	}

	#[test]
	fn a_locked_read_joins_the_commits_conflict_set() {
		use std::sync::Mutex as StdMutex;
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let replica = replica_over(&store);
		let seen: Arc<StdMutex<Vec<Vec<u64>>>> = Arc::default();
		let sink = Arc::clone(&seen);
		let hook = CommitHook::new(move |_snap, _section, hashes| {
			sink.lock().expect("seen").push(hashes);
			Box::pin(async { Ok(()) })
		});
		let backend = LogBackend { replica: Arc::new(replica), hook: Arc::new(hook) };
		futures::executor::block_on(AssertSend(async {
			let (tx, _) = backend.new_transaction(TransactionType::Write).await.expect("tx");
			tx.getu(Key::from(key(10))).await.expect("getu");
			tx.set(Key::from(key(30)), val("a", 30)).await.expect("set");
			tx.commit().await.expect("commit");
		}));
		let seen = seen.lock().expect("seen");
		let want: Vec<u64> = [key(10), key(30)].iter().map(|k| surrealds_section::hash(k)).collect();
		assert_eq!(seen[0], want, "the key read under a lock is screened with the writes");
	}

	#[test]
	fn a_commit_is_a_section_made_in_the_packing_loop() {
		use std::sync::Mutex as StdMutex;
		// The heap counted is the whole process's: tests running beside this one hold tens of
		// MiB, and this one is about the commit, not the budget.
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let replica = replica_over(&store);
		let seen: Arc<StdMutex<Vec<(u64, Vec<u8>, Vec<u64>)>>> = Arc::default();
		let sink = Arc::clone(&seen);
		let hook = CommitHook::new(move |snap, section, hashes| {
			sink.lock().expect("seen").push((snap, section, hashes));
			Box::pin(async { Ok(()) })
		});
		let backend = LogBackend { replica: Arc::new(replica), hook: Arc::new(hook) };
		futures::executor::block_on(AssertSend(async {
			let (tx, _) = backend.new_transaction(TransactionType::Write).await.expect("tx");
			tx.set(Key::from(key(30)), val("a", 30)).await.expect("set");
			tx.set(Key::from(key(10)), val("a", 10)).await.expect("set");
			tx.set(Key::from(key(30)), val("b", 30)).await.expect("overwrite");
			tx.del(Key::from(key(20))).await.expect("del");
			tx.commit().await.expect("commit");
		}));
		let seen = seen.lock().expect("seen");
		let (snap, section, hashes) = &seen[0];
		assert_eq!(*snap, 20);
		// Sorted, one write per key, the last write winning, a delete a tombstone.
		let entries = surrealds_section::entries(section).expect("entries");
		assert_eq!(entries, vec![(key(10).as_slice(), Some(val("a", 10).as_slice())), (key(20).as_slice(), None), (key(30).as_slice(), Some(val("b", 30).as_slice()))]);
		// The hashes are the keys', in order, for the sequencer's screen; the bloom is built from them.
		assert_eq!(*hashes, entries.iter().map(|(k, _)| surrealds_section::hash(k)).collect::<Vec<_>>());
		let meta = surrealds_section::Meta::parse_section(section).expect("meta");
		assert!(entries.iter().all(|(k, _)| meta.may_hold(k)));
		assert!(!meta.may_hold(&key(5)) && !meta.may_hold(&key(31)));
		// A reader that only knows the packed write set reads its prefix.
		let packed = unpack(section).expect("unpack");
		assert_eq!(packed.len(), 3);
		assert_eq!(packed[1], (key(20).as_slice(), None));
		// And a pointer object built from it is the same as from the plain packed write set.
		let (obj, _, _) = crate::ingest::build(section).expect("build");
		let (plain, _, _) = crate::ingest::build(&section[..section.len() - surrealds_section::meta_len(section).expect("trailer")]).expect("build");
		assert_eq!(obj, plain);
	}

	#[test]
	fn snapshots_pin_at_most_a_budget_of_applied_log() {
		let all = commits(5, 200);
		let frames = frames_of(&all, 20);
		let total: usize = frames.iter().map(Vec::len).sum();
		let oldest = |budget: u64| {
			let (store, _) = base_store();
			let replica = replica_over(&store);
			replica.keep_snapshots_within(128, budget);
			for f in &frames {
				replica.apply_frame(f, u64::MAX).expect("apply");
			}
			let (lo, hi) = replica.snapshot_range().expect("snapshots");
			assert_eq!(hi, 220, "the newest is always kept");
			lo
		};
		// No bound: every batch's snapshot is held (the first of them is the position the replica
		// started at).
		assert_eq!(oldest(0), 20);
		// Bounded to a fifth of what was applied: the oldest ones were let go, the newest kept.
		let lo = oldest(total as u64 / 5);
		assert!(lo > 100, "the oldest snapshot held is {lo}");
		// One batch's worth keeps the newest alone.
		assert_eq!(oldest(1), 220);
	}

	#[test]
	fn a_swap_rebuilds_the_store_only_when_enough_is_garbage_and_reads_the_same_throughout() {
		crate::set_heap_budget(u64::MAX);
		let (store, mut model) = base_store();
		let all = commits(17, 120);
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		for (_, w) in &all {
			apply_to_model(&mut model, w);
		}
		let replica = replica_over(&store);
		for frame in frames_of(&all, 20) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		let secs: Vec<(u64, u32, u32)> = entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		let total = replica.suffix_bytes();
		assert!(total > 100_000, "{total}");
		// A floor above what the first swap frees: the store is kept (nothing copied), and still
		// reads the same, the swapped commits being in the layers and in the store.
		replica.set_rebuild_floor(total / 3);
		let old = replica.suffix();
		let first = replica.roll_swap("t/log/run-1.b", &secs[..20]).expect("swap");
		assert_eq!((first.layers, first.freed), (20, 0), "deferred");
		assert!(Arc::ptr_eq(&old, &replica.suffix()), "the store stayed");
		let (rebuilds, refused, pending, bytes) = replica.rebuild_stats();
		assert_eq!((rebuilds, refused, pending), (0, 0, 20));
		assert!(bytes > 0 && bytes < total / 3);
		let fx = fixture_over(replica, model.clone());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
		// A swap that takes the garbage past the floor and the eighth of what stays rebuilds,
		// dropping what both swaps left.
		let second = fx.backend.replica.roll_swap("t/log/run-1.b", &secs[20..90]).expect("swap");
		assert_eq!(second.layers, 70);
		assert_eq!(second.freed, entries_left_at(&all, 110), "the entries of both swaps");
		assert!(!Arc::ptr_eq(&old, &fx.backend.replica.suffix()), "the store was rebuilt");
		let (rebuilds, _, pending, bytes) = fx.backend.replica.rebuild_stats();
		assert_eq!((rebuilds, pending, bytes), (1, 0, 0));
		assert!(fx.backend.replica.suffix_bytes() < total / 2, "{} of {total}", fx.backend.replica.suffix_bytes());
		futures::executor::block_on(AssertSend(check_reads(&fx)));
	}

	/// How many suffix entries are at a position up to `upto`: a key has one entry, at its newest write.
	fn entries_left_at(all: &[(u64, Writes)], upto: u64) -> usize {
		let mut last: BTreeMap<&Vec<u8>, u64> = BTreeMap::new();
		for (p, w) in all {
			for k in w.keys() {
				last.insert(k, *p);
			}
		}
		last.values().filter(|p| **p <= upto).count()
	}

	#[test]
	fn the_fraction_rule_keeps_a_large_tail_from_being_copied_for_a_small_swap() {
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let all = commits(19, 120);
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		replica.set_rebuild_floor(0);
		for frame in frames_of(&all, 20) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		let secs: Vec<(u64, u32, u32)> = entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		// Two commits of 120 are far under an eighth of what stays: not worth copying the rest.
		let old = replica.suffix();
		assert_eq!(replica.roll_swap("t/log/run-1.b", &secs[..2]).expect("swap").freed, 0);
		assert!(Arc::ptr_eq(&old, &replica.suffix()));
		// Twenty are over it.
		assert!(replica.roll_swap("t/log/run-1.b", &secs[2..30]).expect("swap").freed > 0);
		assert!(!Arc::ptr_eq(&old, &replica.suffix()));
	}

	#[test]
	fn a_rebuild_needs_heap_for_its_copy_and_refuses_without() {
		// The copy is the entries that stay, at 5/4, beside the heap already in use.
		assert!(rebuild_allowed(40 << 20, 8 << 20, 64 << 20));
		assert!(!rebuild_allowed(60 << 20, 8 << 20, 64 << 20));
		assert!(!rebuild_allowed(u64::MAX, 1, 64 << 20));
		assert!(rebuild_allowed(0, 0, 0));
	}

	#[test]
	fn a_rebuild_moves_the_newest_snapshot_to_the_new_store_and_lets_go_of_the_older_ones() {
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let all = commits(23, 60);
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		replica.set_rebuild_floor(0);
		replica.keep_snapshots(128);
		for frame in frames_of(&all, 20) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		// A budget under the store's size: the older snapshots, which pin the old store whole, go
		// at a rebuild (set directly: setting it would trim them now).
		replica.snapshot_bytes.store(50_000, Ordering::Relaxed);
		let before = replica.snapshot_range().expect("snapshots");
		assert!(before.0 < before.1, "several are held: {before:?}");
		let secs: Vec<(u64, u32, u32)> = entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		let old = replica.suffix();
		assert!(replica.roll_swap("t/log/run-1.b", &secs).expect("swap").freed > 0);
		assert!(!Arc::ptr_eq(&old, &replica.suffix()));
		let newest = replica.applied();
		assert_eq!(replica.snapshot_range(), Some((newest, newest)), "only the newest, on the new store");
		// And it reads: the layers it was given hold what the old store did.
		let (snap, _, ing) = replica.snapshot(newest).expect("held");
		assert_eq!(ing.len(), 1);
		drop(snap);
		assert_eq!(Arc::strong_count(&old), 1, "nothing pins the old store");
	}

	#[test]
	fn a_swap_reports_the_bytes_it_swapped_and_a_meta_that_does_not_parse_loses_nothing() {
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let all = commits(29, 30);
		let (run, entries) = run_of(&all);
		futures::executor::block_on(store.put("t/log/run-1.b", &run, PutCondition::None)).expect("put");
		let replica = replica_over(&store);
		replica.set_rebuild_floor(0);
		for frame in frames_of(&all, 20) {
			replica.apply_frame(&frame, u64::MAX).expect("apply");
		}
		let secs: Vec<(u64, u32, u32)> = entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect();
		let held = replica.live_bytes();
		assert_eq!(held, secs.iter().map(|s| u64::from(s.2)).sum::<u64>());
		// One meta in the held map is not a meta: the swap refuses, and every meta is still held, so
		// the same notice can be tried again (taking them first would have lost them).
		let bad_pos = secs[10].0;
		let good = replica.live.lock().expect("live").insert(bad_pos, (secs[10].2, Box::from(&b"garbage"[..]))).expect("held");
		assert!(replica.roll_swap("t/log/run-1.b", &secs).is_err());
		assert_eq!((replica.live_sections(), replica.layer_sections()), (30, 0));
		replica.live.lock().expect("live").insert(bad_pos, good);
		// The notice swaps what it names, and says how many bytes: what the node stops holding.
		let done = replica.roll_swap("t/log/run-1.b", &secs[..10]).expect("swap");
		assert_eq!((done.layers, done.missing), (10, 0));
		assert_eq!(done.bytes, secs[..10].iter().map(|s| u64::from(s.2)).sum::<u64>());
		assert_eq!(replica.live_bytes(), held - done.bytes);
		// The same notice again swaps nothing and frees no bytes (a node must not count them twice).
		let again = replica.roll_swap("t/log/run-1.b", &secs[..10]).expect("again");
		assert_eq!((again.layers, again.missing, again.bytes), (0, 10, 0));
		// A notice for a section named twice swaps it once.
		let twice = replica.roll_swap("t/log/run-1.b", &[secs[10], secs[10]]).expect("twice");
		assert_eq!((twice.layers, twice.bytes), (1, u64::from(secs[10].2)));
	}

	#[test]
	fn a_swap_replaces_the_store_so_the_entries_it_swapped_are_not_kept_by_it() {
		// The in-memory store keeps what is deleted from it until its sweeps reach it (and wasm has no
		// background sweep): a roll swaps the store itself for one holding the unrolled tail. A native
		// run cannot see the memory come back (the store's background threads hold it), so this
		// checks the replacement and what it holds.
		let all: Vec<(u64, Writes)> = (0..40u64)
			.map(|c| (21 + c, (0..60u32).map(|i| (key(((c * 60) as u32 + i) % N), Some(vec![c as u8; 4096]))).collect()))
			.collect();
		let secs: Vec<(u64, u32, u32)> = {
			let (_, entries) = run_of(&all);
			entries.iter().map(|(p, o, l, _)| (*p, *o, *l)).collect()
		};
		let frames = frames_of(&all, 20);
		crate::set_heap_budget(u64::MAX);
		let (store, _) = base_store();
		let replica = replica_over(&store);
		replica.set_rebuild_floor(0);
		for f in &frames {
			replica.apply_frame(f, u64::MAX).expect("apply");
		}
		assert_eq!(replica.suffix_len().expect("len"), 2400);
		let old = replica.suffix();
		// The first 30 commits roll; the last 10 are still the unrolled tail.
		let swapped = replica.roll_swap("t/log/run-1.b", &secs[..30]).expect("swap");
		assert_eq!((swapped.layers, swapped.missing, swapped.freed), (30, 0, 1800));
		let now = replica.suffix();
		assert!(!Arc::ptr_eq(&old, &now), "the store was replaced");
		assert_eq!(replica.suffix_len().expect("len"), 600, "only the unrolled tail is in it");
		assert_eq!((replica.live_sections(), replica.layer_sections()), (10, 30));
		// A notice for what was swapped already, and a seal covering nothing held, copy nothing.
		assert_eq!(replica.roll_swap("t/log/run-1.b", &secs[..30]).expect("again").layers, 0);
		replica.rebuild_suffix(|pos| pos > 1_000_000).expect("nothing to drop");
		assert!(Arc::ptr_eq(&now, &replica.suffix()), "the store stayed");
		// A reader that began on the old store still reads what it did (it holds its own).
		futures::executor::block_on(AssertSend(async {
			let tx = old.transaction(TransactionType::Read).await.expect("old");
			let rows = tx.scan(KeyRange::from(vec![]..vec![0xff; 16]), 10_000, 0, None).await.expect("scan").values;
			assert_eq!(rows.len(), 2400);
		}));
	}
}
