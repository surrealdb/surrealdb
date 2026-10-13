//! Wake-ups a committing transaction fires, and the count of its datastore's
//! catalog commits ([`CatalogEpoch`]).
//!
//! The wake-ups are notified after a commit makes the relevant work visible,
//! never before. They are bundled behind one `Arc` rather than carried as separate
//! fields because `TransactionFactory` is held by value inside
//! `IndexBuildReservationRelease`, and `Transaction` is held by value inside
//! large async futures: adding a second pointer to either grew those futures
//! enough to overflow the test stack in debug builds. One `Arc` keeps both
//! types the size they were.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::values::graph::GraphFoldScope;

/// How many indexes' guard fences are held before the ones nobody holds or
/// awaits are dropped. Dropping one is harmless: a fence orders only the holders
/// and waiters it has, and the next caller for that index starts a fresh one,
/// whose first commit reads the guards the dropped fence knew to be armed.
/// After a prune, the next one waits until the map has doubled from what the
/// prune kept, so the fences it cannot drop are not scanned on every new index.
const PENDING_GUARD_FENCE_PRUNE_AT: usize = 1024;

/// How many read-observed hot adjacency scopes are held before further
/// observations are dropped. Dropping is harmless: the scope stays hot, so
/// a later traversal re-observes it, and deletions reach the fold queue
/// through the durable commit-time path instead.
const GRAPH_FOLD_OBSERVATION_CAP: usize = 4096;

/// How many commits of transactions that stamped a catalog version have
/// started and finished through one datastore.
///
/// Where the datastore is the store's sole writer
/// ([`surrealdb_kvs::Transactable::sole_writer`]), the catalog a transaction
/// sees is named by the count of such commits finished before it opened, as
/// long as none was under way when it opened and none has started since
/// ([`crate::tx::Transaction::catalog_epoch`]): every catalog write stamps a
/// version, so a commit that changes the catalog is one of these.
#[derive(Default)]
pub(crate) struct CatalogEpoch {
	started: AtomicU64,
	finished: AtomicU64,
}

impl CatalogEpoch {
	/// The count of finished catalog commits, where none is under way.
	///
	/// `finished` is read first: a commit under way when it is read has
	/// started by the time `started` is, so the two differ.
	pub(crate) fn settled(&self) -> Option<u64> {
		let finished = self.finished.load(Ordering::SeqCst);
		let started = self.started.load(Ordering::SeqCst);
		(started == finished).then_some(finished)
	}

	/// Count a catalog commit as under way until the guard is dropped, which
	/// the caller does once the commit has returned, whether or not it took.
	pub(crate) fn committing(&self) -> CatalogCommit<'_> {
		self.started.fetch_add(1, Ordering::SeqCst);
		CatalogCommit(self)
	}
}

/// A catalog commit under way; see [`CatalogEpoch::committing`].
pub(crate) struct CatalogCommit<'a>(&'a CatalogEpoch);

impl Drop for CatalogCommit<'_> {
	fn drop(&mut self) {
		self.0.finished.fetch_add(1, Ordering::SeqCst);
	}
}

/// Per-index fences ordering the arming of DiskANN pending-state guards
/// against the steps that return them to `Empty`, and what this datastore knows
/// of those guards.
///
/// A committing transaction that wrote pending updates holds its indexes'
/// fences shared from before it arms their guards until its own commit has
/// returned ([`crate::tx::Transaction::commit`]). A guard step holds the
/// index's fence exclusively from before its transaction opens until it
/// commits ([`crate::tx::Transaction::step_diskann_pending_guards`]), and so
/// do a commit that wipes the index's data and the release of this datastore's
/// writer leases. So no step of this datastore commits between a writer's
/// arming and its commit, and a step that waited reads a snapshot holding every
/// writer that held the fence before it.
///
/// Where the datastore is the store's sole writer
/// ([`surrealdb_kvs::Transactable::sole_writer`]), that ordering covers every
/// writer, and each fence also records which of its index's guards are known to
/// read `NonEmpty`, so a commit into such a shard arms nothing and reads
/// nothing. A shard is recorded only while the fence is held shared, once the
/// guard has been read `NonEmpty` or armed, and forgotten while it is held
/// exclusively by whatever may change the guard back: a step, or a wipe of the
/// index's data.
///
/// Elsewhere other datastores write to the store too, and a writer's guard is
/// covered by a writer lease (`!dz`) its datastore holds instead. The fence then
/// records which leases this datastore holds, and when it last committed under
/// them, so an idle index's leases can be released.
#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
pub(crate) struct PendingGuardFences {
	fences: Mutex<FenceMap>,
	/// This datastore instance, as its writer leases name it.
	process: uuid::Uuid,
	/// Whether this datastore runs the writer-lease protocol on a store it is
	/// the sole writer of, as the tests of that protocol need.
	#[cfg(any(test, feature = "test-hooks"))]
	assume_shared_store: std::sync::atomic::AtomicBool,
	/// Whether this datastore covers DiskANN guards as on a store that
	/// validates every read, as the tests of that path need.
	#[cfg(any(test, feature = "test-hooks"))]
	assume_validated_reads: std::sync::atomic::AtomicBool,
	/// How many guards compaction found `Empty` over pending entries and
	/// repaired, which the tests of the covering protocol expect to stay zero.
	#[cfg(any(test, feature = "test-hooks"))]
	repairs: std::sync::atomic::AtomicUsize,
}

/// The fences by the encoded `!dy` prefix of their index, and the size at
/// which they are next pruned.
#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
struct FenceMap {
	fences: HashMap<Vec<u8>, Arc<IndexFence>>,
	prune_at: usize,
}

impl Default for FenceMap {
	fn default() -> Self {
		Self {
			fences: HashMap::new(),
			prune_at: PENDING_GUARD_FENCE_PRUNE_AT,
		}
	}
}

/// One index's fence, the guards it knows to read `NonEmpty`, and the writer
/// leases this datastore holds on the index.
#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
struct IndexFence {
	/// The index this fence orders.
	index: crate::values::index_delta::BufferedIndex,
	lock: Arc<RwLock<()>>,
	/// Bit `shard` is set while the shard's guard is known to read `NonEmpty`.
	armed: AtomicU64,
	/// The writer leases this datastore holds on the index.
	leases: Mutex<HeldLeases>,
}

/// The writer leases a datastore holds on one index.
#[derive(Default)]
pub(crate) struct HeldLeases {
	/// Bit `shard` is set while this datastore holds a lease on the shard.
	pub(crate) shards: u64,
	/// When a commit last relied on them.
	pub(crate) last_used: Option<web_time::Instant>,
}

const _: () = assert!(
	crate::values::diskann::DISKANN_PENDING_STATE_SHARDS as u32 <= u64::BITS,
	"every guard shard needs a bit of `IndexFence::armed`"
);

/// An index's fence held shared, by a committing writer.
#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
pub(crate) struct SharedGuardFence {
	fence: Arc<IndexFence>,
	_held: OwnedRwLockReadGuard<()>,
}

#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
impl SharedGuardFence {
	/// Whether the shard's guard is known to read `NonEmpty`.
	pub(crate) fn armed(&self, shard: u16) -> bool {
		self.fence.armed.load(Ordering::Acquire) & (1 << shard) != 0
	}

	/// Record that the shard's guard reads `NonEmpty`: it was read so, or
	/// armed, while this fence was held.
	pub(crate) fn mark_armed(&self, shard: u16) {
		self.fence.armed.fetch_or(1 << shard, Ordering::AcqRel);
	}

	/// The shards of `shards` this datastore holds writer leases on. A release
	/// forgets its shards under the exclusive fence, so a commit holding the
	/// fence shared relies on a lease only while this still names it.
	pub(crate) fn holds_leases(&self, shards: u64) -> u64 {
		self.fence.leases.lock().shards & shards
	}

	/// Record that a commit relies on this datastore's leases on `shards`,
	/// which it holds from now on.
	pub(crate) fn use_leases(&self, shards: u64) {
		let mut leases = self.fence.leases.lock();
		leases.shards |= shards;
		leases.last_used = Some(web_time::Instant::now());
	}
}

/// An index's fence held exclusively, by a guard step, a wipe or a lease
/// release.
pub(crate) struct ExclusiveGuardFence {
	fence: Arc<IndexFence>,
	_held: OwnedRwLockWriteGuard<()>,
}

#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
impl ExclusiveGuardFence {
	/// The index this fence orders.
	pub(crate) fn index(&self) -> &crate::values::index_delta::BufferedIndex {
		&self.fence.index
	}

	/// Forget what is known of the shard's guard, because it may have changed.
	pub(crate) fn forget(&self, shard: u16) {
		self.fence.armed.fetch_and(!(1 << shard), Ordering::AcqRel);
	}

	/// Forget what is known of every guard of the index, and every lease held
	/// on it, because the index's data was wiped.
	pub(crate) fn forget_all(&self) {
		self.fence.armed.store(0, Ordering::Release);
		*self.fence.leases.lock() = HeldLeases::default();
	}

	/// The leases this datastore holds on the index, if no commit has relied on
	/// them for `idle`.
	pub(crate) fn idle_leases(&self, idle: std::time::Duration) -> u64 {
		let leases = self.fence.leases.lock();
		match leases.last_used {
			Some(used) if used.elapsed() >= idle => leases.shards,
			_ => 0,
		}
	}

	/// Record that this datastore no longer holds leases on `shards`.
	pub(crate) fn release_leases(&self, shards: u64) {
		let mut leases = self.fence.leases.lock();
		leases.shards &= !shards;
		if leases.shards == 0 {
			*leases = HeldLeases::default();
		}
	}
}

impl Default for PendingGuardFences {
	fn default() -> Self {
		Self {
			fences: Mutex::default(),
			process: uuid::Uuid::new_v4(),
			#[cfg(any(test, feature = "test-hooks"))]
			assume_shared_store: std::sync::atomic::AtomicBool::new(false),
			#[cfg(any(test, feature = "test-hooks"))]
			assume_validated_reads: std::sync::atomic::AtomicBool::new(false),
			#[cfg(any(test, feature = "test-hooks"))]
			repairs: std::sync::atomic::AtomicUsize::new(0),
		}
	}
}

#[cfg_attr(not(diskann), allow(dead_code, reason = "only DiskANN pending-state guards are fenced"))]
impl PendingGuardFences {
	/// This datastore instance, as its writer leases name it.
	pub(crate) fn process(&self) -> uuid::Uuid {
		self.process
	}

	/// Whether this datastore runs the writer-lease protocol although the store
	/// reports it the sole writer.
	pub(crate) fn assumes_shared_store(&self) -> bool {
		#[cfg(any(test, feature = "test-hooks"))]
		{
			self.assume_shared_store.load(Ordering::Acquire)
		}
		#[cfg(not(any(test, feature = "test-hooks")))]
		{
			false
		}
	}

	/// Count guards repaired by compaction, for the tests.
	#[cfg(any(test, feature = "test-hooks"))]
	pub(crate) fn count_repairs(&self, n: usize) {
		self.repairs.fetch_add(n, Ordering::AcqRel);
	}

	/// Count guards repaired by compaction, which only the tests read.
	#[cfg(not(any(test, feature = "test-hooks")))]
	pub(crate) fn count_repairs(&self, _n: usize) {}

	/// Whether this datastore covers DiskANN guards as on a store that
	/// validates every read, although the store does not.
	pub(crate) fn assumes_validated_reads(&self) -> bool {
		#[cfg(any(test, feature = "test-hooks"))]
		{
			self.assume_validated_reads.load(Ordering::Acquire)
		}
		#[cfg(not(any(test, feature = "test-hooks")))]
		{
			false
		}
	}

	/// The fence of the index whose `!dy` prefix encodes to `prefix`.
	fn fence(
		&self,
		prefix: &[u8],
		index: impl FnOnce() -> crate::values::index_delta::BufferedIndex,
	) -> Arc<IndexFence> {
		let mut map = self.fences.lock();
		if let Some(fence) = map.fences.get(prefix) {
			return Arc::clone(fence);
		}
		if map.fences.len() >= map.prune_at {
			// The map holds the only reference to a fence nobody holds or awaits.
			// Dropping one forgets what it knew of its guards, and of no lease
			// still held: a fence holding one is kept.
			map.fences
				.retain(|_, fence| Arc::strong_count(fence) > 1 || fence.leases.lock().shards != 0);
			map.prune_at = PENDING_GUARD_FENCE_PRUNE_AT.max(map.fences.len() * 2);
		}
		Arc::clone(map.fences.entry(prefix.to_vec()).or_insert_with(|| {
			Arc::new(IndexFence {
				index: index(),
				lock: Arc::default(),
				armed: AtomicU64::new(0),
				leases: Mutex::default(),
			})
		}))
	}

	/// Hold the index's fence shared, as a committing writer does.
	pub(crate) async fn shared(
		&self,
		prefix: &[u8],
		index: impl FnOnce() -> crate::values::index_delta::BufferedIndex,
	) -> SharedGuardFence {
		let fence = self.fence(prefix, index);
		let held = Arc::clone(&fence.lock).read_owned().await;
		SharedGuardFence {
			fence,
			_held: held,
		}
	}

	/// Hold the index's fence exclusively, as a guard step, a wipe or a lease
	/// release does.
	pub(crate) async fn exclusive(
		&self,
		prefix: &[u8],
		index: impl FnOnce() -> crate::values::index_delta::BufferedIndex,
	) -> ExclusiveGuardFence {
		let fence = self.fence(prefix, index);
		let held = Arc::clone(&fence.lock).write_owned().await;
		ExclusiveGuardFence {
			fence,
			_held: held,
		}
	}

	/// The fences holding leases nothing has relied on for `idle`, by prefix,
	/// each with its index.
	pub(crate) fn idle_lease_fences(
		&self,
		idle: std::time::Duration,
	) -> Vec<(Vec<u8>, crate::values::index_delta::BufferedIndex)> {
		self.fences
			.lock()
			.fences
			.iter()
			.filter(|(_, fence)| {
				let leases = fence.leases.lock();
				leases.shards != 0 && leases.last_used.is_some_and(|used| used.elapsed() >= idle)
			})
			.map(|(prefix, fence)| (prefix.clone(), fence.index.clone()))
			.collect()
	}
}

/// The set of post-commit wake-ups shared by a datastore and its transactions.
#[derive(Default)]
pub struct CommitTriggers {
	/// The catalog commits made through the datastore.
	pub(crate) catalog_epoch: CatalogEpoch,
	/// The fences between DiskANN pending-state guard arming and stepping.
	pub(crate) pending_guard_fences: PendingGuardFences,
	/// Async event processing, notified when a commit queues events.
	pub async_event: Notify,
	/// Index compaction, notified when a commit queues compaction work so the
	/// compactor runs on the write instead of waiting out its interval.
	pub index_compaction: Notify,
	/// Graph adjacency folding, notified when a commit queues fold work or a
	/// read observes a scope over the fold threshold.
	pub graph_fold: Notify,
	/// Adjacency scopes a read found carrying enough unfolded keys to be
	/// worth packing. In memory because reads cannot write a queue entry;
	/// the fold task drains this set into the durable queue on its pass.
	graph_fold_observations: Mutex<HashSet<GraphFoldScope>>,
	/// Work the next commit through this datastore runs once its DiskANN guards
	/// are covered and before its storage commit, as the tests of a commit that
	/// stalls there need.
	#[cfg(any(test, feature = "test-hooks"))]
	before_commit: Mutex<Option<BeforeCommitHook>>,
}

/// Work a test runs inside a commit, between its covering of DiskANN guards and
/// its storage commit ([`CommitTriggers::set_before_commit_hook`]).
#[cfg(any(test, feature = "test-hooks"))]
pub type BeforeCommitHook =
	Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

impl CommitTriggers {
	pub fn new() -> Self {
		Self::default()
	}

	/// Run the DiskANN writer-lease protocol, which a store this datastore is
	/// not the sole writer of needs, although the store reports it the sole
	/// writer. For the tests of that protocol, which run two datastores over
	/// one in-memory store.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn assume_shared_store(&self) {
		self.pending_guard_fences.assume_shared_store.store(true, Ordering::Release);
	}

	/// Cover DiskANN guards as on a store that validates every read
	/// ([`surrealdb_kvs::api::Transactable::validates_reads`]), although the
	/// store does not. For the tests of that path.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn assume_validated_reads(&self) {
		self.pending_guard_fences.assume_validated_reads.store(true, Ordering::Release);
	}

	/// How many DiskANN guards this datastore's compaction has repaired
	/// (`Transaction::repair_diskann_pending_guards`).
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn diskann_guard_repairs(&self) -> usize {
		self.pending_guard_fences.repairs.load(Ordering::Acquire)
	}

	/// Have the next commit through this datastore run `hook` once its DiskANN
	/// guards are covered, before its storage commit.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn set_before_commit_hook(&self, hook: BeforeCommitHook) {
		*self.before_commit.lock() = Some(hook);
	}

	/// The hook a test set for the next commit, taken so it runs once.
	#[cfg(any(test, feature = "test-hooks"))]
	pub(crate) fn take_before_commit_hook(&self) -> Option<BeforeCommitHook> {
		self.before_commit.lock().take()
	}

	/// Record a hot adjacency scope observed by a read, waking the fold task.
	pub fn observe_graph_fold(&self, scope: GraphFoldScope) {
		{
			let mut observations = self.graph_fold_observations.lock();
			if observations.len() >= GRAPH_FOLD_OBSERVATION_CAP {
				return;
			}
			observations.insert(scope);
		}
		self.graph_fold.notify_one();
	}

	/// Drain the read-observed scopes for the fold task's pass.
	pub fn drain_graph_fold_observations(&self) -> Vec<GraphFoldScope> {
		self.graph_fold_observations.lock().drain().collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::values::index_delta::BufferedIndex;

	fn index(ix: u32) -> BufferedIndex {
		BufferedIndex {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: "t".into(),
			ix: IndexId(ix),
		}
	}

	fn prefix(ix: u32) -> Vec<u8> {
		ix.to_be_bytes().to_vec()
	}

	/// A prune drops the fences nobody holds and keeps those still holding
	/// leases, and the next one waits for the map to double from what it kept,
	/// so fences it cannot drop are not scanned again for every new index.
	#[test]
	fn a_prune_keeps_leased_fences_and_backs_off() {
		let fenced = PendingGuardFences::default();
		let leased = PENDING_GUARD_FENCE_PRUNE_AT as u32;
		for ix in 0..leased {
			fenced.fence(&prefix(ix), || index(ix)).leases.lock().shards = 1;
		}
		fenced.fence(&prefix(leased), || index(leased));
		{
			let map = fenced.fences.lock();
			assert_eq!(map.fences.len(), PENDING_GUARD_FENCE_PRUNE_AT + 1);
			assert_eq!(map.prune_at, 2 * PENDING_GUARD_FENCE_PRUNE_AT);
		}
		// Unheld fences accumulate until the map reaches the new threshold.
		let top = 2 * leased;
		for ix in leased + 1..top {
			fenced.fence(&prefix(ix), || index(ix));
		}
		assert_eq!(fenced.fences.lock().fences.len(), 2 * PENDING_GUARD_FENCE_PRUNE_AT);
		fenced.fence(&prefix(top), || index(top));
		let map = fenced.fences.lock();
		assert_eq!(
			map.fences.len(),
			PENDING_GUARD_FENCE_PRUNE_AT + 1,
			"only the leased fences and the new one remain"
		);
		assert!((0..leased).all(|ix| map.fences.contains_key(&prefix(ix))));
	}
}
