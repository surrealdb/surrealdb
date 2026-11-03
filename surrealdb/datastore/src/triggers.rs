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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

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

/// How many commits of transactions that wrote durable index build state
/// (`!bs`) have started and finished through one datastore.
///
/// Counted exactly as [`CatalogEpoch`] counts catalog commits, and named by a
/// transaction the same way ([`crate::tx::Transaction::build_epoch`]): where the
/// datastore is the store's sole writer, the build state a transaction sees is
/// named by the count of such commits finished before it opened, as long as none
/// was under way when it opened and none has started since. Every write of a
/// `!bs` key or index value shape key, and every range delete that may cover one, marks the
/// writing transaction ([`WrittenTables::wrote_build_state`]), so a phase change, a
/// generation rotation, an owner change, a heartbeat or a removal of an index's
/// build state is a commit of this kind.
#[derive(Default)]
pub(crate) struct BuildEpoch(CatalogEpoch);

impl BuildEpoch {
	/// The count of finished build-state commits, where none is under way.
	pub(crate) fn settled(&self) -> Option<u64> {
		self.0.settled()
	}

	/// Count a build-state commit as under way until the guard is dropped,
	/// which the caller does once the commit has returned, whether or not it
	/// took.
	pub(crate) fn committing(&self) -> CatalogCommit<'_> {
		self.0.committing()
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
/// commits ([`crate::tx::Transaction::step_pending_guards`]), and so
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
/// covered by a writer lease its datastore holds instead. The fence then
/// records which leases this datastore holds, and when it last committed under
/// them, so an idle index's leases can be released.
pub(crate) struct PendingGuardFences {
	fences: Mutex<FenceMap>,
	/// This datastore instance, as its writer leases name it.
	process: uuid::Uuid,
	/// Whether this datastore runs the writer-lease protocol on a store it is
	/// the sole writer of, as the tests of that protocol need.
	#[cfg(any(test, feature = "test-hooks"))]
	assume_shared_store: std::sync::atomic::AtomicBool,
	/// Whether this datastore covers pending-state guards as on a store that
	/// validates every read, as the tests of that path need.
	#[cfg(any(test, feature = "test-hooks"))]
	assume_validated_reads: std::sync::atomic::AtomicBool,
	/// Whether this datastore leaves its writes unfenced against later catalog changes, as on a
	/// store whose locked reads conflict with one another, as the tests of the write barrier's
	/// answers from other nodes need.
	#[cfg(any(test, feature = "test-hooks"))]
	assume_unfenced_catalog: std::sync::atomic::AtomicBool,
	/// How many guards compaction found `Empty` over pending entries and
	/// repaired, which the tests of the covering protocol expect to stay zero.
	#[cfg(any(test, feature = "test-hooks"))]
	repairs: std::sync::atomic::AtomicUsize,
}

/// The fences by the encoded `!dy` prefix of their index, and the size at
/// which they are next pruned.
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
struct IndexFence {
	/// The index this fence orders.
	index: crate::pending_guard::GuardedIndex,
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
	crate::pending_guard::PENDING_GUARD_SHARDS as u32 <= u64::BITS,
	"every guard shard needs a bit of `IndexFence::armed`"
);

/// An index's fence held shared, by a committing writer.
pub(crate) struct SharedGuardFence {
	fence: Arc<IndexFence>,
	_held: OwnedRwLockReadGuard<()>,
}

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

impl ExclusiveGuardFence {
	/// The index this fence orders.
	pub(crate) fn index(&self) -> &crate::pending_guard::GuardedIndex {
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
			assume_unfenced_catalog: std::sync::atomic::AtomicBool::new(false),
			#[cfg(any(test, feature = "test-hooks"))]
			repairs: std::sync::atomic::AtomicUsize::new(0),
		}
	}
}

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

	/// Whether this datastore covers pending-state guards as on a store that
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

	/// Whether this datastore leaves its writes unfenced against later catalog changes although
	/// the store would fence them ([`crate::tx::Transaction::fences_catalog_changes`]).
	pub(crate) fn assumes_unfenced_catalog(&self) -> bool {
		#[cfg(any(test, feature = "test-hooks"))]
		{
			self.assume_unfenced_catalog.load(Ordering::Acquire)
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
		index: impl FnOnce() -> crate::pending_guard::GuardedIndex,
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
		index: impl FnOnce() -> crate::pending_guard::GuardedIndex,
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
		index: impl FnOnce() -> crate::pending_guard::GuardedIndex,
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
	) -> Vec<(Vec<u8>, crate::pending_guard::GuardedIndex)> {
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

	/// How long until a writer lease this datastore holds will have had no
	/// commit rely on it for `idle`: zero for one that already has, `None`
	/// while it holds none. Reads only this datastore's memory.
	pub(crate) fn next_idle_lease(&self, idle: std::time::Duration) -> Option<std::time::Duration> {
		// The fences are copied out under the map's lock and their leases read after it is
		// released, so a call holds the map only as long as it takes to clone its handles.
		let fences: Vec<Arc<IndexFence>> = self.fences.lock().fences.values().cloned().collect();
		fences
			.iter()
			.filter_map(|fence| {
				let leases = fence.leases.lock();
				if leases.shards == 0 {
					return None;
				}
				Some(
					leases.last_used.map_or(std::time::Duration::ZERO, |used| {
						idle.saturating_sub(used.elapsed())
					}),
				)
			})
			.min()
	}
}

/// The set of post-commit wake-ups shared by a datastore and its transactions.
#[derive(Default)]
pub struct CommitTriggers {
	/// The catalog commits made through the datastore.
	pub(crate) catalog_epoch: CatalogEpoch,
	/// The commits of durable index build state made through the datastore.
	pub(crate) build_epoch: BuildEpoch,
	/// The catalog definitions read under each catalog epoch
	/// ([`crate::cache::epoch`]).
	pub(crate) epoch_catalog: crate::cache::epoch::EpochCatalog,
	/// The fences between DiskANN pending-state guard arming and stepping.
	pub(crate) pending_guard_fences: PendingGuardFences,
	/// Async event processing, notified when a commit queues events.
	pub async_event: Notify,
	/// How many commits have queued async events on this datastore, counted
	/// as each such commit notifies. A drain compares the count at the end of
	/// a lap over its queue with the count the lap began at: a higher count
	/// means an entry was queued during the lap — by an event the lap ran, or
	/// by a concurrent write — possibly behind the scan's position, so the
	/// queue is not at rest.
	async_events_queued: AtomicU64,
	/// Bounded index-build passes, notified when a statement leaves an index
	/// build for them to run.
	pub index_build: Notify,
	/// Index compaction, notified when a commit queues compaction work so the
	/// compactor runs on the write instead of waiting out its interval.
	pub index_compaction: Notify,
	/// Graph adjacency folding, notified when a commit queues fold work or a
	/// read observes a scope over the fold threshold.
	pub graph_fold: Notify,
	/// Table migration work, notified when a commit queues a table's rewrite
	/// or verification so the job starts on the commit instead of waiting
	/// out its interval.
	pub table_migration: Notify,
	/// The reclaim of removed namespaces, databases and indexes, notified when a commit queues
	/// a reclaim entry.
	pub reclaim: Notify,
	/// The tables a transaction open through the datastore is removing, whose rows the table
	/// migration job leaves alone until that transaction ends; see [`TableRemovals`].
	pub(crate) table_removals: TableRemovals,
	/// The transactions open through the datastore, by the epoch they opened
	/// in; see [`LiveTransactions`].
	pub(crate) live: LiveTransactions,
	/// The write transactions open through the datastore, by the epoch they
	/// opened in; see [`LiveWrites`].
	pub(crate) live_writes: LiveWrites,
	/// The live-query registrations a write transaction open through the
	/// datastore may have missed; see [`LiveRegistrations`].
	pub live_registrations: LiveRegistrations,
	/// When this datastore last recorded its heartbeat, for the fence that stops a datastore
	/// whose heartbeat has lapsed from committing writes to a store it shares; see
	/// [`WriteFence`].
	pub(crate) write_fence: WriteFence,
	/// Adjacency scopes a read found carrying enough unfolded keys to be
	/// worth packing. In memory because reads cannot write a queue entry;
	/// a flush drains this set into the durable queue: the fold drain's own
	/// at its start, or one a host runs on a datastore that does not drain.
	graph_fold_observations: Mutex<HashSet<GraphFoldScope>>,
	/// Work the next commit through this datastore runs once its pending-state guards
	/// are covered and before its storage commit, as the tests of a commit that
	/// stalls there need.
	#[cfg(any(test, feature = "test-hooks"))]
	before_commit: Mutex<Option<BeforeCommitHook>>,
	/// Work the next pending-state guard step through this datastore runs once it has
	/// read and written what it steps, before its commit, as the tests of a
	/// covering that lands inside a step need.
	#[cfg(any(test, feature = "test-hooks"))]
	before_guard_step_commit: Mutex<Option<BeforeCommitHook>>,
	/// Whether the next commit through this datastore that has rollback
	/// actions registered reports its outcome unknown once it has applied, as
	/// the tests of a commit that applied but could not say so need, with the
	/// work to run between the commit applying and the report.
	#[cfg(any(test, feature = "test-hooks"))]
	commit_applied_unknown: Mutex<Option<Option<BeforeCommitHook>>>,
}

/// Work a test runs inside a commit, between its covering of pending-state guards and
/// its storage commit ([`CommitTriggers::set_before_commit_hook`]).
#[cfg(any(test, feature = "test-hooks"))]
pub type BeforeCommitHook =
	Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

impl CommitTriggers {
	pub fn new() -> Self {
		Self::default()
	}

	/// Count one commit that queued async events, after the commit.
	pub fn async_event_queued(&self) {
		self.async_events_queued.fetch_add(1, Ordering::Release);
	}

	/// How many commits have queued async events so far.
	pub fn async_events_queued(&self) -> u64 {
		self.async_events_queued.load(Ordering::Acquire)
	}

	/// Run the DiskANN writer-lease protocol, which a store this datastore is
	/// not the sole writer of needs, although the store reports it the sole
	/// writer. For the tests of that protocol, which run two datastores over
	/// one in-memory store.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn assume_shared_store(&self) {
		self.pending_guard_fences.assume_shared_store.store(true, Ordering::Release);
	}

	/// Cover pending-state guards as on a store that validates every read
	/// ([`surrealdb_kvs::api::Transactable::validates_reads`]), although the
	/// store does not. For the tests of that path.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn assume_validated_reads(&self) {
		self.pending_guard_fences.assume_validated_reads.store(true, Ordering::Release);
	}

	/// Leave this datastore's writes unfenced against later catalog changes, as on a store whose
	/// locked reads conflict with one another, although the store would fence them
	/// ([`crate::tx::Transaction::fences_catalog_changes`]). For the tests of the write barrier's
	/// answers from other nodes, which such a store relies on.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn assume_unfenced_catalog(&self) {
		self.pending_guard_fences.assume_unfenced_catalog.store(true, Ordering::Release);
	}

	/// How many pending-state guards this datastore's compaction has repaired
	/// (`Transaction::repair_pending_guards`).
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn pending_guard_repairs(&self) -> usize {
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

	/// Have the next pending-state guard step through this datastore run `hook` once
	/// it has written its steps, before it commits
	/// (`Transaction::step_pending_guards`).
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn set_before_guard_step_commit_hook(&self, hook: BeforeCommitHook) {
		*self.before_guard_step_commit.lock() = Some(hook);
	}

	/// The hook a test set for the next guard step, taken so it runs once.
	#[cfg(any(test, feature = "test-hooks"))]
	pub(crate) fn take_before_guard_step_commit_hook(&self) -> Option<BeforeCommitHook> {
		self.before_guard_step_commit.lock().take()
	}

	/// Have the next commit through this datastore that has rollback actions
	/// registered, a statement's, apply, run `between` if given, and then
	/// report `CommitOutcomeUnknown`, running its rollback actions as such a
	/// failure does.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn set_next_commit_applied_unknown(&self, between: Option<BeforeCommitHook>) {
		*self.commit_applied_unknown.lock() = Some(between);
	}

	/// What a test asked of the next such commit, taken so it applies once.
	#[cfg(any(test, feature = "test-hooks"))]
	pub(crate) fn take_commit_applied_unknown(&self) -> Option<Option<BeforeCommitHook>> {
		self.commit_applied_unknown.lock().take()
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

	/// The live-transaction epochs of the datastore; see
	/// [`LiveTransactions`].
	pub fn live_transactions(&self) -> &LiveTransactions {
		&self.live
	}

	/// The write transactions open through the datastore; see [`LiveWrites`].
	pub fn live_writes(&self) -> &LiveWrites {
		&self.live_writes
	}

	/// The heartbeat fence on this datastore's writes; see [`WriteFence`].
	pub fn write_fence(&self) -> &WriteFence {
		&self.write_fence
	}

	/// The tables being removed by transactions open through the datastore; see
	/// [`TableRemovals`].
	pub fn table_removals(&self) -> &TableRemovals {
		&self.table_removals
	}

	/// Drain the read-observed scopes, for a flush into the durable queue.
	pub fn drain_graph_fold_observations(&self) -> Vec<GraphFoldScope> {
		self.graph_fold_observations.lock().drain().collect()
	}
}

/// The tables that transactions open through one datastore are removing, each until its
/// removing transaction ends, so the table migration job does not rewrite their rows meanwhile.
///
/// `REMOVE TABLE` deletes the table's rows key by key inside its own transaction, and every row a
/// page of the job rewrites after that transaction's snapshot is a key both wrote: whichever
/// commits second fails. A page is short and the removal of a large table is not, so while the job
/// commits pages the removal loses every attempt. A removal therefore registers its table here
/// before it reads or deletes anything of it ([`Self::begin`]), and the job commits a page that
/// rewrites rows only while holding a [`TableRemovalGate`] for the table ([`Self::admit`]), which
/// is refused while a removal of the table is registered. Registration waits for the table's gate,
/// held, to be dropped, so no page commits once it returns.
///
/// A page that committed between the removing transaction's snapshot and its registration still
/// fails that transaction's commit, which is then run again or reported as a retryable conflict.
/// The registrations live in the datastore that runs the removal: a page committed through another
/// datastore on a shared store is not held back by them.
#[derive(Default)]
pub struct TableRemovals {
	tables: Mutex<HashMap<FencedTable, TableRemovalEntry>>,
}

/// One table's registrations and gate. Kept while a removal is registered or the gate is held or
/// waited for, and dropped once neither is, so tables never removed cost nothing.
#[derive(Default)]
struct TableRemovalEntry {
	/// How many open transactions are removing the table.
	removing: usize,
	/// Held by the job across the commit of a page that rewrites the table's rows, and by a
	/// registration while it records the table, so only work on the same table waits on it.
	gate: Arc<tokio::sync::Mutex<()>>,
}

/// Leave to commit a page of the table migration job that rewrites a table's rows: no removal of
/// the table is registered until it is dropped ([`TableRemovals::admit`]).
pub struct TableRemovalGate<'a> {
	removals: &'a TableRemovals,
	table: FencedTable,
	gate: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for TableRemovalGate<'_> {
	fn drop(&mut self) {
		self.gate.take();
		self.removals.tidy(&self.table);
	}
}

impl TableRemovals {
	/// Register a removal of `table`, once any page of the job committing meanwhile for the table
	/// has committed. Every registration is ended by exactly one [`Self::end`].
	pub async fn begin(&self, table: FencedTable) {
		let gate = self.gate(&table);
		let _gate = gate.lock().await;
		// The entry stands while `gate` is held here ([`Self::tidy`]).
		self.tables.lock().entry(table).or_default().removing += 1;
	}

	/// End one registration of a removal of `table`: its transaction has committed, rolled back
	/// or been dropped.
	pub fn end(&self, table: &FencedTable) {
		if let Some(entry) = self.tables.lock().get_mut(table) {
			entry.removing = entry.removing.saturating_sub(1);
		}
		self.tidy(table);
	}

	/// Whether a removal of `table` is registered now.
	pub fn removing(&self, table: &FencedTable) -> bool {
		self.tables.lock().get(table).is_some_and(|entry| entry.removing > 0)
	}

	/// Leave to commit a page that rewrites rows of `table`, held until the page's commit has
	/// returned, or `None` while a removal of the table is registered.
	pub async fn admit(&self, table: &FencedTable) -> Option<TableRemovalGate<'_>> {
		let gate = self.gate(table).lock_owned().await;
		let admitted = TableRemovalGate {
			removals: self,
			table: table.clone(),
			gate: Some(gate),
		};
		(!self.removing(table)).then_some(admitted)
	}

	/// The gate of `table`, its entry made if it has none.
	fn gate(&self, table: &FencedTable) -> Arc<tokio::sync::Mutex<()>> {
		Arc::clone(&self.tables.lock().entry(table.clone()).or_default().gate)
	}

	/// Drop the entry of `table` if no removal is registered and nothing holds or waits for its
	/// gate, every one of which holds a reference to it.
	fn tidy(&self, table: &FencedTable) {
		let mut tables = self.tables.lock();
		if tables
			.get(table)
			.is_some_and(|entry| entry.removing == 0 && Arc::strong_count(&entry.gate) == 1)
		{
			tables.remove(table);
		}
	}
}

/// The transactions open through one datastore, counted by the epoch they
/// opened in, so a caller can wait until every transaction that opened before
/// a moment has ended.
///
/// Two buckets alternate. A transaction counts itself into the bucket of the
/// current epoch before its snapshot is taken and out again when it is
/// dropped. [`Self::advance`] moves new transactions to the other bucket,
/// once that bucket is empty, and answers the bucket it left; once
/// [`Self::drained`] reports that bucket empty, every transaction whose
/// snapshot was taken before the advance has ended.
///
/// A transaction that read the epoch just before an advance and counts itself
/// into the bucket left behind is waited for too; one that counts itself
/// late into the other bucket takes its snapshot after the advance. Either
/// way no transaction whose snapshot predates the advance is missed, provided
/// a single caller advances and waits for each bucket to drain before it
/// advances again: the table migration job, under its lease.
#[derive(Default)]
pub struct LiveTransactions {
	epoch: AtomicU64,
	counts: [AtomicUsize; 2],
}

/// The bucket a transaction that counts nothing is in.
pub(crate) const NO_LIVE_BUCKET: u8 = u8::MAX;

impl LiveTransactions {
	/// Count a transaction about to open into the current epoch, answering
	/// the bucket to hand to [`Self::close`].
	pub(crate) fn open(&self) -> u8 {
		let bucket = (self.epoch.load(Ordering::SeqCst) % 2) as usize;
		self.counts[bucket].fetch_add(1, Ordering::SeqCst);
		bucket as u8
	}

	/// Count a transaction counted into `bucket` out again.
	pub(crate) fn close(&self, bucket: u8) {
		if let Some(count) = self.counts.get(bucket as usize) {
			count.fetch_sub(1, Ordering::SeqCst);
		}
	}

	/// Move new transactions to the next epoch, answering the bucket of the
	/// epoch left behind; `None`, advancing nothing, while transactions of
	/// the epoch before it are still open.
	pub fn advance(&self) -> Option<u8> {
		let epoch = self.epoch.load(Ordering::SeqCst);
		let next = ((epoch + 1) % 2) as usize;
		if self.counts[next].load(Ordering::SeqCst) != 0 {
			return None;
		}
		self.epoch.store(epoch + 1, Ordering::SeqCst);
		Some((epoch % 2) as u8)
	}

	/// Whether every transaction counted into `bucket` has ended.
	pub fn drained(&self, bucket: u8) -> bool {
		self.counts.get(bucket as usize).is_none_or(|count| count.load(Ordering::SeqCst) == 0)
	}
}

/// The longest a write commit to a store other datastores write may run before it reports its
/// outcome unknown, for a backend that reports no bound of its own
/// ([`surrealdb_kvs::api::Transactable::write_commit_bound`]). A build elsewhere leaves a node out
/// of its wait only once the node's heartbeat is older than its write fence, the store's bound and
/// the margin within which clocks must agree together, so a commit admitted just before the fence
/// lapsed has landed, or never will, by then.
/// Abandoning the commit stops the client's retries and backoff, which are what would otherwise
/// leave it running for any length of time.
pub const WRITE_COMMIT_BOUND: std::time::Duration = surrealdb_kvs::api::DEFAULT_WRITE_COMMIT_BOUND;

/// How long a write commit abandoned at [`WRITE_COMMIT_BOUND`] still counts as under way in its
/// epoch ([`LiveWrites::hold_commit`]). A storage engine that commits in two phases may still
/// land a commit its client stopped waiting for, so the node answers no write barrier that would
/// pass it until this has elapsed: by then the commit has landed, or never will, within the
/// window a build allows a node left out of its wait.
pub const ABANDONED_COMMIT_SETTLE: std::time::Duration = std::time::Duration::from_secs(30);

/// The fence that stops a datastore whose heartbeat has lapsed from committing writes to a store
/// other datastores write too.
///
/// A build that waits for every write transaction older than one of its commits to end
/// ([`LiveWrites`], and the write barrier every live node answers) does not wait for a node whose
/// heartbeat it finds older than the liveness window: that node may have stopped, or stalled for
/// longer than the window. Such a node must not then commit a write the build missed, so a
/// datastore that answers write barriers arms this fence. From then on a write commits only while
/// the last heartbeat is younger than `stale_after`, and, once a heartbeat has ended a lapse, only
/// for a transaction that opened after that heartbeat was recorded: a transaction open across the
/// lapse may be one a build elsewhere stopped waiting for.
///
/// A heartbeat is dated by an instant taken before it read the clock it records, so the fence
/// lapses no later than other nodes stop counting the node live. `stale_after` is shorter than the
/// liveness window by a margin within which the nodes' clocks must agree, as for every lease
/// between nodes. The heartbeat's own commit is exempt, or a lapsed node could never end its
/// lapse.
#[derive(Default)]
pub struct WriteFence {
	state: parking_lot::Mutex<Option<FenceState>>,
	/// Set once the datastore shuts down, before it archives its node: from then on no write
	/// commits, as no build elsewhere waits for an archived node ([`Self::close`]).
	closed: std::sync::atomic::AtomicBool,
}

#[derive(Clone, Copy)]
struct FenceState {
	/// When the newest recorded heartbeat began.
	beat: web_time::Instant,
	stale_after: std::time::Duration,
	/// When the newest heartbeat that ended a lapse was recorded.
	recovered: Option<web_time::Instant>,
}

impl WriteFence {
	/// Record a heartbeat this datastore has committed, begun at `at`, arming the fence with
	/// `stale_after`.
	pub fn heartbeat(&self, at: web_time::Instant, stale_after: std::time::Duration) {
		let mut state = self.state.lock();
		let (beat, recovered) = match *state {
			None => (at, None),
			Some(previous) => {
				let lapsed = previous.beat.elapsed() >= previous.stale_after;
				let recovered = if lapsed {
					Some(web_time::Instant::now())
				} else {
					previous.recovered
				};
				(previous.beat.max(at), recovered)
			}
		};
		*state = Some(FenceState {
			beat,
			stale_after,
			recovered,
		});
	}

	/// Refuse every write commit from now on, whether or not the fence is armed: the datastore is
	/// shutting down, and once it archives its node no build elsewhere waits for its writes.
	pub fn close(&self) {
		self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
	}

	/// Whether a write transaction that opened at `opened` may commit now: the fence is open, and
	/// unarmed or with a recent last heartbeat and the transaction opened after the last lapse
	/// ended.
	pub fn admits(&self, opened: Option<web_time::Instant>) -> bool {
		if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
			return false;
		}
		match *self.state.lock() {
			None => true,
			Some(state) => {
				state.beat.elapsed() < state.stale_after
					&& state
						.recovered
						.is_none_or(|recovered| opened.is_some_and(|o| o >= recovered))
			}
		}
	}
}

/// The write transactions open through one datastore, counted by the epoch they opened in, so
/// any number of callers can wait, each on its own, until every write transaction that opened
/// before it asked has ended ([`Self::ended_since`]).
///
/// A write transaction counts itself into the current epoch before its snapshot is taken, and out
/// again once its outcome is known — when its commit or cancel returns, or when it is dropped,
/// whichever comes first — so a transaction whose commit then runs actions or waits does not hold
/// up a wait that the work it waits for depends on. A wait moves the epoch on as it begins
/// ([`Self::advance`]) and then waits for every epoch up to the one it moved from: a transaction
/// counted into a later epoch took its snapshot after the wait began.
///
/// A wait that has run long enough fences the epochs it waits for, for the tables it waits on
/// ([`Self::fence_tables_through`]): a transaction of a fenced epoch that writes one of them is
/// refused at commit, so the wait stops waiting for those epochs once no commit of theirs is
/// under way ([`Self::commits_settled`]). Only a wait that cannot tell which tables it waits on
/// fences every table ([`Self::fence_through`]). A commit counts itself as under way before it
/// reads the fence, and a wait reads the commits under way after it sets the fence, so no commit a
/// fence should refuse lands after the wait ends.
#[derive(Default)]
pub struct LiveWrites {
	epoch: AtomicU64,
	/// Open write transactions, and commits under way, by the epoch they opened in, in shards a
	/// transaction keeps from its opening to its last commit ([`Self::shard`]): every write
	/// transaction counts itself in and out, and its commit too, four times under one lock for all
	/// of them on a machine of many cores. A transaction stays in its shard while it is counted,
	/// so a wait that reads the shards one after another finds it; only the rare moves of the
	/// epoch take them all.
	epochs: [EpochShard; EPOCH_SHARDS],
	/// Write transactions of an epoch below this are refused at commit, whatever they write.
	fence: AtomicU64,
	/// Write transactions of an epoch below a table's entry that write that table are refused at
	/// commit. Keyed by the table's encoded root, as [`WrittenTables`] records it.
	table_fences: parking_lot::Mutex<HashMap<Vec<u8>, u64>>,
	/// Whether [`Self::table_fences`] holds any fence, set once one is, so a commit that no
	/// table fence can refuse reads neither the fences nor the tables it wrote.
	has_table_fences: AtomicBool,
	/// The waits running and the transactions set aside, so a transaction set aside for a while
	/// learns which tables the waits that may have passed it wait on ([`Self::park`]).
	waits: parking_lot::Mutex<Waits>,
	/// The write barriers builds through this datastore raised and its answer has not yet passed
	/// ([`Self::answers_only_own`]).
	own_barriers: parking_lot::Mutex<std::collections::BTreeSet<u64>>,
	/// Held across a raise of the write barrier and its record as this datastore's own, and by an
	/// answer while it reads the barrier and judges whose it is, so no answer sees a barrier this
	/// datastore raised before it is recorded as its own.
	raises: tokio::sync::Mutex<()>,
	/// The definitions whose transactions have not ended, each holding back the writes of its
	/// table that cannot see its index ([`DefinitionHold`]).
	holds: Mutex<Vec<Arc<DefinitionHold>>>,
	/// Whether [`Self::holds`] holds any, so a write reads nothing of them while there are none.
	has_holds: AtomicBool,
	/// How long a write waits for a definition's transaction to end, in milliseconds, or 0 for
	/// [`DEFINITION_HOLD_WAIT`] ([`Self::set_definition_hold_wait`]).
	hold_wait_ms: AtomicU64,
}

/// A table a fence refuses the writes of.
pub type FencedTable =
	(surrealdb_catalog::NamespaceId, surrealdb_catalog::DatabaseId, surrealdb_expr::val::TableName);

/// How long a write waits for the transaction of a definition that holds its table back before it
/// is refused with a retryable conflict ([`DefinitionHold`]): a transaction a client leaves open
/// holds a table's writes back for no longer than this at a time.
pub const DEFINITION_HOLD_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// A new index that is built inside the transaction that defines it, until that transaction ends.
///
/// A write maintains, or queues for the build of, only the indexes its own snapshot shows, so a
/// write that cannot see the definition is in neither the index nor its build. A build that reads
/// only once the definition has committed waits for every write older than that commit, so no such
/// write is left out ([`LiveWrites::advance`]); one that reads before the commit, inside its own
/// transaction, cannot wait for a commit that has not happened. This holds back, until that
/// transaction ends, the writes that began after the build began waiting and that write the table:
/// a write that reaches the table meanwhile waits for the transaction to end
/// ([`Self::ended`]), and is refused if the transaction was committing, since its snapshot lacks
/// the index.
///
/// The transaction itself is never held back, and neither are the writes that began before the
/// build began waiting, which the build waits for and then reads past.
///
/// The holds live in the datastore that runs the definition: a write through another datastore on
/// a shared store is not held back by it.
pub struct DefinitionHold {
	table: FencedTable,
	/// The defining transaction ([`crate::tx::Transaction::serial`]), which is not held back.
	owner: u64,
	/// The epoch of [`LiveWrites`] the hold's first build began waiting through: a write counted
	/// into a later one is held back. [`u64::MAX`] until that wait begins
	/// ([`LiveWrites::advance_holding`]), so no write is.
	from: AtomicU64,
	phase: tokio::sync::watch::Sender<HoldPhase>,
}

/// Where a [`DefinitionHold`]'s transaction stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HoldPhase {
	/// Open, or not yet committing.
	Open,
	/// Handing its commit to the storage engine, which may land it.
	Committing,
	/// Ended, having been committing or not.
	Ended {
		committing: bool,
	},
}

/// How a wait for a [`DefinitionHold`] ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldVerdict {
	/// The transaction ended without committing: the index it defined does not exist.
	Released,
	/// The transaction ended having handed a commit to the storage engine, which may have landed
	/// it: the index may exist, and a write that cannot see it must not commit.
	Committing,
	/// The transaction was still open when the wait ran out.
	TimedOut,
}

impl DefinitionHold {
	/// The table whose writes this holds back.
	pub fn table(&self) -> &FencedTable {
		&self.table
	}

	/// Wait for the defining transaction to end, for at most `bound`.
	pub async fn ended(&self, bound: std::time::Duration) -> HoldVerdict {
		let mut phase = self.phase.subscribe();
		let ended = phase.wait_for(|phase| matches!(phase, HoldPhase::Ended { .. }));
		match common::time::timeout(bound, ended).await {
			// `wait_for` returns once the phase has ended: an answer for one that has not is the
			// conservative one, the write is refused rather than let through.
			Ok(Ok(phase)) => match *phase {
				HoldPhase::Ended {
					committing: true,
				} => HoldVerdict::Committing,
				HoldPhase::Ended {
					committing: false,
				} => HoldVerdict::Released,
				HoldPhase::Open | HoldPhase::Committing => HoldVerdict::TimedOut,
			},
			// The sender lives as long as the hold, so it is gone only once the hold is.
			Ok(Err(_)) => HoldVerdict::Released,
			Err(_) => HoldVerdict::TimedOut,
		}
	}
}

/// The tables a transaction has written keys of, read off each key it writes: a key in a table's
/// subtree (`/*{ns}*{db}*{tb}`) writes that table. A write that may reach a table's subtree but
/// cannot be told to stay in one table's writes every table.
///
/// Each table is held as the encoded root of its subtree ([`table_root`]), which every key in
/// that subtree begins with, so recording a write only compares bytes, and a fence is keyed the
/// same way ([`LiveWrites::fence_tables_through`]).
#[derive(Default)]
pub struct WrittenTables {
	/// The encoded root of each table written.
	roots: Vec<Vec<u8>>,
	/// Whether a write may have reached any table.
	every: bool,
	/// Whether a write may have reached a durable index build state key
	/// (`!bs`, directly under a table's root) or a value shape key of an
	/// index (`!it`, `!ia`, `!ir`, under the index's subtree). Kept for the
	/// transaction's life: a rollback to a save point does not clear it.
	build_state: bool,
	/// A count of the events of writes that may reach a doc-ID forward mapping (`!dj`, directly
	/// under a table's root), a range delete that may hold one included: each such write adds one
	/// when it begins and one when it ends. It only grows, so two reads of it with no write under
	/// way at either bracket a stretch in which none began or ended
	/// ([`Self::doc_lookup_writes`]).
	doc_lookups: u64,
	/// How many of those writes have begun and not ended.
	doc_lookups_open: u32,
}

/// What [`WrittenTables`] held at a save point, for a rollback to it to return to.
#[derive(Clone, Copy, Debug)]
pub struct WrittenMark {
	roots: usize,
	every: bool,
}

/// Where every table's subtree lies: `/*` and the keys after it, up to `/+`.
const TABLE_SPACE: (&[u8], &[u8]) = (b"/*", b"/+");

impl WrittenTables {
	/// Record a write of `key`, before it is made. Answers whether it may reach a doc-ID forward
	/// mapping, in which case the caller ends it with [`Self::end_doc_lookup_write`] once the
	/// write has returned, whatever it returned.
	#[must_use]
	pub fn note_key(&mut self, key: &[u8]) -> bool {
		if self.every {
			return false;
		}
		if let Some(root) = self.roots.iter().find(|root| key.starts_with(root)) {
			self.build_state |= is_build_state_tail(&key[root.len()..]);
			return self.begin_doc_lookup_write(is_doc_lookup_tail(&key[root.len()..]));
		}
		match table_root(key) {
			Some(Some(len)) => {
				self.build_state |= is_build_state_tail(&key[len..]);
				self.roots.push(key[..len].to_vec());
				self.begin_doc_lookup_write(is_doc_lookup_tail(&key[len..]))
			}
			Some(None) => {
				self.every = true;
				self.build_state = true;
				false
			}
			None => false,
		}
	}

	/// Whether a key of `table` was recorded as written.
	#[cfg(test)]
	pub(crate) fn writes_table(&self, table: &FencedTable) -> bool {
		self.every || encoded_root(table).is_some_and(|root| self.roots.contains(&root))
	}

	/// Count the beginning of a write that may reach a doc-ID forward mapping, if `may`.
	fn begin_doc_lookup_write(&mut self, may: bool) -> bool {
		if may {
			self.doc_lookups += 1;
			self.doc_lookups_open += 1;
		}
		may
	}

	/// Count the end of a write [`Self::note_key`] or [`Self::note_range`] answered `true` for.
	pub fn end_doc_lookup_write(&mut self) {
		self.doc_lookups += 1;
		self.doc_lookups_open -= 1;
	}

	/// What is recorded now, for [`Self::restore`]: the record only grows, so its length and
	/// whether it reached every table say what it held.
	pub fn mark(&self) -> WrittenMark {
		WrittenMark {
			roots: self.roots.len(),
			every: self.every,
		}
	}

	/// Forget what was recorded since `mark`, whose writes a rollback undid. Whether a build
	/// state key may have been written is not forgotten.
	pub fn restore(&mut self, mark: WrittenMark) {
		self.roots.truncate(mark.roots);
		self.every = mark.every;
	}

	/// Count a rollback to a save point as a write of a doc-ID forward mapping: it undoes writes
	/// a read of one may have run alongside, so no read it overlapped is covered by the count
	/// ([`Self::doc_lookup_writes`]).
	pub fn note_rollback(&mut self) {
		self.doc_lookups += 1;
	}

	/// Record a write of every key in `[start, end)`, before it is made. The answer is that of
	/// [`Self::note_key`].
	#[must_use]
	pub fn note_range(&mut self, start: &[u8], end: &[u8]) -> bool {
		if self.every || start >= end {
			return false;
		}
		if let Some(root) =
			self.roots.iter().find(|root| start.starts_with(root) && within_subtree(root, end))
		{
			self.build_state |= range_may_hold_build_state(root.len(), start, end);
			let may = range_may_hold_doc_lookups(root.len(), start, end);
			return self.begin_doc_lookup_write(may);
		}
		match table_root(start) {
			Some(Some(len)) if within_subtree(&start[..len], end) => {
				self.build_state |= range_may_hold_build_state(len, start, end);
				self.roots.push(start[..len].to_vec());
				self.begin_doc_lookup_write(range_may_hold_doc_lookups(len, start, end))
			}
			_ if start < TABLE_SPACE.1 && end > TABLE_SPACE.0 => {
				self.every = true;
				self.build_state = true;
				false
			}
			_ => false,
		}
	}

	/// Whether a key written may be a durable index build state key or an index's value shape
	/// key, or a range deleted may hold one (`CommitTriggers::build_epoch`): a key's tag directly
	/// under a table's root is `!bs`, a key's tag under an index's subtree is `!it`, `!ia` or
	/// `!ir`, a range is judged by the spans of the table's subtree it covers, and a write whose
	/// table cannot be told, or a range that leaves its table, may have reached one.
	pub fn wrote_build_state(&self) -> bool {
		self.build_state
	}

	/// The count of doc-ID forward mapping write events, or `None` while
	/// a write that may reach a forward mapping is under way, or once a write whose table cannot
	/// be told has been recorded: from then on later writes are not examined, so no count stands
	/// for them.
	///
	/// A read of a forward mapping is covered by a count read before it and again after it only if
	/// both are `Some` and equal: no such write began, ended or was under way in between, so the
	/// store held no write of this transaction's that the read could have missed. A rollback to a
	/// save point moves the count, so a read it overlapped is not covered.
	pub fn doc_lookup_writes(&self) -> Option<u64> {
		(!self.every && self.doc_lookups_open == 0).then_some(self.doc_lookups)
	}
}

/// The key under a table's root that holds a record id's doc-ID forward mapping: the tag, then
/// the record id's identity.
pub(crate) const DOC_LOOKUP_TAG: &[u8] = b"!dj";

/// Whether `tail`, the bytes of a key after its table's root, begins a doc-ID forward mapping.
fn is_doc_lookup_tail(tail: &[u8]) -> bool {
	tail.starts_with(DOC_LOOKUP_TAG)
}

/// Whether the range `[start, end)`, which starts in the subtree of the table whose root is the
/// first `root` bytes of `start` and ends no later than that subtree's successor
/// ([`within_subtree`]), may hold a doc-ID forward mapping: it starts before the tags after `!dj`
/// and ends after the tag itself.
fn range_may_hold_doc_lookups(root: usize, start: &[u8], end: &[u8]) -> bool {
	let before_tag_after = &start[root..] < b"!dk".as_slice();
	let after_tag = match end.get(..root) {
		Some(head) if head == &start[..root] => &end[root..] > DOC_LOOKUP_TAG,
		_ => true,
	};
	before_tag_after && after_tag
}

/// The key under a table's root that holds an index's durable build state: the tag, then the
/// index id.
const BUILD_STATE_TAG: &[u8] = b"!bs";

/// The keys under an index's own subtree (`+`, the index id, then the tag) that hold the value
/// shapes of a B-tree index: `!it`, `!ia` and `!ir`.
const INDEX_SHAPE_TAGS: [&[u8]; 3] = [b"!it", b"!ia", b"!ir"];

/// The byte that begins the tail of every key in an index's subtree, and the byte after it.
const INDEX_SUBTREE: (&[u8], &[u8]) = (b"+", b",");

/// Whether `tail`, the bytes of a key after its table's root, begins a build state key or is a
/// value shape key of an index: `+`, a four-byte index id and a shape tag. A write of the second
/// kind moves the build epoch by itself, so the shapes kept under it do not depend on a `!bs`
/// write beside them.
fn is_build_state_tail(tail: &[u8]) -> bool {
	tail.starts_with(BUILD_STATE_TAG)
		|| (tail.starts_with(INDEX_SUBTREE.0)
			&& tail.get(5..8).is_some_and(|tag| INDEX_SHAPE_TAGS.contains(&tag)))
}

/// Whether the range `[start, end)`, which starts in the subtree of the table whose root is the
/// first `root` bytes of `start` and ends no later than that subtree's successor
/// ([`within_subtree`]), may hold a build state key or a value shape key: it overlaps the
/// span of the table's subtree that holds `!bs` or the span that holds its indexes' subtrees.
fn range_may_hold_build_state(root: usize, start: &[u8], end: &[u8]) -> bool {
	let overlaps = |first: &[u8], after: &[u8]| {
		let before_after = &start[root..] < after;
		let after_first = match end.get(..root) {
			Some(head) if head == &start[..root] => &end[root..] > first,
			_ => true,
		};
		before_after && after_first
	};
	overlaps(BUILD_STATE_TAG, b"!bt") || overlaps(INDEX_SUBTREE.0, INDEX_SUBTREE.1)
}

/// The length of the encoded root of the table whose subtree holds `key` — `/*{ns}*{db}*{tb}`,
/// each id four bytes and the name escaped and ended as storekey writes a string — which `key`
/// begins with: `None` for a key in no table's subtree, `Some(None)` for one that may be in a
/// table's subtree but whose root cannot be read off it.
fn table_root(key: &[u8]) -> Option<Option<usize>> {
	// A key without the `*` before the database or the table is a namespace's or a database's
	// own, in no table's subtree.
	if !key.starts_with(TABLE_SPACE.0) || key.get(6) != Some(&b'*') || key.get(11) != Some(&b'*') {
		return None;
	}
	// The name's bytes `0` and `1` are each escaped by a `1`, and an unescaped `0` ends it.
	let mut at = 12;
	while let Some(byte) = key.get(at) {
		match byte {
			0 => return Some(Some(at + 1)),
			1 => at += 2,
			_ => at += 1,
		}
	}
	Some(None)
}

/// Whether `end`, the exclusive end of a range starting in the subtree of `root`, stays within
/// that subtree: no later than the root's successor, its final `0` raised to `1`.
fn within_subtree(root: &[u8], end: &[u8]) -> bool {
	let Some((last, stem)) = root.split_last() else {
		return false;
	};
	let successor = [last.saturating_add(1)];
	end.iter().cmp(stem.iter().chain(successor.iter())).is_le()
}

/// The encoded root of `table`'s subtree, as [`table_root`] reads it off a key.
fn encoded_root(table: &FencedTable) -> Option<Vec<u8>> {
	use surrealdb_kvs::key::KVSubspace;
	let (ns, db, tb) = table;
	crate::key::schema::TblRoot {
		ns: *ns,
		db: *db,
		tb: std::borrow::Cow::Borrowed(tb),
	}
	.encode_bound()
	.ok()
	.map(|bound| bound.as_slice().to_vec())
}

/// The tables one wait for older write transactions waits on, each once, as the encoded root of
/// its subtree ([`encoded_root`]), or `None` for a wait on every table.
type WaitScope = Option<Vec<Vec<u8>>>;

/// The scope of a wait on `tables`: every table for `None`, or where one of them cannot be named.
fn wait_scope(tables: Option<&[FencedTable]>) -> WaitScope {
	let mut roots: Vec<Vec<u8>> = Vec::new();
	for table in tables? {
		let root = encoded_root(table)?;
		if !roots.contains(&root) {
			roots.push(root);
		}
	}
	Some(roots)
}

/// The waits running through one datastore, and what each transaction set aside has learned of
/// the waits that may pass it ([`LiveWrites::park`]).
#[derive(Default)]
struct Waits {
	/// How many waits have begun: the id of the next one.
	begun: u64,
	/// The scope of each wait running, by id.
	running: HashMap<u64, WaitScope>,
	/// How many transactions have been set aside: the id of the next one.
	parked_count: u64,
	/// The waits that may pass each transaction set aside, by the id [`LiveWrites::park`]
	/// answered.
	parked: HashMap<u64, PassedWaits>,
}

/// The waits that may have passed a write transaction while it was set aside
/// ([`LiveWrites::park`]): each may not have waited for it, so the build behind it may read
/// before the transaction commits. Counted per table, so the waits of the transaction's own build
/// can be taken back out ([`Self::credit`]).
///
/// The transaction's commit is refused when one of these waits is on a table it writes, or on
/// every table ([`Self::refuses`]). A build's wait is on the table it builds an index of, whose
/// writes are all its read depends on — the scope a fence refuses by
/// ([`LiveWrites::fence_tables_through`]) — and a node's answer to other nodes' barriers is on the
/// tables of the builds it lets read, so a transaction that writes none of the passed waits'
/// tables commits nothing such a build reads.
#[derive(Default, Debug)]
pub struct PassedWaits {
	/// How many passed waits are on each table, by its encoded root.
	tables: HashMap<Vec<u8>, u64>,
	/// How many passed waits are on every table.
	every: u64,
}

impl PassedWaits {
	fn add(&mut self, scope: &WaitScope) {
		let Some(roots) = scope else {
			self.every += 1;
			return;
		};
		for root in roots {
			match self.tables.get_mut(root) {
				Some(count) => *count += 1,
				None => {
					self.tables.insert(root.clone(), 1);
				}
			}
		}
	}

	/// The waits of a set-aside [`LiveWrites::park`] holds no record of: one on every table, so
	/// the transaction commits nothing a build may have read past.
	fn unrecorded() -> Self {
		Self {
			tables: HashMap::new(),
			every: 1,
		}
	}

	/// Take back out `own` waits on `table`, begun by the build the transaction was set aside for,
	/// which waits for the transaction by awaiting it, not by reading its epoch.
	pub fn credit(&mut self, own: u64, table: &FencedTable) {
		if own == 0 {
			return;
		}
		match encoded_root(table) {
			Some(root) => {
				if let Some(count) = self.tables.get_mut(&root) {
					*count = count.saturating_sub(own);
					if *count == 0 {
						self.tables.remove(&root);
					}
				}
			}
			None => self.every = self.every.saturating_sub(own),
		}
	}

	/// Count the waits of `other` among these: a transaction set aside more than once is judged
	/// by every wait that passed it.
	pub fn merge(&mut self, other: PassedWaits) {
		for (root, count) in other.tables {
			*self.tables.entry(root).or_default() += count;
		}
		self.every += other.every;
	}

	/// Whether no wait passed.
	pub fn is_empty(&self) -> bool {
		self.tables.is_empty() && self.every == 0
	}

	/// Whether a transaction that wrote `written` may not commit: a passed wait is on every table,
	/// a write may have reached any table, or a passed wait is on a table written.
	pub fn refuses(&self, written: &WrittenTables) -> bool {
		if self.is_empty() {
			return false;
		}
		self.every > 0
			|| written.every
			|| written.roots.iter().any(|root| self.tables.contains_key(root))
	}
}

#[derive(Default)]
struct EpochCounts {
	open: usize,
	committing: usize,
}

/// How many shards [`LiveWrites`] counts its transactions in.
const EPOCH_SHARDS: usize = 32;

/// One shard of [`LiveWrites::epochs`], on a cache line of its own.
#[derive(Default)]
#[repr(align(128))]
struct EpochShard(parking_lot::Mutex<std::collections::BTreeMap<u64, EpochCounts>>);

/// A shard of [`LiveWrites`] a write transaction is counted in from its opening to its last
/// commit ([`LiveWrites::shard`]).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiveShard(u8);

impl LiveWrites {
	/// The shard a write transaction opening on this thread is counted in, which it keeps, and
	/// passes to every count of it, until its last commit has ended. Each thread keeps to one
	/// shard, so the transactions of a thread contend for no lock with most other threads'.
	pub(crate) fn shard() -> LiveShard {
		static NEXT: AtomicU64 = AtomicU64::new(0);
		thread_local! {
			static SHARD: LiveShard =
				LiveShard((NEXT.fetch_add(1, Ordering::Relaxed) % EPOCH_SHARDS as u64) as u8);
		}
		SHARD.with(|shard| *shard)
	}

	fn counts(
		&self,
		shard: LiveShard,
	) -> &parking_lot::Mutex<std::collections::BTreeMap<u64, EpochCounts>> {
		&self.epochs[usize::from(shard.0) % EPOCH_SHARDS].0
	}

	/// Count a write transaction about to take its snapshot into the current epoch, in `shard`,
	/// answering the epoch to hand to [`Self::close`].
	pub(crate) fn open(&self, shard: LiveShard) -> u64 {
		let mut epochs = self.counts(shard).lock();
		let epoch = self.epoch.load(Ordering::SeqCst);
		epochs.entry(epoch).or_default().open += 1;
		epoch
	}

	/// Count a write transaction counted into `epoch`, in `shard`, out again.
	pub(crate) fn close(&self, epoch: u64, shard: LiveShard) {
		let mut epochs = self.counts(shard).lock();
		if let Some(counts) = epochs.get_mut(&epoch) {
			counts.open = counts.open.saturating_sub(1);
			self.forget_if_settled(&mut epochs, epoch);
		}
	}

	/// Count a commit of a transaction of `epoch` that wrote `written` as under way, answering
	/// whether a fence refuses it. Counted before the fences are read; a wait sets a fence before
	/// it reads the commits under way, so one of the two sees the other. `written` is read only
	/// where some table is fenced.
	pub(crate) fn begin_commit(
		&self,
		epoch: u64,
		shard: LiveShard,
		written: &parking_lot::Mutex<WrittenTables>,
	) -> bool {
		self.counts(shard).lock().entry(epoch).or_default().committing += 1;
		if epoch < self.fence.load(Ordering::SeqCst) {
			return true;
		}
		if !self.has_table_fences.load(Ordering::SeqCst) {
			return false;
		}
		let written = written.lock();
		let fences = self.table_fences.lock();
		if written.every {
			return fences.values().any(|fence| epoch < *fence);
		}
		written.roots.iter().any(|root| fences.get(root).is_some_and(|fence| epoch < *fence))
	}

	/// Count a commit of a transaction of `epoch`, abandoned before its outcome was known, as under
	/// way until [`Self::end_commit`]: a wait that would pass it waits for it as for a commit still
	/// running ([`ABANDONED_COMMIT_SETTLE`]).
	pub(crate) fn hold_commit(&self, epoch: u64, shard: LiveShard) {
		self.counts(shard).lock().entry(epoch).or_default().committing += 1;
	}

	/// Count a commit counted by [`Self::begin_commit`] as no longer under way.
	pub(crate) fn end_commit(&self, epoch: u64, shard: LiveShard) {
		let mut epochs = self.counts(shard).lock();
		if let Some(counts) = epochs.get_mut(&epoch) {
			counts.committing = counts.committing.saturating_sub(1);
			self.forget_if_settled(&mut epochs, epoch);
		}
	}

	/// Drop `epoch`'s counts once nothing of it is open or committing, unless it is the current
	/// epoch, whose entry the next transaction would only add back:
	/// [`Self::advance`] drops it once it is past.
	fn forget_if_settled(
		&self,
		epochs: &mut std::collections::BTreeMap<u64, EpochCounts>,
		epoch: u64,
	) {
		if epoch < self.epoch.load(Ordering::SeqCst)
			&& epochs.get(&epoch).is_some_and(|counts| counts.open == 0 && counts.committing == 0)
		{
			epochs.remove(&epoch);
		}
	}

	/// Begin a wait: move new write transactions to the next epoch, and answer the epoch the wait
	/// waits through ([`Self::ended_since`]).
	pub fn advance(&self) -> u64 {
		self.advance_holding(None)
	}

	/// [`Self::advance`], which also begins `hold`, if its first build has not begun waiting yet:
	/// from the epoch answered on, the writes of its table that cannot see its index are held back.
	/// Begun under the lock a write counts itself in under, so a write counted into a later epoch
	/// finds the hold begun, and one counted into an earlier epoch is the build's to wait for.
	pub fn advance_holding(&self, hold: Option<&DefinitionHold>) -> u64 {
		// Every shard is held, in order, so no transaction is counted in while the epoch moves.
		let mut shards: Vec<_> = self.epochs.iter().map(|shard| shard.0.lock()).collect();
		let since = self.epoch.fetch_add(1, Ordering::SeqCst);
		if let Some(hold) = hold {
			hold.from.fetch_min(since, Ordering::SeqCst);
		}
		for epochs in &mut shards {
			self.forget_if_settled(epochs, since);
		}
		since
	}

	/// Whether every shard's counts of `since` and the epochs before it satisfy `ended`. A
	/// transaction stays in one shard while it is counted, and none is counted into an epoch the
	/// epoch has moved past, so the shards are read one after another.
	fn all_through(&self, since: u64, ended: impl Fn(u64, &EpochCounts) -> bool) -> bool {
		self.epochs.iter().all(|shard| {
			shard.0.lock().range(..=since).all(|(epoch, counts)| ended(*epoch, counts))
		})
	}

	/// Whether every write transaction counted into `since` or an earlier epoch has ended, or, for
	/// a fenced epoch, can no longer commit: none of its commits is under way.
	pub fn ended_since(&self, since: u64) -> bool {
		let fence = self.fence.load(Ordering::SeqCst);
		self.all_through(since, |epoch, counts| {
			counts.committing == 0 && (counts.open == 0 || epoch < fence)
		})
	}

	/// [`Self::ended_since`] for a wait on the writes of `targets`: an epoch every one of those
	/// tables is fenced through, by an earlier wait on them that ran its bound, has ended for it
	/// once none of its commits is under way. `None`, or no tables, is [`Self::ended_since`].
	pub fn ended_since_for(&self, since: u64, targets: Option<&[FencedTable]>) -> bool {
		let Some(targets) = targets.filter(|targets| !targets.is_empty()) else {
			return self.ended_since(since);
		};
		let fence = self.fence.load(Ordering::SeqCst);
		// Through which epoch every target table is fenced.
		let tables = {
			let fences = self.table_fences.lock();
			targets
				.iter()
				.map(|table| {
					encoded_root(table).and_then(|root| fences.get(&root).copied()).unwrap_or(0)
				})
				.min()
				.unwrap_or(0)
		};
		let fence = fence.max(tables);
		self.all_through(since, |epoch, counts| {
			counts.committing == 0 && (counts.open == 0 || epoch < fence)
		})
	}

	/// Refuse at commit every write transaction counted into `since` or an earlier epoch, which a
	/// wait that has run long enough, and cannot tell which tables it waits on, no longer waits
	/// for.
	pub fn fence_through(&self, since: u64) {
		self.fence.fetch_max(since.saturating_add(1), Ordering::SeqCst);
	}

	/// Refuse at commit every write transaction counted into `since` or an earlier epoch that
	/// writes one of `tables`, which a wait that has run long enough on those tables no longer
	/// waits for.
	/// A table whose root cannot be encoded is fenced as every table is.
	pub fn fence_tables_through(&self, since: u64, tables: &[FencedTable]) {
		let mut fences = self.table_fences.lock();
		for table in tables {
			let Some(root) = encoded_root(table) else {
				drop(fences);
				self.fence_through(since);
				return;
			};
			let fence = fences.entry(root).or_default();
			*fence = (*fence).max(since.saturating_add(1));
		}
		self.has_table_fences.store(true, Ordering::SeqCst);
	}

	/// Whether no commit of a transaction counted into `since` or an earlier epoch is under way:
	/// what a wait that has fenced those epochs waits for.
	pub fn commits_settled(&self, since: u64) -> bool {
		self.all_through(since, |_, counts| counts.committing == 0)
	}

	/// Count a wait that is about to read the epoch it waits past, on `tables`, or on every table
	/// for `None`, until [`Self::end_wait`] with the id answered: a build's, on the table it
	/// builds, or a node's answer to write barriers other nodes' builds raised, on their tables.
	/// Every transaction set aside learns of it ([`Self::park`]). See
	/// [`crate::factory::WriteWait`].
	pub(crate) fn begin_wait(&self, tables: Option<&[FencedTable]>) -> u64 {
		let scope = wait_scope(tables);
		let mut waits = self.waits.lock();
		for passed in waits.parked.values_mut() {
			passed.add(&scope);
		}
		let id = waits.begun;
		waits.begun += 1;
		waits.running.insert(id, scope);
		id
	}

	/// Count the wait [`Self::begin_wait`] answered `id` for as no longer running.
	pub(crate) fn end_wait(&self, id: u64) {
		self.waits.lock().running.remove(&id);
	}

	/// How many waits have begun, and how many are running.
	pub fn waits(&self) -> (u64, u64) {
		let waits = self.waits.lock();
		(waits.begun, waits.running.len() as u64)
	}

	/// Set aside a write transaction about to count itself out of its epoch, answering the id to
	/// hand to [`Self::rejoin`]. Every wait running now, and every wait that begins before the
	/// rejoin, may read its epoch once the transaction has left it, so may not wait for it: each
	/// is recorded as one that may pass it. Called before the transaction counts itself out, so no
	/// wait reads its epoch without it and goes unrecorded.
	pub(crate) fn park(&self) -> u64 {
		let mut waits = self.waits.lock();
		let mut passed = PassedWaits::default();
		for scope in waits.running.values() {
			passed.add(scope);
		}
		let id = waits.parked_count;
		waits.parked_count += 1;
		waits.parked.insert(id, passed);
		id
	}

	/// Stop recording the waits that may pass the transaction set aside as `id`, answering those
	/// recorded. Called once the transaction has counted itself back into an epoch, so a wait that
	/// begins after this waits for it. An `id` with no record answers a wait on every table, so
	/// the transaction is refused rather than admitted unjudged.
	pub(crate) fn rejoin(&self, id: u64) -> PassedWaits {
		self.waits.lock().parked.remove(&id).unwrap_or_else(PassedWaits::unrecorded)
	}

	/// Serialise a raise of the write barrier, with its record as this datastore's own, against an
	/// answer's reading of the barrier ([`Self::answers_only_own`]).
	pub async fn lock_raises(&self) -> tokio::sync::MutexGuard<'_, ()> {
		self.raises.lock().await
	}

	/// Record that a build through this datastore raised write barrier `barrier`.
	pub fn note_own_barrier(&self, barrier: u64) {
		self.own_barriers.lock().insert(barrier);
	}

	/// Whether every write barrier after `answered` up to `asked`, which an answer of `asked`
	/// covers, was raised by a build through this datastore. A build
	/// waits for its own datastore's transactions directly, so answering only its own barriers
	/// passes no transaction another build waits for.
	pub fn answers_only_own(&self, answered: u64, asked: u64) -> bool {
		let mut own = self.own_barriers.lock();
		// Those through `answered` have an answer recorded; those it covers are forgotten only
		// once its answer is ([`Self::forget_own_barriers_through`]), so an answer that fails
		// before then judges them the same way when it is made again.
		own.retain(|barrier| *barrier > answered);
		let covered = own.range(answered.saturating_add(1)..=asked).count() as u64;
		covered == asked.saturating_sub(answered)
	}

	/// The write barriers after `answered` up to `asked`, which an answer of `asked` covers, that a
	/// build through this datastore raised. Read while the raises are held ([`Self::lock_raises`]),
	/// so none of them is between its commit and its record as this datastore's own.
	pub fn own_barriers_covered(
		&self,
		answered: u64,
		asked: u64,
	) -> std::collections::BTreeSet<u64> {
		self.own_barriers.lock().range(answered.saturating_add(1)..=asked).copied().collect()
	}

	/// Forget the barriers this datastore raised through `answered`, whose answer is recorded.
	pub fn forget_own_barriers_through(&self, answered: u64) {
		self.own_barriers.lock().retain(|barrier| *barrier > answered);
	}

	/// Register the definition of an index on `table`, which the transaction numbered `owner`
	/// builds inside itself, as holding back the table's writes that cannot see the index
	/// ([`DefinitionHold`]), and answer whether this registered it rather than finding it. The
	/// hold holds nothing back until a build of the definition begins waiting for the writes older
	/// than itself ([`Self::advance_holding`]), and ends with [`Self::end_hold`].
	pub fn hold_table(&self, table: FencedTable, owner: u64) -> (Arc<DefinitionHold>, bool) {
		let mut holds = self.holds.lock();
		if let Some(held) = holds.iter().find(|hold| hold.owner == owner && hold.table == table) {
			return (Arc::clone(held), false);
		}
		let (phase, _) = tokio::sync::watch::channel(HoldPhase::Open);
		let hold = Arc::new(DefinitionHold {
			table,
			owner,
			from: AtomicU64::new(u64::MAX),
			phase,
		});
		holds.push(Arc::clone(&hold));
		self.has_holds.store(true, Ordering::SeqCst);
		(hold, true)
	}

	/// Whether any definition holds back writes now. A write asks before it asks which
	/// ([`Self::hold_on`]), so it costs one load while none does.
	pub fn holds_writes(&self) -> bool {
		self.has_holds.load(Ordering::SeqCst)
	}

	/// The hold of `owner`'s definition of an index on `table`, if it has one.
	pub fn hold_of(&self, table: &FencedTable, owner: u64) -> Option<Arc<DefinitionHold>> {
		self.holds.lock().iter().find(|hold| hold.owner == owner && hold.table == *table).cloned()
	}

	/// The definition that holds back a write counted into `epoch`, which writes `table` and is
	/// the transaction numbered `writer`: one of another transaction, whose build began waiting for
	/// the writes older than itself before this one was counted in.
	pub fn hold_on(
		&self,
		table: &FencedTable,
		epoch: u64,
		writer: u64,
	) -> Option<Arc<DefinitionHold>> {
		self.holds
			.lock()
			.iter()
			.find(|hold| {
				hold.owner != writer
					&& hold.table == *table
					&& epoch > hold.from.load(Ordering::SeqCst)
			})
			.cloned()
	}

	/// Note that the transaction numbered `owner` hands its commit to the storage engine, which
	/// may land it. A write held back by its definitions is refused once it has ended, since
	/// the index may exist where the write's snapshot cannot see it.
	pub fn begin_definition_commit(&self, owner: u64) {
		let holds = self.holds.lock();
		for hold in holds.iter().filter(|hold| hold.owner == owner) {
			hold.phase.send_if_modified(|phase| {
				let open = *phase == HoldPhase::Open;
				if open {
					*phase = HoldPhase::Committing;
				}
				open
			});
		}
	}

	/// End a definition's hold: its transaction has committed, rolled back or been dropped.
	/// Idempotent.
	///
	/// A transaction that handed a commit to the storage engine may have landed it. Every write
	/// counted in so far, that writes the table, is then fenced, since none of them can see the
	/// index: those that were held back are refused as they learn it ([`HoldVerdict::Committing`]),
	/// and one that has not reached the table yet when its commit comes. A write counted in after
	/// this took its snapshot after the commit landed, and sees the index.
	pub fn end_hold(&self, hold: &Arc<DefinitionHold>) {
		{
			let mut holds = self.holds.lock();
			holds.retain(|held| !Arc::ptr_eq(held, hold));
			self.has_holds.store(!holds.is_empty(), Ordering::SeqCst);
		}
		let mut committing = false;
		hold.phase.send_if_modified(|phase| {
			if matches!(phase, HoldPhase::Ended { .. }) {
				return false;
			}
			committing = *phase == HoldPhase::Committing;
			*phase = HoldPhase::Ended {
				committing,
			};
			true
		});
		if committing {
			let since = self.advance();
			self.fence_tables_through(since, std::slice::from_ref(&hold.table));
		}
	}

	/// How long a write waits for a definition's transaction to end.
	pub fn definition_hold_wait(&self) -> std::time::Duration {
		match self.hold_wait_ms.load(Ordering::Relaxed) {
			0 => DEFINITION_HOLD_WAIT,
			ms => std::time::Duration::from_millis(ms),
		}
	}

	/// Shorten, or with a zero restore, how long a write waits for a definition's transaction to
	/// end, for tests of what a wait that runs out does.
	pub fn set_definition_hold_wait(&self, wait: std::time::Duration) {
		self.hold_wait_ms.store(wait.as_millis() as u64, Ordering::Relaxed);
	}
}

/// The live-query registrations through one datastore that a write transaction
/// open through it may not have seen, so that a write that committed unseen by
/// one can still notify it.
///
/// A write computes its live-query notifications from the subscriptions its
/// snapshot holds. A registration that commits after that snapshot is taken and
/// before the write commits is in neither, and nothing conflicts. Each
/// registration is entered here before its transaction commits ([`Self::begin`])
/// and dated once it has committed ([`Self::settle`]) with an epoch of
/// [`LiveWrites`] moved on after the commit: a write counted into an epoch no
/// later than that one may have taken its snapshot before the registration
/// committed. A write asks, after it commits, for the registrations of a table
/// it wrote that are still undated or dated no earlier than its own epoch
/// ([`Self::candidates`]); one undated then may already have committed. Both
/// questions over-answer: the writer reads the table's subscriptions afresh and
/// serves only those it did not see, so an entry that never committed, or that
/// committed before the write's snapshot, costs a read and nothing else. An
/// entry that committed after the write did, before the write asked, is
/// answered too: epochs order a registration against a write's opening, not
/// against its commit, and ordering the two commits would serialise every
/// write commit against every registration.
///
/// A write stops being counted among [`LiveWrites`] as soon as its commit
/// returns, before it has asked, so it pins its epoch while it writes
/// ([`Self::pin`]) and unpins it once it has asked ([`Self::unpin`]). A write
/// set aside while it awaits a build is not counted, and is counted back into a
/// later epoch than its snapshot's, so it pins the epoch it first opened in from
/// when it is first set aside until it is dropped
/// ([`crate::tx::Transaction::park_live_write`]). An entry
/// is dropped once every write transaction counted into an epoch no later than
/// its date has ended and no such epoch is pinned, as no write can still miss
/// it, and when its transaction does not commit ([`Self::abandon`]).
///
/// The registry holds this datastore's registrations only, and answers only its
/// writes: a registration made through another datastore on the same store is
/// not entered here, so a write through this one cannot learn it missed it.
/// That suffices while a node's writes notify only that node's own subscribers,
/// as core's `LocalMessageBroker` does; a broker that
/// routes notifications to other nodes needs the question answered across
/// nodes.
#[derive(Default)]
pub struct LiveRegistrations {
	/// The entries, oldest first.
	entries: Mutex<Vec<LiveRegistration>>,
	/// How many entries there are, so a write asks without the lock when none.
	count: AtomicUsize,
	/// The token the next entry takes.
	next: AtomicU64,
	/// The epochs pinned by writes that have yet to ask, with how many pin each, in shards chosen
	/// by the pinning transaction: every write transaction pins and unpins, and one lock for all
	/// of them is contended on a machine of many cores. Only a prune reads them all.
	pins: [PinShard; PIN_SHARDS],
}

/// How many shards [`LiveRegistrations`] keeps its pins in.
const PIN_SHARDS: usize = 16;

/// One shard of the pins, on a cache line of its own.
#[derive(Default)]
#[repr(align(128))]
struct PinShard(Mutex<std::collections::BTreeMap<u64, usize>>);

/// One registration entered in [`LiveRegistrations`].
struct LiveRegistration {
	token: u64,
	ns: surrealdb_catalog::NamespaceId,
	db: surrealdb_catalog::DatabaseId,
	tb: surrealdb_expr::val::TableName,
	live: uuid::Uuid,
	/// The [`Transaction::serial`](crate::tx::Transaction::serial) of the
	/// transaction registering it, whose own writes are not owed to it.
	owner: u64,
	/// The [`LiveWrites`] epoch the registration's commit was dated with, or
	/// `None` while it has not committed.
	settled: Option<u64>,
}

impl LiveRegistrations {
	/// Enter a registration of `live` on table `tb` by the transaction
	/// numbered `owner`, which has not yet committed, answering the token that
	/// settles or abandons it.
	pub fn begin(
		&self,
		ns: surrealdb_catalog::NamespaceId,
		db: surrealdb_catalog::DatabaseId,
		tb: &surrealdb_expr::val::TableName,
		live: uuid::Uuid,
		owner: u64,
	) -> u64 {
		let token = self.next.fetch_add(1, Ordering::Relaxed);
		let mut entries = self.entries.lock();
		entries.push(LiveRegistration {
			token,
			ns,
			db,
			tb: tb.clone(),
			live,
			owner,
			settled: None,
		});
		self.count.store(entries.len(), Ordering::Release);
		token
	}

	/// Date the registration `token` names, whose transaction has committed,
	/// with an epoch of `writes` moved on after that commit.
	pub fn settle(&self, token: u64, writes: &LiveWrites) {
		let epoch = writes.advance();
		let mut entries = self.entries.lock();
		if let Some(entry) = entries.iter_mut().find(|entry| entry.token == token) {
			entry.settled = Some(epoch);
		}
		self.prune(&mut entries, writes);
		self.count.store(entries.len(), Ordering::Release);
	}

	/// Drop the registration `token` names, whose transaction did not commit.
	pub fn abandon(&self, token: u64) {
		let mut entries = self.entries.lock();
		entries.retain(|entry| entry.token != token);
		self.count.store(entries.len(), Ordering::Release);
	}

	/// The registrations on `tb` that a write counted into `epoch` by the
	/// transaction numbered `writer` may not have seen: those not yet
	/// committed, and those dated with `epoch` or later, other than its own.
	pub fn candidates(
		&self,
		ns: surrealdb_catalog::NamespaceId,
		db: surrealdb_catalog::DatabaseId,
		tb: &surrealdb_expr::val::TableName,
		epoch: u64,
		writer: u64,
		writes: &LiveWrites,
	) -> Vec<uuid::Uuid> {
		if self.count.load(Ordering::Acquire) == 0 {
			return Vec::new();
		}
		let mut entries = self.entries.lock();
		self.prune(&mut entries, writes);
		self.count.store(entries.len(), Ordering::Release);
		entries
			.iter()
			.filter(|entry| {
				entry.ns == ns && entry.db == db && entry.tb == *tb && entry.owner != writer
			})
			.filter(|entry| entry.settled.is_none_or(|settled| settled >= epoch))
			.map(|entry| entry.live)
			.collect()
	}

	/// Whether any registration on `tb` other than the transaction numbered
	/// `writer`'s own is one a write counted into `epoch` may not see: what
	/// [`Self::candidates`] would answer, without the list. Answered without the
	/// lock while no registration is entered.
	pub fn any_candidate(
		&self,
		ns: surrealdb_catalog::NamespaceId,
		db: surrealdb_catalog::DatabaseId,
		tb: &surrealdb_expr::val::TableName,
		epoch: u64,
		writer: u64,
	) -> bool {
		if self.count.load(Ordering::Acquire) == 0 {
			return false;
		}
		self.entries.lock().iter().any(|entry| {
			entry.ns == ns
				&& entry.db == db
				&& entry.tb == *tb
				&& entry.owner != writer
				&& entry.settled.is_none_or(|settled| settled >= epoch)
		})
	}

	/// Keep the entries dated with `epoch` or later until the write counted
	/// into `epoch` has asked ([`Self::unpin`]). Taken while the write is still
	/// counted into `epoch`, so no such entry has been dropped yet. `by` is the
	/// [`Transaction::serial`](crate::tx::Transaction::serial) of the pinning
	/// transaction, which releases the pin with the same.
	pub fn pin(&self, epoch: u64, by: u64) {
		*self.pin_shard(by).lock().entry(epoch).or_default() += 1;
	}

	/// Release a pin of `epoch` the transaction numbered `by` took ([`Self::pin`]).
	pub fn unpin(&self, epoch: u64, by: u64) {
		let mut pins = self.pin_shard(by).lock();
		if let std::collections::btree_map::Entry::Occupied(mut pinned) = pins.entry(epoch) {
			*pinned.get_mut() -= 1;
			if *pinned.get() == 0 {
				pinned.remove();
			}
		}
	}

	fn pin_shard(&self, by: u64) -> &Mutex<std::collections::BTreeMap<u64, usize>> {
		&self.pins[(by % PIN_SHARDS as u64) as usize].0
	}

	/// Drop every dated entry no write can still miss: every write counted
	/// into an epoch no later than its date has ended, and none pins one.
	fn prune(&self, entries: &mut Vec<LiveRegistration>, writes: &LiveWrites) {
		let oldest_pin =
			self.pins.iter().filter_map(|shard| shard.0.lock().keys().next().copied()).min();
		entries.retain(|entry| {
			entry.settled.is_none_or(|settled| {
				oldest_pin.is_some_and(|pin| pin <= settled) || !writes.ended_since(settled)
			})
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The shard the tests count their transactions in.
	const S: LiveShard = LiveShard(0);

	fn named(name: &str) -> surrealdb_expr::val::TableName {
		surrealdb_expr::val::TableName::from(name)
	}

	const OWNER: u64 = 1;
	const WRITER: u64 = 2;
	const NS: surrealdb_catalog::NamespaceId = surrealdb_catalog::NamespaceId(1);
	const DB: surrealdb_catalog::DatabaseId = surrealdb_catalog::DatabaseId(2);

	#[test]
	fn a_write_that_opened_before_a_registration_settled_is_answered_it() {
		let writes = LiveWrites::default();
		let registrations = LiveRegistrations::default();
		let live = uuid::Uuid::from_u128(7);
		let older = writes.open(S);
		let token = registrations.begin(NS, DB, &named("tb"), live, OWNER);
		assert_eq!(
			registrations.candidates(NS, DB, &named("tb"), older, WRITER, &writes),
			vec![live],
			"an undated registration may already have committed"
		);
		registrations.settle(token, &writes);
		let newer = writes.open(S);
		assert_eq!(
			registrations.candidates(NS, DB, &named("tb"), older, WRITER, &writes),
			vec![live]
		);
		assert!(
			registrations.candidates(NS, DB, &named("tb"), newer, WRITER, &writes).is_empty(),
			"a write that opened after the registration settled saw it"
		);
		assert!(
			registrations.candidates(NS, DB, &named("other"), older, WRITER, &writes).is_empty(),
			"another table's registration is not owed"
		);
		writes.close(newer, S);
		writes.close(older, S);
		assert!(
			registrations.candidates(NS, DB, &named("tb"), older, WRITER, &writes).is_empty(),
			"once every write that could have missed it has ended, it is dropped"
		);
		assert_eq!(registrations.count.load(Ordering::Acquire), 0);
	}

	#[test]
	fn a_pinned_epoch_keeps_its_registrations_after_its_write_ends() {
		let writes = LiveWrites::default();
		let registrations = LiveRegistrations::default();
		let live = uuid::Uuid::from_u128(7);
		let write = writes.open(S);
		registrations.pin(write, WRITER);
		let token = registrations.begin(NS, DB, &named("tb"), live, OWNER);
		registrations.settle(token, &writes);
		writes.close(write, S);
		assert_eq!(
			registrations.candidates(NS, DB, &named("tb"), write, WRITER, &writes),
			vec![live],
			"a write that committed and has not asked is still answered"
		);
		registrations.unpin(write, WRITER);
		assert!(registrations.candidates(NS, DB, &named("tb"), write, WRITER, &writes).is_empty());
	}

	#[test]
	fn a_pin_in_any_shard_keeps_the_registrations_of_its_epoch() {
		let writes = LiveWrites::default();
		let registrations = LiveRegistrations::default();
		let live = uuid::Uuid::from_u128(7);
		let write = writes.open(S);
		// Two writers whose pins fall in different shards: the one still pinned keeps the entry.
		let (first, second) = (WRITER, WRITER + 1);
		registrations.pin(write, first);
		registrations.pin(write, second);
		let token = registrations.begin(NS, DB, &named("tb"), live, OWNER);
		registrations.settle(token, &writes);
		writes.close(write, S);
		registrations.unpin(write, first);
		assert_eq!(
			registrations.candidates(NS, DB, &named("tb"), write, second, &writes),
			vec![live],
			"the other shard's pin still holds the epoch"
		);
		registrations.unpin(write, second);
		assert!(registrations.candidates(NS, DB, &named("tb"), write, second, &writes).is_empty());
	}

	#[test]
	fn a_transaction_is_not_answered_its_own_registration() {
		let writes = LiveWrites::default();
		let registrations = LiveRegistrations::default();
		let epoch = writes.open(S);
		registrations.begin(NS, DB, &named("tb"), uuid::Uuid::from_u128(7), WRITER);
		assert!(registrations.candidates(NS, DB, &named("tb"), epoch, WRITER, &writes).is_empty());
		writes.close(epoch, S);
	}

	#[test]
	fn an_abandoned_registration_is_dropped() {
		let writes = LiveWrites::default();
		let registrations = LiveRegistrations::default();
		let epoch = writes.open(S);
		let token = registrations.begin(NS, DB, &named("tb"), uuid::Uuid::from_u128(7), OWNER);
		registrations.abandon(token);
		assert!(registrations.candidates(NS, DB, &named("tb"), epoch, WRITER, &writes).is_empty());
		writes.close(epoch, S);
	}

	#[test]
	fn a_write_fence_refuses_a_lapse_and_what_opened_before_its_end() {
		use std::time::Duration;

		use web_time::Instant;
		let fence = WriteFence::default();
		assert!(fence.admits(None), "an unarmed fence admits every write");
		let before = Instant::now();
		fence.heartbeat(Instant::now(), Duration::from_millis(50));
		assert!(fence.admits(Some(before)), "a recent heartbeat admits every write");
		std::thread::sleep(Duration::from_millis(80));
		assert!(!fence.admits(Some(Instant::now())), "a lapsed heartbeat refuses every write");
		fence.heartbeat(Instant::now(), Duration::from_secs(10));
		assert!(!fence.admits(Some(before)), "a write opened before the lapse ended is refused");
		assert!(fence.admits(Some(Instant::now())), "a write opened after it commits");
		fence.heartbeat(Instant::now(), Duration::from_secs(10));
		assert!(!fence.admits(Some(before)), "a later heartbeat keeps the lapse's end");
	}

	fn record_key(ns: u32, db: u32, tb: &str) -> Vec<u8> {
		use surrealdb_kvs::key::KVKey;
		let tb = surrealdb_expr::val::TableName::from(tb);
		crate::key::schema::RecordKey {
			ns: crate::catalog::NamespaceId(ns),
			db: crate::catalog::DatabaseId(db),
			tb: std::borrow::Cow::Borrowed(&tb),
			id: std::borrow::Cow::Owned(crate::val::RecordIdKey::Number(1)),
		}
		.encode_key()
		.unwrap()
		.as_slice()
		.to_vec()
	}

	fn table(ns: u32, db: u32, tb: &str) -> FencedTable {
		(crate::catalog::NamespaceId(ns), crate::catalog::DatabaseId(db), tb.into())
	}

	fn written(keys: &[&[u8]]) -> WrittenTables {
		let mut written = WrittenTables::default();
		for key in keys {
			let _ = written.note_key(key);
		}
		written
	}

	fn dj_key(tb: &str) -> Vec<u8> {
		use surrealdb_kvs::key::KVKey;
		let tb = surrealdb_expr::val::TableName::from(tb);
		crate::key::schema::DocLookupIdentityKey::new(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(2),
			std::borrow::Cow::Borrowed(&tb),
			crate::val::RecordIdentity(crate::val::RecordIdKey::Number(1)),
		)
		.encode_key()
		.unwrap()
		.as_slice()
		.to_vec()
	}

	/// A forward mapping write is under way from its recording to its end, and the count it
	/// leaves differs from the count before it, so a read that began before the write and ended
	/// after it is not covered by the count whichever way it overlapped the write.
	#[test]
	fn a_forward_mapping_write_is_bracketed_by_its_begin_and_end() {
		let mut w = WrittenTables::default();
		let before = w.doc_lookup_writes();
		assert!(before.is_some());
		let before_n = before.unwrap();
		let began = w.note_key(&dj_key("a"));
		assert!(began, "a forward mapping key");
		assert_eq!(w.doc_lookup_writes(), None, "none is known while it is under way");
		w.end_doc_lookup_write();
		let after = w.doc_lookup_writes();
		assert!(after.is_some());
		assert_eq!(after, Some(before_n + 2), "the write counts when it begins and when it ends");
		assert!(!w.note_key(&record_key(1, 2, "a")), "a record is not a forward mapping");
		assert_eq!(w.doc_lookup_writes(), after, "and moves nothing");
		let mut other = w.doc_lookup_writes();
		w.note_rollback();
		assert_ne!(w.doc_lookup_writes(), other, "a rollback moves the count");
		other = w.doc_lookup_writes();
		let began = w.note_range(b"/*", b"/+");
		assert!(!began && w.doc_lookup_writes().is_none(), "no count stands past every table");
		let _ = other;
	}

	/// A write of an index's value shape key marks the transaction as having written build
	/// state, as does one of the build state key and a range delete over either.
	#[test]
	fn an_index_shape_key_is_a_build_state_write() {
		use surrealdb_kvs::key::{KVKey, KVSubspace};

		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::key::schema::{
			BuildStateKey, IndexBuildShapesKey, IndexShapesKey, IndexShapesTrackedKey,
		};
		let tb = surrealdb_expr::val::TableName::from("t");
		let (ns, db, ix) = (NamespaceId(1), DatabaseId(2), IndexId(7));
		let keys = [
			IndexShapesKey {
				ns,
				db,
				tb: std::borrow::Cow::Borrowed(&tb),
				ix,
			}
			.encode_key()
			.unwrap(),
			IndexBuildShapesKey {
				ns,
				db,
				tb: std::borrow::Cow::Borrowed(&tb),
				ix,
			}
			.encode_key()
			.unwrap(),
			IndexShapesTrackedKey {
				ns,
				db,
				tb: std::borrow::Cow::Borrowed(&tb),
				ix,
			}
			.encode_key()
			.unwrap(),
			BuildStateKey {
				ns,
				db,
				tb: std::borrow::Cow::Borrowed(&tb),
				ix,
			}
			.encode_key()
			.unwrap(),
		];
		for key in &keys {
			assert!(written(&[key.as_slice()]).wrote_build_state(), "{key:?}");
		}
		assert!(!written(&[&record_key(1, 2, "t")]).wrote_build_state(), "a record");
		assert!(!written(&[&dj_key("t")]).wrote_build_state(), "a forward mapping");
		let root = crate::key::schema::TblRoot {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(&tb),
		};
		let subtree = root.raw_range().unwrap();
		let (start, end) = subtree.bounds();
		let mut w = WrittenTables::default();
		let _ = w.note_range(start, end);
		assert!(w.wrote_build_state(), "a table's whole subtree");
	}

	fn tables(written: &WrittenTables) -> Vec<Vec<u8>> {
		written.roots.clone()
	}

	/// The encoded root [`WrittenTables`] records for the table `tb` of database `db` in
	/// namespace `ns`.
	fn root_of(ns: u32, db: u32, tb: &str) -> Vec<u8> {
		encoded_root(&table(ns, db, tb)).unwrap()
	}

	#[test]
	fn a_written_key_names_the_table_whose_subtree_holds_it() {
		use surrealdb_kvs::key::{KVKey, KVSubspace};
		// Names one of which is a prefix of another, and names holding the bytes the key encoding
		// escapes or ends a name with.
		for (name, other) in
			[("a", "ab"), ("a\0", "a"), ("a\u{1}b", "a"), ("*", "**"), ("t", "t\u{ff}")]
		{
			let record = record_key(1, 2, name);
			let other_record = record_key(1, 2, other);
			let tb = surrealdb_expr::val::TableName::from(name);
			let index = crate::key::schema::IdxRoot {
				ns: crate::catalog::NamespaceId(1),
				db: crate::catalog::DatabaseId(2),
				tb: std::borrow::Cow::Borrowed(&tb),
				ix: crate::catalog::IndexId(7),
			}
			.encode_bound()
			.unwrap();
			let w = written(&[&record, index.as_slice()]);
			assert_eq!(
				tables(&w),
				vec![root_of(1, 2, name)],
				"{name:?}: a record and an index key"
			);
			assert!(!w.every);
			let w = written(&[&record, &other_record]);
			assert_eq!(
				tables(&w),
				vec![root_of(1, 2, name), root_of(1, 2, other)],
				"{name:?} and {other:?} are different tables"
			);
		}
		// A database's own keys, and keys outside every namespace, write no table.
		let tb = surrealdb_expr::val::TableName::from("a");
		let definition = crate::key::schema::TableKey {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(2),
			tb: std::borrow::Cow::Borrowed(&tb),
		}
		.encode_key()
		.unwrap();
		let w = written(&[&definition, b"/!wb", b"/!nd\x01"]);
		assert!(tables(&w).is_empty() && !w.every);
	}

	#[test]
	fn a_written_range_names_its_table_only_when_it_stays_in_one() {
		use surrealdb_kvs::key::KVSubspace;
		let tb = surrealdb_expr::val::TableName::from("a");
		let root = crate::key::schema::TblRoot {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(2),
			tb: std::borrow::Cow::Borrowed(&tb),
		};
		let subtree = root.raw_range().unwrap();
		let (start, end) = subtree.bounds();
		let mut w = WrittenTables::default();
		let _ = w.note_range(start, end);
		assert_eq!(tables(&w), vec![root_of(1, 2, "a")], "a table's whole subtree");
		assert!(!w.every);
		let bound = root.encode_bound().unwrap();
		let mut w = WrittenTables::default();
		let _ = w.note_range(bound.as_slice(), end);
		assert_eq!(tables(&w), vec![root_of(1, 2, "a")], "the subtree from its own bound");
		let mut w = WrittenTables::default();
		let _ = w.note_range(start, &record_key(1, 2, "b"));
		assert!(w.every, "a range past the table's subtree");
		let mut w = WrittenTables::default();
		let _ = w.note_range(b"/*", b"/+");
		assert!(w.every, "every table");
		let mut w = WrittenTables::default();
		let _ = w.note_range(b"/!wt", b"/!wu");
		assert!(tables(&w).is_empty() && !w.every, "a range outside every table");
	}

	#[test]
	fn a_later_wait_on_a_fenced_table_does_not_wait_for_its_fenced_epochs() {
		let live = LiveWrites::default();
		let (t, u) = (table(1, 2, "t"), table(1, 2, "u"));
		let held = live.open(S);
		let first = live.advance();
		assert!(!live.ended_since_for(first, Some(std::slice::from_ref(&t))));
		// The first wait on `t` runs its bound.
		live.fence_tables_through(first, std::slice::from_ref(&t));
		let second = live.advance();
		assert!(
			live.ended_since_for(second, Some(std::slice::from_ref(&t))),
			"an epoch fenced for the table holds a later wait on it up no longer"
		);
		assert!(!live.ended_since_for(second, Some(std::slice::from_ref(&u))), "another table");
		assert!(!live.ended_since_for(second, Some(&[t.clone(), u])), "and one more");
		assert!(!live.ended_since_for(second, None), "a wait on every table");
		assert!(!live.ended_since_for(second, Some(&[])), "a wait on no table it can name");
		// A commit of the fenced epoch under way still holds the wait up.
		assert!(
			!live.begin_commit(held, S, &parking_lot::Mutex::new(WrittenTables::default())),
			"it writes no fenced table"
		);
		assert!(!live.ended_since_for(second, Some(std::slice::from_ref(&t))));
		live.end_commit(held, S);
		// A transaction of an epoch after the fence still holds a later wait up.
		let later = live.open(S);
		let third = live.advance();
		assert!(!live.ended_since_for(third, Some(std::slice::from_ref(&t))));
		live.close(later, S);
		assert!(live.ended_since_for(third, Some(std::slice::from_ref(&t))));
		live.close(held, S);
	}

	#[test]
	fn the_current_epochs_counts_are_kept_until_it_is_past() {
		let live = LiveWrites::default();
		let epoch = live.open(S);
		live.close(epoch, S);
		assert!(live.counts(S).lock().contains_key(&epoch), "the current epoch's entry is kept");
		assert!(live.ended_since(epoch), "and counts nothing open");
		let since = live.advance();
		assert_eq!(since, epoch);
		assert!(
			!live.counts(S).lock().contains_key(&epoch),
			"a past epoch's empty entry is dropped"
		);
		// A past epoch's entry is dropped once its last transaction ends.
		let next = live.open(S);
		live.advance();
		assert!(live.counts(S).lock().contains_key(&next));
		live.close(next, S);
		assert!(live.counts(S).lock().is_empty());
	}

	#[test]
	fn a_commit_reads_no_written_table_while_no_table_is_fenced() {
		let live = LiveWrites::default();
		let epoch = live.open(S);
		let written = parking_lot::Mutex::new(written(&[&record_key(1, 2, "a")]));
		// Held, so a commit that reads it would block.
		let held = written.lock();
		assert!(!live.begin_commit(epoch, S, &written), "no table is fenced");
		live.end_commit(epoch, S);
		drop(held);
		live.fence_tables_through(epoch, &[table(1, 2, "a")]);
		assert!(live.begin_commit(epoch, S, &written), "the fenced table's write");
		live.end_commit(epoch, S);
		live.close(epoch, S);
	}

	#[test]
	fn a_wait_passes_a_parked_transaction_only_for_the_tables_it_waits_on() {
		let live = LiveWrites::default();
		let (writes_a, writes_b) =
			(written(&[&record_key(1, 2, "a")]), written(&[&record_key(1, 2, "b")]));
		let parked = live.park();
		let wait = live.begin_wait(Some(&[table(1, 2, "b")]));
		let passed = live.rejoin(parked);
		live.end_wait(wait);
		assert!(!passed.refuses(&writes_a), "a wait on another table");
		assert!(passed.refuses(&writes_b), "a wait on a table it writes");
		assert!(!passed.refuses(&written(&[&record_key(1, 3, "b")])), "another database's table");
		assert!(!passed.refuses(&written(&[])), "a transaction that wrote no table");
		let mut every = WrittenTables::default();
		let _ = every.note_range(b"/*", b"/+");
		assert!(passed.refuses(&every), "a write that may be of any table");
		// A wait on every table refuses every write.
		let parked = live.park();
		let wait = live.begin_wait(None);
		live.end_wait(wait);
		let passed = live.rejoin(parked);
		assert!(passed.refuses(&writes_a) && passed.refuses(&written(&[])));
		// A wait running when the transaction is set aside may pass it too.
		let wait = live.begin_wait(Some(&[table(1, 2, "a")]));
		let parked = live.park();
		live.end_wait(wait);
		assert!(live.rejoin(parked).refuses(&writes_a), "a wait running when it was set aside");
		// One that began and ended before, or begins after the rejoin, does not.
		let wait = live.begin_wait(Some(&[table(1, 2, "a")]));
		live.end_wait(wait);
		let parked = live.park();
		let passed = live.rejoin(parked);
		live.end_wait(live.begin_wait(None));
		assert!(passed.is_empty(), "{passed:?}");
		assert!(live.waits.lock().parked.is_empty(), "a rejoined transaction is forgotten");
		assert!(live.rejoin(parked).refuses(&written(&[])), "a set-aside with no record refuses");
		assert_eq!(live.waits(), (5, 0));
	}

	#[test]
	fn a_parked_transaction_is_credited_only_its_own_builds_waits() {
		let live = LiveWrites::default();
		let writes_a = written(&[&record_key(1, 2, "a")]);
		let parked = live.park();
		// Its own build's wait, and another build's of the same table.
		live.end_wait(live.begin_wait(Some(&[table(1, 2, "a")])));
		live.end_wait(live.begin_wait(Some(&[table(1, 2, "a")])));
		let mut passed = live.rejoin(parked);
		passed.credit(1, &table(1, 2, "b"));
		assert!(passed.refuses(&writes_a), "a credit on another table takes nothing back");
		passed.credit(1, &table(1, 2, "a"));
		assert!(passed.refuses(&writes_a), "the other build's wait still passed it");
		passed.credit(1, &table(1, 2, "a"));
		assert!(passed.is_empty());
		// Passed waits gathered over two set-asides are judged together.
		let mut gathered = PassedWaits::default();
		let parked = live.park();
		live.end_wait(live.begin_wait(Some(&[table(1, 2, "a")])));
		gathered.merge(live.rejoin(parked));
		let parked = live.park();
		gathered.merge(live.rejoin(parked));
		assert!(gathered.refuses(&writes_a), "a wait that passed an earlier set-aside");
	}

	#[test]
	fn a_wait_on_several_tables_passes_a_parked_transaction_for_each() {
		let live = LiveWrites::default();
		let writes = |tb: &str| written(&[&record_key(1, 2, tb)]);
		let parked = live.park();
		// An answer to barriers on `b` and `c`, two of them on `b`.
		let tables = [table(1, 2, "b"), table(1, 2, "c"), table(1, 2, "b")];
		live.end_wait(live.begin_wait(Some(&tables)));
		let mut passed = live.rejoin(parked);
		assert!(!passed.refuses(&writes("a")), "a table it does not wait on");
		assert!(passed.refuses(&writes("b")) && passed.refuses(&writes("c")));
		// Counted once on each of its tables.
		passed.credit(1, &table(1, 2, "b"));
		assert!(!passed.refuses(&writes("b")), "{passed:?}");
		assert!(passed.refuses(&writes("c")));
		// A wait on no table passes nothing.
		let parked = live.park();
		live.end_wait(live.begin_wait(Some(&[])));
		assert!(live.rejoin(parked).is_empty());
	}

	#[test]
	fn an_answer_learns_which_barriers_it_covers_are_its_own() {
		let live = LiveWrites::default();
		live.note_own_barrier(2);
		live.note_own_barrier(4);
		assert_eq!(live.own_barriers_covered(1, 4), [2, 4].into());
		assert_eq!(live.own_barriers_covered(0, 3), [2].into());
		assert!(live.own_barriers_covered(2, 3).is_empty(), "barrier 3 is another datastore's");
	}

	#[test]
	fn an_answer_made_again_judges_its_own_barriers_again() {
		let live = LiveWrites::default();
		live.note_own_barrier(1);
		live.note_own_barrier(2);
		assert!(live.answers_only_own(0, 2));
		// The answer is not recorded, and is made again.
		assert!(live.answers_only_own(0, 2), "the barriers it covers are still its own");
		live.forget_own_barriers_through(2);
		live.note_own_barrier(3);
		assert!(live.answers_only_own(2, 3));
		assert!(!live.answers_only_own(2, 4), "barrier 4 is another datastore's");
		assert!(!live.answers_only_own(0, 3), "barriers 1 and 2, answered, are forgotten");
	}

	#[test]
	fn a_table_fence_refuses_only_the_writes_of_its_tables() {
		let live = LiveWrites::default();
		let epoch = live.advance();
		live.fence_tables_through(epoch, &[table(1, 2, "a")]);
		let refused = |written: WrittenTables| {
			let refused = live.begin_commit(epoch, S, &parking_lot::Mutex::new(written));
			live.end_commit(epoch, S);
			refused
		};
		assert!(refused(written(&[&record_key(1, 2, "a")])), "a write of the fenced table");
		assert!(!refused(written(&[&record_key(1, 2, "b")])), "another table's write");
		assert!(!refused(written(&[&record_key(1, 3, "a")])), "another database's table");
		assert!(!refused(written(&[])), "a transaction that wrote no table");
		let mut every = WrittenTables::default();
		let _ = every.note_range(b"/*", b"/+");
		assert!(refused(every), "a write that may be of any table");
		let later = live.advance();
		assert!(
			!live.begin_commit(
				later,
				S,
				&parking_lot::Mutex::new(written(&[&record_key(1, 2, "a")]))
			),
			"a later epoch"
		);
		live.end_commit(later, S);
	}

	use std::time::Duration;

	const BOUND: Duration = Duration::from_millis(50);

	/// Whether a commit of a transaction of `epoch` that writes a record of table `tb` is refused.
	fn commit_refused(live: &LiveWrites, epoch: u64, tb: &str) -> bool {
		let writes = parking_lot::Mutex::new(written(&[&record_key(1, 2, tb)]));
		let refused = live.begin_commit(epoch, S, &writes);
		live.end_commit(epoch, S);
		refused
	}

	#[test]
	fn a_hold_holds_back_only_the_writes_counted_in_after_its_wait_began() {
		let live = LiveWrites::default();
		let t = table(1, 2, "t");
		assert!(!live.holds_writes());
		let (hold, registered) = live.hold_table(t.clone(), OWNER);
		assert!(registered && live.holds_writes());
		let older = live.open(S);
		assert!(live.hold_on(&t, older, WRITER).is_none(), "no wait has begun");
		assert!(
			live.hold_on(&t, older + 1, WRITER).is_none(),
			"and none is held for one that will"
		);
		// A wait that is not the definition's build's does not begin the hold.
		live.advance();
		let newer = live.open(S);
		assert!(live.hold_on(&t, newer, WRITER).is_none(), "another wait did not begin it");
		let since = live.advance_holding(Some(&hold));
		assert!(live.hold_on(&t, older, WRITER).is_none(), "a write the wait waits for");
		assert!(live.hold_on(&t, since, WRITER).is_none(), "the epoch the wait waits through");
		let later = live.open(S);
		assert!(later > since);
		assert!(live.hold_on(&t, later, WRITER).is_some(), "a write that opened after it");
		assert!(live.hold_on(&t, later, OWNER).is_none(), "the defining transaction");
		assert!(live.hold_on(&table(1, 2, "u"), later, WRITER).is_none(), "another table");
		assert!(live.hold_on(&table(1, 3, "t"), later, WRITER).is_none(), "another database");
		// A second build of the definition waits through a later epoch; the first stands.
		let again = live.advance_holding(Some(&hold));
		assert!(again > since);
		assert!(live.hold_on(&t, later, WRITER).is_some(), "the first wait's epoch stands");
		live.close(older, S);
		live.close(newer, S);
		live.close(later, S);
	}

	#[test]
	fn a_hold_is_registered_once_per_transaction_and_table() {
		let live = LiveWrites::default();
		let t = table(1, 2, "t");
		let (first, registered) = live.hold_table(t.clone(), OWNER);
		assert!(registered);
		let (again, registered) = live.hold_table(t.clone(), OWNER);
		assert!(!registered && Arc::ptr_eq(&first, &again), "one per transaction and table");
		let (other, registered) = live.hold_table(t.clone(), WRITER);
		assert!(registered && !Arc::ptr_eq(&first, &other), "another transaction's");
		let (other_table, registered) = live.hold_table(table(1, 2, "u"), OWNER);
		assert!(registered && !Arc::ptr_eq(&first, &other_table), "another table's");
		assert!(live.hold_of(&t, OWNER).is_some_and(|held| Arc::ptr_eq(&held, &first)));
		assert!(live.hold_of(&t, 9).is_none());
		assert_eq!(first.table(), &t);
		live.end_hold(&first);
		assert!(live.hold_of(&t, OWNER).is_none(), "an ended hold is gone");
		assert!(live.holds_writes(), "the others remain");
		live.end_hold(&other);
		live.end_hold(&other_table);
		assert!(!live.holds_writes());
	}

	/// A page committing for one table holds back the removal of that table alone, and the
	/// entries go once nothing registers or holds them.
	#[tokio::test]
	async fn a_page_committing_holds_back_only_removals_of_its_own_table() {
		let removals = TableRemovals::default();
		let (t, u) = (table(1, 2, "t"), table(1, 2, "u"));
		let page = removals.admit(&t).await.expect("no removal of t is registered");
		let wait = std::time::Duration::from_millis(50);
		tokio::time::timeout(wait, removals.begin(u.clone())).await.expect("u is not held back");
		assert!(tokio::time::timeout(wait, removals.begin(t.clone())).await.is_err(), "t is");
		drop(page);
		removals.begin(t.clone()).await;
		assert!(removals.removing(&t) && removals.removing(&u));
		assert!(removals.admit(&t).await.is_none(), "a page of t is refused while it is removed");
		removals.end(&t);
		removals.end(&u);
		assert!(removals.tables.lock().is_empty(), "nothing is left registered");
	}

	#[tokio::test]
	async fn a_hold_that_ends_without_committing_releases_what_it_held_back() {
		let live = LiveWrites::default();
		let t = table(1, 2, "t");
		let (hold, _) = live.hold_table(t.clone(), OWNER);
		live.advance_holding(Some(&hold));
		let held = live.open(S);
		assert_eq!(hold.ended(BOUND).await, HoldVerdict::TimedOut, "open");
		live.begin_definition_commit(WRITER);
		assert_eq!(hold.ended(BOUND).await, HoldVerdict::TimedOut, "another transaction commits");
		live.end_hold(&hold);
		live.end_hold(&hold);
		assert_eq!(hold.ended(BOUND).await, HoldVerdict::Released);
		assert!(live.hold_on(&t, held, WRITER).is_none() && !live.holds_writes());
		assert!(!commit_refused(&live, held, "t"), "a rollback leaves the index undefined");
		live.close(held, S);
	}

	#[tokio::test]
	async fn a_hold_that_ends_committing_refuses_the_writes_that_could_not_see_its_index() {
		let live = LiveWrites::default();
		let t = table(1, 2, "t");
		let (hold, _) = live.hold_table(t.clone(), OWNER);
		live.advance_holding(Some(&hold));
		let held = live.open(S);
		live.begin_definition_commit(OWNER);
		assert_eq!(hold.ended(BOUND).await, HoldVerdict::TimedOut, "committing is not ended");
		live.end_hold(&hold);
		live.end_hold(&hold);
		assert_eq!(hold.ended(BOUND).await, HoldVerdict::Committing);
		assert!(commit_refused(&live, held, "t"), "a write of the table that began before it");
		assert!(!commit_refused(&live, held, "u"), "another table's");
		let after = live.open(S);
		assert!(!commit_refused(&live, after, "t"), "a write that began after sees the index");
		live.close(held, S);
		live.close(after, S);
	}

	#[tokio::test]
	async fn a_write_waiting_for_a_hold_is_woken_when_it_ends() {
		let live = LiveWrites::default();
		let (hold, _) = live.hold_table(table(1, 2, "t"), OWNER);
		let waiting = tokio::spawn({
			let hold = Arc::clone(&hold);
			async move { hold.ended(Duration::from_secs(60)).await }
		});
		for _ in 0..10 {
			tokio::task::yield_now().await;
		}
		assert!(!waiting.is_finished());
		live.begin_definition_commit(OWNER);
		live.end_hold(&hold);
		assert_eq!(waiting.await.unwrap(), HoldVerdict::Committing);
	}

	#[test]
	fn a_hold_wait_can_be_shortened_and_restored() {
		let live = LiveWrites::default();
		assert_eq!(live.definition_hold_wait(), DEFINITION_HOLD_WAIT);
		live.set_definition_hold_wait(Duration::from_millis(20));
		assert_eq!(live.definition_hold_wait(), Duration::from_millis(20));
		live.set_definition_hold_wait(Duration::ZERO);
		assert_eq!(live.definition_hold_wait(), DEFINITION_HOLD_WAIT);
	}

	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::pending_guard::GuardedIndex;

	fn index(ix: u32) -> GuardedIndex {
		GuardedIndex::new(
			crate::pending_guard::PendingGuardFamily::DiskAnn,
			NamespaceId(1),
			DatabaseId(2),
			"t".into(),
			IndexId(ix),
		)
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
