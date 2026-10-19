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
	/// The catalog definitions read under each catalog epoch
	/// ([`crate::cache::epoch`]).
	pub(crate) epoch_catalog: crate::cache::epoch::EpochCatalog,
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
	/// Table migration work, notified when a commit queues a table's rewrite
	/// or verification so the job starts on the commit instead of waiting
	/// out its interval.
	pub table_migration: Notify,
	/// The transactions open through the datastore, by the epoch they opened
	/// in; see [`LiveTransactions`].
	pub(crate) live: LiveTransactions,
	/// The write transactions open through the datastore, by the epoch they
	/// opened in; see [`LiveWrites`].
	pub(crate) live_writes: LiveWrites,
	/// When this datastore last recorded its heartbeat, for the fence that stops a datastore
	/// whose heartbeat has lapsed from committing writes to a store it shares; see
	/// [`WriteFence`].
	pub(crate) write_fence: WriteFence,
	/// Adjacency scopes a read found carrying enough unfolded keys to be
	/// worth packing. In memory because reads cannot write a queue entry;
	/// the fold task drains this set into the durable queue on its pass.
	graph_fold_observations: Mutex<HashSet<GraphFoldScope>>,
	/// Work the next commit through this datastore runs once its DiskANN guards
	/// are covered and before its storage commit, as the tests of a commit that
	/// stalls there need.
	#[cfg(any(test, feature = "test-hooks"))]
	before_commit: Mutex<Option<BeforeCommitHook>>,
	/// Whether the next commit through this datastore that has rollback
	/// actions registered reports its outcome unknown once it has applied, as
	/// the tests of a commit that applied but could not say so need, with the
	/// work to run between the commit applying and the report.
	#[cfg(any(test, feature = "test-hooks"))]
	commit_applied_unknown: Mutex<Option<Option<BeforeCommitHook>>>,
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

	/// Drain the read-observed scopes for the fold task's pass.
	pub fn drain_graph_fold_observations(&self) -> Vec<GraphFoldScope> {
		self.graph_fold_observations.lock().drain().collect()
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
/// outcome unknown. A build elsewhere leaves a node out of its wait only once the node's heartbeat
/// is older than its write fence, this bound and the margin within which clocks must agree
/// together, so a commit admitted just before the fence lapsed has landed, or never will, by then.
/// Abandoning the commit stops the client's retries and backoff, which are what would otherwise
/// leave it running for any length of time.
pub const WRITE_COMMIT_BOUND: std::time::Duration = std::time::Duration::from_secs(20);

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
	/// Open write transactions, and commits under way, by the epoch they opened in.
	epochs: parking_lot::Mutex<std::collections::BTreeMap<u64, EpochCounts>>,
	/// Write transactions of an epoch below this are refused at commit, whatever they write.
	fence: AtomicU64,
	/// Write transactions of an epoch below a table's entry that write that table are refused at
	/// commit. Keyed by the table's encoded root, as [`WrittenTables`] records it.
	table_fences: parking_lot::Mutex<HashMap<Vec<u8>, u64>>,
	/// Whether [`Self::table_fences`] holds any fence, set once one is, so a commit that no
	/// table fence can refuse reads neither the fences nor the tables it wrote.
	has_table_fences: AtomicBool,
	/// How many waits have begun, and how many are running, so a transaction set aside for a
	/// while can tell whether a wait may have passed it ([`Self::waits`]).
	waits_begun: AtomicU64,
	waits_running: AtomicU64,
	/// The write barriers builds through this datastore raised and its answer has not yet passed
	/// ([`Self::answers_only_own`]).
	own_barriers: parking_lot::Mutex<std::collections::BTreeSet<u64>>,
	/// Held across a raise of the write barrier and its record as this datastore's own, and by an
	/// answer while it reads the barrier and judges whose it is, so no answer sees a barrier this
	/// datastore raised before it is recorded as its own.
	raises: tokio::sync::Mutex<()>,
}

/// A table a fence refuses the writes of.
pub type FencedTable =
	(surrealdb_catalog::NamespaceId, surrealdb_catalog::DatabaseId, surrealdb_expr::val::TableName);

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
	/// Record a write of `key`.
	pub fn note_key(&mut self, key: &[u8]) {
		if self.every || self.roots.iter().any(|root| key.starts_with(root)) {
			return;
		}
		match table_root(key) {
			Some(Some(len)) => self.roots.push(key[..len].to_vec()),
			Some(None) => self.every = true,
			None => {}
		}
	}

	/// What is recorded now, for [`Self::restore`]: the record only grows, so its length and
	/// whether it reached every table say what it held.
	pub fn mark(&self) -> WrittenMark {
		WrittenMark {
			roots: self.roots.len(),
			every: self.every,
		}
	}

	/// Forget what was recorded since `mark`, whose writes a rollback undid.
	pub fn restore(&mut self, mark: WrittenMark) {
		self.roots.truncate(mark.roots);
		self.every = mark.every;
	}

	/// Record a write of every key in `[start, end)`.
	pub fn note_range(&mut self, start: &[u8], end: &[u8]) {
		if self.every
			|| start >= end
			|| self.roots.iter().any(|root| start.starts_with(root) && within_subtree(root, end))
		{
			return;
		}
		match table_root(start) {
			Some(Some(len)) if within_subtree(&start[..len], end) => {
				self.roots.push(start[..len].to_vec())
			}
			_ if start < TABLE_SPACE.1 && end > TABLE_SPACE.0 => self.every = true,
			_ => {}
		}
	}
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

#[derive(Default)]
struct EpochCounts {
	open: usize,
	committing: usize,
}

impl LiveWrites {
	/// Count a write transaction about to take its snapshot into the current epoch, answering the
	/// epoch to hand to [`Self::close`].
	pub(crate) fn open(&self) -> u64 {
		let mut epochs = self.epochs.lock();
		let epoch = self.epoch.load(Ordering::SeqCst);
		epochs.entry(epoch).or_default().open += 1;
		epoch
	}

	/// Count a write transaction counted into `epoch` out again.
	pub(crate) fn close(&self, epoch: u64) {
		let mut epochs = self.epochs.lock();
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
		written: &parking_lot::Mutex<WrittenTables>,
	) -> bool {
		self.epochs.lock().entry(epoch).or_default().committing += 1;
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
	pub(crate) fn hold_commit(&self, epoch: u64) {
		self.epochs.lock().entry(epoch).or_default().committing += 1;
	}

	/// Count a commit counted by [`Self::begin_commit`] as no longer under way.
	pub(crate) fn end_commit(&self, epoch: u64) {
		let mut epochs = self.epochs.lock();
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
		let mut epochs = self.epochs.lock();
		let since = self.epoch.fetch_add(1, Ordering::SeqCst);
		self.forget_if_settled(&mut epochs, since);
		since
	}

	/// Whether every write transaction counted into `since` or an earlier epoch has ended, or, for
	/// a fenced epoch, can no longer commit: none of its commits is under way.
	pub fn ended_since(&self, since: u64) -> bool {
		let fence = self.fence.load(Ordering::SeqCst);
		self.epochs
			.lock()
			.range(..=since)
			.all(|(epoch, counts)| counts.committing == 0 && (counts.open == 0 || *epoch < fence))
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
		self.epochs
			.lock()
			.range(..=since)
			.all(|(epoch, counts)| counts.committing == 0 && (counts.open == 0 || *epoch < fence))
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
		self.epochs.lock().range(..=since).all(|(_, counts)| counts.committing == 0)
	}

	/// Count a wait that is about to read the epoch it waits past, until
	/// [`Self::end_wait`]: a build's, or a node's answer to a write barrier another node's build
	/// raised. See [`crate::factory::WriteWait`].
	pub(crate) fn begin_wait(&self) {
		self.waits_running.fetch_add(1, Ordering::SeqCst);
		self.waits_begun.fetch_add(1, Ordering::SeqCst);
	}

	/// Count a wait counted by [`Self::begin_wait`] as no longer running.
	pub(crate) fn end_wait(&self) {
		self.waits_running.fetch_sub(1, Ordering::SeqCst);
	}

	/// How many waits have begun, and how many are running.
	pub fn waits(&self) -> (u64, u64) {
		(self.waits_begun.load(Ordering::SeqCst), self.waits_running.load(Ordering::SeqCst))
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

	/// Forget the barriers this datastore raised through `answered`, whose answer is recorded.
	pub fn forget_own_barriers_through(&self, answered: u64) {
		self.own_barriers.lock().retain(|barrier| *barrier > answered);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
			written.note_key(key);
		}
		written
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
		w.note_range(start, end);
		assert_eq!(tables(&w), vec![root_of(1, 2, "a")], "a table's whole subtree");
		assert!(!w.every);
		let bound = root.encode_bound().unwrap();
		let mut w = WrittenTables::default();
		w.note_range(bound.as_slice(), end);
		assert_eq!(tables(&w), vec![root_of(1, 2, "a")], "the subtree from its own bound");
		let mut w = WrittenTables::default();
		w.note_range(start, &record_key(1, 2, "b"));
		assert!(w.every, "a range past the table's subtree");
		let mut w = WrittenTables::default();
		w.note_range(b"/*", b"/+");
		assert!(w.every, "every table");
		let mut w = WrittenTables::default();
		w.note_range(b"/!wt", b"/!wu");
		assert!(tables(&w).is_empty() && !w.every, "a range outside every table");
	}

	#[test]
	fn a_later_wait_on_a_fenced_table_does_not_wait_for_its_fenced_epochs() {
		let live = LiveWrites::default();
		let (t, u) = (table(1, 2, "t"), table(1, 2, "u"));
		let held = live.open();
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
			!live.begin_commit(held, &parking_lot::Mutex::new(WrittenTables::default())),
			"it writes no fenced table"
		);
		assert!(!live.ended_since_for(second, Some(std::slice::from_ref(&t))));
		live.end_commit(held);
		// A transaction of an epoch after the fence still holds a later wait up.
		let later = live.open();
		let third = live.advance();
		assert!(!live.ended_since_for(third, Some(std::slice::from_ref(&t))));
		live.close(later);
		assert!(live.ended_since_for(third, Some(std::slice::from_ref(&t))));
		live.close(held);
	}

	#[test]
	fn the_current_epochs_counts_are_kept_until_it_is_past() {
		let live = LiveWrites::default();
		let epoch = live.open();
		live.close(epoch);
		assert!(live.epochs.lock().contains_key(&epoch), "the current epoch's entry is kept");
		assert!(live.ended_since(epoch), "and counts nothing open");
		let since = live.advance();
		assert_eq!(since, epoch);
		assert!(!live.epochs.lock().contains_key(&epoch), "a past epoch's empty entry is dropped");
		// A past epoch's entry is dropped once its last transaction ends.
		let next = live.open();
		live.advance();
		assert!(live.epochs.lock().contains_key(&next));
		live.close(next);
		assert!(live.epochs.lock().is_empty());
	}

	#[test]
	fn a_commit_reads_no_written_table_while_no_table_is_fenced() {
		let live = LiveWrites::default();
		let epoch = live.open();
		let written = parking_lot::Mutex::new(written(&[&record_key(1, 2, "a")]));
		// Held, so a commit that reads it would block.
		let held = written.lock();
		assert!(!live.begin_commit(epoch, &written), "no table is fenced");
		live.end_commit(epoch);
		drop(held);
		live.fence_tables_through(epoch, &[table(1, 2, "a")]);
		assert!(live.begin_commit(epoch, &written), "the fenced table's write");
		live.end_commit(epoch);
		live.close(epoch);
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
			let refused = live.begin_commit(epoch, &parking_lot::Mutex::new(written));
			live.end_commit(epoch);
			refused
		};
		assert!(refused(written(&[&record_key(1, 2, "a")])), "a write of the fenced table");
		assert!(!refused(written(&[&record_key(1, 2, "b")])), "another table's write");
		assert!(!refused(written(&[&record_key(1, 3, "a")])), "another database's table");
		assert!(!refused(written(&[])), "a transaction that wrote no table");
		let mut every = WrittenTables::default();
		every.note_range(b"/*", b"/+");
		assert!(refused(every), "a write that may be of any table");
		let later = live.advance();
		assert!(
			!live.begin_commit(later, &parking_lot::Mutex::new(written(&[&record_key(1, 2, "a")]))),
			"a later epoch"
		);
		live.end_commit(later);
	}
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
