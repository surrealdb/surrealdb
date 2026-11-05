use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{Result, ensure};
use chrono::Utc;
use common::time::{sleep, timeout};
use futures::channel::oneshot::{Sender, channel};
use surrealdb_datastore::close::{ActionScope, CommitAction, RollbackAction};
use tokio::sync::{Notify, OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::Instant;

use super::after_commit::{AwaitBuildAfterCommit, RemoveFailedDefinition};
use super::count::CountPrimaryProgress;
use super::kind::{BaselinePolicy, BatchContext, BatchRange, Kind};
use super::replay::{InitialBatch, ReplayUntil};
use super::state::{
	BuildOwner, analyzers_analyse_alike, build_basis_is_stale, build_claimable, build_owner,
	build_owner_expired, clear_lapsed_online_owner, delete_durable_build_artifacts,
	delete_stale_build_queues, durable_index_error_reason, durable_report_count,
	is_condition_not_met, online_owner_lapsed, report_status_from_phase,
	withdraw_stale_lapsed_build,
};
use super::{
	AcquiredBuild, BUILD_CLOSING_SLEEP, BUILD_OWNER_LEASE_SECS, BUILD_WAITING_HEARTBEAT,
	BuildGeneration, IndexBuildBasis, IndexBuildPhase, IndexBuildReportStatus, IndexBuildState,
	IndexBuilding, build_abort_deadline,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{
	AnalyzerDefinition, DatabaseId, Error as CatalogError, IndexDefinition, IndexId, NamespaceId,
	TableId,
};
use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::{EngineError, Error};
use crate::exec::context::{NamespaceContext, RootContext};
use crate::exec::memory::MemoryHost;
use crate::exec::permission::{PhysicalIndexEntry, index_entry_physical};
use crate::exec::planner::Planner;
use crate::exec::vm::closure::ClosureUnits;
use crate::exec::{DatabaseContext, ExecutionContext};
use crate::idx::IndexKeyBase;
use crate::key::schema::{IdxRoot, RecordKey, RecordPrefix};
use crate::key::{KVKey, KVKeyDecode, Resumable};
use crate::kvs::sequences::Sequences;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	DatastoreError, Direction, INDEXING_BATCH_MAX_BYTES, INDEXING_PROBE_BATCH_SIZE,
	INDEXING_SCAN_BATCH_SIZE, Transaction, TransactionFactory, TransactionType,
	is_retryable_transaction_conflict, is_shutdown_error,
};
/// How long to wait before retrying a conflicting index-build cleanup.
/// Matches the reservation-release pause it runs beside.
const INDEX_BUILD_CLEANUP_RETRY_SLEEP: Duration = Duration::from_millis(100);

/// The pause before a build retries a transaction that conflicted
/// ([`Building::conflict_pause`]): every pause of a build no statement awaits,
/// and the longest of a build a statement awaits.
///
/// Builds of indexes on one table claim the records' mappings in the table's
/// shared doc-ID space, so their batches conflict with one another: the first
/// to commit a batch claims its mappings, and every other build retries that
/// batch, now reading the mappings claimed. A pause this long lets the build
/// that won run ahead, so the others follow it batch after batch rather than
/// racing it for the next one.
const BUILD_CONFLICT_RETRY_MAX: Duration = Duration::from_millis(100);

/// The longest pause before the first retry of a build a statement awaits,
/// after a commit of its own: each further conflict in a row doubles it, up to
/// [`BUILD_CONFLICT_RETRY_MAX`]. A build that lost a batch to a background
/// build retries it at once, so the statement waits for little more than its
/// own work.
const BUILD_CONFLICT_RETRY_MIN: Duration = Duration::from_millis(2);
/// The keys one page of a bounded pass's wipe of an index deletes
/// ([`Building::wipe_index_pages`]).
const WIPE_PAGE_KEYS: u32 = 10_000;

/// How long an owned state transition waits before retrying a conflict or a
/// missed compare-and-swap. Matches the pause of the builder's other retries.
const BUILD_STATE_RETRY_SLEEP: Duration = Duration::from_millis(100);

/// How many times a [`TransitionMode::BestEffort`] transition retries before it
/// gives up, leaving `!bs` as it found it.
pub(super) const BEST_EFFORT_TRANSITION_RETRIES: usize = 5;

/// How many times [`Building::committed_catalog_holds_this_build`] reads the
/// committed catalog before it answers that the catalog does not hold the
/// build.
const COMMITTED_CATALOG_READ_ATTEMPTS: u32 = 4;

/// The pause after the first failed read of the committed catalog, doubled
/// after each one that follows.
const COMMITTED_CATALOG_READ_RETRY_SLEEP: Duration = Duration::from_millis(50);

/// Writes an owned state transition stages in its transaction, after the
/// compare-and-swap on `!bs` and before the commit, so they commit with it or
/// not at all. A stage returns `None` when it has nothing to write, which
/// costs no allocation.
type StagedWrites<'t> = Option<Pin<Box<dyn Future<Output = Result<()>> + Send + 't>>>;

/// The stage of a transition that writes nothing beside `!bs`.
fn no_staged_writes(_: &Transaction) -> StagedWrites<'_> {
	None
}

/// What an owned state transition does when it cannot apply, and how long it
/// keeps trying.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransitionMode {
	/// The transition must apply. A missing `!bs` or a generation this builder
	/// no longer owns is an error, and a conflict is retried until the
	/// transition commits or fails otherwise.
	Required,
	/// The transition applies only while this builder still owns the
	/// generation, and is otherwise a no-op that writes nothing. A conflict is
	/// retried at most [`BEST_EFFORT_TRANSITION_RETRIES`] times, so a caller
	/// that cannot fail — one running as a transaction's close action — is
	/// never held for long.
	BestEffort,
	/// The transition applies only while this builder still owns the
	/// generation, and is otherwise a no-op that writes nothing, as
	/// [`TransitionMode::BestEffort`]; but a conflict is retried until the
	/// transition commits or fails otherwise, as [`TransitionMode::Required`].
	/// For a transition no later writer would make in its place, whose loss
	/// would outlive the build. With a deadline, no retry starts once the pause
	/// before it would reach the deadline: the transition gives up, as a
	/// best-effort one does, leaving `!bs` as it found it.
	Persistent(Option<Instant>),
}

use crate::mem::ALLOC;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Process-local key used only to deduplicate active builder tasks.
pub(super) type SharedIndexKey = Arc<IndexKey>;

/// The build task has stopped writing (see [`Building::release_gate`]).
const RELEASE_GATE_BUILDER: u8 = 1 << 0;
/// The statement that publishes this build's definition has closed its
/// transaction (see [`Building::release_gate`]).
const RELEASE_GATE_STATEMENT: u8 = 1 << 1;
/// Both halves have arrived, so durable ownership can be released.
const RELEASE_GATE_OPEN: u8 = RELEASE_GATE_BUILDER | RELEASE_GATE_STATEMENT;

/// [`Building::rollback`]: nothing for the release to decide. The statement
/// committed, or its commit's outcome is unknown and the committed catalog
/// describes the build, which goes on.
const ROLLBACK_NONE: u8 = 0;
/// [`Building::rollback`]: the statement did not commit, and nothing has read
/// the committed catalog for it; the release reads it.
const ROLLBACK_UNDECIDED: u8 = 1;
/// [`Building::rollback`]: the statement's commit's outcome is unknown, and
/// the committed catalog, read as it rolled back, does not describe the build;
/// the release withdraws it without reading again.
const ROLLBACK_WITHDRAW: u8 = 2;

/// How [`Building::await_statement_closed`] ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatementWait {
	/// The statement's transaction committed or rolled back: its half of
	/// [`Building::release_gate`] arrived.
	Closed,
	/// The statement's transaction was dropped without closing, so neither
	/// close action will run.
	Dropped,
	/// The build was aborted.
	Aborted,
}

/// How often a build heartbeats its ownership while it waits on its
/// statement, per node, where a test shortens it.
#[cfg(test)]
pub(super) mod statement_heartbeat {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};
	use std::time::Duration;

	use uuid::Uuid;

	static EVERY: LazyLock<Mutex<HashMap<Uuid, Duration>>> = LazyLock::new(Default::default);

	/// Heartbeats every `every` on `node` until the returned guard drops.
	pub(crate) fn set(node: Uuid, every: Duration) -> Guard {
		EVERY.lock().expect("cadence lock").insert(node, every);
		Guard(node)
	}

	pub(super) fn every_for(node: Uuid) -> Option<Duration> {
		EVERY.lock().expect("cadence lock").get(&node).copied()
	}

	pub(crate) struct Guard(Uuid);

	impl Drop for Guard {
		fn drop(&mut self) {
			EVERY.lock().expect("cadence lock").remove(&self.0);
		}
	}
}

/// How many times builds on each node read the committed catalog to decide a
/// build whose statement did not commit
/// ([`Building::committed_catalog_holds_this_build`]), for the tests that
/// count them.
#[cfg(test)]
pub(super) mod committed_catalog_reads {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};

	use uuid::Uuid;

	static READS: LazyLock<Mutex<HashMap<Uuid, usize>>> = LazyLock::new(Default::default);

	pub(super) fn note(node: Uuid) {
		*READS.lock().expect("reads lock").entry(node).or_default() += 1;
	}

	/// How many reads `node` has made.
	pub(crate) fn count(node: Uuid) -> usize {
		READS.lock().expect("reads lock").get(&node).copied().unwrap_or_default()
	}
}

/// Whether an error reports that the build crossed the process memory
/// threshold (see [`Building::is_beyond_threshold`]).
///
/// Memory pressure is transient. The interrupted build can be retried: the
/// resume scan adopts it again once the owner lease expires. Such an error
/// must not be recorded as a permanent build failure.
fn is_memory_threshold_error(err: &anyhow::Error) -> bool {
	matches!(err.downcast_ref::<DatastoreError>(), Some(DatastoreError::QueryBeyondMemoryThreshold))
}

/// How many times a build on each node found write transactions older than its commit still
/// open, for the tests that commit one while a build waits for it.
#[cfg(test)]
pub(super) mod write_barrier_waits {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};

	use uuid::Uuid;

	static WAITS: LazyLock<Mutex<HashMap<Uuid, usize>>> = LazyLock::new(Default::default);

	pub(in crate::kvs::index) fn note(node: Uuid) {
		*WAITS.lock().unwrap_or_else(|e| e.into_inner()).entry(node).or_default() += 1;
	}

	/// How many times a build on `node` has waited at the write barrier.
	pub(in crate::kvs::index) fn count(node: Uuid) -> usize {
		WAITS.lock().unwrap_or_else(|e| e.into_inner()).get(&node).copied().unwrap_or(0)
	}
}

/// What each build a node started was lent, for the tests that check a
/// blocking statement lends its registration to its build.
#[cfg(test)]
pub(super) mod lent_places {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};

	use uuid::Uuid;

	/// Whether the last build of an index, by node and index name, carried
	/// the registration of a statement waiting on it.
	static NOTED: LazyLock<Mutex<HashMap<(Uuid, String), bool>>> = LazyLock::new(Default::default);

	pub(in crate::kvs::index) fn note(node: Uuid, index: &str, lent: bool) {
		NOTED.lock().unwrap_or_else(|e| e.into_inner()).insert((node, index.to_owned()), lent);
	}

	/// Whether the last build of `index` on `node` carried a lent
	/// registration, or `None` when no build of it started; the note is
	/// taken.
	pub(in crate::kvs::index) fn take(node: Uuid, index: &str) -> Option<bool> {
		NOTED.lock().unwrap_or_else(|e| e.into_inner()).remove(&(node, index.to_owned()))
	}
}

/// Probe the initial-batch commit injection sites, so tests can fail the
/// commit with a generic non-retryable error, with the shutdown-class error
/// the storage engines return during graceful shutdown, or with the memory
/// threshold error.
#[cfg(test)]
fn maybe_inject_initial_batch_commit_error(node_id: Uuid) -> Result<()> {
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchCommit,
		node_id,
	)?;
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown,
		node_id,
	)?;
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		node_id,
	)
}

#[derive(Hash, PartialEq, Eq)]
pub(super) struct IndexKey {
	pub(super) ns: NamespaceId,
	pub(super) db: DatabaseId,
	pub(super) tb: TableName,
	pub(super) ix: IndexId,
}

impl IndexKey {
	pub(super) fn new(ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) -> Self {
		Self {
			ns,
			db,
			tb: tb.to_owned(),
			ix,
		}
	}
}

/// How many pending entries an HNSW index's compaction must have deleted since
/// its pending range was last compacted in storage before a pass that drains
/// the range compacts it again.
///
/// A deleted pending entry stays in the storage engine as a tombstone until the
/// engine compacts it away, and on RocksDB a tombstone still in the memtable is
/// invisible to the deletion-triggered compaction it configures. Whenever a kNN
/// query or guard probe scans a pending range before the engine compacts those
/// tombstones, it steps over them: about 8 ns apiece, so 50 000 add about
/// 0.4 ms per scan. Below this many the cost is small, and compacting would
/// cost more I/O than it saves.
pub(crate) const PENDING_RANGE_COMPACTION_MIN_DELETES: usize = 10_000;

/// The least time between two compactions of one HNSW index's pending range,
/// so that ingest which the compactor keeps draining compacts once a minute at
/// most rather than after every pass.
pub(crate) const PENDING_RANGE_COMPACTION_INTERVAL: Duration = Duration::from_secs(60);

/// Pending deletions an HNSW index's compaction has made since its pending
/// range was last compacted in storage.
#[derive(Default)]
struct PendingDeletes {
	deleted: usize,
	compacted: Option<Instant>,
}

impl PendingDeletes {
	/// Whether this entry, at `now`, behaves as a fresh one: no deletions
	/// counted, and no compaction recent enough to space the next one from.
	fn is_idle(&self, now: Instant) -> bool {
		self.deleted == 0
			&& self.compacted.is_none_or(|at| {
				now.checked_duration_since(at)
					.is_none_or(|d| d >= PENDING_RANGE_COMPACTION_INTERVAL)
			})
	}

	/// Whether the range is due a compaction at `now`: enough deletions since
	/// the last one, and that one at least [`PENDING_RANGE_COMPACTION_INTERVAL`]
	/// ago. A due range starts counting again from `now`.
	fn take_due(&mut self, now: Instant) -> bool {
		let recent = self.compacted.is_some_and(|at| {
			now.checked_duration_since(at).is_some_and(|d| d < PENDING_RANGE_COMPACTION_INTERVAL)
		});
		if self.deleted < PENDING_RANGE_COMPACTION_MIN_DELETES || recent {
			return false;
		}
		self.deleted = 0;
		self.compacted = Some(now);
		true
	}
}

/// A materialiser pass over one index running in this process, from
/// [`IndexBuilder::enter_pass`] until it drops.
pub(crate) struct ActivePass {
	passes: Arc<parking_lot::Mutex<HashMap<IndexKeyBase, usize>>>,
	ikb: IndexKeyBase,
}

impl Drop for ActivePass {
	fn drop(&mut self) {
		let mut passes = self.passes.lock();
		if let Some(count) = passes.get_mut(&self.ikb) {
			*count -= 1;
			if *count == 0 {
				passes.remove(&self.ikb);
			}
		}
	}
}

/// Process-local launcher for durable index builds.
///
/// The active-builder map is not the source of truth for build status. It only
/// tracks builder tasks running in this process so duplicate local tasks can be
/// rejected and removal can signal abort. Durable `!bs` state decides
/// cluster-wide ownership, planner visibility, and user-facing status.
#[derive(Clone)]
pub(crate) struct IndexBuilder {
	pub(super) tf: TransactionFactory,
	/// Set once the datastore has begun to shut down
	/// ([`Self::lapse_local_build_leases`]): a build registered from then on
	/// stops writing and has its lease lapsed as it registers.
	shutting_down: Arc<AtomicBool>,
	pub(super) indexes: Arc<RwLock<HashMap<SharedIndexKey, IndexBuilding>>>,
	/// One mutex per index that a materialiser pass holds while it runs
	/// ([`Self::materialise_lock`]). Held weakly: an entry lives only while a
	/// pass holds or waits for it, and a dead one is replaced on the next ask.
	materialising: Arc<parking_lot::Mutex<HashMap<IndexKeyBase, Weak<tokio::sync::Mutex<()>>>>>,
	/// Per index, the materialiser passes running over it in this process
	/// ([`Self::enter_pass`]). An index is present only while at least one
	/// runs.
	active_passes: Arc<parking_lot::Mutex<HashMap<IndexKeyBase, usize>>>,
	/// Per HNSW index, the pending entries this process's compaction passes
	/// have deleted since its pending range was last compacted in storage
	/// ([`Self::take_pending_range_compaction`]).
	pending_deletes: Arc<parking_lot::Mutex<HashMap<IndexKeyBase, PendingDeletes>>>,
	/// Signalled whenever a build in this process commits a transition out of
	/// `Closing`, so writers that admission holds while a build is `Closing`
	/// re-read its state at once rather than at their next poll. A build owned
	/// by another node signals nothing here; its waiters rely on the poll.
	pub(super) closing_settled: Arc<Notify>,
	/// Overrides [`BUILD_CLOSING_SLEEP`] for this process's admissions, so a
	/// test can tell a wait the signal ended from one the poll ended.
	#[cfg(test)]
	pub(super) closing_poll: Arc<parking_lot::Mutex<Option<Duration>>>,
	/// How many times an admission in this process has begun waiting on a
	/// `Closing` build, so a test can tell a writer has reached the wait.
	#[cfg(test)]
	pub(super) closing_waits: Arc<AtomicU64>,
	/// Whether a build no statement waits for is left to bounded passes
	/// ([`Self::run_pass`]) instead of run on a task of its own, and a build a
	/// statement waits for runs on that statement's task; see
	/// [`Builder::without_index_build_tasks`](crate::kvs::Builder::without_index_build_tasks).
	/// It is whether `spawn` is `None`.
	defers: bool,
	/// Runs an acquired build on a task of its own ([`BuildTasks`]), where
	/// builds are not deferred. A deferring builder never spawns one, so a
	/// build that constructs only deferring builders links none of it.
	spawn: Option<BuildTasks>,
}

enum BuildStart {
	Started(IndexBuilding),
	RemoteOwner(IndexBuilding),
	/// Acquired and left for bounded passes, which the statement does not wait
	/// for ([`IndexBuilder::defers`]).
	Deferred,
	/// Acquired for the statement that waits for it to run on its own task, to
	/// its end, where builds are deferred ([`IndexBuilder::defers`]).
	Inline(IndexBuilding, AcquiredBuild),
}

/// What one bounded pass did with the build of one index
/// ([`IndexBuilder::run_pass`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildPass {
	/// No build is pending: there is none, it has ended, or the catalog does not
	/// hold its definition (yet).
	Idle,
	/// A build is pending and a pass may claim it now. Reported only by a pass
	/// given no budget, which claims nothing.
	Pending,
	/// A live owner holds a pending build: a builder in this process, with
	/// `None`, or one whose lease lapses after the given time.
	Held(Option<Duration>),
	/// The pass ran `batches` batches of the build, which then `finished`:
	/// published, failed or aborted, or, when not, was handed back.
	Ran {
		batches: usize,
		finished: bool,
	},
}

/// Why the query a blocking statement waits for its build in has ended, if it
/// has: cancelled, or past its own deadline.
///
/// Memory pressure is not a reason. A statement waiting for a build holds
/// none of the memory the build uses, and the build carries on whatever the
/// statement does: ending the wait would only report a failure, or remove a
/// definition, over a build that is proceeding normally, whether on this node
/// or another. So this makes the shallow check — the cancellation flags and
/// token — and reads the query's deadline directly, which a context inherits
/// from its parent and only ever tightens, rather than the deep check that
/// also measures memory.
pub(super) fn query_ended(ctx: &Context) -> Option<Reason> {
	if ctx.cancel_token().is_some_and(|token| token.is_cancelled())
		|| matches!(ctx.done(false), Ok(Some(Reason::Canceled)))
	{
		return Some(Reason::Canceled);
	}
	match (ctx.timeout(), ctx.deadline_timeout()) {
		(Some(left), Some(timeout)) if left.is_zero() => Some(Reason::Timedout(timeout.into())),
		_ => None,
	}
}

/// A build a blocking statement started, resolving with the outcome the
/// statement reports once the build has finished.
///
/// Independent of the statement's transaction: it holds nothing of it, so a
/// statement can let its transaction commit first and await this afterwards.
///
/// A build that has published its generation `Online` resolves `Ok`, whatever
/// the rest of it does. The index is complete and queryable from the publish
/// on: what a build does after it — the doc-ID reclaim sweep and the release
/// of its ownership — writes nothing of the index, and readers merge the
/// writes queued during the build until the materialiser folds them.
/// [`Self::is_online`] reports the publish while that work is still running,
/// for a waiter that stops waiting before the build finishes.
pub(crate) struct BuildCompletion {
	building: IndexBuilding,
	/// Whether this node runs the build, rather than waiting on a remote
	/// owner's.
	started_here: bool,
	finished: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
	/// The statement's transaction, set aside from the writes a build waits for while it awaits
	/// this build inside it, and counted back in once the build has finished or the wait for it
	/// is dropped ([`surrealdb_datastore::tx::ParkedWrite`]).
	parked: Option<surrealdb_datastore::tx::ParkedWrite>,
}

impl BuildCompletion {
	fn new(
		building: IndexBuilding,
		started_here: bool,
		finished: impl Future<Output = Result<()>> + Send + 'static,
	) -> Self {
		Self {
			building,
			started_here,
			finished: Box::pin(finished),
			parked: None,
		}
	}

	/// Count `parked` back in once this build has finished, crediting it the waits the build
	/// began.
	fn with_parked(mut self, parked: Option<surrealdb_datastore::tx::ParkedWrite>) -> Self {
		self.parked = parked;
		self
	}

	/// Whether this node runs the build, rather than waiting on a remote
	/// owner's.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn started_here(&self) -> bool {
		self.started_here
	}

	/// Stop a build the waiting statement runs itself on its own task
	/// ([`BuildStart::Inline`]), or would take over, before its next batch, and
	/// wait until it has stopped: handed back for passes, which are woken,
	/// unless it ended first. A wait that ends without this drops the build
	/// partway through a batch, owned and unheartbeated until its lease lapses.
	/// Nothing for a build that runs on a task of its own.
	///
	/// Answers the build's own error when it failed before it stopped, which
	/// the statement reports in place of its wait ending: such a build has
	/// ended in `Error`, and nothing carries it on.
	pub(crate) async fn stop(&mut self) -> Result<()> {
		if self.building.statement.is_none() {
			return Ok(());
		}
		self.building.stop_requested.store(true, Ordering::Release);
		match (&mut *self).await {
			Err(err)
				if !err.is::<StatementEnded>()
					&& !self.building.stopped_for_statement.load(Ordering::Acquire) =>
			{
				Err(err)
			}
			_ => Ok(()),
		}
	}

	/// Whether this build's generation is known to be `Online`.
	pub(crate) fn is_online(&self) -> bool {
		self.building.is_online()
	}

	/// Whether `err`, which ended a statement's wait for this build, is
	/// excused because the build's generation is known to be `Online`.
	///
	/// The one place the rule "once the index is online the statement has
	/// succeeded" is applied to what this process saw of the build: the build
	/// reporting an error once it has published, and the statement's time
	/// running out or its cancellation after the publish. An excused error is
	/// logged here, once. A publish this process did not see is a different question,
	/// answered from the durable build state by [`Self::published`].
	pub(crate) fn excused_once_online(&self, err: &dyn std::fmt::Display) -> bool {
		if !self.building.is_online() {
			return false;
		}
		warn!(
			index = %self.building.ix.name,
			table = %self.building.ix.table_name,
			error = %err,
			"a blocking index build ended with an error after publishing its index, which \
			 stays online; compaction folds the writes queued during the build"
		);
		true
	}

	/// The definition this build is building.
	pub(crate) fn index(&self) -> &IndexDefinition {
		&self.building.ix
	}

	/// The build generation this completion waits for (see
	/// [`Building::awaited_generation`]).
	pub(crate) fn generation(&self) -> BuildGeneration {
		self.building.awaited_generation()
	}

	/// Whether the generation this completion waits for is durably `Online`,
	/// on the table the build was started on.
	///
	/// What this process saw of the build is not the last word on it: a publish
	/// can land after the waiter stopped looking, from the node that owned the
	/// build all along or from one that took it over. Only that generation's
	/// publish counts, and only on that table. A later build of the same index
	/// publishes a generation of its own, and the build state is keyed by the
	/// table's name and an index id allocated per table, so once the table is
	/// removed and defined again an unrelated index can hold the same key.
	pub(crate) fn published(&self) -> impl Future<Output = Result<bool>> + Send + 'static {
		let building = Arc::clone(&self.building);
		async move { building.awaited_generation_published().await }
	}
}

impl Future for BuildCompletion {
	type Output = Result<()>;

	fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		let polled = match this.finished.as_mut().poll(cx) {
			Poll::Ready(Err(err)) if this.excused_once_online(&err) => Poll::Ready(Ok(())),
			finished @ (Poll::Ready(_) | Poll::Pending) => finished,
		};
		if polled.is_ready()
			&& let Some(mut parked) = this.parked.take()
		{
			let key = &this.building.ix_key;
			parked.own_waits(
				this.building.write_barrier_waits_begun.load(Ordering::SeqCst),
				(key.ns, key.db, key.tb.clone()),
			);
		}
		polled
	}
}

/// Document write data needed by the index builder.
///
/// The mutation can either be indexed immediately or written to the durable
/// distributed queue. Keeping the values and COUNT predicate result together
/// avoids re-evaluating write conditions during asynchronous replay.
pub(crate) struct IndexMutation<'a> {
	/// Values currently present in the index before the user write.
	pub(crate) old_values: Option<Vec<Value>>,
	/// Values that should be present in the index after the user write.
	pub(crate) new_values: Option<Vec<Value>>,
	/// Record whose index entries are changing.
	pub(crate) rid: &'a RecordId,
	/// Cached `(old_matches, new_matches)` for conditional COUNT indexes.
	pub(crate) count_cond_match: Option<(bool, bool)>,
}

/// Runs an acquired build on a task of its own.
type SpawnBuild = for<'a> fn(
	&'a IndexBuilder,
	IndexBuilding,
	AcquiredBuild,
	Option<Sender<Result<()>>>,
	bool,
) -> crate::exec::BoxFut<'a, Result<()>>;

/// How an [`IndexBuilder`] that does not defer builds runs one: on a task of
/// its own. Only a datastore builder that spawns builds names it
/// ([`BuildTasks::spawned`]), so a build that never constructs one links none
/// of the build task.
#[derive(Clone, Copy)]
pub(crate) struct BuildTasks(SpawnBuild);

impl BuildTasks {
	/// Builds run on tasks of their own.
	pub(crate) fn spawned() -> Self {
		Self(spawn_build)
	}
}

fn spawn_build<'a>(
	builder: &'a IndexBuilder,
	building: IndexBuilding,
	acquired: AcquiredBuild,
	sdr: Option<Sender<Result<()>>>,
	waits_for_statement: bool,
) -> crate::exec::BoxFut<'a, Result<()>> {
	Box::pin(builder.spawn_acquired_building(building, acquired, sdr, waits_for_statement))
}

impl IndexBuilder {
	/// A builder that runs the builds statements start through `spawn`, or,
	/// where it is `None`, leaves a build no statement waits for to bounded
	/// passes and runs one a statement waits for on that statement's task.
	pub(in crate::kvs) fn new(tf: TransactionFactory, spawn: Option<BuildTasks>) -> Self {
		Self {
			tf,
			shutting_down: Default::default(),
			indexes: Default::default(),
			materialising: Default::default(),
			active_passes: Default::default(),
			pending_deletes: Default::default(),
			closing_settled: Default::default(),
			#[cfg(test)]
			closing_poll: Default::default(),
			#[cfg(test)]
			closing_waits: Default::default(),
			defers: spawn.is_none(),
			spawn,
		}
	}

	/// How this builder spawns a build: `None` where it defers builds.
	pub(crate) fn spawner(&self) -> Option<BuildTasks> {
		self.spawn
	}

	/// How long admission waits between reads of a `Closing` build's state
	/// when nothing in this process signals that it has left `Closing`.
	pub(super) fn closing_poll(&self) -> Duration {
		#[cfg(test)]
		if let Some(poll) = *self.closing_poll.lock() {
			return poll;
		}
		BUILD_CLOSING_SLEEP
	}

	/// The process-local mutex the materialiser's passes over `ikb` take.
	///
	/// Background compaction's per-index passes and a blocking statement's
	/// wait for its index to be materialised both hold it across a pass, so two
	/// passes in this process never plan the same work. Correctness does not
	/// rest on it: two passes from anywhere are ordered by the index's
	/// compaction generation. Every caller asking while a holder or a waiter
	/// exists gets the same mutex.
	pub(crate) fn materialise_lock(&self, ikb: &IndexKeyBase) -> Arc<tokio::sync::Mutex<()>> {
		let mut materialising = self.materialising.lock();
		if let Some(lock) = materialising.get(ikb).and_then(Weak::upgrade) {
			return lock;
		}
		// Entries whose last holder has gone are dropped as new ones are made,
		// so the map holds no more than the indexes being materialised and the
		// dead entries since the last insert.
		materialising.retain(|_, lock| lock.strong_count() > 0);
		let lock = Arc::new(tokio::sync::Mutex::new(()));
		materialising.insert(ikb.clone(), Arc::downgrade(&lock));
		lock
	}

	/// Marks a materialiser pass over `ikb` as running in this process until
	/// the returned guard drops, which ends it on every exit, a dropped pass
	/// future included.
	pub(crate) fn enter_pass(&self, ikb: &IndexKeyBase) -> ActivePass {
		*self.active_passes.lock().entry(ikb.clone()).or_default() += 1;
		ActivePass {
			passes: Arc::clone(&self.active_passes),
			ikb: ikb.clone(),
		}
	}

	/// Whether a materialiser pass over `ikb` is running in this process, as
	/// `INFO FOR INDEX` reports it in `building.active`.
	///
	/// Node-local: a pass another node runs, under the cluster's compaction
	/// lease, is not seen here. It reads a count the passes keep and never
	/// touches the materialise mutex, so asking cannot delay a pass taking it.
	pub(crate) fn is_materialising(&self, ikb: &IndexKeyBase) -> bool {
		self.active_passes.lock().contains_key(ikb)
	}

	/// Records that a committed HNSW compaction batch deleted `deleted` pending
	/// entries of `ikb`'s index.
	pub(crate) fn note_pending_deleted(&self, ikb: &IndexKeyBase, deleted: usize) {
		if deleted > 0 {
			let now = Instant::now();
			let mut pending = self.pending_deletes.lock();
			// Entries that hold no count and no recent compaction say nothing a
			// fresh entry would not, so the map holds no more than the indexes
			// with deletions to count or a compaction to space out.
			pending.retain(|_, entry| !entry.is_idle(now));
			pending.entry(ikb.clone()).or_default().deleted += deleted;
		}
	}

	/// Forgets what `ikb`'s passes counted: its index is gone, being removed,
	/// or no longer HNSW, so an index later created under the same key starts
	/// from nothing.
	pub(crate) fn forget_pending_deletes(&self, ikb: &IndexKeyBase) {
		self.pending_deletes.lock().remove(ikb);
	}

	/// Whether `ikb`'s pending range, which a pass has just drained, is due a
	/// compaction in storage, and if so resets its count. See
	/// [`PENDING_RANGE_COMPACTION_MIN_DELETES`].
	pub(crate) fn take_pending_range_compaction(&self, ikb: &IndexKeyBase) -> bool {
		self.pending_deletes.lock().get_mut(ikb).is_some_and(|entry| entry.take_due(Instant::now()))
	}

	pub(crate) fn transaction_factory(&self) -> TransactionFactory {
		self.tf.clone()
	}

	/// Forget a shutdown begun by [`Self::lapse_local_build_leases`], as the
	/// fresh builder of a restarted datastore over the same store would. For a
	/// test whose in-memory store cannot be reopened.
	#[cfg(all(test, feature = "kv-mem"))]
	pub(crate) fn restart_after_shutdown(&self) {
		self.shutting_down.store(false, Ordering::SeqCst);
	}

	/// Lapse the owner lease of every build this process is running, ahead of
	/// a datastore shutdown, and answer how many it lapsed.
	///
	/// A build left running by a shutdown makes no durable change once the
	/// storage engine refuses commits: it stays at its last committed
	/// checkpoint for a builder to continue once its owner's lease lapses
	/// ([`Building::lapse_lease`]). Each build stops opening write transactions
	/// first, so it cannot renew the lease again. Lapsing it while commits are still
	/// accepted lets the resume scan of the restarted node, or of any other,
	/// continue it on the scan's next pass instead of a whole lease later, and
	/// with it a write waiting for a `Closing` build to publish.
	pub(crate) async fn lapse_local_build_leases(&self) -> usize {
		self.shutting_down.store(true, Ordering::SeqCst);
		let running: Vec<IndexBuilding> = self
			.indexes
			.read()
			.await
			.values()
			.filter(|building| !building.is_finished())
			.cloned()
			.collect();
		let mut lapsed = 0;
		for building in running {
			if lapse_for_shutdown(&building).await {
				lapsed += 1;
			}
		}
		lapsed
	}

	/// Whether any local builder task is still running.
	///
	/// Used by the stalled-build resume scan to serialize recovery: while a
	/// build is active on this node, further adoptions are deferred to a
	/// later scan pass so concurrent initial scans cannot multiply the
	/// node's memory and CPU footprint.
	pub(crate) async fn has_unfinished_build(&self) -> bool {
		self.indexes.read().await.values().any(|building| !building.is_finished())
	}

	#[allow(clippy::too_many_arguments)]
	async fn start_building(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
		sdr: Option<Sender<Result<()>>>,
		waits_for_statement: bool,
	) -> Result<BuildStart> {
		let carried = carried_places(ctx, sdr.is_some());
		#[cfg(test)]
		lent_places::note(ctx.node_id(), &ix.name, carried.is_some());
		let mut building =
			Building::with_places(ctx, self.tf.clone(), opt, tb, ix, ix_key, carried)?
				.signalling(&self.closing_settled);
		// A sender is what a blocking statement waits on.
		building.awaited = sdr.is_some();
		if sdr.is_some() {
			building.analyzer = building.statement_analyzer(ctx).await?;
		}
		// A deferred build a statement awaits runs on that statement's task, to
		// its end, where a task would have run it: after the statement's commit,
		// or inside its transaction.
		let inline = self.defers && sdr.is_some();
		building.to_passes = self.defers;
		building.inline_run = inline;
		if inline {
			building.statement = Some(Arc::clone(ctx));
		}
		let building = Arc::new(building);
		building.read_key_structure_in(ctx).await?;
		let acquired = match building.acquire_build_state().await {
			Ok(Some(acquired)) => acquired,
			Ok(None) => return Ok(BuildStart::RemoteOwner(building)),
			Err(err) => return Err(err),
		};
		building.defer_ownership_release(ctx).await;
		if waits_for_statement && acquired.staged {
			building.reads_after_its_stamp.store(true, Ordering::Release);
		}
		if inline {
			return Ok(BuildStart::Inline(building, acquired));
		}
		if self.defers {
			// No task runs this build, so its half of the release gate arrives at once and the
			// statement's close hands the build to passes ([`Building::release_owned`]); a
			// statement already closed hands it back now.
			if building.arrive_at_release_gate(RELEASE_GATE_BUILDER) {
				building.release_build_ownership(acquired.generation, None).await;
			}
			return Ok(BuildStart::Deferred);
		}
		self.start_acquired_building(Arc::clone(&building), acquired, sdr, waits_for_statement)
			.await?;
		Ok(BuildStart::Started(building))
	}

	/// Register `building` as this process's build of its index, rejecting it
	/// while another build of the index here has not finished.
	async fn register(&self, building: &IndexBuilding) -> Result<()> {
		let mut indexes = self.indexes.write().await;
		if let Some(existing) = indexes.get(&building.ix_key) {
			ensure!(
				existing.is_finished(),
				DatastoreError::IndexAlreadyBuilding {
					name: building.ix.name.to_string(),
				}
			);
			// A finished build may still keep its published generation for a
			// statement that is open ([`Building::keep_online_ownership`]).
			// This build replaces that generation, so its keeper stops rather
			// than finding its ownership gone and cancelling that statement,
			// which may be the one starting this build.
			existing.abort();
		}
		indexes.insert(Arc::clone(&building.ix_key), Arc::clone(building));
		drop(indexes);
		// The flag is set before the shutdown reads the registered builds, and
		// read here after this build is registered, so a build the shutdown
		// did not find finds the flag.
		if self.shutting_down.load(Ordering::SeqCst) {
			lapse_for_shutdown(building).await;
		}
		Ok(())
	}

	/// Register `building` and spawn its task, which reports its result to
	/// `sdr` when a statement awaits it.
	///
	/// When `waits_for_statement`, the task first waits for the statement that
	/// started the build to close its transaction
	/// ([`Building::await_statement_closed`]). Every caller asks for that
	/// except one whose build is awaited inside that transaction, which cannot
	/// close before the build ends.
	///
	/// Only a builder that does not defer builds spawns one; every caller is
	/// on a path a deferring builder leaves before it.
	async fn start_acquired_building(
		&self,
		building: IndexBuilding,
		acquired: AcquiredBuild,
		sdr: Option<Sender<Result<()>>>,
		waits_for_statement: bool,
	) -> Result<()> {
		let Some(BuildTasks(spawn)) = self.spawn else {
			anyhow::bail!(Error::unreachable(
				"a builder that defers index builds spawned a build task"
			));
		};
		spawn(self, building, acquired, sdr, waits_for_statement).await
	}

	/// [`Self::start_acquired_building`] where builds run on tasks of their own.
	async fn spawn_acquired_building(
		&self,
		building: IndexBuilding,
		acquired: AcquiredBuild,
		sdr: Option<Sender<Result<()>>>,
		waits_for_statement: bool,
	) -> Result<()> {
		self.register(&building).await?;
		let b = Arc::clone(&building);
		// Created before the spawn and moved in, so a task the executor drops
		// without ever polling it still marks the build finished. Waiters in
		// `Building::wait_finished` and the `is_finished` checks above would
		// otherwise never see a build that produced no writes at all.
		//
		// The `drop(guard)` below is what makes `async move` capture this, and it
		// also has to stay where it is: it must run after the build's last write
		// of index data or of its build's progress, and before the result is
		// sent, so a blocking `DEFINE INDEX` that has returned cannot be rejected
		// as `IndexAlreadyBuilding`. What follows it is only the keeper of a
		// published generation ([`Building::keep_online_ownership`]), whose writes
		// are owned transitions of `!bs`: a compare-and-swap against the state it
		// read, which writes nothing once `!bs` is deleted or its generation
		// replaced, so a waiter that deletes the build state after the flag still
		// writes last (see [`Building::wait_finished`]). Letting the guard fall out
		// of scope instead would drop it here, at the end of this function, and
		// report every build finished before it had begun.
		let guard = BuildingFinishGuard(Arc::clone(&building));
		common::rt::spawn_detached(async move {
			let r = if waits_for_statement {
				match b.await_statement_closed(acquired.generation, acquired.phase).await {
					Ok(StatementWait::Closed) => b.run_acquired(acquired).await,
					// A transaction dropped without closing never committed,
					// so its build ends as a rolled-back statement's does:
					// aborted, recorded below, with its ownership cleared.
					// Left `Building`, the generation would keep an owner no
					// task heartbeats until its lease lapsed, and a blocking
					// `REBUILD INDEX` would wait that lease out.
					Ok(StatementWait::Dropped) => {
						b.abort();
						Ok(())
					}
					Ok(StatementWait::Aborted) => Ok(()),
					Err(err) => Err(err),
				}
			} else {
				b.run_acquired(acquired).await
			};
			let generation = b.build_generation.load(Ordering::Acquire);
			if let Err(err) = &r {
				// Shutdown and memory-threshold failures are transient: the
				// storage engine went away under this builder, or the process
				// was under memory pressure — the build itself did not fail.
				// The durable state is deliberately left in
				// `Building`/`Closing` with its last committed checkpoint:
				// once the owner lease expires, the periodic resume scan (or
				// a blocking statement takeover) continues the build from
				// there. Recording a durable `Error` here would instead end
				// the build for good: `Error` is terminal, so the index stays
				// out of the planner, and the writes admitted meanwhile queue
				// under a generation nothing replays, until an operator runs
				// `REBUILD INDEX`.
				//
				// The builder still arrives at the release gate, as it writes
				// nothing more. A release leaves a generation that is not
				// `Online` as it is, so one whose statement committed resumes.
				// One whose statement did not commit, to a definition that
				// does not describe this build, is withdrawn instead
				// ([`Building::release_build_ownership`]): the resume scan
				// would carry it on through the committed definition from
				// checkpoints written for the one that never committed.
				let transient = if is_shutdown_error(err) {
					info!(
						index = %b.ix.name,
						table = %b.ix.table_name,
						"index build interrupted by datastore shutdown; \
						 it will resume after restart"
					);
					true
				} else if is_memory_threshold_error(err) {
					warn!(
						index = %b.ix.name,
						table = %b.ix.table_name,
						"index build interrupted by the memory threshold; \
						 it will resume from its checkpoint once the owner \
						 lease expires"
					);
					true
				} else {
					false
				};
				if transient {
					if generation != 0 && b.arrive_at_release_gate(RELEASE_GATE_BUILDER) {
						b.release_build_ownership(generation, None).await;
					}
				} else if generation != 0 {
					let _ = b.mark_durable_error(generation, err.to_string()).await;
				}
			} else if b.aborted.load(Ordering::Acquire) && generation != 0 {
				let _ = b.mark_durable_aborted(generation).await;
			}
			// Publishes `finished` after the last write of index data and
			// progress and before the result is sent; it is also the only reason
			// the guard is captured by this future at all. See where the guard is
			// constructed.
			drop(guard);
			let published = r.is_ok() && generation != 0 && b.online.load(Ordering::Acquire);
			if let Some(s) = sdr
				&& s.send(r).is_err()
			{
				warn!("Failed to send index building result to the consumer");
			}
			if published {
				b.keep_online_ownership(generation).await;
			}
		});
		Ok(())
	}

	/// Wait for a remote owner to finish a blocking build.
	///
	/// A blocking `DEFINE INDEX` or `REBUILD INDEX` must not return simply
	/// because another node already owns the durable generation. This waits for
	/// the generation the statement joined, the one a live owner elsewhere was
	/// building when acquisition found it ([`Building::awaited_generation`]),
	/// and for no other: it ends once that generation is `Online` or `Error`, or
	/// takes that same generation over once its owner's lease expires. It never
	/// creates a fresh generation, because the blocking statement is waiting
	/// for that work.
	///
	/// The wait may first look long after it joined: a statement that awaits
	/// its build after its commit polls nothing before then. A generation other
	/// than the joined one in the durable state means the joined one has ended
	/// — a later build replaces only a generation that is `Online` or in
	/// `Error` ([`Building::acquire_build_state`]), and a table removed and
	/// defined again starts the index afresh — and nothing durable records how.
	/// So the wait succeeds if this process saw the joined generation `Online`,
	/// and otherwise fails as superseded, leaving the index to the build that
	/// replaced it. It never waits on, or reports, that later build.
	async fn wait_for_remote_building(
		&self,
		ctx: &FrozenContext,
		building: IndexBuilding,
	) -> Result<()> {
		let joined = building.awaited_generation();
		loop {
			if let Some(reason) = query_ended(ctx) {
				return Err(Error::from(reason).into());
			}
			if building.stop_requested.load(Ordering::Acquire) {
				return Err(anyhow::Error::new(StatementEnded(None)));
			}
			let Some(state) = building.read_durable_build_state().await? else {
				return Err(DatastoreError::IndexingBuildingCancelled {
					reason: format!("Index {} build state no longer exists", building.ix.name),
				}
				.into());
			};
			if state.generation != joined {
				if building.is_online() {
					return Ok(());
				}
				return Err(DatastoreError::IndexingBuildingCancelled {
					reason: format!(
						"Index {} build generation {joined} was superseded by generation {} \
						 before this statement saw it finish; that build decides the index",
						building.ix.name, state.generation
					),
				}
				.into());
			}
			match state.phase {
				IndexBuildPhase::Online => {
					building.online.store(true, Ordering::Release);
					return Ok(());
				}
				IndexBuildPhase::Error => {
					return Err(DatastoreError::IndexingBuildingCancelled {
						reason: format!(
							"{}. Run `REBUILD INDEX {} ON {}` to retry the build",
							durable_index_error_reason(&building.ix, &state),
							building.ix.name,
							building.ix.table_name
						),
					}
					.into());
				}
				IndexBuildPhase::Building | IndexBuildPhase::Closing => {
					if build_owner_expired(&state, Utc::now())
						&& let Some(acquired) = building
							.takeover_expired_build_state(Some(joined), false, false)
							.await?
					{
						building.defer_ownership_release(ctx).await;
						// Where no task runs a build, the waiting statement runs the build it
						// took over itself, to its end, as it runs one it started.
						if self.defers {
							return self.run_awaited_inline(&building, acquired).await;
						}
						let (s, r) = channel();
						// A waiter awaiting inside its statement's transaction
						// would never see it close, and one awaiting after the
						// commit has no open transaction left to wait for.
						self.start_acquired_building(
							Arc::clone(&building),
							acquired,
							Some(s),
							false,
						)
						.await?;
						// The build taken over is this node's now, and the wait
						// for it answers to the query as the wait for the
						// remote owner did: its deadline or cancellation ends
						// the wait, and the build goes on.
						let mut r = std::pin::pin!(r);
						loop {
							if let Some(reason) = query_ended(ctx) {
								return Err(Error::from(reason).into());
							}
							if let Ok(outcome) =
								common::time::timeout(BUILD_CLOSING_SLEEP, &mut r).await
							{
								return outcome.map_err(|_| {
									DatastoreError::IndexingBuildingCancelled {
										reason: "Channel shutdown".to_string(),
									}
								})?;
							}
						}
					}
					sleep(BUILD_CLOSING_SLEEP).await;
				}
			}
		}
	}

	/// Start a build task if this node can acquire durable ownership.
	///
	/// Durable ownership is acquired before this returns, so every write that
	/// commits afterwards finds the build and is admitted to it. Non-blocking
	/// callers get `None` and nothing to wait for, including when another node
	/// owns a fresh lease. Blocking callers get the [`BuildCompletion`] to
	/// await: it resolves once the durable generation is queryable or failed,
	/// taking over the same generation if a remote owner's lease expires.
	///
	/// A blocking build started here is awaited inside the calling statement's
	/// transaction, so it reads from the start. A non-blocking one reads only
	/// once that transaction has closed.
	pub(crate) async fn build(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
	) -> Result<Option<BuildCompletion>> {
		self.start_build(ctx, opt, tb, ix, blocking, !blocking).await
	}

	/// [`Self::build`], with `waits_for_statement` saying whether the build
	/// waits for the calling statement's transaction to close before it reads
	/// anything (see [`Self::start_acquired_building`]).
	async fn start_build(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
		waits_for_statement: bool,
	) -> Result<Option<BuildCompletion>> {
		crate::idx::check_index_kernel(&ix.index)?;
		expect_not_prepare_remove(&ix)?;
		// A build started by the calling transaction changes what its later
		// writes must decide about every index.
		if let Some(txn) = ctx.try_tx() {
			txn.clear_cached_index_admissions();
		}
		let (ns, db) = ctx.expect_ns_db_ids(&opt).await?;
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		if let Some(existing) = self.indexes.read().await.get(&key) {
			ensure!(
				existing.is_finished(),
				DatastoreError::IndexAlreadyBuilding {
					name: ix.name.to_string(),
				}
			);
		}
		// A blocking build awaited inside its statement's open transaction would otherwise wait for
		// that transaction at the write barrier: it is set aside until the build has finished
		// ([`BuildCompletion`]), and counted back in however the wait for it ends.
		let parked = match blocking && !waits_for_statement {
			true => ctx.try_tx().and_then(|txn| txn.park_live_write()),
			false => None,
		};
		match self.start_building(ctx, opt, tb, ix, key, sdr, waits_for_statement).await? {
			BuildStart::Started(building) => Ok(rcv.map(|rcv| {
				BuildCompletion::new(building, true, async move {
					rcv.await.map_err(|_| DatastoreError::IndexingBuildingCancelled {
						reason: "Channel shutdown".to_string(),
					})?
				})
				.with_parked(parked)
			})),
			BuildStart::Deferred => Ok(None),
			BuildStart::Inline(building, acquired) => {
				let builder = self.clone();
				let run = Arc::clone(&building);
				Ok(Some(
					BuildCompletion::new(building, true, async move {
						builder.run_awaited_inline(&run, acquired).await
					})
					.with_parked(parked),
				))
			}
			BuildStart::RemoteOwner(building) if blocking => {
				let builder = self.clone();
				let ctx = Arc::clone(ctx);
				let waited = Arc::clone(&building);
				Ok(Some(
					BuildCompletion::new(building, false, async move {
						builder.wait_for_remote_building(&ctx, waited).await
					})
					.with_parked(parked),
				))
			}
			BuildStart::RemoteOwner(_) => Ok(None),
		}
	}

	/// Stages in `tx`, the transaction of a `REBUILD INDEX` that moves `ix` to another format, the
	/// generation the rebuild builds, and answers it; `None` for a rebuild that leaves the format
	/// as the committed catalog holds it, which builds as [`Self::build`] builds.
	///
	/// A build writes its layout in transactions of its own, which commit as they go, while the
	/// format that layout follows is the one the statement stamps, which commits only with the
	/// statement. So the generation is installed ownerless beside the stamp, and nothing is wiped
	/// or scanned until the transaction has committed and [`Self::start_staged_rebuild`] claims
	/// it. Until that commit every reader and writer keeps the previous format over the previous
	/// layout, and a rollback leaves the index, its data and its build state as it found them.
	/// From the commit on, every reader finds the index building, every writer queues for the
	/// build, and a resumed build reads the same definition its checkpoint was made under. A build
	/// in flight is replaced rather than joined, since it builds the previous format.
	///
	/// The flip compare-and-swaps the index's build state as `tx` reads it, so a build state
	/// written since `tx` began fails the commit with a retryable conflict.
	pub(crate) async fn stage_rebuild(
		&self,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<RebuildStaging> {
		let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
		let (moves, committed) = self.committed_rebuild_state(tx.sequences(), &ikb, ix).await?;
		if !moves {
			return Ok(RebuildStaging::Unstaged);
		}
		let existing: Option<IndexBuildState> = tx.get_key(&ikb.new_bs_key(), None).await?;
		// A generation this transaction staged already, for an earlier rebuild of the index in
		// it, is built once the transaction commits; a second would only supersede the first.
		if let Some(staged) = &existing
			&& staged.owner.is_none()
			&& staged.phase == IndexBuildPhase::Building
			&& committed.as_ref().map(|state| state.generation) != Some(staged.generation)
		{
			return Ok(RebuildStaging::AlreadyStaged(staged.generation));
		}
		// The writes that follow in this transaction decide afresh about this index.
		tx.clear_cached_index_admissions();
		stage_new_generation(tx, &ikb, existing.as_ref(), None, None)
			.await
			.map(RebuildStaging::Staged)
	}

	/// Whether `ix`'s format differs from the one the committed catalog holds for the index, and
	/// the index's committed build state, read together.
	async fn committed_rebuild_state(
		&self,
		sequences: Sequences,
		ikb: &IndexKeyBase,
		ix: &IndexDefinition,
	) -> Result<(bool, Option<IndexBuildState>)> {
		use crate::catalog::FromStored as _;
		let (ns, db) = (ikb.ns(), ikb.db());
		let tx = self.tf.transaction(TransactionType::Read, sequences).await?;
		let state: Option<IndexBuildState> = catch!(tx, tx.get_key(&ikb.new_bs_key(), None).await);
		let committed = catch!(
			tx,
			async {
				let tb = Cow::Borrowed(&ix.table_name);
				let name = crate::key::schema::IndexNameKey {
					ns,
					db,
					tb: tb.clone(),
					ix: ix.index_id,
				};
				let Some(name) = tx.get_key(&name, None).await? else {
					return Ok(None);
				};
				let definition = crate::key::schema::IndexDefKey {
					ns,
					db,
					tb,
					ix: Cow::Borrowed(name.as_str()),
				};
				match tx.get_key(&definition, None).await? {
					Some(stored) => IndexDefinition::from_stored(&stored).map(Some),
					None => Ok(None),
				}
			}
			.await
		);
		tx.cancel().await?;
		let moves =
			committed.is_some_and(|committed| committed.format_version != ix.format_version);
		Ok((moves, state))
	}

	/// Starts the build of `generation`, which the committed transaction of a `REBUILD INDEX`
	/// staged ownerless ([`Self::stage_rebuild`]), and when `blocking` answers the
	/// [`BuildCompletion`] to await.
	///
	/// The claim takes that generation and no other, whatever its lease: the statement's commit is
	/// what installed it. A local build of the generation it replaced is stopped first, and waited
	/// for at most [`SUPERSEDED_BUILD_STOP`], so a generation is never claimed without a task to
	/// build it; a local build already building the staged generation is joined instead. A claim
	/// another builder won — the resume scan, once the generation has stood a whole lease, here
	/// or on another node — and one left unmade because the superseded build did not stop in time
	/// are waited for as a remote owner's build, which the resume scan or that wait takes over once
	/// its lease lapses. Nothing here fails a statement that has committed.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn start_staged_rebuild(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		generation: BuildGeneration,
		blocking: bool,
	) -> Result<Option<BuildCompletion>> {
		crate::idx::check_index_kernel(&ix.index)?;
		// The staged generation stands ownerless, which a bounded pass claims
		// where no statement waits for it.
		if self.defers && !blocking {
			self.tf.triggers().index_build.notify_one();
			return Ok(None);
		}
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		// A local build already building the staged generation — the resume scan adopted it
		// first, or another statement's start of it ran first — is joined, and one building a
		// later generation is left to build it. One of the generation the staged one replaced
		// loses its ownership at its next check; it is stopped, and waited for, so the claim below
		// has a task to build with.
		let local = self.indexes.read().await.get(&key).filter(|b| !b.is_finished()).cloned();
		let claimable = match local {
			Some(local) if local.build_generation.load(Ordering::Acquire) >= generation => false,
			Some(superseded) => {
				superseded.abort();
				timeout(SUPERSEDED_BUILD_STOP, superseded.wait_finished()).await.is_ok()
			}
			None => true,
		};
		let carried = carried_places(ctx, blocking);
		#[cfg(test)]
		lent_places::note(ctx.node_id(), &ix.name, carried.is_some());
		let mut building = Building::with_places(ctx, self.tf.clone(), opt, tb, ix, key, carried)?
			.signalling(&self.closing_settled);
		building.awaited = blocking;
		// Where no task runs a build, the statement runs it, and one it leaves
		// unfinished goes back to passes.
		building.to_passes = self.defers;
		if self.defers {
			building.statement = Some(Arc::clone(ctx));
		}
		let building = Arc::new(building);
		let claimed = if claimable {
			building.claim_staged_generation(generation).await?
		} else {
			None
		};
		match claimed {
			// Where no task runs a build, the statement that waits for it runs it.
			Some(acquired) if self.defers => {
				let builder = self.clone();
				let run = Arc::clone(&building);
				Ok(Some(BuildCompletion::new(building, true, async move {
					builder.run_awaited_inline(&run, acquired).await
				})))
			}
			Some(acquired) => {
				let (rcv, sdr) = if blocking {
					let (s, r) = channel();
					(Some(r), Some(s))
				} else {
					(None, None)
				};
				self.start_acquired_building(Arc::clone(&building), acquired, sdr, false).await?;
				Ok(rcv.map(|rcv| {
					BuildCompletion::new(building, true, async move {
						rcv.await.map_err(|_| DatastoreError::IndexingBuildingCancelled {
							reason: "Channel shutdown".to_string(),
						})?
					})
				}))
			}
			None if blocking => {
				building.remote_generation.store(generation, Ordering::Release);
				let builder = self.clone();
				let ctx = Arc::clone(ctx);
				let waited = Arc::clone(&building);
				Ok(Some(BuildCompletion::new(building, false, async move {
					builder.wait_for_remote_building(&ctx, waited).await
				})))
			}
			None => Ok(None),
		}
	}

	/// Build index `ix` of table `tb` as [`Self::build`] starts it, and when
	/// `blocking`, wait for this node's build to finish, failing as it fails.
	pub(crate) async fn run(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
	) -> Result<()> {
		match self.build(ctx, opt, tb, ix, blocking).await? {
			Some(completion) => completion.await,
			None => Ok(()),
		}
	}

	/// Build index `ix` of table `tb` for a blocking statement, and queue the
	/// wait for the build on `txn`, for its owner to await once it has
	/// committed. The caller has established that the owner offers that
	/// ([`Transaction::accepts_after_commit_waits`]); one that does not is a
	/// caller's error.
	///
	/// Awaiting after the commit keeps the statement's transaction short: held
	/// open across the build's own commits, it would outlast the write history
	/// an optimistic storage engine validates a commit against, and fail once
	/// the build had already published. The caller is either a `REBUILD INDEX`,
	/// whose index is already defined and is handed the definition it builds,
	/// so a failed build leaves the index in `Error` for the next rebuild
	/// rather than anything to take back (`undo` is `None`); or a `DEFINE
	/// INDEX` of a new index on an existing table, whose definition is taken
	/// back by `undo` should its build fail before the index is online (see
	/// [`AwaitBuildAfterCommit`]).
	///
	/// A rebuild awaited after the commit, started on this node, is unfinished
	/// when `txn` closes, so it is aborted should `txn` not commit: its start
	/// registers the release of its ownership on `txn`, whose rollback half
	/// settles the build ([`Building::defer_ownership_release`]). A new
	/// definition's build is also cleaned up by the rollback action its
	/// statement registers.
	pub(crate) async fn run_awaited_after_commit(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		txn: &Transaction,
		undo: Option<RemoveFailedDefinition>,
	) -> Result<()> {
		// Awaited once `txn` has committed, so the build reads nothing until
		// then (see [`Building::await_statement_closed`]).
		let Some(completion) = self.start_build(ctx, opt, tb, ix, true, true).await? else {
			return Ok(());
		};
		let wait = Box::new(AwaitBuildAfterCommit::new(Arc::clone(ctx), completion, undo));
		if txn.wait_after_commit(wait).await.is_some() {
			return Err(EngineError::unreachable(
				"a transaction accepting after-commit waits refused one",
			)
			.into());
		}
		Ok(())
	}

	/// Resume an index build left unfinished by a crashed or expired owner.
	///
	/// A `CONCURRENTLY` build is fire-and-forget: the initiating statement
	/// returns as soon as the builder task is spawned. If the owning node then
	/// dies mid-build, nothing ever waits on that generation again, so the
	/// durable `!bs` state is stranded in `Building`/`Closing` and the index
	/// reports `status: indexing` with a frozen counter indefinitely. This
	/// performs the same expired-lease takeover the blocking path performs in
	/// [`Self::wait_for_remote_building`], but proactively, driven by the
	/// periodic resume scan rather than by a statement that is waiting.
	///
	/// An `Online` generation whose owner's lease has lapsed is not resumed:
	/// its build has finished, and its owner is cleared instead, where the
	/// store allows it ([`Building::clear_lapsed_owner`]).
	///
	/// Returns `Ok(true)` if this call adopted the generation and spawned a
	/// resume task. It is a no-op (`Ok(false)`) when the durable state is
	/// missing, `Online`, in `Error`, still covered by a live owner lease, or
	/// already being built by a task in this process, so it is safe to call for
	/// every index on every scan. The takeover and the clear are CAS-guarded,
	/// so racing scans on other cluster nodes resolve to a single winner.
	///
	/// `tb` and `ix` come from a snapshot of the catalog the caller took
	/// earlier, which a table removed and defined again may have outdated
	/// under the same index id. The transaction that claims the build state
	/// also checks that the catalog still names table `tb` and holds `ix` as it
	/// is, and claims nothing otherwise.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn resume_stalled(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		scope: ResumeScope,
	) -> Result<bool> {
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		// Skip if a builder task for this index is already running locally.
		if let Some(existing) = self.indexes.read().await.get(&key)
			&& !existing.is_finished()
		{
			return Ok(false);
		}
		let building = Arc::new(
			Building::new(ctx, self.tf.clone(), opt, tb, ix, key)?
				.signalling(&self.closing_settled),
		);
		// Cheap read-only pre-check: only an unfinished build whose owner lease
		// has expired is eligible. Healthy indexes and live builds bail out here
		// without opening a write transaction.
		let Some(state) = building.read_durable_build_state().await? else {
			return Ok(false);
		};
		if !matches!(
			state.phase,
			IndexBuildPhase::Building | IndexBuildPhase::Closing | IndexBuildPhase::Online
		) || !build_owner_expired(&state, Utc::now())
		{
			return Ok(false);
		}
		if state.phase == IndexBuildPhase::Online {
			// An `Online` owner whose keeper let its heartbeat lapse is cleared,
			// where the store allows it, and nothing is resumed.
			if online_owner_lapsed(&state, Utc::now()) {
				building.clear_lapsed_owner().await?;
			}
			return Ok(false);
		}
		if scope == ResumeScope::ClosingOnly && state.phase != IndexBuildPhase::Closing {
			return Ok(false);
		}
		// Claim the expired generation and spawn the resume task. A no-op result
		// (`None`) means another scanner or a concurrent statement won the race.
		match building.takeover_expired_build_state(None, true, false).await? {
			Some(acquired) => {
				self.start_acquired_building(building, acquired, None, true).await?;
				Ok(true)
			}
			None => Ok(false),
		}
	}

	/// Run at most `budget` batches of the pending build of index `ix`, on the
	/// calling task.
	///
	/// A build is pending while its state is `Building` or `Closing`. One that
	/// no live owner holds — handed back by its statement or by an earlier
	/// pass, or left by an owner whose lease lapsed — is claimed and run as a
	/// build taken over runs ([`Building::run_acquired`]): from its last
	/// committed batch, each batch committing its own checkpoint, so a pass
	/// stopped anywhere loses at most the batch it was in. A pass that spends
	/// its budget before the build ends hands the build back, for the next pass
	/// to claim at once ([`Building::spend_batch`]). A `budget` of zero claims
	/// nothing and only reports.
	///
	/// The build fails as a spawned build fails: an error of its own ends the
	/// generation in `Error`, reported here as finished. One that leaves the
	/// build resumable — the memory threshold — hands it back, and a datastore
	/// shutdown is returned with the build left to its lease.
	///
	/// `tb` and `ix` come from a catalog snapshot the caller took. The claim
	/// checks that the catalog still holds them, as [`Self::resume_stalled`]
	/// does, so an index whose definition has not committed reads as idle.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn run_pass(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		budget: usize,
	) -> Result<BuildPass> {
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		if let Some(existing) = self.indexes.read().await.get(&key)
			&& !existing.is_finished()
		{
			return Ok(BuildPass::Held(None));
		}
		let mut building = Building::new(ctx, self.tf.clone(), opt, tb, ix, key)?;
		building.budget = Some(BatchBudget::new(budget));
		let building = Arc::new(building);
		let Some(state) = building.read_durable_build_state().await? else {
			return Ok(BuildPass::Idle);
		};
		let now = Utc::now();
		match state.phase {
			IndexBuildPhase::Building | IndexBuildPhase::Closing => {}
			IndexBuildPhase::Online => {
				if budget > 0 && online_owner_lapsed(&state, now) {
					building.clear_lapsed_owner().await?;
				}
				return Ok(BuildPass::Idle);
			}
			IndexBuildPhase::Error => return Ok(BuildPass::Idle),
		}
		if !build_claimable(&state, now) {
			let lapses = state.owner_heartbeat_at.unwrap_or(state.updated_at)
				+ chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS);
			return Ok(BuildPass::Held(Some((lapses - now).to_std().unwrap_or_default())));
		}
		if budget == 0 {
			return Ok(BuildPass::Pending);
		}
		let Some(acquired) = building.takeover_expired_build_state(None, true, true).await? else {
			return Ok(BuildPass::Idle);
		};
		let ran = self.run_claimed(&building, acquired).await?;
		Ok(BuildPass::Ran {
			batches: ran.batches,
			finished: ran.finished,
		})
	}

	/// Run the build `building` has claimed for the statement that awaits it, on
	/// that statement's task, to its end ([`BuildStart::Inline`]), and answer
	/// what the statement reports: the build's own error, or none once it has
	/// ended. A build the memory threshold stopped, or the statement's own
	/// deadline or cancellation, is handed back for passes, which are woken,
	/// and fails the statement with that error, as the wait for a build on a
	/// task of its own does; passes carry it on from its last committed batch.
	///
	/// The statement may await it while its program runs, which is sealed off
	/// the catalog and the planner; the build is a seam of that seal
	/// ([`crate::exec::compiles::seam`]): its catalog reads and the plans it
	/// builds are the builder's, not the program's, as they are on a task of
	/// its own.
	async fn run_awaited_inline(
		&self,
		building: &IndexBuilding,
		acquired: AcquiredBuild,
	) -> Result<()> {
		let ran = crate::exec::compiles::seam(self.run_claimed(building, acquired)).await?;
		if let Some(stopped) = ran.stopped {
			building.tf.triggers().index_build.notify_one();
			return Err(stopped);
		}
		ran.failure.map_or(Ok(()), Err)
	}

	/// Run the build `building` has claimed, on the calling task, until it ends
	/// or spends its budget, and settle it as a spawned build is settled: an
	/// error of its own ends the generation in `Error`, an abort in `aborted`,
	/// and a build that spent its budget or met the memory threshold is handed
	/// back. A datastore shutdown is returned with the build left to its lease.
	async fn run_claimed(
		&self,
		building: &IndexBuilding,
		acquired: AcquiredBuild,
	) -> Result<ClaimedRun> {
		let generation = acquired.generation;
		self.register(building).await?;
		// Marks the build finished however the run leaves, so a later build of
		// the index here is not refused as one still running.
		let guard = BuildingFinishGuard(Arc::clone(building));
		let res = building.run_acquired(acquired).await;
		let batches = building.budget.as_ref().map_or(0, BatchBudget::spent);
		let (finished, failure, stopped) = match res {
			Ok(()) => {
				if building.aborted.load(Ordering::Acquire) {
					let _ = building.mark_durable_aborted(generation).await;
				}
				(true, None, None)
			}
			Err(err) if err.is::<BuildYielded>() => {
				building.hand_back(generation).await?;
				(false, None, None)
			}
			Err(err) if is_memory_threshold_error(&err) => {
				building.hand_back(generation).await?;
				(false, None, Some(err))
			}
			Err(err) if is_shutdown_error(&err) => return Err(err),
			// The statement running the build ended: the build itself did not fail. A
			// retryable conflict it stopped retrying for that reason is no failure either.
			Err(err)
				if err.is::<StatementEnded>()
					|| (is_retryable_transaction_conflict(&err)
						&& building.statement_stopping()) =>
			{
				building.hand_back(generation).await?;
				building.stopped_for_statement.store(true, Ordering::Release);
				let stopped = match err.downcast::<StatementEnded>() {
					Ok(StatementEnded(Some(reason))) => Error::from(reason).into(),
					Ok(ended) => anyhow::Error::new(ended),
					Err(_) => match building.statement_ended() {
						Some(reason) => Error::from(reason).into(),
						None => anyhow::Error::new(StatementEnded(None)),
					},
				};
				(false, None, Some(stopped))
			}
			Err(err) => {
				warn!(
					index = %building.ix.name,
					table = %building.ix.table_name,
					error = %err,
					"index build failed"
				);
				let _ = building.mark_durable_error(generation, err.to_string()).await;
				(true, Some(err), None)
			}
		};
		drop(guard);
		Ok(ClaimedRun {
			batches,
			finished,
			failure,
			stopped,
		})
	}
}

/// How [`IndexBuilder::run_claimed`] left a build.
struct ClaimedRun {
	/// The batches it ran.
	batches: usize,
	/// Whether the build ended, rather than being handed back.
	finished: bool,
	/// The error the build failed with, if it did.
	failure: Option<anyhow::Error>,
	/// The error that stopped the build without failing it — the memory
	/// threshold, or the end of the statement running it — which was handed
	/// back rather than failed.
	stopped: Option<anyhow::Error>,
}

/// The most keys one transaction of [`Building::delete_range_in_batches`]
/// deletes.
#[cfg(not(test))]
const STALE_QUEUE_DELETE_BATCH: u32 = 10_000;
/// Small enough that the tests' rebuilds wipe their stale queues over several
/// transactions.
#[cfg(test)]
const STALE_QUEUE_DELETE_BATCH: u32 = 3;

/// Stop `building` writing and lapse its owner lease, as a datastore shutdown
/// does to every build it runs ([`IndexBuilder::lapse_local_build_leases`]).
/// Answers whether the lease was lapsed; a failure is logged and leaves the
/// lease to run out.
async fn lapse_for_shutdown(building: &IndexBuilding) -> bool {
	building.shutting_down.store(true, Ordering::Release);
	let generation = building.build_generation.load(Ordering::Acquire);
	if generation == 0 {
		return false;
	}
	match building.lapse_lease(generation).await {
		Ok(lapsed) => lapsed,
		Err(err) => {
			warn!(
				index = %building.ix.name,
				table = %building.ix.table_name,
				error = %err,
				"could not lapse the lease of an index build ahead of shutdown; \
				 it resumes once the lease runs out"
			);
			false
		}
	}
}

/// Which stalled builds [`IndexBuilder::resume_stalled`] may adopt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResumeScope {
	/// A stalled build in any unfinished phase.
	Any,
	/// Only a stalled build in `Closing`, which has no scan left to run.
	ClosingOnly,
}

/// How long the build of a committed `REBUILD INDEX` waits for a local build of the generation it
/// replaced to stop before it leaves its own generation to be taken over.
const SUPERSEDED_BUILD_STOP: Duration = Duration::from_secs(30);

/// What [`IndexBuilder::stage_rebuild`] did in a `REBUILD INDEX`'s transaction.
pub(crate) enum RebuildStaging {
	/// The rebuild leaves the format as it is, so it builds as any rebuild does.
	Unstaged,
	/// The generation the rebuild builds, staged ownerless, to be built once the transaction
	/// commits.
	Staged(BuildGeneration),
	/// An earlier rebuild of the index in the same transaction staged this generation, which is
	/// built once the transaction commits.
	AlreadyStaged(BuildGeneration),
}

/// Stages in `tx` the flip from `existing`, the index's durable build state as `tx` reads it, to
/// the next generation, in `Building` and owned by `owner`, and answers that generation.
///
/// The previous generation's ticket counter is removed with a read-then-write delete, which
/// conflicts with any concurrent writer allocation, so once the flip commits no further reservation
/// of the previous generation can be made: that is what lets the new generation's drain of prior
/// reservations converge. A blind delete would not conflict on last-writer-wins backends. The new
/// generation gets a counter of its own, so writer admission always compare-and-swaps a key that is
/// present. `!bs` is compare-and-swapped against `existing`, and a miss is the caller's to retry.
///
/// An ownerless generation (`owner` is `None`) is adopted by the first builder to claim it
/// ([`Building::claim_staged_generation`]), or, once it has stood for a whole lease unclaimed, by
/// the takeover of an expired build. `basis` is what `owner` builds against
/// ([`IndexBuildState::basis`]); an ownerless generation records none until it is claimed.
pub(super) async fn stage_new_generation(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	existing: Option<&IndexBuildState>,
	owner: Option<Uuid>,
	basis: Option<IndexBuildBasis>,
) -> Result<BuildGeneration> {
	let generation = existing.map(|s| s.generation.saturating_add(1)).unwrap_or(1);
	let now = Utc::now();
	let state = IndexBuildState {
		generation,
		phase: IndexBuildPhase::Building,
		owner,
		next_ticket: 0,
		initial_complete: false,
		updated_at: now,
		owner_heartbeat_at: owner.map(|_| now),
		error: None,
		report_status: Some(IndexBuildReportStatus::Started),
		initial: None,
		updated: None,
		pending: None,
		initial_cursor: None,
		basis,
	};
	if let Some(previous) = existing.map(|s| s.generation) {
		let previous_bt = ikb.new_bt_key(previous);
		if let Some(current) = tx.get_key(&previous_bt, None).await? {
			tx.del_compare_key(&previous_bt, Some(&current)).await?;
		}
	}
	tx.set_key(&ikb.new_bt_key(generation), &0).await?;
	tx.put_compare_key(&ikb.new_bs_key(), &state, existing).await?;
	Ok(generation)
}

pub(super) struct Building {
	/// Context used during the build.
	pub(super) ctx: FrozenContext,
	/// Fencing token for this concrete builder task.
	pub(super) owner: Uuid,
	/// Options used during the build.
	pub(super) opt: Options,
	/// Table id being indexed.
	pub(super) tb: TableId,
	/// Base key for both queryable index data and durable build metadata.
	pub(super) ikb: IndexKeyBase,
	/// Transaction factory for new transactions.
	pub(super) tf: TransactionFactory,
	/// Index definition being built.
	pub(super) ix: Arc<IndexDefinition>,
	/// Index key (namespace/db/table/index ids).
	pub(super) ix_key: SharedIndexKey,
	/// Durable generation currently owned by this builder; zero before acquire.
	pub(super) build_generation: AtomicU64,
	/// The generation a live owner elsewhere was building when
	/// [`Building::acquire_build_state`] found it, which a blocking statement
	/// then waits for instead of building one of its own; zero otherwise.
	remote_generation: AtomicU64,
	/// Abort flag for the build process.
	pub(super) aborted: AtomicBool,
	/// Set once the datastore running this build has begun to shut down: from
	/// then on the build opens no write transaction, so it cannot renew the
	/// owner lease the shutdown lapses ([`IndexBuilder::lapse_local_build_leases`]).
	shutting_down: AtomicBool,
	/// Set when the spawned task exits so a later local build can start.
	pub(super) finished: AtomicBool,
	/// How many waits for older write transactions this build has begun
	/// ([`crate::kvs::write_barrier`]).
	pub(super) write_barrier_waits_begun: AtomicU64,
	/// How many retryable conflicts this build has met since its last commit,
	/// which sizes the pause before its next retry
	/// ([`Self::conflict_pause`]).
	conflict_streak: AtomicU32,
	/// Whether a statement awaits this build, which then retries a conflict
	/// sooner than a background build does ([`Self::conflict_pause`]).
	pub(super) awaited: bool,
	/// Rendezvous controlling when durable build ownership is released.
	///
	/// Ownership is what keeps every node's background index compaction off an
	/// index whose layout only the builder knows, so it may not be released
	/// while either the builder or the statement that publishes the definition
	/// is still in flight. Each arrives once, and whichever arrives second
	/// performs the release.
	///
	/// A build with no statement behind it — a maintenance resume, whose
	/// definition was committed long before — starts with
	/// [`RELEASE_GATE_STATEMENT`] already set, so its builder releases alone.
	/// [`Building::defer_ownership_release`] clears it for every build a
	/// statement started, before that build is spawned.
	release_gate: AtomicU8,
	/// What the release must decide for the statement that started this build
	/// ([`ROLLBACK_NONE`], [`ROLLBACK_UNDECIDED`] or [`ROLLBACK_WITHDRAW`]),
	/// as its close settled it ([`Building::settle_uncommitted_statement`]).
	/// The committed catalog is read for it at most once: by the rollback for
	/// a commit whose outcome is unknown, whose verdict this carries, or by the
	/// release otherwise.
	///
	/// Stored before the statement's half of `release_gate` arrives, so
	/// whichever half arrives second reads it: the release then decides
	/// whether a generation this builder still owns is withdrawn rather than
	/// kept `Online`, or left to the resume scan after a transient error
	/// ([`Building::release_build_ownership`]).
	rollback: AtomicU8,
	/// Wakes [`Building::await_statement_closed`] once the statement half of
	/// `release_gate` has arrived or the build has been aborted.
	statement_closed: Notify,
	/// The transaction of the statement that started this build, unset for a
	/// build no statement started. A transaction dropped without being
	/// committed or cancelled runs neither hook, so the half it owes never
	/// arrives; [`Building::await_statement_closed`] reads its liveness here.
	statement_tx: std::sync::OnceLock<std::sync::Weak<Transaction>>,
	/// Wakes [`Building::wait_finished`] once `finished` is set. Private because
	/// the two must move together: a notification without the flag is a lost
	/// wake-up for every waiter that has not registered yet.
	finished_notify: Notify,
	/// Set once this build's generation is known to be `Online`: published by
	/// this builder, or seen published by the owner a blocking statement waits
	/// on. From then on the index is complete and queryable whatever the rest
	/// of the build does (see [`BuildCompletion`] and
	/// [`Building::mark_durable_ended`]).
	online: AtomicBool,
	/// The index's columns and `COUNT` guard, compiled on first use and once
	/// for the whole build.
	///
	/// The expressions are a function of the definition, which is fixed for
	/// the build's life, so they outlive the per-batch transactions the build
	/// opens. That is sound because the planner they are compiled through
	/// holds no transaction: its sources resolve when the expression runs, so
	/// nothing from the compiling batch is baked in.
	compiled: OnceCell<Arc<PhysicalIndexEntry>>,
	/// The analyzer a blocking full-text build analyses with, as the statement
	/// that started it resolved it ([`Building::statement_analyzer`]). `None`
	/// for every other build, which reads the analyzer its index names in each
	/// transaction it opens.
	pub(super) analyzer: Option<Arc<AnalyzerDefinition>>,
	/// The structure of the record ids of the table this index is over, where
	/// its `id` field names their components; read once for the whole build
	/// ([`Self::table_key_structure`]).
	key: OnceCell<Option<Arc<crate::catalog::KeyStructure>>>,
	/// The ticket the build's parallel work takes its places through
	/// ([`Self::places`]).
	places: parking_lot::Mutex<BuildPlaces>,
	/// The node's memory governor and temporary store, which bound each batch
	/// of the build; `None` where the context the build was started from bounds
	/// nothing.
	///
	/// The build holds no query's memory: [`Self::ctx`] carries none, and each
	/// batch's context holds a query memory of its own under this host
	/// ([`Self::new_write_tx_ctx`]). What a batch
	/// charges opens that memory's lease, and the lease closes when the batch's
	/// context drops, so between batches, and once the build has finished, the
	/// build keeps nothing reserved from the governor.
	memory: Option<MemoryHost>,
	/// Signalled whenever this build commits a transition out of `Closing`.
	closing_settled: Arc<Notify>,
	/// Whether this build is left to bounded passes ([`IndexBuilder::run_pass`]) rather than
	/// run by a task of its own: its ownership is handed back, not kept, when it is released
	/// ([`Self::release_owned`]).
	to_passes: bool,
	/// Whether the statement that started this build runs it itself, on its own task
	/// ([`BuildStart::Inline`]): a rollback before the statement has run it finds no task to run
	/// the build, and arrives at the release gate in its place.
	inline_run: bool,
	/// Whether this build builds a generation the statement that started it staged, and reads
	/// only once that statement's transaction has closed. The statement's commit stamps the
	/// table's catalog version (`DEFINE INDEX` and `REBUILD INDEX` both do), so on a store that
	/// fences catalog changes ([`Transaction::fences_catalog_changes`]) no write older than that
	/// commit lands after it, and the build's wait for older writes need not ask the other nodes
	/// ([`crate::kvs::write_barrier::WriteBarrier::start`]).
	reads_after_its_stamp: AtomicBool,
	/// The batches this build may start, where a bounded pass runs it
	/// ([`IndexBuilder::run_pass`]); `None` for a build that runs to its end.
	budget: Option<BatchBudget>,
	/// The context of the statement that runs this build on its own task
	/// ([`BuildStart::Inline`]), whose deadline and cancellation stop the build
	/// before its next batch, handed back for passes ([`Self::spend_batch`]);
	/// `None` for a build no statement runs. [`Self::ctx`] is detached from
	/// both.
	statement: Option<FrozenContext>,
	/// Set by the statement's wait for this build when the wait ends
	/// ([`BuildCompletion::stop`]): the build stops before its next batch, as
	/// it does once [`Self::statement`] has ended.
	stop_requested: AtomicBool,
	/// Set once a run of this build stopped for its statement handed it back
	/// rather than ending it ([`IndexBuilder::run_claimed`]).
	stopped_for_statement: AtomicBool,
}

/// The batches a bounded pass lets one build start ([`Building::spend_batch`]).
struct BatchBudget {
	/// The batches the build may still start.
	left: AtomicUsize,
	/// The batches it has started.
	spent: AtomicUsize,
}

impl BatchBudget {
	fn new(batches: usize) -> Self {
		Self {
			left: AtomicUsize::new(batches),
			spent: AtomicUsize::new(0),
		}
	}

	fn spent(&self) -> usize {
		self.spent.load(Ordering::Acquire)
	}
}

/// The error a build bounded by a pass stops with once it has spent its budget
/// ([`Building::spend_batch`]). Every batch before it has committed and nothing
/// of the next has begun.
#[derive(Debug)]
pub(super) struct BuildYielded;

impl std::fmt::Display for BuildYielded {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("the index build pass spent its batch budget")
	}
}

impl std::error::Error for BuildYielded {}

/// The error a build that a statement runs on its own task stops with before
/// its next batch once that statement has ended: past its deadline or
/// cancelled, with the reason, or its wait for the build ended, without one
/// ([`Building::spend_batch`]). Every batch before it has committed and
/// nothing of the next has begun, so the build is handed back for passes
/// rather than failed.
#[derive(Debug)]
pub(super) struct StatementEnded(Option<Reason>);

impl std::fmt::Display for StatementEnded {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("the statement running the index build ended before the build")
	}
}

impl std::error::Error for StatementEnded {}

/// The ticket of a build, through its life.
///
/// A build holds a registration in the node's pool only while it runs.
/// Finished builds stay in [`IndexBuilder`]'s map, and every registration is
/// an entry that each ask of the pool reads, the queries' asks included.
enum BuildPlaces {
	/// No ticket yet: one is registered when the build first needs it.
	Unregistered,
	/// The ticket the build's work takes places through: the registration a
	/// waiting blocking statement lent it, or the build's own.
	Held(crate::idx::places::Ticket),
	/// The build has finished, and its ticket is dropped.
	Released,
}

/// The registration a blocking statement waiting on a build lends it: the
/// ticket set on the statement's context, when the build is `blocking` and
/// one is set. A `CONCURRENTLY` build has no statement waiting on it, so it
/// carries none.
pub(super) fn carried_places(
	ctx: &FrozenContext,
	blocking: bool,
) -> Option<crate::idx::places::Ticket> {
	blocking.then(|| ctx.index_places().cloned()).flatten()
}

impl Building {
	/// A build with a ticket of its own; see [`Self::with_places`].
	pub(super) fn new(
		ctx: &FrozenContext,
		tf: TransactionFactory,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
	) -> Result<Self> {
		Self::with_places(ctx, tf, opt, tb, ix, ix_key, None)
	}

	/// A build whose parallel work takes its places through `carried`, the
	/// registration of the blocking statement waiting on it, so its workers
	/// count as that statement's places. Without one, the build registers a
	/// build ticket of its own with the node's place pool when it first needs
	/// one ([`Self::places`]).
	pub(super) fn with_places(
		ctx: &FrozenContext,
		tf: TransactionFactory,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
		carried: Option<crate::idx::places::Ticket>,
	) -> Result<Self> {
		let ikb = IndexKeyBase::new(ix_key.ns, ix_key.db, ix.table_name.clone(), ix.index_id);
		let places = match carried {
			Some(lent) => BuildPlaces::Held(lent),
			None => BuildPlaces::Unregistered,
		};
		Ok(Self {
			ctx: Context::new_concurrent(ctx).freeze(),
			owner: Uuid::now_v7(),
			opt,
			tb,
			ikb,
			tf,
			ix,
			ix_key,
			build_generation: AtomicU64::new(0),
			remote_generation: AtomicU64::new(0),
			write_barrier_waits_begun: AtomicU64::new(0),
			conflict_streak: AtomicU32::new(0),
			awaited: false,
			aborted: AtomicBool::new(false),
			shutting_down: AtomicBool::new(false),
			finished: AtomicBool::new(false),
			release_gate: AtomicU8::new(RELEASE_GATE_STATEMENT),
			rollback: AtomicU8::new(ROLLBACK_NONE),
			statement_closed: Notify::new(),
			statement_tx: std::sync::OnceLock::new(),
			finished_notify: Notify::new(),
			online: AtomicBool::new(false),
			compiled: OnceCell::new(),
			analyzer: None,
			key: OnceCell::new(),
			places: parking_lot::Mutex::new(places),
			memory: ctx.memory().map(|memory| memory.host().clone()),
			closing_settled: Default::default(),
			to_passes: false,
			inline_run: false,
			reads_after_its_stamp: AtomicBool::new(false),
			budget: None,
			statement: None,
			stop_requested: AtomicBool::new(false),
			stopped_for_statement: AtomicBool::new(false),
		})
	}

	/// Wake the writers waiting on a `Closing` build once a committed
	/// transition of this build's state from `from` to `to` has left `Closing`.
	fn note_closing_left(&self, from: IndexBuildPhase, to: IndexBuildPhase) {
		if from == IndexBuildPhase::Closing && to != IndexBuildPhase::Closing {
			self.closing_settled.notify_waiters();
		}
	}

	/// This build, signalling `settled` whenever it commits a transition out
	/// of `Closing` (see [`IndexBuilder::closing_settled`]).
	pub(super) fn signalling(mut self, settled: &Arc<Notify>) -> Self {
		self.closing_settled = Arc::clone(settled);
		self
	}

	/// Why the statement that runs this build on its own task has ended, if it
	/// has: cancelled, or past its deadline ([`Self::statement`]).
	fn statement_ended(&self) -> Option<Reason> {
		self.statement.as_deref().and_then(query_ended)
	}

	/// Whether this build is run by a statement on its own task that has ended,
	/// or whose wait for the build has ([`Self::statement`],
	/// [`Self::stop_requested`]): the build stops at its next batch, at its
	/// next retry of a conflict inside one, or at its next poll of a wait for
	/// other transactions ([`Self::stop_for_statement`]), and is handed back.
	fn statement_stopping(&self) -> bool {
		self.statement.is_some()
			&& (self.stop_requested.load(Ordering::Acquire) || self.statement_ended().is_some())
	}

	/// Fail with [`StatementEnded`] once the statement running this build on
	/// its own task is stopping it ([`Self::statement_stopping`]), which
	/// [`IndexBuilder::run_claimed`] hands back for passes rather than failing.
	///
	/// Polled by every wait inside the build that lasts as long as another
	/// transaction stays open — the write barrier and the drains of writers'
	/// reservations — so a client holding a write open cannot keep a stopped
	/// build, and the statement waiting for it, past the statement's deadline
	/// or cancellation.
	pub(super) fn stop_for_statement(&self) -> Result<()> {
		if self.statement_stopping() {
			return Err(anyhow::Error::new(StatementEnded(self.statement_ended())));
		}
		Ok(())
	}

	/// The error to answer for `generation`, which [`Self::acquire_build_state`]
	/// installed under this builder's ownership and then did not return because
	/// `err` ended the acquisition.
	///
	/// No run exists yet to hand such a generation back
	/// ([`IndexBuilder::run_claimed`]), so a stop by the statement running the
	/// build ([`StatementEnded`]) hands it back here, and wakes passes: the next
	/// pass claims it at once, rather than once this builder's lease lapses,
	/// and repeats the drain and wipe its restarted scan begins with. Any other
	/// error is answered as it is, the generation left to its lease.
	///
	/// The statement reports its timeout or cancellation, as any statement does that is stopped
	/// before its transaction commits, even a `REBUILD INDEX` whose generation passes now carry on
	/// in the background. It does not report a rebuild that goes on in the background, as the wait
	/// for a build after the commit does ([`AwaitBuildAfterCommit`]), because the executor reports
	/// a statement interrupted before its commit by the interruption alone. Running the statement
	/// again is safe: the generation handed back has no live owner, so a later `REBUILD INDEX`
	/// takes it over and resumes it rather than starting another.
	async fn hand_back_unacquired(
		&self,
		generation: BuildGeneration,
		err: anyhow::Error,
	) -> anyhow::Error {
		let stopped = match err.downcast::<StatementEnded>() {
			Ok(stopped) => stopped,
			Err(err) => return err,
		};
		match self.hand_back(generation).await {
			Ok(_) => self.tf.triggers().index_build.notify_one(),
			Err(err) => warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"could not hand back a build generation its statement stopped acquiring; \
				 it is left to its owner lease"
			),
		}
		match stopped {
			StatementEnded(Some(reason)) => Error::from(reason).into(),
			ended => anyhow::Error::new(ended),
		}
	}

	/// Spend one batch of a bounded pass's budget, before a batch that writes,
	/// failing with [`BuildYielded`] once the budget is spent.
	///
	/// Called only between committed transactions: before a scan batch that
	/// found records, a replay batch that found queued writes, and a drain of
	/// writer reservations that found some. A probe that finds nothing to do
	/// spends nothing, so every pass that claims a build reaches a batch that
	/// advances it. A restarted scan's wipe of the index's data spends one per
	/// page it deletes ([`Self::wipe_index_pages`]), and a pass resuming a scan
	/// skips the rebuild of its primary-append markers
	/// ([`Self::markers_need_no_rebuild`]). What runs whole wherever a build
	/// reaches it spends nothing: a `COUNT` scan's tail, one transaction that
	/// grows with the records written past the scan's end while it ran, and a
	/// DiskANN index's post-scan pass and wipe, which no WebAssembly build has.
	/// A build no pass bounds spends nothing.
	///
	/// A build a statement runs on its own task stops here with
	/// [`StatementEnded`] once the statement's deadline has passed, it has been
	/// cancelled ([`Self::statement`]), or its wait for the build has ended
	/// ([`Self::stop_requested`]).
	pub(super) fn spend_batch(&self) -> Result<()> {
		self.stop_for_statement()?;
		let Some(budget) = &self.budget else {
			return Ok(());
		};
		if budget
			.left
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
			.is_err()
		{
			return Err(anyhow::Error::new(BuildYielded));
		}
		budget.spent.fetch_add(1, Ordering::AcqRel);
		Ok(())
	}

	/// Hand the build of `generation` back: clear its owner, so that any
	/// builder may claim the generation at once ([`build_claimable`]) and
	/// continue it from its last committed batch. Answers whether it did.
	///
	/// Nothing is handed back where this builder no longer owns the
	/// generation, or where it is neither `Building` nor `Closing`.
	pub(super) async fn hand_back(&self, generation: BuildGeneration) -> Result<bool> {
		let handed = self
			.transition_owned_build_state(
				generation,
				TransitionMode::BestEffort,
				false,
				|state| {
					if !matches!(state.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
					{
						return false;
					}
					state.owner = None;
					true
				},
				no_staged_writes,
			)
			.await?;
		Ok(handed.is_some())
	}

	/// Stop this builder from writing and lapse its owner lease on the build
	/// of `generation`, so that any builder may take the generation over at
	/// once as one whose owner lapsed ([`build_owner_expired`]). Answers
	/// whether it lapsed it.
	///
	/// The owner stays as it is, and a takeover replaces it as any lapsed
	/// owner is replaced. This builder opens no write transaction from here on
	/// ([`Self::new_write_tx`]), so it cannot renew the lease; a renewal it
	/// had already staged writes `!bs` too, so of the two the later commit
	/// fails, and a renewal that commits first is lapsed again below. Nothing
	/// is written where this builder no longer owns the generation, or where it
	/// is neither `Building` nor `Closing`; a conflict is retried a few times
	/// and then given up, leaving the lease to run out on its own.
	pub(super) async fn lapse_lease(&self, generation: BuildGeneration) -> Result<bool> {
		self.shutting_down.store(true, Ordering::Release);
		let state_key = self.ikb.new_bs_key();
		for _ in 0..=BEST_EFFORT_TRANSITION_RETRIES {
			let tx = self
				.tf
				.transaction(TransactionType::Write, self.ctx.try_get_sequences()?.clone())
				.await?;
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(false);
			};
			if current.generation != generation
				|| current.owner != Some(self.owner)
				|| !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				tx.cancel().await?;
				return Ok(false);
			}
			let lapsed = Utc::now() - chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS);
			let mut next = current.clone();
			next.owner_heartbeat_at = Some(lapsed);
			if let Err(err) = tx.put_compare_key(&state_key, &next, Some(&current)).await {
				// The write's own error decides what follows; a cancellation that
				// fails as well leaves nothing staged to undo.
				let _ = tx.cancel().await;
				if is_condition_not_met(&err) {
					continue;
				}
				return Err(err);
			}
			match tx.commit().await {
				Ok(()) => return Ok(true),
				// A backend that checks the compare at commit reports a renewal that
				// committed first here, rather than at the write: read it and lapse
				// it again.
				Err(err)
					if is_condition_not_met(&err) || is_retryable_transaction_conflict(&err) =>
				{
					continue;
				}
				Err(err) => return Err(err),
			}
		}
		Ok(false)
	}

	/// The analyzer a blocking full-text build analyses with: the one its index
	/// names, as the transaction of the statement that started the build reads
	/// it.
	///
	/// The build starts while that transaction is open, and nothing the
	/// transaction staged — an analyzer defined beside the index included — is
	/// visible to the transactions the build reads in. A build no statement
	/// awaits reads nothing until the transaction has closed
	/// ([`Building::await_statement_closed`]), but a blocking build that its
	/// statement awaits inside the transaction — one in `BEGIN … COMMIT`, nested
	/// in a block, or replacing an index — cannot wait for it: the transaction
	/// stays open for the whole build. The statement already hands the build
	/// the definition it builds; this hands over the analyzer that definition
	/// names in the same way. The two commit together or not at all, and a
	/// statement that rolls back discards what the build wrote with them
	/// ([`CleanUncommittedBuild`]). A statement that awaits its build after
	/// committing ([`IndexBuilder::run_awaited_after_commit`]) is alone in its
	/// transaction, so the analyzer it hands over is the one that commits.
	///
	/// `None` for an index that is not full-text, for a context with no
	/// transaction, and for an analyzer the statement cannot see either: the
	/// build then reads its own and fails as it would without this.
	pub(super) async fn statement_analyzer(
		&self,
		ctx: &FrozenContext,
	) -> Result<Option<Arc<AnalyzerDefinition>>> {
		let Some(analyzer) = self.kind().analyzer() else {
			return Ok(None);
		};
		let Some(tx) = ctx.try_tx() else {
			return Ok(None);
		};
		// Made while the statement's program runs, which is sealed off the
		// catalog: this is the builder's load, not the program's.
		let read = tx.get_db_analyzer(self.ix_key.ns, self.ix_key.db, analyzer, None);
		match crate::kvs::catalog_reads::sealed(false, read).await {
			Ok(az) => Ok(Some(az)),
			Err(err)
				if matches!(
					err.downcast_ref::<CatalogError>(),
					Some(CatalogError::AzNotFound { .. })
				) =>
			{
				Ok(None)
			}
			Err(err) => Err(err),
		}
	}

	/// This build's index kind, through which the protocol reaches every
	/// kind-specific hook.
	pub(super) fn kind(&self) -> Kind<'_> {
		Kind::of(&self.ix)
	}

	/// The per-batch context of one batch covering `range`, or of a replay
	/// batch, which covers no key range: a full-text build's index analyses
	/// with the analyzer the statement handed over
	/// ([`Building::statement_analyzer`]), if any.
	pub(super) async fn batch_context(
		&self,
		ctx: &FrozenContext,
		range: Option<BatchRange<'_>>,
	) -> Result<BatchContext> {
		self.kind()
			.batch_context(ctx, (self.ix_key.ns, self.ix_key.db), self.analyzer.clone(), range)
			.await
	}

	/// Whether this build's generation is known to be `Online`.
	///
	/// Loads `Acquire` to pair with the `Release` store that follows the
	/// publish, so a caller that observes `true` also observes the publish.
	pub(super) fn is_online(&self) -> bool {
		self.online.load(Ordering::Acquire)
	}

	/// The generation a blocking statement waiting on this build waits for:
	/// the one this builder acquired or took over, or else the one a live
	/// owner elsewhere was building when acquisition found it. Zero before
	/// either.
	pub(super) fn awaited_generation(&self) -> BuildGeneration {
		match self.build_generation.load(Ordering::Acquire) {
			0 => self.remote_generation.load(Ordering::Acquire),
			owned => owned,
		}
	}

	/// Whether [`Self::awaited_generation`] is durably `Online`, on the table
	/// this build was started on (see [`BuildCompletion::published`]).
	async fn awaited_generation_published(&self) -> Result<bool> {
		let generation = self.awaited_generation();
		let tx = self.new_read_tx().await?;
		let table =
			catch!(tx, tx.get_tb(self.ix_key.ns, self.ix_key.db, &self.ix_key.tb, None).await);
		let state = catch!(tx, tx.get_key(&self.ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		Ok(table.is_some_and(|table| table.table_id == self.tb)
			&& state.is_some_and(|state| {
				state.generation == generation && state.phase == IndexBuildPhase::Online
			}))
	}

	/// The ticket the build's parallel work takes its places through, which
	/// every batch context the build opens carries: the registration the
	/// waiting statement lent it, or a build ticket registered with the node's
	/// pool on the first call. Once the build has finished it holds no ticket,
	/// and work that still asks runs on the calling task.
	pub(super) fn places(&self) -> crate::idx::places::Ticket {
		let mut places = self.places.lock();
		match &*places {
			BuildPlaces::Held(ticket) => ticket.clone(),
			BuildPlaces::Released => crate::idx::places::Ticket::serial(),
			BuildPlaces::Unregistered => {
				let ticket = crate::ctx::IndexPlaces::node(
					crate::exec::planner::placement::Policy::from_config(&self.ctx.config.exec),
				);
				*places = BuildPlaces::Held(ticket.clone());
				ticket
			}
		}
	}

	/// Drops the build's ticket, so a finished build keeps no registration in
	/// the node's pool.
	pub(super) fn release_places(&self) {
		*self.places.lock() = BuildPlaces::Released;
	}

	/// The engine's context for one batch of this build.
	///
	/// A build runs with no session and no executor behind it, so this is
	/// assembled from what the build owns: the batch's own context — which
	/// carries that batch's transaction and the datastore's function registry
	/// — and the `Options` the build was started with, whose authority is the
	/// definer's for a build a statement started and root Owner's for one a
	/// maintenance scan resumed.
	///
	/// No session is bound, because a build's context carries no parameters:
	/// a `$param` inside an index expression reads `NONE` during a build and
	/// the writer's binding during a write.
	///
	/// The cancellation token is fresh and nothing signals it. A build's abort
	/// signal is [`Building::aborted`], checked between records; the token is
	/// what the context is shaped to hold rather than a second path.
	///
	/// The context holds a table of closure-body units of its own
	/// ([`ClosureUnits`]), which every record of the batch shares. An index's
	/// columns and guard are invoked as closure units: the batch's first record
	/// links each into the table, and a linked unit keeps a handle on the
	/// transaction it last checked its catalog reads in. The table is dropped
	/// with the batch's context, so it keeps no transaction open past the batch.
	pub(super) async fn exec_ctx(&self, ctx: &FrozenContext) -> Result<ExecutionContext> {
		let tx = ctx.tx();
		let ns = tx.expect_ns_by_name(self.opt.ns()?).await?;
		let db = tx.expect_db_by_name(self.opt.ns()?, self.opt.db()?).await?;
		let root = RootContext {
			ctx: Arc::clone(ctx),
			options: Some(self.opt.clone()),
			datastore: None,
			cancellation: Arc::new(CancellationToken::new()),
			auth: Arc::clone(&self.opt.auth),
			session: None,
			current_value: None,
			current_row: None,
			forbidden_row: None,
			inherited_knn: None,
			// Suppression is derived from the options the build carries, not
			// fixed: a build started from a frame that suppressed enforcement
			// must key a record the way a write in that frame keys it, or the
			// two disagree about the same record. It is expressed as
			// suppression rather than as `skip_fetch_perms`, which also
			// refuses a write — the frame a permission predicate runs in, and
			// not the frame a build does.
			skip_fetch_perms: false,
			perms_suppressed: !self.opt.perms,
			caller_perms_suppressed: !self.opt.caller_perms,
			computing_record: None,
			no_write: None,
			version_stamp: None,
			guard_verdicts: Default::default(),
			writing: false,
			caller_view_checked: false,
			nested_runs: Default::default(),
			session_effects: None,
		};
		Ok(ExecutionContext::Database(DatabaseContext {
			ns_ctx: NamespaceContext {
				root,
				ns,
			},
			db,
			field_state_cache: Arc::new(RwLock::new(HashMap::new())),
			table_def_cache: Arc::new(RwLock::new(HashMap::new())),
			index_def_cache: Arc::new(RwLock::new(HashMap::new())),
			select_permission_cache: Arc::new(RwLock::new(HashMap::new())),
		})
		.with_vm_closures(Arc::new(ClosureUnits::default())))
	}

	/// This index's columns and `COUNT` guard, compiled on first use and once
	/// for the whole build, so the scan evaluates the same expressions the
	/// write path evaluates for the same definition.
	///
	/// The planner is built without a transaction, as the write path's is: a
	/// planner that resolves no table context cannot recurse into an index
	/// expression that names the table it indexes, and it is what makes the
	/// compiled form outlive the batch it was compiled in.
	///
	/// The structure of the table's record ids is read from the catalog, so
	/// that a column or a guard that names a component of the id is compiled as
	/// the write path compiles it ([`Self::table_key_structure`]).
	pub(super) async fn compiled_index(
		&self,
		exec: &ExecutionContext,
	) -> Result<Arc<PhysicalIndexEntry>> {
		self.compiled
			.get_or_try_init(|| async {
				let planner = Planner::new(exec.ctx(), exec.function_registry());
				let key = self.table_key_structure().await?;
				let entry = index_entry_physical(&self.ix, &planner, key.as_deref()).await?;
				Ok::<_, anyhow::Error>(Arc::new(entry))
			})
			.await
			.cloned()
	}

	/// The structure of the record ids of the table this index is over, where
	/// its `id` field names their components; `None` for every other table.
	///
	/// A build a statement started holds the structure that statement's
	/// transaction reads ([`Self::read_key_structure_in`]), which sees an `id`
	/// the same transaction named before defining the index. A build with no
	/// statement behind it, a maintenance resume, reads it in a transaction of
	/// its own, where the definition it resumes is committed. The names of the
	/// components an index reads cannot change while the index exists, so every
	/// structure the build can read names them alike.
	async fn table_key_structure(&self) -> Result<Option<Arc<crate::catalog::KeyStructure>>> {
		self.key
			.get_or_try_init(|| async {
				let tx = self.new_read_tx().await?;
				let key = self.key_structure_through(&tx).await;
				tx.cancel().await?;
				key
			})
			.await
			.cloned()
	}

	/// Read the structure of the table's record ids through the transaction of
	/// `ctx`, the statement that starts this build, when it holds one.
	///
	/// The statement's program runs sealed off the catalog; this is a load the
	/// build makes of its own, as its own transactions do, so it is declared
	/// unsealed ([`crate::kvs::catalog_reads::sealed`]).
	pub(super) async fn read_key_structure_in(&self, ctx: &FrozenContext) -> Result<()> {
		if let Some(tx) = ctx.try_tx() {
			let key =
				crate::kvs::catalog_reads::sealed(false, self.key_structure_through(tx)).await?;
			// The cell is empty: nothing compiles the index before the build is
			// started.
			let _ = self.key.set(key);
		}
		Ok(())
	}

	/// The structure of the table's record ids where its `id` field names
	/// their components, as `tx` reads the catalog.
	async fn key_structure_through(
		&self,
		tx: &Transaction,
	) -> Result<Option<Arc<crate::catalog::KeyStructure>>> {
		let (ns, db, table) = (self.ix_key.ns, self.ix_key.db, &self.ix_key.tb);
		let Some(definition) = tx.get_tb(ns, db, table, None).await? else {
			return Ok(None);
		};
		let fields = tx.all_tb_fields(ns, db, table, None).await?;
		Ok(crate::catalog::table_key(&definition, &fields)
			.filter(|key| key.origin == crate::catalog::KeyOrigin::IdField))
	}

	/// Acquire durable ownership for this build.
	///
	/// A fresh build creates the next generation. If another node is already in
	/// `Building` or `Closing`, this returns `None` while the owner lease is
	/// fresh. Once the lease expires, this builder takes over the same
	/// generation and resumes from the persisted phase instead of starting a new
	/// scan.
	pub(super) async fn acquire_build_state(&self) -> Result<Option<AcquiredBuild>> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let existing = tx.get_key(&state_key, None).await?;
			if let Some(current) = existing.as_ref()
				&& matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				if !build_owner_expired(current, Utc::now()) {
					tx.cancel().await?;
					self.remote_generation.store(current.generation, Ordering::Release);
					return Ok(None);
				}
				// A lapsed generation built against what the catalog does not
				// describe is withdrawn rather than carried on, and this build
				// starts a generation of its own on the next pass.
				if build_basis_is_stale(&tx, &self.ikb, current).await? {
					match withdraw_stale_lapsed_build(&tx, &self.ikb, current).await {
						Ok(()) => {
							self.commit_and_retryable_conflict(
								&tx,
								"transient conflict withdrawing a stale build, retrying",
							)
							.await?;
						}
						Err(err) if is_condition_not_met(&err) => {
							let _ = tx.cancel().await;
						}
						Err(err) => {
							let _ = tx.cancel().await;
							return Err(err);
						}
					}
					continue;
				}
				let mut next = current.clone();
				next.owner = Some(self.owner);
				next.basis = Some(self.basis());
				self.rescan_without_barrier_proof(&tx, &mut next).await?;
				next.error = None;
				next.report_status =
					next.report_status.or_else(|| Some(report_status_from_phase(next.phase)));
				let now = Utc::now();
				next.updated_at = now;
				next.owner_heartbeat_at = Some(now);
				let res = tx.put_compare_key(&state_key, &next, Some(current)).await;
				match res {
					Ok(()) => {
						if self
							.commit_and_retryable_conflict(
								&tx,
								"transient conflict acquiring build ownership, retrying",
							)
							.await?
						{
							continue;
						}
						self.build_generation.store(current.generation, Ordering::Release);
						self.note_closing_left(current.phase, next.phase);
						return Ok(Some(AcquiredBuild {
							generation: current.generation,
							phase: next.phase,
							initial_complete: next.initial_complete,
							initial_count: durable_report_count(next.initial),
							updates_count: durable_report_count(next.updated),
							initial_cursor: next.initial_cursor.clone(),
							staged: false,
						}));
					}
					Err(err) if is_condition_not_met(&err) => {
						let _ = tx.cancel().await;
						continue;
					}
					Err(err) => {
						let _ = tx.cancel().await;
						return Err(err);
					}
				}
			}
			// New-generation takeover. The next generation's state is
			// installed FIRST, in the same transaction that removes the
			// previous generation's `!bt` ticket counter: allocation
			// compare-and-swaps that counter, so committing this transaction
			// fences off any further old-generation admissions (builds in
			// `Error` admit like `Building`). Only after that fence can the
			// prior-generation reservation drain converge to a stable empty
			// state; draining before the flip would race a
			// writer that reserves between the drain and the state commit —
			// its queued mutation would be wiped while its main-table write
			// could land after the new initial scan had already passed the
			// record.
			tx.cancel().await?;
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let existing = tx.get_key(&state_key, None).await?;
			if let Some(current) = existing.as_ref()
				&& matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				tx.cancel().await?;
				continue;
			}

			let res = stage_new_generation(
				&tx,
				&self.ikb,
				existing.as_ref(),
				Some(self.owner),
				Some(self.basis()),
			)
			.await;
			let generation = match res {
				Ok(generation) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict acquiring build ownership, retrying",
						)
						.await?
					{
						continue;
					}
					generation
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			};
			self.build_generation.store(generation, Ordering::Release);
			// Old-generation writers holding tickets either commit their
			// queue entries (their rows become visible before the initial
			// scan starts) or fail their admission fence on the generation
			// change; wait for the stragglers, then wipe the stale queues
			// they can no longer extend. A brand-new index (generation 1,
			// no prior durable state) has no prior generations, so skip the
			// extra transactions: they would only widen the window between
			// installing `!bs` and registering the local task, which a
			// racing cancel-path cleanup uses to abort the build.
			if generation > 1 {
				if let Err(err) = self.wait_for_prior_generation_reservations(generation).await {
					return Err(self.hand_back_unacquired(generation, err).await);
				}
				self.wipe_stale_build_queues(generation).await?;
			}
			return Ok(Some(AcquiredBuild {
				generation,
				phase: IndexBuildPhase::Building,
				initial_complete: false,
				initial_count: 0,
				updates_count: 0,
				initial_cursor: None,
				staged: true,
			}));
		}
	}

	/// Delete every key of `rng` in write transactions of at most
	/// [`STALE_QUEUE_DELETE_BATCH`] keys each, retrying a batch on a retryable
	/// conflict.
	///
	/// Only for a range nothing writes to any more, so that emptying it across
	/// several commits is equivalent to emptying it in one.
	async fn delete_range_in_batches<V>(&self, rng: crate::key::TypedRange<V>) -> Result<()> {
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let batch: Result<Option<bool>> = async {
				let keys = tx.keys(rng.clone(), STALE_QUEUE_DELETE_BATCH, 0, None).await?;
				if keys.is_empty() {
					return Ok(None);
				}
				// A batch short of the limit read the rest of the range.
				let more = keys.len() == STALE_QUEUE_DELETE_BATCH as usize;
				for key in keys {
					tx.del(crate::key::Key::from(key)).await?;
				}
				self.commit_build_tx(&tx).await?;
				Ok(Some(more))
			}
			.await;
			match batch {
				Ok(Some(true)) => {}
				Ok(Some(false)) => return Ok(()),
				Ok(None) => {
					tx.cancel().await?;
					return Ok(());
				}
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict deleting stale build queues, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			}
		}
	}

	/// Delete the queues of every generation below `below`, in its own
	/// retry-looped transaction.
	///
	/// Runs after [`Self::wait_for_prior_generation_reservations`], so no
	/// old-generation writer can re-create the deleted entries. Idempotent: a
	/// crash in between leaves only unreferenced keys, and the restarting
	/// initial scan re-runs both steps.
	async fn wipe_stale_build_queues(&self, below: BuildGeneration) -> Result<()> {
		// The bulk of a large backlog goes in bounded transactions first, so
		// the final sweep below never has to delete it in one: a store with a
		// cap on the keys one transaction may delete (TiKV) would refuse it,
		// and the rebuild would fail on every attempt.
		self.delete_range_in_batches(self.ikb.new_bg_range_below(below)?).await?;
		self.delete_range_in_batches(self.ikb.new_bp_range_below(below)?).await?;
		self.delete_range_in_batches(self.ikb.new_br_range_below(below)?).await?;
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			if let Err(err) = delete_stale_build_queues(&tx, &self.ikb, below).await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict wiping stale build queues, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			match self.commit_build_tx(&tx).await {
				Ok(()) => return Ok(()),
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict wiping stale build queues, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			}
		}
	}

	/// Read the durable build-state record without changing ownership.
	async fn read_durable_build_state(&self) -> Result<Option<IndexBuildState>> {
		let tx = self.new_read_tx().await?;
		let state = catch!(tx, tx.get_key(&self.ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		Ok(state)
	}

	/// Claim `generation` while it stands ownerless in `Building`, as the transaction of a
	/// `REBUILD INDEX` staged it ([`IndexBuilder::stage_rebuild`]); `None` once anything else
	/// holds the index's build state.
	pub(super) async fn claim_staged_generation(
		&self,
		generation: BuildGeneration,
	) -> Result<Option<AcquiredBuild>> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(None);
			};
			if current.generation != generation
				|| current.owner.is_some()
				|| current.phase != IndexBuildPhase::Building
			{
				tx.cancel().await?;
				return Ok(None);
			}
			let mut next = current.clone();
			next.owner = Some(self.owner);
			next.basis = Some(self.basis());
			self.rescan_without_barrier_proof(&tx, &mut next).await?;
			let now = Utc::now();
			next.updated_at = now;
			next.owner_heartbeat_at = Some(now);
			match tx.put_compare_key(&state_key, &next, Some(&current)).await {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict claiming a staged rebuild, retrying",
						)
						.await?
					{
						continue;
					}
					self.build_generation.store(generation, Ordering::Release);
					return Ok(Some(AcquiredBuild {
						generation,
						phase: next.phase,
						initial_complete: next.initial_complete,
						initial_count: durable_report_count(next.initial),
						updates_count: durable_report_count(next.updated),
						initial_cursor: next.initial_cursor.clone(),
						staged: false,
					}));
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
		}
	}

	/// Take over an existing active generation once its owner's lease has
	/// expired, or, with `ownerless`, once it names no owner: handed back by a
	/// bounded pass or a statement ([`Building::hand_back`]), or staged by a
	/// committed `REBUILD INDEX` ([`IndexBuilder::stage_rebuild`]).
	///
	/// This is intentionally narrower than `acquire_build_state`: it never
	/// creates a fresh generation. Blocking callers use it only after they have
	/// observed a remote active build, so racing with a remote completion must
	/// resolve to "wait completed" rather than "start a replacement build".
	///
	/// With `only` set, a generation other than that one is never taken over:
	/// the read that checks the phase and the lease checks the generation in the
	/// same transaction, so a later build that replaces the expected generation
	/// meanwhile is left to its own owner.
	///
	/// With `catalog` set, nothing is taken over unless the catalog, read in
	/// that same transaction, still names this build's table id and holds its
	/// definition unchanged ([`Self::catalog_holds_this_build`]). A removal or a
	/// replacement that commits after the read conflicts with the claim's own
	/// compare-and-swap of `!bs`, which the removal deletes or rewrites.
	async fn takeover_expired_build_state(
		&self,
		only: Option<BuildGeneration>,
		catalog: bool,
		ownerless: bool,
	) -> Result<Option<AcquiredBuild>> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(None);
			};
			if !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
				|| !if ownerless {
					build_claimable(&current, Utc::now())
				} else {
					build_owner_expired(&current, Utc::now())
				} || only.is_some_and(|only| current.generation != only)
			{
				tx.cancel().await?;
				return Ok(None);
			}
			// A generation built against what the catalog does not describe is
			// withdrawn rather than taken over: its checkpoints were written for
			// a statement that did not commit.
			if build_basis_is_stale(&tx, &self.ikb, &current).await? {
				match withdraw_stale_lapsed_build(&tx, &self.ikb, &current).await {
					Ok(()) => {
						if self
							.commit_and_retryable_conflict(
								&tx,
								"transient conflict withdrawing a stale build, retrying",
							)
							.await?
						{
							continue;
						}
						warn!(
							index = %self.ix.name,
							table = %self.ix.table_name,
							"withdrew a lapsed index build made against a definition that did not \
							 commit; run `REBUILD INDEX` to rebuild the index"
						);
						return Ok(None);
					}
					Err(err) if is_condition_not_met(&err) => {
						let _ = tx.cancel().await;
						continue;
					}
					Err(err) => {
						let _ = tx.cancel().await;
						return Err(err);
					}
				}
			}
			if catalog && !self.catalog_holds_this_build(&tx).await? {
				tx.cancel().await?;
				return Ok(None);
			}
			let mut next = current.clone();
			next.owner = Some(self.owner);
			next.basis = Some(self.basis());
			self.rescan_without_barrier_proof(&tx, &mut next).await?;
			next.error = None;
			next.report_status =
				next.report_status.or_else(|| Some(report_status_from_phase(next.phase)));
			let now = Utc::now();
			next.updated_at = now;
			next.owner_heartbeat_at = Some(now);
			let res = tx.put_compare_key(&state_key, &next, Some(&current)).await;
			match res {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict taking over build ownership, retrying",
						)
						.await?
					{
						continue;
					}
					self.build_generation.store(current.generation, Ordering::Release);
					self.note_closing_left(current.phase, next.phase);
					return Ok(Some(AcquiredBuild {
						generation: current.generation,
						phase: next.phase,
						initial_complete: next.initial_complete,
						initial_count: durable_report_count(next.initial),
						updates_count: durable_report_count(next.updated),
						initial_cursor: next.initial_cursor.clone(),
						staged: false,
					}));
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
		}
	}

	/// CAS-update build state only if this builder still owns the generation.
	///
	/// This is the durable fencing point for state transitions. A builder that
	/// loses ownership stops before it can publish `Online` for work completed by
	/// another owner.
	async fn update_owned_build_state<F>(
		&self,
		generation: BuildGeneration,
		update: F,
	) -> Result<IndexBuildState>
	where
		F: FnMut(&mut IndexBuildState),
	{
		self.update_owned_build_state_inner(generation, update, false).await
	}

	/// [`Self::update_owned_build_state`], additionally advancing the
	/// generation's ticket counter in the same transaction when
	/// `fence_ticket_allocation` is set.
	///
	/// Writer admission compare-and-swaps that key, so an allocation that has
	/// read the current phase but not yet committed conflicts with this
	/// transition and retries against the phase it publishes. The ticket it
	/// burns is never issued, which is harmless.
	async fn update_owned_build_state_inner<F>(
		&self,
		generation: BuildGeneration,
		mut update: F,
		fence_ticket_allocation: bool,
	) -> Result<IndexBuildState>
	where
		F: FnMut(&mut IndexBuildState),
	{
		self.transition_owned_build_state(
			generation,
			TransitionMode::Required,
			fence_ticket_allocation,
			|state| {
				update(state);
				true
			},
			no_staged_writes,
		)
		.await?
		.ok_or_else(|| {
			DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into()
		})
	}

	/// The compare-and-swap loop behind every owned transition of `!bs`.
	///
	/// Each attempt runs in a fresh write transaction. It reads `!bs`, checks
	/// that this builder still owns `generation`, and lets `update` edit a copy;
	/// `update` returning `false` ends the transition without a write. It then
	/// stamps the copy, advances the ticket counter if
	/// `fence_ticket_allocation` is set, compare-and-swaps `!bs`, runs `stage`
	/// for the writes that must commit with the transition, and commits.
	///
	/// Returns the state it committed, or `None` when it wrote nothing. A
	/// retryable conflict, or a compare-and-swap that misses, cancels the
	/// attempt and retries after [`BUILD_STATE_RETRY_SLEEP`], whichever step
	/// raised it — the `!bs` read, the ticket fence, the swap, `stage` or the
	/// commit; any other error cancels the attempt and is returned. Either way nothing of a failed
	/// attempt commits. `mode` decides what a missing or foreign `!bs` means and
	/// whether retries are bounded.
	async fn transition_owned_build_state<F, S>(
		&self,
		generation: BuildGeneration,
		mode: TransitionMode,
		fence_ticket_allocation: bool,
		mut update: F,
		mut stage: S,
	) -> Result<Option<IndexBuildState>>
	where
		F: FnMut(&mut IndexBuildState) -> bool,
		S: for<'t> FnMut(&'t Transaction) -> StagedWrites<'t>,
	{
		let state_key = self.ikb.new_bs_key();
		let mut retries = 0;
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			// One attempt. Every error it returns is classified below, whichever
			// step raised it: the `!bs` read, the ticket fence, the swap, the
			// staged writes or the commit.
			let attempt: Result<Option<IndexBuildState>> = async {
				#[cfg(test)]
				maybe_inject_retryable_conflict(
					RetryableConflictSite::ConcurrentIndexBuildStateRead,
					self.ctx.node_id(),
				)?;
				let Some(current) = tx.get_key(&state_key, None).await? else {
					self.end_decided_attempt(&tx).await;
					return match mode {
						TransitionMode::Required => Err(DatastoreError::CorruptedIndex(
							"Index build state is missing during state update",
						)
						.into()),
						TransitionMode::BestEffort | TransitionMode::Persistent(_) => Ok(None),
					};
				};
				if current.generation != generation || current.owner != Some(self.owner) {
					self.end_decided_attempt(&tx).await;
					return match mode {
						TransitionMode::Required => {
							Err(DatastoreError::IndexingBuildingCancelled {
								reason: format!(
									"Index build ownership was lost for {}",
									self.ix.name
								),
							}
							.into())
						}
						TransitionMode::BestEffort | TransitionMode::Persistent(_) => Ok(None),
					};
				}
				let mut next = current.clone();
				if !update(&mut next) {
					self.end_decided_attempt(&tx).await;
					return Ok(None);
				}
				let now = Utc::now();
				next.updated_at = now;
				next.owner_heartbeat_at = if next.owner == Some(self.owner) {
					Some(now)
				} else {
					None
				};
				if fence_ticket_allocation {
					let bt = self.ikb.new_bt_key(generation);
					// A generation predating the counter has none; its admissions
					// still CAS `!bs`, so the state write below fences them.
					//
					// The counter is advanced rather than rewritten: some backends
					// validate a transaction by value rather than by version, and
					// there a write that stores the value it read is invisible to
					// a concurrent allocation, making the fence a silent no-op.
					// Burning a ticket costs nothing, since tickets are opaque
					// ordering tokens and nothing depends on them being contiguous.
					//
					// The compare reads this transaction's own view, so it cannot
					// fail; a conflict with a concurrent allocation surfaces at
					// commit instead, which is where the retry loop handles it.
					if let Some(ticket) = tx.get_key(&bt, None).await? {
						tx.put_compare_key(&bt, &ticket.saturating_add(1), Some(&ticket)).await?;
					}
				}
				tx.put_compare_key(&state_key, &next, Some(&current)).await?;
				if let Some(staged) = stage(&tx) {
					staged.await?;
				}
				tx.commit().await?;
				self.note_closing_left(current.phase, next.phase);
				Ok(Some(next))
			}
			.await;
			let err = match attempt {
				Ok(outcome) => return Ok(outcome),
				Err(err) => err,
			};
			// The attempt's error decides the retry and is what is returned. The
			// attempt did not commit, so a cancel that fails discards nothing
			// that could apply, and its own error would only mask that one.
			let _ = tx.cancel().await;
			// A swap that misses is retried like a conflict: the state it was
			// checked against has moved, and the next attempt reads it afresh.
			if !is_condition_not_met(&err) && !is_retryable_transaction_conflict(&err) {
				return Err(err);
			}
			retries += 1;
			if mode == TransitionMode::BestEffort && retries > BEST_EFFORT_TRANSITION_RETRIES {
				return Err(err.context(format!(
					"gave up on the build-state transition after {BEST_EFFORT_TRANSITION_RETRIES} retries"
				)));
			}
			if let TransitionMode::Persistent(Some(deadline)) = mode
				&& Instant::now() + BUILD_STATE_RETRY_SLEEP >= deadline
			{
				return Err(err.context("gave up on the build-state transition at its deadline"));
			}
			debug!(
				target: "surrealdb::core::kvs::index",
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"transient conflict updating build state, retrying"
			);
			sleep(BUILD_STATE_RETRY_SLEEP).await;
		}
	}

	/// Cancel an attempt of [`Self::transition_owned_build_state`] whose
	/// outcome is already decided: no `!bs`, a generation this builder does not
	/// own, or an update that declined to write.
	///
	/// The attempt wrote nothing, so a cancel that fails discards nothing that
	/// could commit. Its error is logged rather than returned, so it can neither
	/// replace the decided outcome nor be classified as a conflict and retried.
	async fn end_decided_attempt(&self, tx: &Transaction) {
		let res = tx.cancel().await;
		#[cfg(test)]
		let res = res.and(maybe_inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexBuildStateCancel,
			self.ctx.node_id(),
		));
		if let Err(err) = res {
			debug!(
				target: "surrealdb::core::kvs::index",
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"could not cancel a build-state transition that wrote nothing"
			);
		}
	}

	/// Replace the user-facing `INFO FOR INDEX` report fields on a state value.
	fn set_report(
		state: &mut IndexBuildState,
		status: IndexBuildReportStatus,
		initial: Option<usize>,
		pending: Option<usize>,
		updated: Option<usize>,
	) {
		state.report_status = Some(status);
		state.initial = initial.map(|v| v as u64);
		state.pending = pending.map(|v| v as u64);
		state.updated = updated.map(|v| v as u64);
		if status != IndexBuildReportStatus::Error {
			state.error = None;
		}
	}

	/// Persist a progress update without changing the durable lifecycle phase.
	pub(super) async fn mark_durable_report(
		&self,
		generation: BuildGeneration,
		status: IndexBuildReportStatus,
		initial: Option<usize>,
		pending: Option<usize>,
		updated: Option<usize>,
	) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			Self::set_report(state, status, initial, pending, updated);
		})
		.await?;
		Ok(())
	}

	/// Mark that the initial record scan has finished for this generation.
	pub(super) async fn mark_durable_initial_complete(
		&self,
		generation: BuildGeneration,
	) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			if state.phase == IndexBuildPhase::Building {
				state.initial_complete = true;
				state.initial_cursor = None;
				state.error = None;
			}
		})
		.await?;
		Ok(())
	}

	/// Fenced, transaction-local update of the durable build state.
	///
	/// Reads the state through `tx` (observing writes already staged in the
	/// same transaction, such as the ownership heartbeat), verifies this
	/// builder still owns the generation, applies `update`, and stages the
	/// CAS write so it commits atomically with the rest of the transaction.
	///
	/// Must run after [`Self::maintain_build_ownership`] in the same
	/// transaction, which has already fenced this builder's ownership.
	async fn update_build_state_in_tx<F>(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		update: F,
	) -> Result<()>
	where
		F: FnOnce(&mut IndexBuildState),
	{
		let state_key = self.ikb.new_bs_key();
		let Some(current) = tx.get_key(&state_key, None).await? else {
			return Err(DatastoreError::CorruptedIndex(
				"Index build state is missing during build-state update",
			)
			.into());
		};
		if current.generation != generation || current.owner != Some(self.owner) {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		let mut next = current.clone();
		update(&mut next);
		tx.put_compare_key(&state_key, &next, Some(&current)).await?;
		Ok(())
	}

	/// Persist the initial-scan continuation cursor inside a batch transaction.
	///
	/// The cursor and the `initial` counter commit atomically with the batch
	/// they describe, so the durable state never points into an uncommitted
	/// span. A takeover that finds a cursor resumes the scan right after it
	/// instead of wiping the partial index data and rescanning from the start.
	pub(super) async fn checkpoint_initial_scan(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		cursor: &RecordIdKey,
		initial_count: usize,
	) -> Result<()> {
		self.update_build_state_in_tx(tx, generation, |state| {
			state.initial_cursor = Some(cursor.clone());
			state.initial = Some(initial_count as u64);
		})
		.await
	}

	/// Complete the initial scan inside the COUNT tail-pass transaction.
	///
	/// The tail pass baselines `!bp` old states past the last checkpoint and
	/// does not delete the markers it consumes, so it is not idempotent:
	/// completion must commit atomically with it. If durable state still said
	/// "incomplete, resume after cursor" once the tail baselines were
	/// committed, a takeover would re-enter the tail pass and double-count
	/// the same records. Non-COUNT builds have no tail pass and complete via
	/// [`Self::mark_durable_initial_complete`] instead.
	async fn complete_initial_scan(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		initial_count: usize,
	) -> Result<()> {
		self.update_build_state_in_tx(tx, generation, |state| {
			state.initial_complete = true;
			state.initial_cursor = None;
			state.initial = Some(initial_count as u64);
			state.error = None;
		})
		.await
	}

	/// Enter `Closing`, which blocks new admissions before the final drain.
	///
	/// The transition advances the generation's ticket counter in the same
	/// transaction. Allocation compare-and-swaps that counter, so an admission
	/// that read `Building` but has not yet committed conflicts here and
	/// retries, observing `Closing` and waiting instead of reserving. Without
	/// that fence a reservation could commit after `Closing` is durable, be
	/// missed by the drain that follows, and let the build publish `Online`
	/// without ever replaying the write. The counter is advanced rather than
	/// rewritten because a same-value write does not conflict on backends that
	/// validate by value.
	pub(super) async fn mark_durable_closing(&self, generation: BuildGeneration) -> Result<()> {
		self.update_owned_build_state_inner(
			generation,
			|state| {
				if state.phase == IndexBuildPhase::Building {
					state.phase = IndexBuildPhase::Closing;
					state.error = None;
					state.report_status = Some(IndexBuildReportStatus::Indexing);
				}
			},
			true,
		)
		.await?;
		Ok(())
	}

	/// Refuse to publish a generation whose tickets came from two allocators.
	///
	/// A generation that owns a `!bt` counter has `next_ticket` initialised to
	/// zero, and nothing advances it. Only the fallback branch of writer
	/// admission advances it, and that branch runs solely when the counter is
	/// absent. A non-zero value therefore proves that a node predating the
	/// counter allocated against this generation, from a sequence that also
	/// starts at zero and cannot conflict with `!bt`. The two can hand out the
	/// same ticket, in which case one writer's `!br` and `!bg` entries
	/// overwrite the other's and its mutation is missing from the queue this
	/// build has just replayed.
	///
	/// Publishing would leave a silently incomplete index reporting `ready`.
	/// Failing instead records a durable error naming the rebuild, which starts
	/// a fresh generation and rescans the table from the records themselves.
	///
	/// This cannot catch a generation that ran to completion entirely on a node
	/// without the counter, which has no way to know `!bt` exists.
	async fn ensure_single_ticket_allocator(&self, generation: BuildGeneration) -> Result<()> {
		let tx = self.new_read_tx().await?;
		let counter = catch!(tx, tx.get_key(&self.ikb.new_bt_key(generation), None).await);
		let state = catch!(tx, tx.get_key(&self.ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		// A rotated generation means this builder has already lost the build;
		// `mark_durable_online` fails on its own fence. Reporting a version
		// skew here would send the operator after the wrong problem.
		let Some(state) = state.filter(|state| state.generation == generation) else {
			return Ok(());
		};
		if counter.is_some() && state.next_ticket != 0 {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!(
					"Index {} was built while nodes of different versions allocated writer \
					 tickets for build generation {generation}, so queued writes may have been \
					 overwritten. Run `REBUILD INDEX {} ON {}` to rebuild it",
					self.ix.name, self.ix.name, self.ix.table_name
				),
			}
			.into());
		}
		Ok(())
	}

	/// Publish the index as queryable once all admitted work has been replayed.
	///
	/// The build writes nothing of the index after this: what writers queued
	/// is the materialiser's to fold. With `release`, the same transaction
	/// releases durable ownership and stages the compaction request a release
	/// owes ([`Self::stage_compaction_request`]), which the caller asks
	/// for once the statement that started the build has closed. Otherwise
	/// ownership is retained past the publish, because that statement may not
	/// have committed its catalog change yet, so background maintenance
	/// elsewhere in the cluster must keep seeing this index as owned, and
	/// [`Self::release_build_ownership`] ends that window.
	///
	/// An aborted build is not published. Every attempt of the transition reads
	/// the abort before it stages `Online`, so an abort that arrives while the
	/// publish retries a conflict stops it at its next attempt: every stage
	/// since the scan stops short on an abort and reports success, so the work
	/// it left queued would be missing from an index reported ready. An abort
	/// that arrives while the last attempt commits is settled at the release
	/// gate instead ([`Self::release_build_ownership`]).
	///
	/// # Arguments
	/// * `generation` - the generation this builder owns and publishes.
	/// * `initial`, `updated` - the counts the published report carries.
	/// * `release` - whether the publish also releases ownership.
	///
	/// # Returns
	/// Whether it published: `false` for a build aborted before it did.
	pub(super) async fn mark_durable_online(
		&self,
		generation: BuildGeneration,
		initial: usize,
		updated: usize,
		release: bool,
	) -> Result<bool> {
		let request = (release && self.ix.index.queues_compaction()).then(|| self.ikb.clone());
		let node_id = self.ctx.node_id();
		let published = self
			.transition_owned_build_state(
				generation,
				TransitionMode::Required,
				false,
				|state| {
					if self.aborted.load(Ordering::Acquire) {
						return false;
					}
					state.phase = IndexBuildPhase::Online;
					state.initial_complete = true;
					state.error = None;
					if release {
						state.owner = None;
					}
					Self::set_report(
						state,
						IndexBuildReportStatus::Ready,
						Some(initial),
						Some(0),
						Some(updated),
					);
					true
				},
				|tx| {
					// Stands in for a publish that conflicts, so a test can hold the
					// task after its last batch and before it reaches the release gate.
					#[cfg(test)]
					if let Err(err) = maybe_inject_retryable_conflict(
						RetryableConflictSite::ConcurrentIndexOnlinePublish,
						node_id,
					) {
						return Some(Box::pin(std::future::ready(Err(err))));
					}
					let ikb = request.clone()?;
					Some(Box::pin(Self::stage_compaction_request(tx, ikb, node_id)))
				},
			)
			.await?;
		Ok(published.is_some())
	}

	/// Whether a publish of `generation` whose commit reported `err` applied
	/// after all, as the durable build state says: `Online`, still this
	/// builder's generation, and owned by this builder, or by no one where the
	/// publish `released` its ownership.
	///
	/// `false` when the state cannot be read either, which the caller treats
	/// as a publish that did not apply.
	async fn published_despite(
		&self,
		generation: BuildGeneration,
		released: bool,
		err: &anyhow::Error,
	) -> bool {
		// A publish that released its ownership leaves none to check: nothing
		// else clears the owner of this generation before the builder arrives
		// at the release gate, which it has not.
		let owned =
			|owner: Option<Uuid>| owner == Some(self.owner) || (released && owner.is_none());
		match self.read_durable_build_state().await {
			Ok(Some(state))
				if state.generation == generation
					&& state.phase == IndexBuildPhase::Online
					&& owned(state.owner) =>
			{
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					error = %err,
					"the commit that published an index reported an error but applied; \
					 the build carries on"
				);
				true
			}
			Ok(_) => false,
			Err(read_err) => {
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					error = %err,
					read_error = %read_err,
					"could not read back whether the commit that published an index applied"
				);
				false
			}
		}
	}

	/// Hold durable ownership until the statement that started this build has
	/// closed its transaction.
	///
	/// A statement stages the definition it builds against in its own
	/// transaction, which is still open when the build starts and, for a
	/// blocking build awaited inside that transaction, until after the build
	/// completes. Every other node reads the previous definition for that whole
	/// window, so ownership — the one signal that is durable before the
	/// definition is — has to outlast it. That holds however the statement
	/// started the build: a `CONCURRENTLY` build, and a blocking build awaited
	/// after the commit, own their generation from the moment they are
	/// spawned, and wait for this half before they read anything
	/// ([`Building::await_statement_closed`]). Registered on both outcomes: a
	/// rolled-back statement leaves the previous definition in place, which is
	/// the one background compaction was already reading, so no fence is needed
	/// for it. Where that definition does not describe the layout this build
	/// writes, the release withdraws the build instead of keeping it published
	/// ([`Building::release_build_ownership`]).
	///
	/// A no-op for a build with no open statement transaction behind it — a
	/// maintenance resume, whose definition was committed long before, or a
	/// takeover by a blocking statement awaiting after its transaction
	/// committed — which keeps the statement half of [`Building::release_gate`]
	/// as it starts, already arrived.
	///
	/// Must be called before the build is spawned, so the release cannot be
	/// reached before both halves of the gate are in play.
	pub(super) async fn defer_ownership_release(self: &Arc<Self>, ctx: &FrozenContext) {
		// A closed transaction has already published or discarded the
		// definition, and will run no action registered on it now.
		let Some(txn) = ctx.try_tx().filter(|txn| !txn.closed()) else {
			return;
		};
		self.release_gate.store(0, Ordering::Release);
		let _ = self.statement_tx.set(Arc::downgrade(txn));
		txn.on_commit(Box::new(ReleaseBuildOwnership(Arc::clone(self)))).await;
		txn.on_rollback(Box::new(ReleaseBuildOwnership(Arc::clone(self)))).await;
	}

	/// Whether `tx` holds this build's table under its table id, and a
	/// definition of its index that describes the layout this build writes: the
	/// same index id, table, columns, kind, `COUNT` guard and format, not staged
	/// for removal, and, for a build handed its statement's analyzer, an
	/// analyzer of that name with the same function, tokenizers and filters. A
	/// change that leaves the layout alone, such as a comment, does not count.
	/// Read from the keys themselves rather than through the catalog: a build
	/// is started by work sealed off catalog reads.
	async fn catalog_holds_this_build(&self, tx: &Transaction) -> Result<bool> {
		use crate::catalog::FromStored as _;
		let (ns, db, tb) = (self.ix_key.ns, self.ix_key.db, &self.ix_key.tb);
		let table = crate::key::schema::TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let Some(table) = tx.get_key(&table, None).await? else {
			return Ok(false);
		};
		if crate::catalog::TableDefinition::from_stored(&table)?.table_id != self.tb {
			return Ok(false);
		}
		let name = crate::key::schema::IndexNameKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: self.ix.index_id,
		};
		let Some(name) = tx.get_key(&name, None).await? else {
			return Ok(false);
		};
		let definition = crate::key::schema::IndexDefKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: Cow::Borrowed(name.as_str()),
		};
		let Some(stored) = tx.get_key(&definition, None).await? else {
			return Ok(false);
		};
		let current = IndexDefinition::from_stored(&stored)?;
		let built = &self.ix;
		if current.prepare_remove
			|| current.index_id != built.index_id
			|| current.table_name != built.table_name
			|| current.cols != built.cols
			|| current.index != built.index
			|| current.count_cond != built.count_cond
			|| current.format_version != built.format_version
		{
			return Ok(false);
		}
		// A blocking full-text build analyses with the analyzer as its
		// statement's transaction read it ([`Self::statement_analyzer`]), which
		// that transaction may have redefined under the same name. The terms it
		// indexed are the ones queries and writes look up only if `tx` holds an
		// analyzer that analyses the same way; its comment does not count.
		let Some(analyzer) = &self.analyzer else {
			return Ok(true);
		};
		let key = crate::key::schema::AnalyzerKey {
			ns,
			db,
			az: Cow::Borrowed(analyzer.name.as_str()),
		};
		let Some(stored) = tx.get_key(&key, None).await? else {
			return Ok(false);
		};
		let committed = AnalyzerDefinition::from_stored(&stored)?;
		Ok(analyzers_analyse_alike(&committed, analyzer))
	}

	/// What this build is made against beyond its index's id, as its build
	/// state records it when this builder takes ownership
	/// ([`IndexBuildState::basis`]): the parts [`Self::catalog_holds_this_build`]
	/// compares that a statement can change without giving the index a new id.
	fn basis(&self) -> IndexBuildBasis {
		IndexBuildBasis {
			format_version: self.ix.format_version,
			analyzer: self.analyzer.as_deref().cloned(),
		}
	}

	/// Whether the commit of the statement that started this build failed with
	/// an outcome that is unknown ([`Transaction::commit_outcome_unknown`]).
	/// `false` for a build no statement started, and for a statement whose
	/// transaction is gone.
	fn statement_commit_outcome_unknown(&self) -> bool {
		self.statement_tx
			.get()
			.and_then(std::sync::Weak::upgrade)
			.is_some_and(|statement| statement.commit_outcome_unknown())
	}

	/// Whether the committed catalog, read in a transaction of its own, holds a
	/// definition that describes the layout this build writes
	/// ([`Self::catalog_holds_this_build`]).
	///
	/// A read that fails is attempted up to [`COMMITTED_CATALOG_READ_ATTEMPTS`]
	/// times in all, with a doubling pause between attempts, and none is started
	/// or waited on past `deadline`. A read that still fails, or runs out of
	/// time, answers `false`: a build undone or withdrawn in error is reported
	/// `aborted` and restored by the next `REBUILD INDEX`, where one kept in
	/// error could report `ready` through a definition that does not read what
	/// it wrote.
	async fn committed_catalog_holds_this_build(&self, deadline: Option<Instant>) -> bool {
		#[cfg(test)]
		committed_catalog_reads::note(self.ctx.node_id());
		let out_of_time = || {
			warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				"could not read whether the committed catalog describes this build within the \
				 close's allowance; treating it as not described"
			);
			false
		};
		let mut pause = COMMITTED_CATALOG_READ_RETRY_SLEEP;
		for attempt in 1..=COMMITTED_CATALOG_READ_ATTEMPTS {
			let read = async {
				let tx = self.new_read_tx().await?;
				let holds = self.catalog_holds_this_build(&tx).await;
				// A read-only transaction: its cancel discards nothing, and the
				// read's outcome is what is returned.
				let _ = tx.cancel().await;
				holds
			};
			let read = match deadline {
				Some(deadline) => {
					let Some(left) = deadline.checked_duration_since(Instant::now()) else {
						return out_of_time();
					};
					match common::time::timeout(left, read).await {
						Ok(read) => read,
						Err(_) => return out_of_time(),
					}
				}
				None => read.await,
			};
			match read {
				Ok(holds) => return holds,
				Err(err) if attempt < COMMITTED_CATALOG_READ_ATTEMPTS => {
					debug!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						error = %err,
						"could not read whether the committed catalog describes this build, \
						 retrying"
					);
					if deadline.is_some_and(|deadline| Instant::now() + pause >= deadline) {
						return out_of_time();
					}
					sleep(pause).await;
					pause *= 2;
				}
				Err(err) => {
					warn!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						error = %err,
						"could not read whether the committed catalog describes this build; \
						 treating it as not described"
					);
				}
			}
		}
		false
	}

	/// Settle this build for the statement that started it, which closed its
	/// transaction without committing. The one place a statement's rollback
	/// decides its build ([`ReleaseBuildOwnership`]), before the statement's
	/// half of [`Building::release_gate`] arrives.
	///
	/// The rollback is recorded, and the build is aborted, so a build still
	/// running stops at its next check and publishes nothing. A build may have
	/// published already, or a builder that stopped on a transient error may
	/// still own its generation; either is decided only at the release
	/// ([`Self::release_build_ownership`]), which reads the committed catalog
	/// for it then. So a rolled-back build that never published, and that no
	/// builder still owns, costs the rollback no read at all.
	///
	/// A commit whose outcome is unknown is the one exception: it may have
	/// applied ([`Transaction::commit_outcome_unknown`]), and nothing the
	/// statement writes is a record of it a later statement cannot overwrite.
	/// So the build goes on, unaborted, exactly when the committed catalog,
	/// read here within `deadline`, describes its layout: that definition is
	/// the statement's own if it committed — a new index's, under an id drawn
	/// only for it, or a rebuilt one's, restamped at the current format, which
	/// a later statement on the same table leaves as it is — or one the
	/// rollback kept that the build writes correctly as well. A catalog that
	/// cannot be read in time undoes the build. Either way that read is the
	/// only one this rollback makes: its verdict is what the release acts on
	/// ([`Building::rollback`]), so a build it confirmed is released as any
	/// other, and one it did not is withdrawn without reading again.
	async fn settle_uncommitted_statement(&self, deadline: Option<Instant>) {
		let verdict = if !self.statement_commit_outcome_unknown() {
			ROLLBACK_UNDECIDED
		} else if self.committed_catalog_holds_this_build(deadline).await {
			// The build goes on, and its release has nothing to decide.
			return;
		} else {
			ROLLBACK_WITHDRAW
		};
		self.rollback.store(verdict, Ordering::Release);
		self.abort();
	}

	/// Keep ownership of a published generation while the statement that
	/// started the build stays open, and settle it when that statement goes.
	///
	/// A build awaited inside its statement's transaction publishes `Online`
	/// before that transaction closes, and its ownership fences compaction
	/// until it does. This keeper heartbeats the owner meanwhile, so a lapse
	/// means the keeper stopped, not that the statement is slow:
	///
	/// - The statement commits or rolls back: its close releases the ownership, and the keeper
	///   stops.
	/// - The statement's transaction is dropped without either: no close action will run, so the
	///   keeper arrives in its place and releases. The statement did not commit, so the release
	///   withdraws the generation unless the committed catalog describes it
	///   ([`Building::release_build_ownership`]).
	/// - The heartbeat finds the ownership gone — cleared after a lapse, or the generation replaced
	///   — and the keeper cancels the statement's transaction. A clear already fences that
	///   statement ([`super::state::clear_lapsed_online_owner`]); the cancel ends it at once rather
	///   than at its commit.
	///
	/// A no-op once both halves of [`Building::release_gate`] have arrived.
	async fn keep_online_ownership(&self, generation: BuildGeneration) {
		if self.release_gate.load(Ordering::Acquire) == RELEASE_GATE_OPEN {
			return;
		}
		match self.await_statement_closed(generation, IndexBuildPhase::Online).await {
			// An aborted build writes nothing more: whatever aborted it — a
			// removal, a later build of the index, the statement's rollback —
			// settles the ownership itself.
			Ok(StatementWait::Closed | StatementWait::Aborted) => {}
			Ok(StatementWait::Dropped) => {
				self.rollback.store(ROLLBACK_UNDECIDED, Ordering::Release);
				if self.arrive_at_release_gate(RELEASE_GATE_STATEMENT) {
					self.release_build_ownership(generation, None).await;
				}
			}
			Err(err) => {
				let lost = matches!(
					err.downcast_ref::<DatastoreError>(),
					Some(DatastoreError::IndexingBuildingCancelled { .. })
				);
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					error = %err,
					"stopped keeping the ownership of a published index build"
				);
				if lost
					&& let Some(statement) =
						self.statement_tx.get().and_then(std::sync::Weak::upgrade)
				{
					let _ = statement.cancel().await;
				}
			}
		}
	}

	/// Clear the `Online` owner of this build's index whose heartbeat lapsed,
	/// where the store allows it, with the compaction request a release owes
	/// staged beside it ([`super::state::clear_lapsed_online_owner`]). A
	/// generation the clear withdraws instead is owed no request.
	///
	/// Answers whether this call cleared it. A transaction that finds the
	/// owner held, released or replaced, or that loses a conflict to whatever
	/// changed it, clears nothing.
	pub(super) async fn clear_lapsed_owner(&self) -> Result<bool> {
		let tx = self.new_write_tx().await?;
		let staged = async {
			let BuildOwner::Lapsed(state) = build_owner(&tx, &self.ikb, true).await? else {
				return Ok(false);
			};
			let withdrew = clear_lapsed_online_owner(&tx, &self.ikb, &state).await?;
			if !withdrew && self.ix.index.queues_compaction() {
				Self::stage_compaction_request(&tx, self.ikb.clone(), self.ctx.node_id()).await?;
			}
			Ok::<_, anyhow::Error>(true)
		}
		.await;
		match staged {
			Ok(true) => match tx.commit().await {
				Ok(()) => Ok(true),
				Err(err) if is_retryable_transaction_conflict(&err) => {
					let _ = tx.cancel().await;
					Ok(false)
				}
				Err(err) => Err(err),
			},
			Ok(false) => {
				tx.cancel().await?;
				Ok(false)
			}
			Err(err) => {
				let _ = tx.cancel().await;
				Err(err)
			}
		}
	}

	/// Record one half of [`Building::release_gate`], reporting whether this
	/// arrival was the one that completed it. A half that arrives again reports
	/// `false`, so the release runs once whichever paths arrive.
	fn arrive_at_release_gate(&self, arrival: u8) -> bool {
		let before = self.release_gate.fetch_or(arrival, Ordering::AcqRel);
		before != RELEASE_GATE_OPEN && (before | arrival) == RELEASE_GATE_OPEN
	}

	/// Wait until the statement that started this build has closed its
	/// transaction, answering whether the build goes on.
	///
	/// A statement spawns its build while its transaction is still open. Until
	/// that transaction commits, what it stages is invisible to every other
	/// transaction: the definition, which no writer can see either, so a record
	/// written meanwhile is neither indexed by its writer nor queued for the
	/// build; and anything else it stages, such as an analyzer the index names
	/// or a `PREPARE REMOVE` of the index. Where the statement waits for its
	/// build decides whether the build waits here for the transaction to close:
	///
	/// - `CONCURRENTLY`: the statement does not wait for the build, which waits here and reads
	///   nothing, its scan included, until the transaction has closed.
	/// - Blocking, awaited after the commit: the same. The statement's owner awaits the build only
	///   once the commit has applied, so the wait here cannot hold the commit back.
	/// - Blocking, awaited inside the transaction: the build must not wait here, as that
	///   transaction cannot close before the build ends. It reads from the start.
	///
	/// The statement half of [`Building::release_gate`] arrives on
	/// commit and on rollback, so the wait ends either way, and a build with no
	/// statement behind it starts with that half arrived. Answers how the wait
	/// ended ([`StatementWait`]): the statement closed; the build was aborted,
	/// as the rollback of a fresh definition, a removal or a later build of the
	/// index aborts it; or the transaction was dropped without closing, which
	/// stages nothing and runs neither close action.
	/// Ownership is heartbeated while the transaction stays open, so the lease
	/// outlasts a long one.
	async fn await_statement_closed(
		&self,
		generation: BuildGeneration,
		phase: IndexBuildPhase,
	) -> Result<StatementWait> {
		// A build waiting to read renews its lease as often as any build does
		// while it waits; a published build's keeper, as its ownership is.
		let heartbeat_every = match phase {
			IndexBuildPhase::Online => self.ownership_heartbeat_every(),
			IndexBuildPhase::Building | IndexBuildPhase::Closing | IndexBuildPhase::Error => {
				BUILD_WAITING_HEARTBEAT
			}
		};
		let mut heartbeat_at = Instant::now();
		// Register for the wake-up before checking the gate or `aborted`, as
		// `notify_waiters` only wakes waiters that are already registered.
		// Every notification follows a change that never reverts, `aborted` or
		// the statement half of the gate, and the check below ends the loop
		// on it, so one registration serves the whole wait.
		let notified = self.statement_closed.notified();
		tokio::pin!(notified);
		notified.as_mut().enable();
		loop {
			if self.is_aborted().await {
				return Ok(StatementWait::Aborted);
			}
			// Read before the gate: a transaction runs its hooks before it is
			// dropped, so one seen gone here has already set the gate if it
			// closed.
			let dropped =
				self.statement_tx.get().is_some_and(|statement| statement.strong_count() == 0);
			if self.release_gate.load(Ordering::Acquire) & RELEASE_GATE_STATEMENT != 0 {
				return Ok(StatementWait::Closed);
			}
			if dropped {
				return Ok(StatementWait::Dropped);
			}
			if heartbeat_at.elapsed() >= heartbeat_every {
				self.heartbeat_ownership(generation, phase).await?;
				heartbeat_at = Instant::now();
			}
			// A transaction dropped without closing wakes nothing, so it is
			// polled.
			tokio::select! {
				() = &mut notified => {}
				() = sleep(BUILD_CLOSING_SLEEP) => {}
			}
		}
	}

	/// How often a build heartbeats ownership it holds while it waits: a
	/// quarter of the lease.
	fn ownership_heartbeat_every(&self) -> Duration {
		let every = Duration::from_secs((BUILD_OWNER_LEASE_SECS / 4) as u64);
		#[cfg(test)]
		let every = statement_heartbeat::every_for(self.ctx.node_id()).unwrap_or(every);
		every
	}

	/// Run `work` while heartbeating the ownership of the published
	/// `generation`.
	///
	/// Ownership held past the `Online` publish fences compaction off the
	/// index while `work` runs. Without a heartbeat the lease would lapse under
	/// a long `work`, and a compaction pass or the resume scan would clear the
	/// owner as stranded ([`super::state::clear_lapsed_online_owner`]).
	///
	/// # Returns
	/// What `work` returns, or the error of a heartbeat that finds the
	/// ownership gone. `work` is then dropped where it stands.
	async fn while_owning<T>(
		&self,
		generation: BuildGeneration,
		work: impl Future<Output = Result<T>>,
	) -> Result<T> {
		let every = self.ownership_heartbeat_every();
		let mut work = std::pin::pin!(work);
		loop {
			tokio::select! {
				biased;
				done = &mut work => return done,
				() = sleep(every) => {
					self.heartbeat_ownership(generation, IndexBuildPhase::Online).await?;
				}
			}
		}
	}

	/// Wait until every write transaction that opened before the call has ended on every live
	/// node ([`crate::kvs::write_barrier::WriteBarrier`]), heartbeating ownership of `generation`
	/// meanwhile, since a write transaction a client holds open can outlast the owner lease.
	/// Answers `false` when the build was aborted while it waited, and fails with
	/// [`StatementEnded`] when the statement running the build on its own task stops it
	/// ([`Building::stop_for_statement`]).
	async fn wait_for_older_writes(&self, generation: BuildGeneration) -> Result<bool> {
		let sequences = self.ctx.try_get_sequences()?.clone();
		// Counted as this build's own before the wait begins, for a statement that set its
		// transaction aside for this build.
		self.write_barrier_waits_begun.fetch_add(1, Ordering::SeqCst);
		// A definition built inside its own transaction holds back the writes that open after this
		// wait begins, until that transaction ends: they cannot see the index, and this build
		// reads before it commits. The build's own context carries no transaction, so the
		// statement's is the one found through [`Self::statement_tx`].
		let hold =
			self.statement_tx.get().and_then(std::sync::Weak::upgrade).and_then(|txn| {
				txn.definition_hold(self.ix_key.ns, self.ix_key.db, &self.ix_key.tb)
			});
		let mut barrier = crate::kvs::write_barrier::WriteBarrier::start(
			&self.tf,
			&sequences,
			self.ctx.node_id(),
			(self.ix_key.ns, self.ix_key.db, self.ix_key.tb.clone()),
			hold.as_deref(),
			self.reads_after_its_stamp.load(Ordering::Acquire),
		)
		.await?;
		let every = self.ownership_heartbeat_every();
		let mut beat = Instant::now();
		loop {
			if self.is_aborted().await {
				return Ok(false);
			}
			self.stop_for_statement()?;
			if barrier.done(&self.tf, &sequences).await? {
				self.record_barrier_proof(generation).await?;
				return Ok(true);
			}
			#[cfg(test)]
			write_barrier_waits::note(self.ctx.node_id());
			if beat.elapsed() >= every {
				self.heartbeat_ownership(generation, IndexBuildPhase::Building).await?;
				beat = Instant::now();
			}
			sleep(barrier.poll()).await;
		}
	}

	/// Make a takeover of `next` rescan the table when its initial scan has scanned anything — it
	/// completed, or checkpointed a cursor — without proof that its owner waited at the write
	/// barrier for its generation (`!bw`), which a build records before it scans. An owner of a
	/// release without the barrier may have scanned past a record before a write transaction
	/// older than the generation's commit committed it, and that write was neither scanned nor
	/// queued; resuming after the cursor would never read it. The scan restarts from the start,
	/// and waits at the barrier before it wipes.
	async fn rescan_without_barrier_proof(
		&self,
		tx: &Transaction,
		next: &mut IndexBuildState,
	) -> Result<()> {
		let scanned = next.phase == IndexBuildPhase::Closing
			|| (next.phase == IndexBuildPhase::Building
				&& (next.initial_complete || next.initial_cursor.is_some()));
		if !scanned {
			return Ok(());
		}
		let proof: Option<BuildGeneration> = tx.get_key(&self.ikb.new_bw_key(), None).await?;
		if proof == Some(next.generation) {
			return Ok(());
		}
		next.phase = IndexBuildPhase::Building;
		next.initial_complete = false;
		next.initial_cursor = None;
		next.initial = None;
		next.report_status = Some(IndexBuildReportStatus::Started);
		Ok(())
	}

	/// Delete what the index holds, a page of [`WIPE_PAGE_KEYS`] keys to a transaction and each
	/// page a batch of a bounded pass's budget ([`Self::spend_batch`]), so the wipe that starts a
	/// restarted scan leaves only its last step to one transaction: the index's compaction
	/// generations, which that step reads and writes back one above.
	///
	/// A build no pass bounds wipes in that one transaction, as does a DiskANN index, whose wipe
	/// also forgets the pending-state guards this datastore knows of. Pages need no record of
	/// their own: a deleted key stays deleted, so a pass stopped anywhere resumes the wipe where
	/// the index's keys begin. Nothing reads the index meanwhile: it is `Building`, which the
	/// planner does not use, writers queue for, and compaction is fenced off. Answers `false`
	/// when the build was aborted.
	async fn wipe_index_pages(&self, generation: BuildGeneration) -> Result<bool> {
		if self.budget.is_none() || matches!(self.ix.index, crate::catalog::Index::DiskAnn(_)) {
			return Ok(true);
		}
		loop {
			if self.is_aborted().await {
				return Ok(false);
			}
			let tx = self.new_write_tx().await?;
			let staged = async {
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
					.await?;
				crate::idx::wipe_index_page(&tx, &self.ikb, WIPE_PAGE_KEYS).await
			}
			.await;
			let deleted = match staged {
				Ok(deleted) => deleted,
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict wiping a page of index data, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			};
			// A page that found nothing to delete spends nothing.
			if deleted == 0 {
				tx.cancel().await?;
				return Ok(true);
			}
			if let Err(err) = self.spend_batch() {
				tx.cancel().await?;
				return Err(err);
			}
			if let Err(err) = tx.commit().await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict committing a page of index data, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
		}
	}

	/// Whether a build bounded by a pass resumes `generation` with its `!bp` markers already at the
	/// keys this release looks them up by, so the rebuild that starts a resumed scan
	/// ([`Self::rebuild_primary_appendings`]) has nothing to move.
	///
	/// The write barrier's proof for the generation (`!bw`) says its owner was a release that
	/// spells markers as this one does: only such a release records it. Writers spell their own
	/// markers, so this holds only where one release serves every writer, which is where builds
	/// are left to passes
	/// ([`Builder::without_index_build_tasks`](crate::kvs::Builder::without_index_build_tasks)).
	/// Skipping it is what keeps a resumed scan's first step from growing with the writes queued
	/// during the build, which a pass's budget does not bound. Any other build rebuilds them.
	async fn markers_need_no_rebuild(&self, generation: BuildGeneration) -> Result<bool> {
		if self.budget.is_none() {
			return Ok(false);
		}
		let tx = self.new_read_tx().await?;
		let proof: Option<BuildGeneration> =
			catch!(tx, tx.get_key(&self.ikb.new_bw_key(), None).await);
		tx.cancel().await?;
		Ok(proof == Some(generation))
	}

	/// Record that this build waited at the write barrier for `generation` (`!bw`), so a takeover
	/// of its completed scan need not rescan ([`Self::rescan_without_barrier_proof`]). Recorded
	/// with a heartbeat of the build's ownership, so a build whose generation another replaced
	/// records nothing over the newer generation's proof, and stops as it loses its ownership.
	pub(super) async fn record_barrier_proof(&self, generation: BuildGeneration) -> Result<()> {
		loop {
			let tx = self.new_write_tx().await?;
			let staged = async {
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
					.await?;
				tx.set_key(&self.ikb.new_bw_key(), &generation).await
			}
			.await;
			if let Err(err) = staged {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict recording the write barrier's proof, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict recording the write barrier's proof, retrying",
				)
				.await?
			{
				continue;
			}
			return Ok(());
		}
	}

	/// Heartbeat ownership of `generation` in a transaction of its own.
	async fn heartbeat_ownership(
		&self,
		generation: BuildGeneration,
		phase: IndexBuildPhase,
	) -> Result<()> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			if let Err(err) = self.maintain_build_ownership(&tx, generation, &[phase]).await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict maintaining build ownership, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict committing build ownership, retrying",
				)
				.await?
			{
				continue;
			}
			return Ok(());
		}
	}

	/// Release durable ownership of a published generation.
	///
	/// Until it commits, an `Online` generation still names this builder as its
	/// owner, which is what keeps every node's background compaction off a
	/// vector index whose definition the publishing statement has not committed
	/// yet. The release commits in one transaction with the compaction request
	/// it owes ([`Self::requeue_compaction_with_release`]).
	///
	/// A no-op for a generation this builder no longer owns: a build that ended
	/// before publishing had its owner cleared by the transition that ended it.
	/// A no-op too for one it still owns that is not `Online`: a builder that
	/// stopped on a transient error leaves its generation `Building` or
	/// `Closing` for the resume scan.
	///
	/// A build whose statement did not commit ([`Self::settle_uncommitted_statement`])
	/// is withdrawn instead, unless the committed catalog describes what it
	/// wrote ([`Self::committed_catalog_holds_this_build`]). The catalog is read
	/// only for a generation this builder still owns and has not ended —
	/// published `Online`, or left `Building` or `Closing` by a builder that
	/// stopped on a transient error — and no later than `catalog_deadline`; a
	/// read that does not answer in time withdraws. A statement that does not
	/// commit leaves the catalog on the definition from before it, which need
	/// not read the layout the build wrote: an index still at an earlier format
	/// version reads a doc-ID layout of its own, not the shared one a rebuild at
	/// the current format writes, and a full-text build handed its statement's
	/// analyzer indexed terms the committed analyzer need not produce. A
	/// generation kept `Online` would report `ready` and answer from data its
	/// definition cannot find, and one left `Building` would be carried on by
	/// the resume scan from checkpoints written for a definition that never
	/// committed. Withdrawn, the generation moves to `Error`, reported
	/// `aborted`, so no query reads it and the resume scan leaves it, and no
	/// compaction request is staged for it; writes to the table are still
	/// admitted, and the next `REBUILD INDEX` rescans the table. Ownership is
	/// still held at that point, so the withdrawal is fenced like every other
	/// owned transition. A catalog that does describe the build is served by it
	/// as well, and the generation is released as any other.
	///
	/// A withdrawal retries conflicts until it commits
	/// ([`Self::try_withdraw_build`]). A release is best-effort by design: a
	/// build that reaches here has already published, so failing to release
	/// must not fail the statement waiting on it. A
	/// release that does not commit leaves the owner set and queues nothing.
	/// Nothing heartbeats an owner whose release failed, so once its lease has
	/// lapsed a compaction pass or the resume scan clears it, where the store
	/// allows ([`super::state::lapsed_online_owners_clear`]); until then, or on a
	/// store that does not allow it until the next build rewrites `!bs`, it keeps
	/// a vector index out of compaction. Any other index is compacted under a
	/// live owner, from the request a pass that could not see its definition
	/// kept, at the next drain. Either way readers merge the backlog meanwhile.
	pub(super) async fn release_build_ownership(
		&self,
		generation: BuildGeneration,
		catalog_deadline: Option<Instant>,
	) {
		match self.rollback.load(Ordering::Acquire) {
			ROLLBACK_WITHDRAW => {
				// The rollback's own read found the catalog not describing the
				// build: withdraw without reading it again. A no-op for a
				// generation this builder no longer owns or has ended.
				self.withdraw_build(generation, catalog_deadline).await;
				return;
			}
			ROLLBACK_UNDECIDED => {}
			_ => {
				self.release_owned(generation).await;
				return;
			}
		}
		{
			// Whether this builder still owns a generation it has not ended,
			// the only kind that has anything to decide. A state that cannot be
			// read is decided as one that might be: the withdrawal below is a
			// no-op for a generation this builder does not own.
			let live = match self.read_durable_build_state().await {
				Ok(Some(state)) => {
					state.generation == generation
						&& state.owner == Some(self.owner)
						&& state.phase != IndexBuildPhase::Error
				}
				Ok(None) => false,
				Err(err) => {
					warn!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						error = %err,
						"could not read the build state of an index whose statement did not \
						 commit; withdrawing its build"
					);
					true
				}
			};
			if !live {
				return;
			}
			if !self.committed_catalog_holds_this_build(catalog_deadline).await {
				self.withdraw_build(generation, catalog_deadline).await;
				return;
			}
		}
		self.release_owned(generation).await;
	}

	/// The ordinary release of [`Self::release_build_ownership`], logging a
	/// failure.
	async fn release_owned(&self, generation: BuildGeneration) {
		// A build left to passes that has not published is handed back for them to claim, and
		// wakes what drives them; one that has published is released as any other.
		if self.to_passes {
			match self.hand_back(generation).await {
				Ok(true) => {
					self.tf.triggers().index_build.notify_one();
					return;
				}
				Ok(false) => {}
				Err(err) => warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					error = %err,
					"could not hand an index build back; a pass claims it once its owner lease lapses"
				),
			}
		}
		if let Err(err) = self.try_release_build_ownership(generation).await {
			warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"could not release durable build ownership; compaction skips a vector \
				 index until the ownership lapses and is cleared, and any other index \
				 until its next write"
			);
		}
	}

	/// Withdraw `generation`, as [`Self::release_build_ownership`] does for a
	/// build whose statement did not commit, retrying conflicts until
	/// `deadline` ([`Self::try_withdraw_build`]), and logging the outcome.
	async fn withdraw_build(&self, generation: BuildGeneration, deadline: Option<Instant>) {
		match self.try_withdraw_build(generation, deadline).await {
			Ok(Some(_)) => warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				"withdrew an index build whose statement did not commit, as the committed \
				 definition does not read what it built; run `REBUILD INDEX` to rebuild the index"
			),
			Ok(None) => {}
			Err(err) => warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"could not withdraw an index build whose statement did not commit; it is \
				 withdrawn once its owner's lease lapses, or run `REBUILD INDEX` to rebuild the \
				 index against its committed definition"
			),
		}
	}

	/// The transaction behind [`Self::release_build_ownership`]: a best-effort
	/// transition that clears the owner of an `Online` generation and stages the
	/// compaction request beside it, so both commit or neither does.
	async fn try_release_build_ownership(&self, generation: BuildGeneration) -> Result<()> {
		let request = self.ix.index.queues_compaction().then(|| self.ikb.clone());
		let node_id = self.ctx.node_id();
		self.transition_owned_build_state(
			generation,
			TransitionMode::BestEffort,
			false,
			|state| {
				if state.phase != IndexBuildPhase::Online {
					return false;
				}
				state.owner = None;
				true
			},
			// A kind that queues no compaction work stages nothing. The staged
			// future may borrow only the attempt's transaction, so it owns its
			// handle on the index key base: a reference count, not a copy.
			|tx| {
				let ikb = request.clone()?;
				Some(Box::pin(Self::requeue_compaction_with_release(tx, ikb, node_id)))
			},
		)
		.await?;
		Ok(())
	}

	/// The transaction that withdraws a generation in place of its release
	/// ([`Self::release_build_ownership`]): a transition that moves a
	/// generation this builder still owns — published `Online`, or left
	/// `Building` or `Closing` by a builder that stopped on a transient error —
	/// to `Error`, reported `aborted`, and clears its owner. Answers the state
	/// it wrote, or `None` where it wrote nothing.
	///
	/// [`TransitionMode::Persistent`]: a conflict is retried until the
	/// withdrawal commits, the generation is no longer this builder's, or the
	/// store fails otherwise, as it does when it shuts down. Recovery from the
	/// generation's recorded basis would withdraw it too, but only once its
	/// owner's lease has lapsed ([`super::state::clear_lapsed_online_owner`]),
	/// and every reader meanwhile finds it `ready` under a definition that
	/// does not read it; so the withdrawal lands here, at the release, rather
	/// than waiting a lease for recovery.
	///
	/// With a `deadline` — the close's allowance, when a statement's rollback
	/// performs the release — no retry starts past it, so a close is never held
	/// longer than that allowance. A withdrawal given up there leaves the
	/// generation as it found it, owned and published; that is safe because
	/// its build state records the basis it was built against
	/// ([`IndexBuildState::basis`]), so lapsed-owner recovery withdraws it once
	/// its owner's lease lapses. Without one, as the builder, its keeper and a
	/// transient exit release, the withdrawal retries without limit.
	async fn try_withdraw_build(
		&self,
		generation: BuildGeneration,
		deadline: Option<Instant>,
	) -> Result<Option<IndexBuildState>> {
		self.transition_owned_build_state(
			generation,
			TransitionMode::Persistent(deadline),
			false,
			|state| {
				if state.phase == IndexBuildPhase::Error {
					return false;
				}
				state.phase = IndexBuildPhase::Error;
				state.owner = None;
				state.error = None;
				state.report_status = Some(IndexBuildReportStatus::Aborted);
				true
			},
			|_| {
				// Stands in for the datastore shutting down under the
				// withdrawal, which no retry outlasts.
				#[cfg(test)]
				if let Err(err) = maybe_inject_non_retryable_error(
					NonRetryableErrorSite::ConcurrentIndexWithdrawalShutdown,
					self.ctx.node_id(),
				) {
					return Some(Box::pin(std::future::ready(Err(err))));
				}
				None
			},
		)
		.await
	}

	/// Stage the compaction request a release owes the index `ikb` names, in the
	/// transaction that commits the release. Only a kind whose writes queue
	/// compaction work ([`Index::queues_compaction`]) is owed one.
	///
	/// Owed by every kind whose writes queue compaction work, because a pass
	/// may have been turned away from the index before the release: a vector
	/// index's by the ownership fence, which holds until the release commits,
	/// and a full-text or count index's because the statement's definition was
	/// invisible to other sessions. Such a pass keeps one of the index's
	/// requests queued, so no request is lost, but keeping it writes nothing
	/// and wakes nothing, so what the build left would wait for the next drain.
	/// The release's request, and the commit that carries it, wake the
	/// compactor as soon as the condition has ended. Nothing records whether a
	/// pass was turned away, so the request is staged whenever the kind queues
	/// compaction at all; one with nothing behind it costs a pass over an empty
	/// plan.
	///
	/// Staged in the release's own transaction because a compaction pass reads
	/// the queue before it reads `!bs`. Committed together, a pass that sees the
	/// request also sees the release, and neither can commit without the other.
	/// The commit wakes the compactor. B-tree kinds write final entries and are
	/// owed nothing.
	async fn requeue_compaction_with_release(
		tx: &Transaction,
		ikb: IndexKeyBase,
		node_id: Uuid,
	) -> Result<()> {
		Self::stage_compaction_request(tx, ikb, node_id).await?;
		// Stand in for a release commit that conflicts, and for one that fails
		// outright. A real failed commit leaves the transaction finished with its
		// writes discarded, as the transition does with an error raised here.
		#[cfg(test)]
		maybe_inject_retryable_conflict(
			RetryableConflictSite::ConcurrentIndexOwnershipRelease,
			node_id,
		)?;
		#[cfg(test)]
		maybe_inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexOwnershipReleaseCommit,
			node_id,
		)?;
		Ok(())
	}

	/// Stage in `tx` the compaction request a release of the index `ikb` names
	/// owes, and wake the compactor when `tx` commits (see
	/// [`Self::requeue_compaction_with_release`] for why a release owes it).
	async fn stage_compaction_request(
		tx: &Transaction,
		ikb: IndexKeyBase,
		node_id: Uuid,
	) -> Result<()> {
		tx.set_key(&ikb.new_ic_key(node_id), &()).await?;
		tx.trigger_index_compaction();
		Ok(())
	}

	/// Publish a durable build error if this builder still owns the generation
	/// and has not published it (see [`Self::mark_durable_ended`]).
	pub(super) async fn mark_durable_error(
		&self,
		generation: BuildGeneration,
		error: String,
	) -> Result<()> {
		self.mark_durable_ended(generation, IndexBuildReportStatus::Error, Some(error)).await
	}

	/// Publish an aborted user-facing status if this builder still owns the
	/// generation and has not published it (see [`Self::mark_durable_ended`]).
	async fn mark_durable_aborted(&self, generation: BuildGeneration) -> Result<()> {
		self.mark_durable_ended(generation, IndexBuildReportStatus::Aborted, None).await
	}

	/// End this builder's generation in `Error`, reporting `status` and
	/// `error`, if the builder still owns the generation and has not published
	/// it.
	///
	/// A generation already published `Online` is left `Online`. Its index is
	/// complete and queryable from the publish on: what a build does after it
	/// only folds the writes queued during the build into the index, and
	/// readers see those writes until background compaction folds them once the
	/// build releases its ownership. Demoting the generation would withdraw a
	/// complete index from every reader over the failure of that follow-up
	/// work, and a generation in `Error` is only ever rebuilt. A published
	/// generation that its statement's rollback leaves without a definition to
	/// read it is withdrawn by the release instead
	/// ([`Self::release_build_ownership`]).
	///
	/// Runs after the build has already stopped, and its callers do not act on
	/// the outcome, so it is a [`TransitionMode::BestEffort`] transition: a
	/// missing `!bs` or a generation this builder no longer owns is a no-op,
	/// and a conflict is retried at most [`BEST_EFFORT_TRANSITION_RETRIES`]
	/// times rather than forever. A generation it gives up on keeps its owner
	/// and phase; one still `Building` or `Closing` is adopted by the resume
	/// scan once the owner's lease expires.
	async fn mark_durable_ended(
		&self,
		generation: BuildGeneration,
		status: IndexBuildReportStatus,
		error: Option<String>,
	) -> Result<()> {
		// Hold a stopped builder before it reads the build state, for as long as
		// a test keeps the site armed, so the outcome the statement waiting on
		// the build hears is the one the test arranged.
		#[cfg(test)]
		while let Err(err) = maybe_inject_retryable_conflict(
			RetryableConflictSite::ConcurrentIndexGenerationEnd,
			self.ctx.node_id(),
		) {
			if !self
				.retryable_conflict(
					&err,
					"transient conflict ending the build generation, retrying",
				)
				.await
			{
				return Err(err);
			}
		}
		self.transition_owned_build_state(
			generation,
			TransitionMode::BestEffort,
			false,
			|state| {
				if state.phase == IndexBuildPhase::Online {
					warn!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						?status,
						?error,
						"index build stopped after publishing its index; the index stays online"
					);
					return false;
				}
				state.phase = IndexBuildPhase::Error;
				state.owner = None;
				state.error.clone_from(&error);
				state.report_status = Some(status);
				true
			},
			no_staged_writes,
		)
		.await?;
		Ok(())
	}

	/// Heartbeat the build owner inside an indexing transaction.
	///
	/// The heartbeat is written with the same transaction that applies index
	/// data. That makes lease renewal and batch visibility move together, and it
	/// prevents an expired old owner from continuing after a takeover has fenced
	/// it out.
	///
	/// Call this before other writes in the transaction so the durable-state read
	/// is independent from transaction-local range tombstones, while the
	/// heartbeat still commits atomically with the batch.
	pub(super) async fn maintain_build_ownership(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		allowed: &[IndexBuildPhase],
	) -> Result<()> {
		let state_key = self.ikb.new_bs_key();
		let Some(current) = tx.get_key(&state_key, None).await? else {
			return Err(DatastoreError::CorruptedIndex(
				"Index build state is missing during ownership heartbeat",
			)
			.into());
		};
		if current.generation != generation
			|| current.owner != Some(self.owner)
			|| !allowed.contains(&current.phase)
		{
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		let mut next = current.clone();
		let now = Utc::now();
		next.updated_at = now;
		next.owner_heartbeat_at = Some(now);
		tx.put_compare_key(&state_key, &next, Some(&current)).await?;
		Ok(())
	}

	/// The pause before retrying after a conflict: [`BUILD_CONFLICT_RETRY_MAX`]
	/// for a build no statement awaits. For one a statement awaits, a random
	/// time between half and all of a ceiling that starts at
	/// [`BUILD_CONFLICT_RETRY_MIN`] and doubles with each conflict since this
	/// build last committed, up to [`BUILD_CONFLICT_RETRY_MAX`]; the randomness
	/// keeps awaited builds that conflict with one another from retrying in
	/// step.
	fn conflict_pause(&self) -> Duration {
		use rand::Rng as _;
		if !self.awaited {
			return BUILD_CONFLICT_RETRY_MAX;
		}
		let streak = self.conflict_streak.fetch_add(1, Ordering::Relaxed).min(16);
		let ceiling =
			BUILD_CONFLICT_RETRY_MIN.saturating_mul(1 << streak).min(BUILD_CONFLICT_RETRY_MAX);
		rand::rng().random_range(ceiling / 2..=ceiling)
	}

	/// Commit `tx`, a transaction of this build's, ending its run of conflicts
	/// when the commit succeeds ([`Self::conflict_pause`]).
	pub(super) async fn commit_build_tx(&self, tx: &Transaction) -> Result<()> {
		let res = tx.commit().await;
		if res.is_ok() {
			self.note_committed();
		}
		res
	}

	/// Note a commit of this build's, which ends its run of conflicts
	/// ([`Self::conflict_pause`]).
	pub(super) fn note_committed(&self) {
		self.conflict_streak.store(0, Ordering::Relaxed);
	}

	/// Whether `err` is a retryable conflict to retry, after the pause before
	/// the retry. A build whose statement is stopping it
	/// ([`Self::statement_stopping`]) retries nothing: the conflict ends its run,
	/// which hands it back rather than failing it ([`IndexBuilder::run_claimed`]).
	async fn retryable_conflict(&self, err: &anyhow::Error, action: &str) -> bool {
		if is_retryable_transaction_conflict(err) {
			if self.statement_stopping() {
				return false;
			}
			debug!(
				target: "surrealdb::core::kvs::index",
				index = %self.ix.name,
				table = %self.ix.table_name,
				action,
				error = %err,
				"retryable conflict during concurrent index build, retrying"
			);
			sleep(self.conflict_pause()).await;
			true
		} else {
			false
		}
	}

	pub(super) async fn cancel_and_retryable_conflict(
		&self,
		tx: &Transaction,
		err: &anyhow::Error,
		action: &str,
	) -> bool {
		let _ = tx.cancel().await;
		self.retryable_conflict(err, action).await
	}

	pub(super) async fn commit_and_retryable_conflict(
		&self,
		tx: &Transaction,
		action: &str,
	) -> Result<bool> {
		match tx.commit().await {
			Ok(()) => {
				self.note_committed();
				Ok(false)
			}
			Err(err) => {
				if self.cancel_and_retryable_conflict(tx, &err, action).await {
					Ok(true)
				} else {
					Err(err)
				}
			}
		}
	}

	pub(super) async fn new_read_tx(&self) -> Result<Transaction> {
		self.tf.transaction(TransactionType::Read, self.ctx.try_get_sequences()?.clone()).await
	}

	/// A write transaction of this build's own, outside any batch context.
	///
	/// Refused with the storage engine's shutdown error once the datastore has
	/// begun to shut down ([`Self::shutting_down`]), which the build reports as
	/// an interruption that leaves it to its lease.
	pub(super) async fn new_write_tx(&self) -> Result<Transaction> {
		self.refuse_writes_after_shutdown()?;
		self.tf.transaction(TransactionType::Write, self.ctx.try_get_sequences()?.clone()).await
	}

	/// Fails with the storage engine's shutdown error once this build's
	/// datastore has begun to shut down.
	fn refuse_writes_after_shutdown(&self) -> Result<()> {
		if self.shutting_down.load(Ordering::Acquire) {
			return Err(anyhow::Error::new(crate::kvs::err::Error::Shutdown));
		}
		Ok(())
	}

	/// A read transaction of this build's own, in a context batch work can
	/// evaluate the index's columns in.
	#[cfg_attr(all(target_family = "wasm", target_os = "unknown"), allow(dead_code))]
	pub(super) async fn new_read_tx_ctx(&self) -> Result<FrozenContext> {
		let tx = self
			.tf
			.transaction(TransactionType::Read, self.ctx.try_get_sequences()?.clone())
			.await?
			.into();
		let mut ctx = Context::new_child(&self.ctx);
		ctx.set_transaction(tx);
		ctx.set_index_places(self.places());
		ctx.set_memory(self.memory.as_ref().map(MemoryHost::query));
		Ok(ctx.freeze())
	}

	/// Creates a child context for one batch of the build, backed by a write
	/// transaction and holding a query memory of the batch's own (see
	/// [`Building::memory`]). Refused once the datastore has begun to shut down,
	/// as [`Self::new_write_tx`] is.
	pub(super) async fn new_write_tx_ctx(&self) -> Result<FrozenContext> {
		self.refuse_writes_after_shutdown()?;
		let tx = self
			.tf
			.transaction(TransactionType::Write, self.ctx.try_get_sequences()?.clone())
			.await?
			.into();
		let mut ctx = Context::new_child(&self.ctx);
		ctx.set_transaction(tx);
		ctx.set_index_places(self.places());
		ctx.set_memory(self.memory.as_ref().map(MemoryHost::query));
		Ok(ctx.freeze())
	}

	/// Runs the initial-batch sink's post-scan pass, one step per transaction, each committed
	/// with the build's ownership checked in it. Returns `false` when the build was aborted
	/// meanwhile.
	async fn run_post_scan_pass(&self, generation: BuildGeneration) -> Result<bool> {
		if !self.kind().has_post_scan_pass() {
			return Ok(true);
		}
		let mut from = Some(0);
		while let Some(start) = from {
			if self.is_aborted().await {
				return Ok(false);
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			if let Err(err) =
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building]).await
			{
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict maintaining build ownership, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			let sink = match self.kind().initial_batch_sink(&ctx, self.tb, &self.ikb).await {
				Ok(sink) => sink,
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			};
			let step = match sink.post_scan_step(&ctx, &self.aborted, start).await {
				Ok(step) => step,
				Err(err) => {
					if self.is_aborted().await {
						let _ = tx.cancel().await;
						return Ok(false);
					}
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict in the post-scan pass, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			};
			let Some(frame) = step.frame else {
				tx.cancel().await?;
				from = step.next;
				continue;
			};
			match self.commit_build_tx(&tx).await {
				Ok(()) => {
					frame.finish().await;
					from = step.next;
				}
				Err(err) => {
					// Aborting the frame cancels the transaction, once.
					frame.abort(&tx).await;
					if self
						.retryable_conflict(
							&err,
							"transient conflict committing the post-scan pass, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			}
		}
		Ok(true)
	}

	pub(super) async fn check_prepare_remove_with_tx(
		&self,
		last_prepare_remove_check: &mut Instant,
		tx: &Transaction,
	) -> Result<()> {
		if last_prepare_remove_check.elapsed() < Duration::from_secs(5) {
			return Ok(());
		};
		// Check the index has not been marked for removal.
		//
		// `ALTER INDEX … PREPARE REMOVE` sets the flag, in any transaction that
		// can read the definition, the one that defines it included. `tx` is one
		// of the build's own, so it sees only what has committed:
		//
		// - A `CONCURRENTLY` build, and a blocking one its statement awaits after committing, read
		//   nothing before the transaction that started them has closed
		//   (`Building::await_statement_closed`), so the first check sees a flag committed with the
		//   definition.
		// - A blocking `DEFINE INDEX` that awaits its build inside its transaction commits its
		//   definition only after the build returns, so every check misses. No flag can be set in
		//   the meantime: no other transaction can read the definition, and its own is waiting on
		//   the build.
		// - Any other miss is a definition that is gone, or was never committed: a removal fences
		//   the build by deleting `!bs`, and a rollback aborts it.
		//
		// A miss therefore tells the build nothing it must act on sooner, and is
		// throttled like a hit. A flag committed after a check is seen at most one
		// throttle interval later.
		if let Some(ix) = tx
			.get_tb_index(
				self.ix_key.ns,
				self.ix_key.db,
				&self.ix.table_name.clone(),
				&self.ix.name,
				None,
			)
			.await?
		{
			expect_not_prepare_remove(&ix)?;
		}
		*last_prepare_remove_check = Instant::now();
		Ok(())
	}

	pub(super) async fn check_prepare_remove(
		&self,
		last_prepare_remove_check: &mut Instant,
	) -> Result<()> {
		let tx = self.new_read_tx().await?;
		catch!(tx, self.check_prepare_remove_with_tx(last_prepare_remove_check, &tx).await);
		tx.cancel().await?;
		Ok(())
	}

	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(super) async fn run(self: &Arc<Self>) -> Result<()> {
		let Some(acquired) = self.acquire_build_state().await? else {
			return Ok(());
		};
		let generation = acquired.generation;
		let res = self.run_acquired(acquired).await;
		if res.is_ok() && self.aborted.load(Ordering::Acquire) {
			let _ = self.mark_durable_aborted(generation).await;
		}
		res
	}

	/// Execute a build after durable ownership has already been acquired.
	///
	/// Takeover from `Building` with an incomplete initial scan resumes the
	/// scan after the last per-batch checkpoint when one exists, and only
	/// restarts it (after cleaning index data for this generation) when the
	/// previous owner never committed a batch, or when resuming a COUNT build
	/// would leave an older node's marker unmet. Takeover from `Closing`, or
	/// from `Building` after `initial_complete`, skips the initial scan and
	/// only drains durable appendings and reservations.
	pub(super) async fn run_acquired(self: &Arc<Self>, acquired: AcquiredBuild) -> Result<()> {
		// The first batch checks whether the index was marked for removal, so a
		// build of an index decommissioned before it started never completes;
		// later batches check at most every few seconds.
		let mut last_prepare_remove_check =
			Instant::now().checked_sub(Duration::from_secs(5)).unwrap_or_else(Instant::now);
		let generation = acquired.generation;
		let scanning_initial =
			acquired.phase == IndexBuildPhase::Building && !acquired.initial_complete;
		// Resume from the durable per-batch checkpoint when one exists: every
		// record up to the cursor was committed atomically with the cursor, so
		// the partial index data is consistent and does not need to be wiped.
		let mut resume_cursor = if scanning_initial {
			acquired.initial_cursor.clone()
		} else {
			None
		};
		// A COUNT build's merge counts a gone record from an older node's marker
		// that sorts past the record's span only when a later span meets it. The
		// rebuild of the markers that starts a resumed scan would move such a
		// marker behind the cursor, so a build holding one rescans instead.
		if self.kind().baseline() == BaselinePolicy::MergeQueuedSpans
			&& let Some(cursor) = &resume_cursor
			&& self.resume_strands_an_older_marker(generation, cursor).await?
		{
			resume_cursor = None;
		}
		let restarting_initial_scan = scanning_initial && resume_cursor.is_none();
		// A restarted incomplete scan discards previous progress because it first
		// cleans index data. Resumed and checkpoint-continued builds keep the
		// durable counters that have already been reported for this generation.
		let mut initial_count = if restarting_initial_scan {
			0
		} else {
			acquired.initial_count
		};
		let mut updates_count = if restarting_initial_scan {
			0
		} else {
			acquired.updates_count
		};
		// A scan resumed from a checkpoint reads the rest of the table only once the write
		// transactions older than now have ended too: the checkpoint may come from an owner that
		// never waited for those older than the generation's commit, a release without the write
		// barrier, and a second wait after an owner that did only waits for writes that predate it.
		if scanning_initial
			&& !restarting_initial_scan
			&& !self.wait_for_older_writes(generation).await?
		{
			return Ok(());
		}

		if restarting_initial_scan {
			self.mark_durable_report(
				generation,
				IndexBuildReportStatus::Cleaning,
				None,
				None,
				None,
			)
			.await?;
			// A restarted scan can follow a crash between the generation flip
			// and the stale-queue wipe in `acquire_build_state`. Prior
			// generations' writers may then still hold live reservations
			// whose main-table writes must become visible before this scan
			// starts; re-run the drain and the wipe — both are cheap no-ops
			// when the previous owner already completed them.
			self.wait_for_prior_generation_reservations(generation).await?;
			self.wipe_stale_build_queues(generation).await?;
			// The wipe and the scan read only once every write transaction whose snapshot
			// predates the commit this generation builds against — the index's definition, or the
			// flip that installed the generation — has ended, on every live node: each has then
			// committed before the wipe's snapshot, or not at all, and every later one sees the
			// generation and queues for it (`crate::kvs::write_barrier`).
			if !self.wait_for_older_writes(generation).await? {
				return Ok(());
			}
			if !self.wipe_index_pages(generation).await? {
				return Ok(());
			}
			loop {
				if self.is_aborted().await {
					return Ok(());
				}
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				if let Err(err) = self
					.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
					.await
				{
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict maintaining build ownership, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				// A cleared B-tree, spatial or vector index records its value shapes from here on:
				// the scan and every write it admits go through the writer that
				// records them, or, for a batch a kind's sink takes, record them beside it.
				let cleaned =
					match crate::idx::wipe_index_data(&tx, &self.ikb, &self.ix.index).await {
						Ok(()) if self.kind().tracks_value_shapes() => {
							tx.track_index_shapes(
								self.ikb.ns(),
								self.ikb.db(),
								self.ikb.table(),
								self.ix.index_id,
								&crate::idx::index::expected_shapes(&self.ix),
							)
							.await
						}
						cleaned => cleaned,
					};
				if let Err(err) = cleaned {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict while cleaning existing index data, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				#[cfg(test)]
				if let Err(err) = maybe_inject_retryable_conflict(
					RetryableConflictSite::ConcurrentIndexInitialCleanup,
					self.ctx.node_id(),
				) {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict while cleaning existing index data, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				match self.commit_build_tx(&tx).await {
					Ok(()) => break,
					Err(err) => {
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict while cleaning existing index data, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
				}
			}
		}
		if scanning_initial {
			// The scan looks a record's `!bp` marker up by the key it builds for
			// the record, so every marker has to sit at that key before the scan
			// starts. A build taken over from an owner on a previous release keeps
			// that owner's generation and the markers it spelled differently;
			// they are rebuilt from the queued mutations, which carry the ids
			// losslessly.
			if !self.markers_need_no_rebuild(generation).await? {
				self.rebuild_primary_appendings().await?;
			}
			// First pass: index every record, resuming immediately after the
			// checkpointed record when the previous owner committed batches.
			let mut range = RecordPrefix {
				ns: self.ix_key.ns,
				db: self.ix_key.db,
				tb: Cow::Borrowed(self.ikb.table()),
			}
			.range()?;
			if let Some(cursor) = &resume_cursor {
				// Resume at the smallest key strictly greater than the cursor
				// record's own key, so the checkpointed record is not re-indexed.
				let checkpoint = RecordKey {
					ns: self.ix_key.ns,
					db: self.ix_key.db,
					tb: Cow::Borrowed(self.ikb.table()),
					id: Cow::Borrowed(cursor),
				}
				.encode_key()?;
				range = range.resume_after(&checkpoint, Direction::Forward);
			}
			let mut next = Some(range);
			let mut v1_appending_sentinel = false;
			// On resume the COUNT primary-appending catch-up also continues from
			// the checkpoint: records at or before the cursor were baselined
			// atomically with the batch that covered them, the rebuild above left
			// every marker at its record's own key, and a build whose checkpoint
			// left an older node's marker unmet was rescanned instead. A
			// checkpoint an owner on a previous release wrote covered its markers
			// in the index format's order, so a gone record whose two spellings
			// sort on opposite sides of the cursor can be counted twice or not at
			// all.
			let mut count_progress = (self.kind().baseline() == BaselinePolicy::MergeQueuedSpans)
				.then(|| CountPrimaryProgress::resuming_at(resume_cursor.clone()));
			// Batches are fetched by record count, so their memory footprint is
			// unbounded for large documents. The scan starts with a small probe
			// batch and then sizes each batch from the record sizes observed so
			// far, keeping a batch's raw record data around
			// `INDEXING_BATCH_MAX_BYTES` (small records keep using full
			// `INDEXING_SCAN_BATCH_SIZE` batches). The checkpoint protocol is
			// per-batch and does not depend on a fixed batch size.
			let mut scan_batch_size = INDEXING_PROBE_BATCH_SIZE;
			#[cfg(test)]
			let scan_batch_override = super::kind::test_sink::scan_batch(self.ctx.node_id());
			#[cfg(test)]
			if let Some(n) = scan_batch_override {
				scan_batch_size = n;
			}
			// Set the initial status.
			self.mark_durable_report(
				generation,
				IndexBuildReportStatus::Indexing,
				Some(initial_count),
				Some(0),
				None,
			)
			.await?;

			// A kind whose batches commit in any order indexes them on several
			// tasks; the serial loop below then finds nothing left to scan.
			#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
			if count_progress.is_none() {
				let tasks = self.initial_scan_tasks();
				if tasks > 1
					&& let Some(range) = next.take()
					&& !self
						.scan_in_parallel(
							generation,
							range,
							&mut initial_count,
							&mut last_prepare_remove_check,
							tasks,
							scan_batch_size,
						)
						.await?
				{
					return Ok(());
				}
			}
			while let Some(rng) = next {
				if self.is_aborted().await {
					return Ok(());
				}
				self.is_beyond_threshold(None)?;
				let batch = {
					let tx = self.new_read_tx().await?;
					// Check if the index has been marked for removal
					catch!(
						tx,
						self.check_prepare_remove_with_tx(&mut last_prepare_remove_check, &tx)
							.await
					);
					// Get the next batch of records.
					let res = catch!(
						tx,
						tx.batch_keys_vals_raw(rng.clone(), scan_batch_size, None).await
					);
					tx.cancel().await?;
					res
				};
				// Set the next scan range: a full page resumes just after the last
				// key it returned, while a short page ends the scan.
				next = batch
					.next
					.and(batch.result.last())
					.map(|(last, _)| rng.clone().resume_after(last, Direction::Forward));
				// Check whether any records remain.
				if batch.result.is_empty() {
					// If not, initial indexing is complete.
					break;
				}
				self.spend_batch()?;
				// Size the next batch from the average record size seen in this
				// one, so the scan converges on the byte budget within one batch.
				{
					let bytes: usize = batch.result.iter().map(|(k, v)| k.len() + v.len()).sum();
					let avg = (bytes / batch.result.len()).max(1);
					scan_batch_size = (INDEXING_BATCH_MAX_BYTES / avg)
						.clamp(1, INDEXING_SCAN_BATCH_SIZE as usize) as u32;
					#[cfg(test)]
					if let Some(n) = scan_batch_override {
						scan_batch_size = n;
					}
				}
				// Create a new context with a write transaction.
				{
					let values = batch.result;
					if values.is_empty() {
						// Unreachable: emptiness was checked above.
						break;
					}
					let (indexed, consumed) = loop {
						if self.is_aborted().await {
							return Ok(());
						}
						let ctx = self.new_write_tx_ctx().await?;
						let tx = ctx.tx();
						// The merge's progress for this attempt, kept only once
						// the batch commits.
						let mut attempt = count_progress.clone();
						if let Err(err) = self
							.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
							.await
						{
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict maintaining build ownership, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
						// Where this kind's initial batch goes. Resolved per attempt: a
						// sink frame lives in, and ends with, its attempt's transaction.
						let sink =
							match self.kind().initial_batch_sink(&ctx, self.tb, &self.ikb).await {
								Ok(sink) => sink,
								Err(err) => {
									let _ = tx.cancel().await;
									return Err(err);
								}
							};
						// Index the batch.
						let InitialBatch {
							indexed,
							consumed,
							frame,
						} = match self
							.index_initial_batch(
								&ctx,
								&tx,
								&values,
								initial_count,
								&mut v1_appending_sentinel,
								&mut attempt,
								&sink,
							)
							.await
						{
							Ok(batch) => batch,
							Err(err) => {
								// A sink frame stopped by the build's abort fails its batch;
								// the build ends as an aborted one, not a failed one.
								if self.is_aborted().await {
									let _ = tx.cancel().await;
									return Ok(());
								}
								if self
									.cancel_and_retryable_conflict(
										&tx,
										&err,
										"transient conflict in initial index batch, retrying",
									)
									.await
								{
									continue;
								}
								return Err(err);
							}
						};
						// An abort observed inside `index_initial_batch` truncates
						// the batch, so its checkpoint would cover records that were
						// never indexed. Discard the transaction instead. The abort
						// flag is sticky, so this re-check cannot miss a mid-batch
						// abort.
						if self.is_aborted().await {
							match frame {
								Some(frame) => frame.abort(&tx).await,
								None => tx.cancel().await?,
							}
							return Ok(());
						}
						// The continuation checkpoint, committed with the batch it
						// covers so a takeover resumes the scan here instead of
						// wiping and rescanning from the start: the last record the
						// batch indexed, which a sink may have cut short of the
						// batch's end.
						let committed: Result<()> = async {
							let (last_key, _) = &values[consumed - 1];
							let batch_cursor = RecordKey::decode_key(last_key)?.id.into_owned();
							self.checkpoint_initial_scan(
								&tx,
								generation,
								&batch_cursor,
								initial_count + indexed,
							)
							.await?;
							#[cfg(test)]
							super::kind::test_sink::maybe_hold_batch(self.ctx.node_id())?;
							#[cfg(test)]
							maybe_inject_initial_batch_commit_error(self.ctx.node_id())?;
							// Without a sync of its own: the commit that completes the scan
							// syncs, and makes every batch before it durable with it. A crash
							// loses at most the batches after the last synced commit, whose
							// checkpoints go with them, so the scan resumes before them.
							tx.defer_sync();
							tx.commit().await
						}
						.await;
						match committed {
							Ok(()) => {
								self.note_committed();
								if let Some(frame) = frame {
									frame.finish().await;
								}
								#[cfg(test)]
								super::kind::test_sink::record_commit(self.ctx.node_id());
								count_progress = attempt;
								break (indexed, consumed);
							}
							Err(err) => {
								// The frame is undone, and its lock released, before the
								// transaction is cancelled.
								let action =
									"transient conflict on initial index batch commit, retrying";
								let retry = match frame {
									Some(frame) => {
										frame.abort(&tx).await;
										self.retryable_conflict(&err, action).await
									}
									None => {
										self.cancel_and_retryable_conflict(&tx, &err, action).await
									}
								};
								if retry {
									continue;
								}
								return Err(err);
							}
						}
					};
					// A sink that cut the batch leaves the rest of it to the next
					// transaction, which starts just after the last record indexed.
					if consumed < values.len() {
						next = Some(rng.resume_after(&values[consumed - 1].0, Direction::Forward));
					}
					// The batch's checkpoint committed the `initial` progress
					// counter with it, so no separate report is written.
					initial_count += indexed;
				}
			}
			if count_progress.is_some() {
				let indexed = loop {
					if self.is_aborted().await {
						return Ok(());
					}
					let ctx = self.new_write_tx_ctx().await?;
					let tx = ctx.tx();
					// Each attempt at the tail starts from the scan's progress.
					let mut attempt = count_progress.clone();
					if let Err(err) = self
						.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
						.await
					{
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict maintaining build ownership, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
					let indexed = match self
						.index_remaining_count_primary_appendings(
							&ctx,
							&tx,
							&mut attempt,
							initial_count,
						)
						.await
					{
						Ok(indexed) => indexed,
						Err(err) => {
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict in initial count appending range, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
					};
					// An abort observed inside the tail pass truncates the
					// `!bp` catch-up. Discard the transaction instead of
					// committing a completion marker over baselines that were
					// never written.
					if self.is_aborted().await {
						tx.cancel().await?;
						return Ok(());
					}
					// The tail baselines are not idempotent, so the scan must
					// be marked complete atomically with them: a takeover that
					// still saw "incomplete, resume after cursor" would replay
					// the same `!bp` old states and double-count them.
					if let Err(err) =
						self.complete_initial_scan(&tx, generation, initial_count + indexed).await
					{
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict completing the initial scan, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
					match self.commit_build_tx(&tx).await {
						Ok(()) => {
							#[cfg(test)]
							maybe_inject_non_retryable_error(
								NonRetryableErrorSite::ConcurrentIndexCountTailCommitted,
								self.ctx.node_id(),
							)?;
							break indexed;
						}
						Err(err) => {
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict on initial count appending commit, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
					}
				};
				initial_count += indexed;
			} else {
				// The sink's pass over what the scan fed it, before the scan
				// counts as complete: a build resumed before then scans on from
				// its checkpoint and runs the pass again from the start.
				if !self.run_post_scan_pass(generation).await? {
					return Ok(());
				}
				// Mark initial build as complete before entering the appending
				// phase. COUNT builds completed above, atomically with the
				// tail-pass transaction.
				self.mark_durable_initial_complete(generation).await?;
			}
		}
		// First replay pass: catch up with writes that were admitted while the
		// initial scan was running. The build is still in `Building`, so new
		// writers may continue to reserve tickets, and the pass returns once it
		// has caught up with them rather than once they pause; the passes after
		// `Closing` replay whatever they committed meanwhile. A takeover from
		// `Closing` has nothing left to admit and drains outright.
		let first_pass = if acquired.phase == IndexBuildPhase::Building {
			ReplayUntil::CaughtUp
		} else {
			ReplayUntil::Drained
		};
		self.mark_durable_report(
			generation,
			IndexBuildReportStatus::Indexing,
			Some(initial_count),
			Some(0),
			Some(updates_count),
		)
		.await?;
		self.index_appending_loop(
			first_pass,
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		if acquired.phase == IndexBuildPhase::Building {
			self.mark_durable_closing(generation).await?;
		}
		// Second replay pass: after `Closing`, no new admissions are created, but
		// writers that already reserved tickets may still be committing their
		// durable appendings.
		self.index_appending_loop(
			ReplayUntil::Drained,
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		self.wait_for_durable_reservations(generation, &mut last_prepare_remove_check).await?;
		// Final replay pass: reservations have cleared, so any remaining queued
		// appendings are the last work that can exist before publishing `Online`.
		self.index_appending_loop(
			ReplayUntil::Drained,
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		self.ensure_single_ticket_allocator(generation).await?;
		// Once the statement that started this build has closed, the publish
		// releases the ownership in its own transaction. Not for an index over
		// the table's doc-ID space: its builder runs the doc-ID reclaim sweep
		// after the publish, and releases only once the sweep has run, so no
		// compaction pass resolves doc-IDs through mappings the sweep is
		// reclaiming.
		//
		// The gate is read before the publish reads the abort: a statement that
		// rolls back aborts its build before its half arrives, so a publish that
		// would release with it declines, and one that does not release is
		// settled by whichever half arrives second
		// ([`Self::release_build_ownership`]).
		let release_with_online = !self.kind().defers_doc_ids()
			&& self.release_gate.load(Ordering::Acquire) & RELEASE_GATE_STATEMENT != 0;
		let published = self
			.mark_durable_online(generation, initial_count, updates_count, release_with_online)
			.await;
		#[cfg(test)]
		let published = published.and_then(|published| {
			if published {
				maybe_inject_non_retryable_error(
					NonRetryableErrorSite::ConcurrentIndexOnlineCommitAppliedUnknown,
					self.ctx.node_id(),
				)?;
			}
			Ok(published)
		});
		match published {
			Ok(true) => {}
			// Aborted before it published: its task ends the generation
			// `aborted`, which also clears its ownership.
			Ok(false) => return Ok(()),
			// A publish whose commit reported an error may still have applied,
			// and the durable state says which. One that did is carried on as
			// published: the generation stays `Online` whatever this builder
			// reports (see `mark_durable_ended`).
			Err(err) if self.published_despite(generation, release_with_online, &err).await => {}
			Err(err) => {
				// This builder writes nothing more. A release is a no-op for a
				// generation that did not go `Online`, and brings the ownership
				// down for one whose publish applied although the state could not
				// be read back.
				if self.arrive_at_release_gate(RELEASE_GATE_BUILDER) {
					self.release_build_ownership(generation, None).await;
				}
				return Err(err);
			}
		}
		self.online.store(true, Ordering::Release);
		// Drain the table's durable pending doc-ID reclaim markers, best-effort:
		// an error must not fail a build that already published successfully.
		// The markers are durable, so anything left behind is reclaimed by the
		// next doc-ID index build. The ownership kept for the sweep is
		// heartbeated while it runs, however long that is.
		let swept = if release_with_online {
			self.reclaim_deferred_doc_ids().await
		} else {
			self.while_owning(generation, self.reclaim_deferred_doc_ids()).await
		};
		if let Err(err) = swept {
			warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"deferred doc-ID reclaim sweep failed; leftover markers will be \
				 reclaimed by the next doc-ID index build on the table"
			);
		}
		// The build writes nothing of the index after `Online`: folding what
		// writers queued meanwhile is the materialiser's. The builder's half of
		// the gate arrives only after the `Online` commit, so a statement that
		// closes in between finds the index `Online` when it releases. A release
		// the publish already committed leaves nothing for the gate to do.
		if self.arrive_at_release_gate(RELEASE_GATE_BUILDER) && !release_with_online {
			self.release_build_ownership(generation, None).await;
		}
		Ok(())
	}

	/// Whether `txn` is the transaction of the statement that started this
	/// build.
	pub(super) fn started_by(&self, txn: &Arc<Transaction>) -> bool {
		self.statement_tx
			.get()
			.is_some_and(|statement| std::ptr::eq(statement.as_ptr(), Arc::as_ptr(txn)))
	}

	/// Abort the current indexing process.
	pub(super) fn abort(&self) {
		self.aborted.store(true, Ordering::Release);
		self.statement_closed.notify_waiters();
	}

	/// Check if the indexing process should stop at the next check point.
	///
	/// Set by a user abort (`REMOVE INDEX`). A datastore shutdown does not use
	/// this: the commit coordinator's shutdown refuses the builder's commits
	/// before they apply, so the task exits on that error without any durable
	/// change and the build resumes after restart.
	pub(super) async fn is_aborted(&self) -> bool {
		self.aborted.load(Ordering::Acquire)
	}

	pub(super) fn is_beyond_threshold(&self, count: Option<usize>) -> Result<()> {
		if let Some(count) = count
			&& count % 100 != 0
		{
			return Ok(());
		}
		if ALLOC.is_beyond_threshold() {
			Err(anyhow::Error::new(DatastoreError::QueryBeyondMemoryThreshold))
		} else {
			Ok(())
		}
	}

	/// Whether the spawned task for this build has finished building.
	///
	/// Loads `Acquire` to pair with the `Release` store in
	/// [`BuildingFinishGuard`]: a caller that observes `true` also observes
	/// everything the task did before then, so this reads as "the builder has
	/// stopped writing index data and progress" without further reasoning about
	/// ordering. A published build's keeper may still be holding its ownership
	/// for an open statement ([`Self::keep_online_ownership`]); a later build of
	/// the index replaces that generation and stops the keeper.
	pub(super) fn is_finished(&self) -> bool {
		self.finished.load(Ordering::Acquire)
	}

	/// Wait until the spawned task for this build has finished building.
	///
	/// The task publishes its last write of index data and progress before
	/// then, so a caller that deletes this index's durable build state waits
	/// here first to make its delete the final write. The only writes that may
	/// follow are a published build's keeper's ([`Self::keep_online_ownership`]):
	/// owned transitions of `!bs`, each a compare-and-swap against the state it
	/// read, so one ordered after the delete writes nothing, and one racing it
	/// conflicts with it on `!bs`. Only a build reachable through
	/// [`IndexBuilder::indexes`] has a task; waiting on a [`Building`] that was
	/// never spawned never returns, so callers bound the wait with the drain's
	/// deadline.
	pub(super) async fn wait_finished(&self) {
		loop {
			// Register for the wake-up before re-checking the flag: the task can
			// finish between the two, and `notify_waiters` only wakes waiters
			// that are already registered.
			let notified = self.finished_notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_finished() {
				return;
			}
			notified.await;
		}
	}
}

/// Releases durable build ownership once the statement that publishes the
/// build's definition has closed its transaction.
///
/// Registered on both outcomes: a committed statement has made its definition
/// visible to every node, and a rolled-back one aborts any build still running
/// and releases the fence on one that already finished, withdrawing it if the
/// committed catalog does not describe what it built. See
/// [`Building::defer_ownership_release`].
pub(super) struct ReleaseBuildOwnership(IndexBuilding);

impl ReleaseBuildOwnership {
	/// Arrive at the release gate in the statement's place, and release if
	/// this arrival opens it, reading the committed catalog no later than
	/// `catalog_deadline` ([`Building::release_build_ownership`]).
	async fn release(self, catalog_deadline: Option<Instant>) {
		let open = self.0.arrive_at_release_gate(RELEASE_GATE_STATEMENT);
		self.0.statement_closed.notify_waiters();
		if open {
			let generation = self.0.build_generation.load(Ordering::Acquire);
			self.0.release_build_ownership(generation, catalog_deadline).await;
		}
	}

	/// The rollback half for `building`, as a statement that started it
	/// registers it.
	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(super) fn rollback_action(building: IndexBuilding) -> Box<dyn RollbackAction> {
		Box::new(Self(building))
	}
}

impl CommitAction for ReleaseBuildOwnership {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin((*self).release(None))
	}

	/// Ownership is released however the transaction ends: this is queued as
	/// a rollback action too, and a save point rollback inside a committing
	/// transaction must not leave it to neither.
	fn scope(&self) -> ActionScope {
		ActionScope::Transaction
	}
}

/// The rollback half: the statement did not commit, so the rollback is
/// recorded and the build aborted unless it goes on, before the statement's
/// half of the release gate arrives (see
/// [`Building::settle_uncommitted_statement`]). Registered for every build a
/// statement starts, a rebuild awaited after the commit included, so it is the
/// only hook that decides a build on rollback. Every catalog read it makes, to
/// settle a commit whose outcome is unknown or to decide a generation its
/// release finds published, ends by the close's allowance, measured from
/// `drain_started_at` ([`build_abort_deadline`]); one that runs out of time
/// aborts and withdraws.
impl RollbackAction for ReleaseBuildOwnership {
	fn run(
		self: Box<Self>,
		drain_started_at: Instant,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		Box::pin(async move {
			let deadline = Some(build_abort_deadline(drain_started_at));
			self.0.settle_uncommitted_statement(deadline).await;
			// A build left to passes has no task to end a generation the rollback aborted, so
			// the rollback records it, as a task would; one that goes on is handed back below.
			if self.0.to_passes && self.0.aborted.load(Ordering::Acquire) {
				let generation = self.0.build_generation.load(Ordering::Acquire);
				if generation != 0 {
					let _ = self.0.mark_durable_aborted(generation).await;
				}
			}
			// The statement that was to run the build itself may not have run it to its end, and
			// no task is left to arrive in its place; an arrival it made already counts once.
			if self.0.inline_run {
				self.0.arrive_at_release_gate(RELEASE_GATE_BUILDER);
			}
			(*self).release(deadline).await;
			Ok(())
		})
	}

	/// The commit-side twin is transaction-scoped too, so ownership is released
	/// exactly once, when the transaction ends.
	fn scope(&self) -> ActionScope {
		ActionScope::Transaction
	}
}

struct BuildingFinishGuard(IndexBuilding);

impl Drop for BuildingFinishGuard {
	fn drop(&mut self) {
		self.0.release_places();
		// `Release` pairs with the `Acquire` load in `Building::wait_finished`,
		// so a waiter woken by the notification observes the flag as set.
		self.0.finished.store(true, Ordering::Release);
		self.0.finished_notify.notify_waiters();
	}
}

/// Rejects an index that is staged for removal.
///
/// A definition with `prepare_remove` set is in the two-phase removal window;
/// building or replaying against it would resurrect index state the removal
/// is about to purge.
fn expect_not_prepare_remove(ix: &IndexDefinition) -> anyhow::Result<()> {
	if ix.prepare_remove {
		Err(anyhow::Error::new(crate::kvs::DatastoreError::IndexingBuildingCancelled {
			reason: "Prepare remove.".to_string(),
		}))
	} else {
		Ok(())
	}
}

/// Stop a process-local index builder once a schema retirement has committed.
///
/// The durable side of retirement — deleting `!bs`, the generation-scoped
/// queues, and the catalog entry — is staged in the schema transaction, and
/// the `!bs` delete is what fences the build. Every builder transaction that
/// writes index data brings `!bs` into its own conflict domain before it
/// writes: a build batch reads and compare-and-swaps it
/// ([`Building::maintain_build_ownership`]). The build writes nothing of the
/// index after `Online`; compaction, which the materialiser runs, takes a
/// locked read of the catalog definition in its write guard
/// ([`super::materialise::compaction_write_guard`]). Either way a transaction
/// that starts after the retirement cannot find the state it needs, and one
/// already in flight loses at commit. This abort is a courtesy that stops the
/// task at its next checkpoint rather than letting it discover the fence by
/// failing a batch, and carries no correctness weight.
///
/// The builder map is process memory while the retirement is durable state, so
/// aborting before the commit would stop a build that a rollback then leaves
/// valid.
pub(crate) struct AbortLocalBuild {
	builder: IndexBuilder,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl AbortLocalBuild {
	pub(crate) fn boxed(
		builder: IndexBuilder,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Box<dyn CommitAction> {
		Box::new(Self {
			builder,
			ns,
			db,
			tb,
			ix,
		})
	}
}

impl CommitAction for AbortLocalBuild {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(async move {
			self.builder.remove_index(self.ns, self.db, &self.tb, self.ix).await;
		})
	}

	/// Follows the removal or redefinition that queued it: a rollback that
	/// undoes that write leaves the index's build wanted.
	fn scope(&self) -> ActionScope {
		ActionScope::SavePoint
	}
}

/// Stop a process-local builder and delete the state it committed on its own.
///
/// Used by the rollback path, where no schema transaction ever committed a
/// fence: the build's `!bs` is still live and still owned, so the deletes below
/// must be ordered after the task's last write rather than relying on a
/// compare-and-swap the builder would lose.
struct IndexBuildCleanup {
	builder: IndexBuilder,
	tf: TransactionFactory,
	sequences: Sequences,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl IndexBuildCleanup {
	fn new(
		builder: IndexBuilder,
		tf: TransactionFactory,
		sequences: Sequences,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Self {
		Self {
			builder,
			tf,
			sequences,
			ns,
			db,
			tb,
			ix,
		}
	}

	async fn stop_builder(&self, abort_deadline: Instant) {
		self.builder
			.remove_index_and_wait(self.ns, self.db, &self.tb, self.ix, abort_deadline)
			.await;
	}

	/// Whether the catalog, as `tx` reads it, holds a definition of this index:
	/// the statement that started the build committed after all.
	async fn definition_committed(&self, tx: &Transaction) -> Result<bool> {
		Ok(tx
			.get_tb_index_by_id(self.ns, self.db, &self.tb, self.ix, None)
			.await?
			.is_some_and(|ix| ix.index_id == self.ix))
	}

	/// Whether the catalog holds a definition of this index, read in a
	/// transaction of its own.
	async fn definition_committed_now(&self) -> Result<bool> {
		let tx = self.tf.transaction(TransactionType::Read, self.sequences.clone()).await?;
		let committed = self.definition_committed(&tx).await;
		let _ = tx.cancel().await;
		committed
	}

	/// Delete the build's durable state, queues and index data in one
	/// transaction, unless the catalog holds the index's definition, which it
	/// reads first in that same transaction; answers whether it deleted.
	/// `EXPUNGE` does not reach this path: it is a policy carried by a removal
	/// statement, and the build this cleans belongs to a statement that never
	/// committed.
	async fn delete_once(&self) -> Result<bool> {
		let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;
		let ikb = IndexKeyBase::new(self.ns, self.db, self.tb.clone(), self.ix);
		let result: Result<bool> = async {
			if self.definition_committed(&tx).await? {
				// Nothing was written: the cancel's own outcome changes nothing.
				let _ = tx.cancel_bare().await;
				return Ok(false);
			}
			delete_durable_build_artifacts(&tx, &ikb, false).await?;
			let index_prefix = IdxRoot {
				ns: self.ns,
				db: self.db,
				tb: Cow::Borrowed(&self.tb),
				ix: self.ix,
			}
			.range()?;
			tx.delr(index_prefix).await?;
			tx.commit_bare().await?;
			Ok(true)
		}
		.await;
		match result {
			Ok(deleted) => Ok(deleted),
			Err(err) => {
				// The failure is what is reported, and nothing of this
				// transaction committed, so its cancel's outcome changes nothing.
				let _ = tx.cancel_bare().await;
				Err(err)
			}
		}
	}
}

/// Remove an index build whose catalog definition never committed.
///
/// `DEFINE INDEX` starts the builder while its schema transaction is still open,
/// so by the time that transaction is cancelled the builder may already have
/// committed build state and index data of its own, from separate transactions.
/// This deletes that provisional state, so a retry sees a clean slate.
///
/// A transaction runs its rollback actions on every failed commit, including
/// one whose outcome is unknown and which may have applied. So this acts only
/// on what the catalog says: while it holds the index's definition, the
/// statement committed, and the build is left to finish and publish as any
/// build of a committed definition does, its builder untouched. The catalog is
/// read before the builder is stopped, and again in the transaction that would
/// delete, so a definition that becomes readable in between is not deleted
/// from under. A definition with no build state would read as a ready index.
///
/// The deletes go through the transaction's ordinary write methods and so count
/// against the write-cardinality guard, unlike the reservation release that runs
/// beside it. Six writes is far below any limit an operator would set, and a
/// cleanup that silently bypassed the guard would be the stranger choice.
pub(crate) struct CleanUncommittedBuild {
	cleanup: IndexBuildCleanup,
}

impl CleanUncommittedBuild {
	pub(crate) fn boxed(
		builder: IndexBuilder,
		tf: TransactionFactory,
		sequences: Sequences,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Box<dyn RollbackAction> {
		Box::new(Self {
			cleanup: IndexBuildCleanup::new(builder, tf, sequences, ns, db, tb, ix),
		})
	}
}

impl RollbackAction for CleanUncommittedBuild {
	fn run(
		self: Box<Self>,
		drain_started_at: Instant,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		let abort_deadline = build_abort_deadline(drain_started_at);
		Box::pin(async move {
			if self.cleanup.definition_committed_now().await? {
				return Ok(());
			}
			// Provisional state must be deleted even when no process-local task is
			// present to stop.
			self.cleanup.stop_builder(abort_deadline).await;
			loop {
				match self.cleanup.delete_once().await {
					Ok(_) => return Ok(()),
					Err(err) if is_retryable_transaction_conflict(&err) => {
						debug!(
							error = %err,
							"retryable conflict while cleaning uncommitted index build, retrying"
						);
						sleep(INDEX_BUILD_CLEANUP_RETRY_SLEEP).await;
					}
					Err(err) => return Err(err),
				}
			}
		})
	}

	/// Compensates for the `DEFINE INDEX` that queued it: a save point rollback
	/// that undoes the definition cleans up the build there and then, since a
	/// transaction that goes on to commit would discard this unrun.
	fn scope(&self) -> ActionScope {
		ActionScope::SavePoint
	}
}

#[cfg(test)]
mod pending_range_compaction_tests {
	use super::*;

	/// A drained range is compacted once enough deletions have built up, then
	/// counts again from zero, and is not compacted twice within the interval.
	#[test]
	fn a_drained_range_is_compacted_after_enough_deletions_at_most_once_per_interval() {
		let start = Instant::now();
		let mut pending = PendingDeletes {
			deleted: PENDING_RANGE_COMPACTION_MIN_DELETES - 1,
			compacted: None,
		};
		assert!(!pending.take_due(start), "below the threshold");
		pending.deleted += 1;
		assert!(pending.take_due(start), "at the threshold");
		assert_eq!(pending.deleted, 0, "the count starts again");
		pending.deleted = 10 * PENDING_RANGE_COMPACTION_MIN_DELETES;
		assert!(!pending.take_due(start + Duration::from_secs(1)), "within the interval");
		assert!(pending.take_due(start + PENDING_RANGE_COMPACTION_INTERVAL), "after the interval");
	}

	/// An entry with nothing counted is idle once its last compaction is at
	/// least an interval old, and never while it counts deletions.
	#[test]
	fn an_entry_is_idle_only_with_nothing_counted_and_no_recent_compaction() {
		let start = Instant::now();
		assert!(PendingDeletes::default().is_idle(start), "a fresh entry");
		let counting = PendingDeletes {
			deleted: 1,
			compacted: None,
		};
		assert!(!counting.is_idle(start), "deletions are counted");
		let compacted = PendingDeletes {
			deleted: 0,
			compacted: Some(start),
		};
		assert!(!compacted.is_idle(start + Duration::from_secs(1)), "compacted recently");
		assert!(compacted.is_idle(start + PENDING_RANGE_COMPACTION_INTERVAL), "an interval ago");
	}
}
