//! The engine's background jobs, and the tasks that run them.
//!
//! A *job* is one unit of maintenance — draining a queue, sweeping a keyspace,
//! refreshing a heartbeat — and every job runs in bounded *passes* that report
//! a [`Progress`]. A *task* is a tokio task that decides when passes run. The
//! two are kept apart so the same jobs can be driven by the datastore's own
//! tasks ([`init`]) or, on a host that cannot run free-running tasks, by the
//! host itself ([`Datastore::run_maintenance_slice`]).

#![cfg_attr(not(feature = "maintenance-tasks"), allow(dead_code, unused_imports))]

use core::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use common::time::{Instant, MissedTickBehavior, sleep};
use futures::StreamExt;
use surrealdb_datastore::triggers::CommitTriggers;
use surrealdb_observe::{
	Outcome, TaskLeaseState, TaskPassEvent, TaskPassEventCtx, TaskPassEventSafe,
};
use surrealdb_types::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::err::{is_query_cancelled, is_query_timedout};
use crate::kvs::tasklease::LeaseCheckFailed;
use crate::kvs::{Datastore, LiveQueryEngine};
use crate::observe::process::RefreshClaim;
use crate::options::EngineOptions;

mod interval;

use self::interval::IntervalStream;

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
type Task = Pin<Box<dyn Future<Output = Result<(), common::rt::JoinError>> + Send + 'static>>;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
type Task = Pin<Box<()>>;

/// Spawns `fut` on the ambient runtime and returns a handle the caller can await
/// to observe the task finishing.
///
/// Awaiting the returned [`Task`] is meaningful on every target but browser
/// wasm. The browser executor yields no join handle, so there the task is
/// detached and the returned `Task` completes immediately without waiting for
/// `fut`.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn into_task<F>(fut: F) -> Task
where
	F: Future<Output = ()> + Send + 'static,
{
	Box::pin(common::rt::spawn(fut))
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn into_task<F>(fut: F) -> Task
where
	F: Future<Output = ()> + 'static,
{
	common::rt::spawn_detached(fut);
	Box::pin(())
}

const NODE_MEMBERSHIP_UPDATE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a trigger-driven index-compaction pass waits before running.
///
/// The compaction interval is the floor on how *stale* the queue may get; this
/// is the floor on how *often* a write may start a pass. It bounds the cost of
/// the wake-up path — most importantly the lease check on nodes that do not
/// hold the compaction lease, which returns almost immediately and would
/// otherwise spin for as long as writes keep arriving — and batches the commits
/// that land during the wait into a single pass.
const INDEX_COMPACTION_TRIGGER_DEBOUNCE: Duration = Duration::from_millis(250);

/// How long the graph fold task waits after a commit-time wake-up before
/// running, for the same batching reason as the compaction debounce above.
const GRAPH_FOLD_TRIGGER_DEBOUNCE: Duration = Duration::from_millis(250);

// --------------------------------------------------
// Jobs and passes
// --------------------------------------------------

/// Whether a pass left work behind.
///
/// This is the contract every job's pass reports against, and the one a host
/// that drives maintenance itself schedules on: `More` means running the job
/// again now would find work, `Done` means the next work arrives with the next
/// write or the next tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
	/// The pass stopped at its budget with work still queued.
	More,
	/// The pass found nothing more to do.
	Done,
}

/// What one pass of a job reports about itself.
#[derive(Clone, Copy, Debug)]
pub struct Pass {
	/// Whether work remained when the pass stopped.
	pub progress: Progress,
	/// Queue entries or work units the pass handled.
	pub items: u64,
	/// Age of the oldest queued entry when the pass started, for a job that
	/// drains a queue and found it non-empty: how long the head of the queue
	/// had waited.
	pub oldest_age: Option<Duration>,
	/// Queue entries the pass fetched but left to another processor holding
	/// them, for a job that drains a queue: they are that processor's to
	/// settle, and not part of this pass's work.
	pub held: u64,
	/// Queue entries the pass ran, for a job that drains a queue: the
	/// `items` it fetched less those it could not run — held by another
	/// processor, or written by a release this binary cannot decode — which
	/// stay in place and would be fetched again by a pass run at once.
	pub ran: u64,
	/// The pass's standing with respect to the job's lease.
	pub lease: TaskLeaseState,
}

impl Pass {
	/// A pass that found nothing more to do.
	pub const fn done(lease: TaskLeaseState) -> Self {
		Self {
			progress: Progress::Done,
			items: 0,
			oldest_age: None,
			held: 0,
			ran: 0,
			lease,
		}
	}

	/// A pass that did no work because another node holds the job's lease.
	pub const fn elsewhere() -> Self {
		Self::done(TaskLeaseState::Elsewhere)
	}

	/// Records a queued entry's age, keeping the oldest seen this pass.
	pub fn observe_age(&mut self, age: Duration) {
		self.oldest_age = self.oldest_age.max(Some(age));
	}

	/// Attaches this pass to the error that ended it, so the observer reports
	/// what the pass did before it failed; see [`PassFailure`]. A pass that
	/// fails stopped short of the end of its work, so it reports a backlog.
	pub(crate) fn failed(mut self, e: anyhow::Error) -> anyhow::Error {
		self.progress = Progress::More;
		e.context(PassFailure(self))
	}
}

/// A pass's report, carried as `anyhow` context by the error that ended the
/// pass.
///
/// A pass that fails has usually done something first: the batches before the
/// one that failed are settled, and the entries the failure left in place are
/// still queued. A job attaches the pass it had accumulated with
/// [`Pass::failed`], and [`observe_pass`] recovers it with `downcast_ref`, so
/// the observer reports those counts and the backlog rather than a pass that
/// did nothing. A failure with no pass attached is reported as one. The error
/// underneath — a lease check's, a cancellation's — still reads as itself.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PassFailure(pub(crate) Pass);

impl std::fmt::Display for PassFailure {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "the pass handled {} items before failing", self.0.items)
	}
}

/// Every background job the engine runs.
///
/// Each variant is one unit of maintenance with its own cadence, its own
/// bounded pass, and — where it works on cluster-wide state — its own lease.
/// How the jobs are spread over tasks is decided by [`init`], not here; a job
/// belongs to exactly one of [`Job::DEDICATED`], [`Job::MAINTENANCE`] or
/// [`Job::SWEEP`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Job {
	/// Refresh this node's row in the cluster membership.
	Heartbeat,
	/// Run the async events this node queued.
	EventProcessing,
	/// Adopt async events queued by archived nodes, or under the root-level
	/// queue by nodes running an older layout.
	EventAdoption,
	/// Compact the indexes the write path has queued.
	IndexCompaction,
	/// Fold graph adjacency keys into packed blocks.
	GraphFold,
	/// Rewrite the rows of the tables a migration queued, and verify the
	/// fields queued for verification.
	TableMigration,
	/// Deliver live-query notifications under the router engine.
	LiveQueryRouter,
	/// Archive cluster members whose heartbeat has gone stale.
	NodeExpire,
	/// Delete archived members and garbage-collect their live queries.
	NodeCleanup,
	/// Garbage-collect expired changefeed data.
	ChangefeedGc,
	/// Garbage-collect live-query events older than the retention window.
	LiveQueryGc,
	/// Destroy data left behind by `REMOVE NAMESPACE/DATABASE/INDEX`.
	ReclaimTombstones,
	/// Adopt index builds stranded by a crashed or expired owner node.
	ResumeIndexBuilds,
	/// Purge expired durable RPC sessions.
	RpcSessionGc,
	/// Advance the backend's MVCC garbage-collection safepoint.
	MvccGc,
	/// Resolve stale backend transactional locks.
	LockCleanup,
	/// Release the DiskANN writer leases this node no longer writes under.
	DiskannWriterLeases,
	/// Refresh the cached process/system utilisation metrics.
	SystemMetricsRefresh,
}

impl Job {
	/// Every variant. A new variant belongs here and in exactly one of
	/// [`Self::DEDICATED`], [`Self::MAINTENANCE`] or [`Self::SWEEP`].
	#[cfg(test)]
	const ALL: [Self; 18] = [
		Self::Heartbeat,
		Self::EventProcessing,
		Self::EventAdoption,
		Self::IndexCompaction,
		Self::GraphFold,
		Self::TableMigration,
		Self::LiveQueryRouter,
		Self::NodeExpire,
		Self::NodeCleanup,
		Self::ChangefeedGc,
		Self::LiveQueryGc,
		Self::ReclaimTombstones,
		Self::ResumeIndexBuilds,
		Self::RpcSessionGc,
		Self::MvccGc,
		Self::LockCleanup,
		Self::DiskannWriterLeases,
		Self::SystemMetricsRefresh,
	];

	/// The jobs that keep a task of their own; see [`init`] for why each does.
	const DEDICATED: [Self; 6] = [
		Self::Heartbeat,
		Self::EventProcessing,
		Self::IndexCompaction,
		Self::GraphFold,
		Self::TableMigration,
		Self::LiveQueryRouter,
	];

	/// The jobs whose cost per pass is bounded up front.
	///
	/// Each of these either reads a fixed number of catalog rows, or bounds its
	/// own pass explicitly: `changefeed_process` and `live_query_gc_process`
	/// each take at most one page of steps through the catalog per pass and
	/// delete at most one key budget from the databases they reach, however far
	/// the retention backlog has run ahead of them, the index-build resume scan takes at most one
	/// page of steps through the catalog per pass, checking at most an index per step,
	/// and node expiry and node cleanup each scan at most one slice of node rows per
	/// pass, cleanup deleting at most one slice of archived nodes' live
	/// queries besides, and the DiskANN writer-lease release touches only the
	/// leases this node holds. So they can share one task without delaying a
	/// peer by more than one short pass, and a job with more work than one
	/// pass allows is due again at once, behind any peer that is already
	/// overdue, rather than holding the task.
	const MAINTENANCE: [Self; 7] = [
		Self::SystemMetricsRefresh,
		Self::NodeExpire,
		Self::NodeCleanup,
		Self::ChangefeedGc,
		Self::LiveQueryGc,
		Self::ResumeIndexBuilds,
		Self::DiskannWriterLeases,
	];

	/// The jobs whose queue is nearly always empty but whose cost per pass is
	/// not bounded by catalog size, or whose pass is a remote call.
	///
	/// `purge_expired_rpc_sessions` pages the entire session keyspace, opening
	/// a write transaction for every expired entry, so one pass is arbitrarily
	/// long on a busy durable-session deployment. `reclaim_tombstones` bounds
	/// its own pass by a key budget and a walk budget, but the queue it drains
	/// is filled by `REMOVE` statements rather than by catalog size, and a
	/// large removal or a long queue takes many passes to finish. The event
	/// adoption sweep finds work only once a node has left, and what it runs is
	/// user-defined SurrealQL. The two maintenance jobs are calls into the storage
	/// cluster whose duration the engine does not control. None of them belongs
	/// alongside the jobs whose pass length follows the catalog; they share a
	/// task with each other instead.
	const SWEEP: [Self; 5] = [
		Self::ReclaimTombstones,
		Self::RpcSessionGc,
		Self::EventAdoption,
		Self::MvccGc,
		Self::LockCleanup,
	];

	/// Description used in log lines.
	fn label(self) -> &'static str {
		match self {
			Self::Heartbeat => "node membership refresh",
			Self::EventProcessing => "async event processing",
			Self::EventAdoption => "async event adoption",
			Self::IndexCompaction => "index compaction",
			Self::GraphFold => "graph fold",
			Self::TableMigration => "table migration",
			Self::LiveQueryRouter => "live-query router",
			Self::NodeExpire => "inactive node expiry",
			Self::NodeCleanup => "archived node cleanup",
			Self::ChangefeedGc => "changefeed garbage collection",
			Self::LiveQueryGc => "live-query event garbage collection",
			Self::ReclaimTombstones => "tombstone reclaim",
			Self::ResumeIndexBuilds => "stalled index build recovery",
			Self::RpcSessionGc => "expired RPC session purge",
			Self::MvccGc => "MVCC GC",
			Self::LockCleanup => "lock cleanup",
			Self::DiskannWriterLeases => "idle DiskANN writer lease release",
			Self::SystemMetricsRefresh => "system metrics refresh",
		}
	}

	/// The bounded name recorded as the `job` attribute of every task metric.
	/// Stable: dashboards key on it.
	pub const fn name(self) -> &'static str {
		match self {
			Self::Heartbeat => "heartbeat",
			Self::EventProcessing => "event_processing",
			Self::EventAdoption => "event_adoption",
			Self::IndexCompaction => "index_compaction",
			Self::GraphFold => "graph_fold",
			Self::TableMigration => "table_migration",
			Self::LiveQueryRouter => "live_query_router",
			Self::NodeExpire => "node_expire",
			Self::NodeCleanup => "node_cleanup",
			Self::ChangefeedGc => "changefeed_gc",
			Self::LiveQueryGc => "live_query_gc",
			Self::ReclaimTombstones => "reclaim_tombstones",
			Self::ResumeIndexBuilds => "resume_index_builds",
			Self::RpcSessionGc => "rpc_session_gc",
			Self::MvccGc => "tikv_gc",
			Self::LockCleanup => "tikv_lock_cleanup",
			Self::DiskannWriterLeases => "diskann_writer_leases",
			Self::SystemMetricsRefresh => "system_metrics_refresh",
		}
	}

	/// The cadence the job runs at under `opts`. Zero disables the job.
	fn interval(self, opts: &EngineOptions) -> Duration {
		match self {
			Self::Heartbeat => opts.node_membership_refresh_interval,
			Self::EventProcessing | Self::EventAdoption => opts.event_processing_interval,
			Self::IndexCompaction => opts.index_compaction_interval,
			Self::GraphFold => opts.graph_fold_interval,
			Self::TableMigration => opts.table_migration_interval,
			Self::LiveQueryRouter => opts.live_query_router_interval,
			Self::NodeExpire => opts.node_membership_check_interval,
			Self::NodeCleanup => opts.node_membership_cleanup_interval,
			Self::ChangefeedGc => opts.changefeed_gc_interval,
			Self::LiveQueryGc => opts.live_query_gc_interval,
			Self::ReclaimTombstones => opts.reclaim_interval,
			Self::ResumeIndexBuilds => opts.index_build_resume_interval,
			Self::RpcSessionGc => opts.rpc_session_gc_interval,
			Self::MvccGc => opts.mvcc_gc_interval,
			Self::LockCleanup => opts.lock_cleanup_interval,
			Self::DiskannWriterLeases => opts.diskann_writer_lease_release_interval,
			Self::SystemMetricsRefresh => opts.system_metrics_refresh_interval,
		}
	}

	/// The lease standing a pass of this job is reported under when it fails
	/// before reporting one, other than on the lease check itself (see
	/// [`failure_lease`]). A leased job's failures are attributed to the
	/// holder: a node that never held the lease returns without doing
	/// anything else that could fail.
	fn lease_on_failure(self) -> TaskLeaseState {
		match self {
			Self::EventAdoption
			| Self::IndexCompaction
			| Self::GraphFold
			| Self::TableMigration
			| Self::ChangefeedGc
			| Self::LiveQueryGc
			| Self::ReclaimTombstones
			| Self::ResumeIndexBuilds
			| Self::RpcSessionGc => TaskLeaseState::Held,
			Self::Heartbeat
			| Self::EventProcessing
			| Self::LiveQueryRouter
			| Self::NodeExpire
			| Self::NodeCleanup
			| Self::MvccGc
			| Self::LockCleanup
			| Self::DiskannWriterLeases
			| Self::SystemMetricsRefresh => TaskLeaseState::NotLeased,
		}
	}
}

/// Runs one pass of `job` against `dbs`.
///
/// This is the single dispatch point from a job to the datastore method that
/// performs it, so every driver — the datastore's own tasks and a host running
/// [`Datastore::run_maintenance_slice`] — runs the same code with the same
/// bounds.
async fn run_job(
	job: Job,
	dbs: &Arc<Datastore>,
	canceller: &CancellationToken,
	opts: &EngineOptions,
	reclaim_grace: Duration,
	metrics_claim: Option<&RefreshClaim>,
) -> anyhow::Result<Pass> {
	let unleased = |res: anyhow::Result<()>| res.map(|()| Pass::done(TaskLeaseState::NotLeased));
	match job {
		Job::Heartbeat => unleased(heartbeat(dbs, NODE_MEMBERSHIP_UPDATE_TIMEOUT, canceller).await),
		Job::EventProcessing => dbs.event_processing_job(canceller).await,
		Job::EventAdoption => {
			dbs.event_processing_adopt(opts.event_processing_interval, canceller).await
		}
		Job::IndexCompaction => {
			Datastore::index_compaction_slice(
				Arc::clone(dbs),
				opts.index_compaction_interval,
				canceller.clone(),
			)
			.await
		}
		Job::GraphFold => {
			Datastore::graph_fold_slice(
				Arc::clone(dbs),
				opts.graph_fold_interval,
				canceller.clone(),
			)
			.await
		}
		Job::TableMigration => {
			Datastore::table_migration_pass(
				Arc::clone(dbs),
				opts.table_migration_interval,
				canceller.clone(),
			)
			.await
		}
		Job::LiveQueryRouter => dbs.live_query_router_process().await,
		Job::NodeExpire => dbs.expire_nodes_slice().await,
		Job::NodeCleanup => dbs.remove_nodes_slice().await,
		Job::ChangefeedGc => dbs.changefeed_process(&opts.changefeed_gc_interval, canceller).await,
		Job::LiveQueryGc => {
			dbs.live_query_gc_process(&opts.live_query_gc_interval, canceller).await
		}
		Job::ReclaimTombstones => {
			Datastore::reclaim_tombstones_slice(
				Arc::clone(dbs),
				opts.reclaim_interval,
				reclaim_grace,
				canceller.clone(),
			)
			.await
		}
		Job::ResumeIndexBuilds => {
			dbs.resume_stalled_index_builds(opts.index_build_resume_interval, canceller.clone())
				.await
		}
		Job::RpcSessionGc => dbs.purge_expired_rpc_sessions(&opts.rpc_session_gc_interval).await,
		Job::MvccGc => unleased(dbs.run_mvcc_gc(opts.mvcc_gc_lifetime).await),
		Job::LockCleanup => unleased(dbs.run_lock_cleanup(opts.mvcc_gc_lifetime).await),
		Job::DiskannWriterLeases => unleased(dbs.release_idle_writer_leases().await.map(|_| ())),
		Job::SystemMetricsRefresh => {
			// The snapshot is one cache per process, and its CPU percentage is a
			// delta since the previous refresh of it, so only the driver holding
			// the process-wide claim refreshes; the others let their pass go by.
			// Retried every pass, so the refresh moves to a surviving driver
			// when the holder ends. A driver that never schedules the refresh
			// holds no claim.
			if metrics_claim.is_some_and(RefreshClaim::take) {
				crate::observe::refresh_process_snapshot().await;
			}
			Ok(Pass::done(TaskLeaseState::NotLeased))
		}
	}
}

/// The lease standing a pass that failed with `e` is reported under, given
/// `standing`: the pass's own, or the job's [`Job::lease_on_failure`] when
/// the pass attached none.
///
/// A failure of the lease check itself — marked [`LeaseCheckFailed`] by the
/// lease handler — is reported as [`TaskLeaseState::Unknown`]: the pass never
/// established whether this node held the lease, or lost track of it, so the
/// failure is neither the holder's nor another node's. Any other failure is
/// reported under `standing`.
fn failure_lease(e: &anyhow::Error, standing: TaskLeaseState) -> TaskLeaseState {
	if e.downcast_ref::<LeaseCheckFailed>().is_some() {
		return TaskLeaseState::Unknown;
	}
	standing
}

/// Reports one pass to the datastore's observer.
///
/// A pass that failed reports the counts and backlog it attached to its error
/// with [`Pass::failed`], or none, and is attributed to the lease standing
/// [`failure_lease`] gives; cancellation is its own outcome so a shutdown does
/// not read as a run of errors.
fn observe_pass(dbs: &Datastore, job: Job, duration: Duration, res: Result<Pass, &anyhow::Error>) {
	let observer = dbs.observer();
	if observer.is_noop() {
		return;
	}
	let (outcome, pass) = match res {
		Ok(pass) => (Outcome::Success, pass),
		Err(e) => {
			let outcome = if is_query_cancelled(e) {
				Outcome::Cancelled
			} else {
				Outcome::Error
			};
			let mut pass = match e.downcast_ref::<PassFailure>() {
				Some(failure) => failure.0,
				None => Pass::done(job.lease_on_failure()),
			};
			pass.lease = failure_lease(e, pass.lease);
			(outcome, pass)
		}
	};
	observer.on_task_pass(&TaskPassEvent {
		safe: TaskPassEventSafe {
			job: job.name(),
			outcome,
			duration,
			items: pass.items,
			oldest_age: pass.oldest_age,
			lease: pass.lease,
			backlog: pass.progress == Progress::More,
		},
		ctx: TaskPassEventCtx::default(),
	});
}

/// What a pass needs besides the job: the datastore it runs against, the token
/// that stops it, and the configuration it reads.
///
/// Holds a [`Weak`] reference to the datastore, never a strong one. The
/// datastore owns the task handles, so a strong reference would close a cycle
/// through them and the datastore could never be dropped. Each pass upgrades
/// for its own duration, and a task exits once the upgrade fails, which is what
/// lets an embedder that simply drops its datastore — rather than calling
/// [`Datastore::shutdown`] — still wind the tasks down.
#[derive(Clone)]
struct Runner {
	dbs: Weak<Datastore>,
	canceller: CancellationToken,
	opts: EngineOptions,
	/// Whether the datastore's data migrations have run, which every task but
	/// the heartbeat waits for before its first pass; see [`Self::migrated`].
	migrated: tokio::sync::watch::Receiver<bool>,
	/// The grace a tombstone must age before reclaim, clamped up to at least
	/// the MVCC GC lifetime: the reclaim's out-of-transaction range destroy
	/// bypasses MVCC, so data must not be reclaimed while a snapshot older
	/// than the GC safepoint (`now - mvcc_gc_lifetime`) could still read it.
	/// Deriving it here means a longer `--tikv-gc-lifetime` can never be
	/// undercut by leaving `--reclaim-grace` at its default.
	reclaim_grace: Duration,
}

impl Runner {
	fn new(dbs: &Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Self {
		Self {
			dbs: Arc::downgrade(dbs),
			canceller,
			opts: *opts,
			migrated: dbs.subscribe_migrated(),
			reclaim_grace: opts.reclaim_grace.max(opts.mvcc_gc_lifetime),
		}
	}

	/// Waits until the datastore's data migrations have run: `true` then, and
	/// `false` if the task is cancelled or the datastore dropped first, when
	/// the task has nothing left to do.
	///
	/// A migration rewrites persisted data, and a pass that ran alongside it
	/// would read that data mid-rewrite. The heartbeat alone does not wait:
	/// the migration's lease is this node's, and holding it depends on the
	/// node's membership row staying fresh. The flag is raised by
	/// [`Datastore::check_version`] once the migrations have run, and already
	/// on a datastore that owes none.
	async fn migrated(&self) -> bool {
		let mut migrated = self.migrated.clone();
		tokio::select! {
			biased;
			_ = self.canceller.cancelled() => false,
			run = migrated.wait_for(|migrated| *migrated) => run.is_ok(),
		}
	}

	/// One observed pass of `job`: timed, reported to the observer, and its
	/// failure logged so that a failing job cannot stop its peers. Cancellation
	/// is not an error. A pass that did not complete reports [`Progress::Done`]:
	/// there is nothing more the caller can do for it before the next tick.
	///
	/// `metrics_claim` is the driver's holder of the process-wide metrics
	/// claim. Only a driver that schedules [`Job::SystemMetricsRefresh`] has
	/// one, and it belongs to that driver rather than to the runner, so the
	/// claim is released when that driver ends — not when the last task that
	/// shares this runner does, which a drain running user-defined SurrealQL
	/// past the shutdown wait could put off indefinitely.
	async fn run(
		&self,
		dbs: &Arc<Datastore>,
		job: Job,
		metrics_claim: Option<&RefreshClaim>,
	) -> Progress {
		let started = Instant::now();
		let res =
			run_job(job, dbs, &self.canceller, &self.opts, self.reclaim_grace, metrics_claim).await;
		observe_pass(dbs, job, started.elapsed(), res.as_ref().copied());
		match res {
			Ok(pass) => pass.progress,
			Err(e) => {
				if !self.canceller.is_cancelled() {
					error!("Error running {}: {e:#}", job.label());
				}
				Progress::Done
			}
		}
	}
}

// --------------------------------------------------
// Tasks
// --------------------------------------------------

pub struct Tasks(
	#[cfg_attr(all(target_family = "wasm", target_os = "unknown"), expect(dead_code))] Vec<Task>,
);

impl Tasks {
	#[cfg(all(target_family = "wasm", target_os = "unknown"))]
	pub async fn resolve(self) -> Result<(), Error> {
		Ok(())
	}
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	pub async fn resolve(self) -> Result<(), Error> {
		for task in self.0 {
			// Surface a task that panicked or was aborted. The maintenance
			// scheduler carries several jobs, so losing it silently would stop
			// all of them while shutdown still reported success.
			if let Err(e) = task.await {
				error!("Background task did not shut down cleanly: {e}");
			}
		}
		Ok(())
	}
}

/// Starts this node's background tasks and returns handles to await at shutdown.
///
/// Work is split across tasks by what its cadence has to guarantee, not one task
/// per job:
///
/// - The **cluster heartbeat** runs alone. A stalled heartbeat gets this node archived by another
///   member's expiry scan and fails its readiness probe, so nothing may ever share its task.
/// - **Async event processing**, **index compaction** and **graph fold** each keep their own task
///   because each is driven by the write path: the task wakes on the commit that queues work and
///   drains the queue in slices until it is empty, so under sustained load it does not return
///   between ticks.
/// - **Table migration** keeps its own task for the same reason, as the commit that queues a
///   table's rewrite or verification wakes it, and because its pass is not bounded: it works
///   through every queued table under the job's lease, a page at a time, so it would delay any job
///   that shared its task.
/// - The remaining jobs run on two [`spawn_task_scheduler`] instances, split by whether one pass
///   has a bound. [`Job::MAINTENANCE`] carries the jobs whose cost is bounded by catalog size, so
///   none can delay a peer by more than one short pass. [`Job::SWEEP`] carries those whose queue is
///   nearly always empty but whose individual entries are not bounded, or whose pass is a remote
///   call: reclaiming one tombstone destroys an entire namespace or database prefix, a session
///   purge pages the whole session keyspace opening a write transaction per expired entry, the
///   event adoption sweep runs the user-defined events a departed node left behind, and the backend
///   maintenance jobs wait on the storage cluster. Keeping the groups apart means a long sweep
///   cannot stall the short jobs — in particular it cannot leave the cached process metrics stale
///   for its duration.
/// - The **live-query router** is spawned only under [`LiveQueryEngine::Router`]; under the default
///   inline engine it has nothing to deliver.
///
/// Every task holds a [`Weak`] reference to the datastore, never a strong one;
/// see [`Runner`]. Every task but the heartbeat waits for the datastore's data
/// migrations before its first pass; see [`Runner::migrated`].
///
/// The datastore starts these itself, so the only reason to call this directly
/// is to start them against a datastore built with
/// [`Builder::without_maintenance_tasks`](crate::kvs::ds::builder::Builder::without_maintenance_tasks).
///
/// On a datastore that [`Datastore::is_read_only`] reports, only the cluster
/// heartbeat and the expired RPC session purge run (see [`read_only_slots`]):
/// they write this node's own membership rows and attached sessions' durable
/// copies, which a read-only datastore still accepts, and every other task
/// writes state it refuses.
///
/// A build without the `maintenance-tasks` feature starts nothing and logs an
/// error naming the feature: its host drives that work instead.
pub fn init(dbs: &Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	#[cfg(not(feature = "maintenance-tasks"))]
	{
		let _ = (dbs, canceller, opts);
		error!(
			"Maintenance tasks were requested, but this build was compiled without the \
			 `maintenance-tasks` feature; none were started"
		);
		Tasks(Vec::new())
	}
	#[cfg(feature = "maintenance-tasks")]
	start(dbs, canceller, opts)
}

#[cfg(feature = "maintenance-tasks")]
fn start(dbs: &Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	let runner = Runner::new(dbs, canceller, opts);
	// The triggers are shared state in their own right, so a task holding them
	// does not keep the datastore alive.
	let triggers = dbs.commit_triggers();
	let mut tasks = Vec::with_capacity(8);
	// A dedicated job's interval of zero disables it, as a scheduled job's
	// does (see [`Schedule::new`]): its task is not spawned, since a ticker
	// cannot run at a period of zero.
	let enabled = |job: Job| {
		let enabled = !job.interval(opts).is_zero();
		if !enabled {
			debug!("{} is disabled by an interval of zero", job.label());
		}
		enabled
	};
	if enabled(Job::Heartbeat) {
		tasks.push(spawn_task_node_membership_refresh(runner.clone()));
	}
	// A read-only datastore accepts this node's own membership rows and
	// attached sessions' durable copies, and refuses the state every other
	// task writes, so only the heartbeat and the session purge run.
	if dbs.is_read_only() {
		let schedule = read_only_slots(opts);
		if !schedule.is_empty() {
			tasks.push(spawn_task_scheduler("sweep", schedule, runner, None));
		}
		return Tasks(tasks);
	}
	// A datastore that answers write barriers arms its write fence, which only the heartbeat
	// rearms: without it the node would refuse its own writes once the fence lapsed, so it answers
	// none, and builds elsewhere take it for a node that predates the barrier.
	if enabled(Job::Heartbeat) {
		tasks.push(spawn_task_write_barrier(runner.clone()));
	}
	// No debounce on the event queue: this node's own queue is drained without a
	// lease, so a wake costs one read of an empty prefix at most, and every
	// millisecond of delay is a millisecond of event latency.
	if enabled(Job::EventProcessing) {
		tasks.push(spawn_task_drain(
			Job::EventProcessing,
			|triggers| &triggers.async_event,
			Duration::ZERO,
			runner.clone(),
			Arc::clone(triggers),
		));
	}
	if enabled(Job::IndexCompaction) {
		tasks.push(spawn_task_drain(
			Job::IndexCompaction,
			|triggers| &triggers.index_compaction,
			INDEX_COMPACTION_TRIGGER_DEBOUNCE,
			runner.clone(),
			Arc::clone(triggers),
		));
	}
	if enabled(Job::GraphFold) {
		tasks.push(spawn_task_drain(
			Job::GraphFold,
			|triggers| &triggers.graph_fold,
			GRAPH_FOLD_TRIGGER_DEBOUNCE,
			runner.clone(),
			Arc::clone(triggers),
		));
	}
	// No debounce on the table migration queue: a commit wakes it only when it
	// queues a table's rewrite or verification, so wakes are as rare as the
	// statements that queue them.
	if enabled(Job::TableMigration) {
		tasks.push(spawn_task_drain(
			Job::TableMigration,
			|triggers| &triggers.table_migration,
			Duration::ZERO,
			runner.clone(),
			Arc::clone(triggers),
		));
	}
	for (group, schedule) in
		[("maintenance", maintenance_slots(opts)), ("sweep", sweep_slots(opts))]
	{
		// Every job in a group can be disabled by interval, so a group can end up
		// empty; spawning a task that immediately exits would serve no purpose.
		if !schedule.is_empty() {
			// The claim lives in the task that schedules the refresh, and in no
			// other, so it is released when that task ends.
			let metrics_claim = schedule
				.jobs()
				.any(|job| job == Job::SystemMetricsRefresh)
				.then(RefreshClaim::process);
			tasks.push(spawn_task_scheduler(group, schedule, runner.clone(), metrics_claim));
		}
	}
	if dbs.live_query_engine() == LiveQueryEngine::Router && enabled(Job::LiveQueryRouter) {
		tasks.push(spawn_task_live_query_router(runner));
	}
	Tasks(tasks)
}

/// Spawns the per-node live-query router task.
///
/// Tails the dedicated `lqe` keyspace and delivers notifications off the write
/// path. The cadence bounds steady-state delivery latency, so it ticks
/// frequently and keeps its own task rather than queueing behind maintenance
/// work. A pass delivers at most a slice of the captured entries, and the task
/// runs another at once while one reports [`Progress::More`], so a backlog
/// drains at the read rate rather than a slice per tick. Only spawned under
/// [`LiveQueryEngine::Router`].
fn spawn_task_live_query_router(runner: Runner) -> Task {
	let interval = Job::LiveQueryRouter.interval(&runner.opts);
	into_task(async move {
		trace!("Running the live-query router every {interval:?}");
		if !runner.migrated().await {
			trace!("Background task exited before its first pass: Running the live-query router");
			return;
		}
		let mut ticker = interval_ticker(interval).await;
		// Whether the last pass left entries behind. While it did, the loop
		// runs another pass instead of waiting for the tick.
		let mut backlog = false;
		loop {
			tokio::select! {
				biased;
				_ = runner.canceller.cancelled() => break,
				Some(_) = ticker.next() => {}
				_ = std::future::ready(()), if backlog => {}
			}
			let Some(dbs) = runner.dbs.upgrade() else {
				break;
			};
			backlog = runner.run(&dbs, Job::LiveQueryRouter, None).await == Progress::More;
		}
		trace!("Background task exited: Running the live-query router");
	})
}

/// This node's heartbeat, followed by a read of the store's feature level
/// once the heartbeat has succeeded: another node may have raised or
/// lowered the level, and the heartbeat is when this node reads it again.
/// A failure to read it leaves the cell as it was, possibly behind the
/// store in either direction; writes stay safe because every floor above
/// the cell's `in_use` is admitted by a locked read of the stored level.
///
/// A heartbeat that commits rearms the write fence of a datastore that
/// answers write barriers ([`crate::kvs::write_barrier`]), so the heartbeat
/// and the read after it are bounded together within the fence's life
/// ([`crate::kvs::write_barrier::HEARTBEAT_ATTEMPT_BOUND`]). A datastore that
/// answers none, such as one a host drives, arms no fence.
async fn heartbeat(
	dbs: &Datastore,
	timeout: Duration,
	canceller: &CancellationToken,
) -> anyhow::Result<()> {
	// Taken before the heartbeat reads the clock it records, so the fence lapses no later than
	// other nodes stop counting this one live.
	let beat = web_time::Instant::now();
	let bound = crate::kvs::write_barrier::HEARTBEAT_ATTEMPT_BOUND;
	dbs.update_node_with_timeout(timeout.min(bound), canceller).await?;
	if !dbs.is_read_only() && dbs.advertises_write_barrier() {
		dbs.transaction_factory()
			.write_fence()
			.heartbeat(beat, crate::kvs::write_barrier::WRITE_FENCE_STALE_AFTER);
	}
	let refresh = timeout.min(bound.saturating_sub(beat.elapsed()));
	match common::time::timeout(refresh, dbs.refresh_feature_level()).await {
		Ok(Ok(())) => {}
		Ok(Err(e)) => warn!("Could not refresh the feature level: {e}"),
		Err(_) => warn!("Timed out refreshing the feature level after {refresh:?}"),
	}
	Ok(())
}

/// Spawns the cluster heartbeat.
///
/// A timed-out refresh is tolerated and the task keeps its cadence, because a
/// node that stops refreshing is archived by another member's expiry scan;
/// only cancellation ends the task. Alone among the tasks it does not wait for
/// the datastore's migrations: the migration's lease is this node's, and
/// holding it depends on this row staying fresh. See [`Runner::migrated`].
fn spawn_task_node_membership_refresh(runner: Runner) -> Task {
	let interval = Job::Heartbeat.interval(&runner.opts);
	into_task(async move {
		trace!("Updating node registration information every {interval:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = runner.canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					let Some(dbs) = runner.dbs.upgrade() else { break };
					let started = Instant::now();
					let res =
						heartbeat(&dbs, NODE_MEMBERSHIP_UPDATE_TIMEOUT, &runner.canceller).await;
					observe_pass(
						&dbs,
						Job::Heartbeat,
						started.elapsed(),
						res.as_ref().map(|()| Pass::done(TaskLeaseState::NotLeased)),
					);
					if !run_node_membership_update(
						NODE_MEMBERSHIP_UPDATE_TIMEOUT,
						classify_node_membership_update(res),
					) {
						break;
					}
				}
			}
		}
		trace!("Background task exited: Updating node registration information");
	})
}

/// Spawns the task that answers this node's write barriers (see
/// [`crate::kvs::write_barrier`]).
///
/// It runs alone, because answering a barrier waits for this node's open write transactions to
/// end, and a datastore that answers barriers is the one whose write commits the heartbeat fence
/// guards, so the fence is armed here. It is no job a host drives: the fence it arms lapses
/// unless a heartbeat commits within [`crate::kvs::write_barrier::WRITE_FENCE_STALE_AFTER`],
/// which a host's schedule does not promise.
fn spawn_task_write_barrier(runner: Runner) -> Task {
	if let Some(dbs) = runner.dbs.upgrade() {
		dbs.begin_answering_write_barriers();
	}
	into_task(async move {
		let mut ticker = interval_ticker(crate::kvs::write_barrier::WRITE_BARRIER_POLL).await;
		loop {
			tokio::select! {
				biased;
				_ = runner.canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					let Some(dbs) = runner.dbs.upgrade() else { break };
					if let Err(e) = dbs.answer_write_barrier(&runner.canceller).await {
						warn!("Could not answer a write barrier: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Answering write barriers");
	})
}

/// Spawns the task for a queue fed by the write path.
///
/// The task wakes on `job`'s commit trigger and on its own tick, and runs the
/// job one pass at a time for as long as a pass reports [`Progress::More`], so
/// the queue drains at the write rate rather than at the tick cadence. Nothing
/// otherwise relates the two, and the gap between them is what would let a
/// count index's delta log — which every read of that index sums — or an
/// event queue grow without bound.
///
/// `debounce` delays a trigger-driven wake so the commits landing meanwhile
/// batch into one pass; it is what keeps a leased job from spinning on its
/// lease check on the nodes that do not hold the lease. The trigger is waited
/// on only while the queue is empty, so a permit stored during a busy stretch
/// costs one pass over an empty queue once it is not.
fn spawn_task_drain(
	job: Job,
	trigger: fn(&CommitTriggers) -> &Notify,
	debounce: Duration,
	runner: Runner,
	triggers: Arc<CommitTriggers>,
) -> Task {
	let interval = job.interval(&runner.opts);
	into_task(async move {
		trace!("Running {} every {interval:?}", job.label());
		if !runner.migrated().await {
			trace!("Background task exited before its first pass: Running {}", job.label());
			return;
		}
		let mut ticker = interval_ticker(interval).await;
		// Whether the last pass left work behind. While it did, the loop runs
		// another pass instead of waiting for a wake, checking only for
		// cancellation and the tick in between. Every other exit from the
		// select is a wake, and a wake runs one pass.
		let mut backlog = false;
		loop {
			tokio::select! {
				biased;
				_ = runner.canceller.cancelled() => break,
				Some(_) = ticker.next() => {}
				_ = trigger(&triggers).notified(), if !backlog => {
					if !debounce.is_zero() {
						tokio::select! {
							biased;
							_ = runner.canceller.cancelled() => break,
							_ = sleep(debounce) => {}
						}
					}
				}
				_ = std::future::ready(()), if backlog => {}
			}
			let Some(dbs) = runner.dbs.upgrade() else {
				break;
			};
			backlog = runner.run(&dbs, job, None).await == Progress::More;
		}
		trace!("Background task exited: Running {}", job.label());
	})
}

// --------------------------------------------------
// Maintenance scheduler
// --------------------------------------------------

/// One job's place in a schedule.
struct Slot {
	job: Job,
	interval: Duration,
	next_due: Instant,
}

/// A group of jobs run one at a time on a shared cadence.
///
/// Pure over the instants it is handed: it never reads a clock, so a host that
/// owns time drives it with its own readings, and a test drives it
/// deterministically.
pub(crate) struct Schedule {
	slots: Vec<Slot>,
}

impl Schedule {
	/// Registers `jobs` at their configured cadences, taking `now` as the moment
	/// of registration.
	///
	/// A zero interval leaves the job unregistered, which is how the documented
	/// "set to zero to disable" options are honoured. Zero is not a valid tick
	/// period for any job, so treating it uniformly as "disabled" also avoids
	/// turning a misconfigured interval into a busy loop.
	///
	/// One `now` for the whole schedule, so jobs sharing an interval share a
	/// deadline exactly and registration order — not nanosecond skew between
	/// clock reads — decides which of them runs first.
	fn new(jobs: &[Job], opts: &EngineOptions, now: Instant) -> Self {
		let slots = jobs
			.iter()
			.map(|&job| (job, job.interval(opts)))
			.filter(|(_, interval)| !interval.is_zero())
			.map(|(job, interval)| Slot {
				job,
				interval,
				// The metrics refresh is due immediately so a cold start reports real
				// utilisation rather than zeroes. Every other job waits out one full
				// interval, which keeps startup from firing several lease acquisitions
				// at the same moment.
				next_due: if matches!(job, Job::SystemMetricsRefresh) {
					now
				} else {
					now + interval
				},
			})
			.collect();
		Self {
			slots,
		}
	}

	fn is_empty(&self) -> bool {
		self.slots.is_empty()
	}

	fn jobs(&self) -> impl Iterator<Item = Job> + '_ {
		self.slots.iter().map(|s| s.job)
	}

	/// Index of the slot to run next: the earliest deadline, and among equal
	/// deadlines the one registered first (`min_by_key` yields the first
	/// minimum).
	///
	/// An overdue job sorts ahead of one whose deadline was just set past the
	/// current instant, so a saturated schedule drains oldest-first and a job
	/// that has just run cannot immediately run again while a peer is waiting.
	fn next(&self) -> Option<usize> {
		self.slots.iter().enumerate().min_by_key(|(_, s)| s.next_due).map(|(i, _)| i)
	}

	/// The slot to run next and how long after `now` it is due; zero when it is
	/// already overdue. `None` when nothing is registered.
	fn wait(&self, now: Instant) -> Option<(usize, Duration)> {
		self.next().map(|i| (i, self.slots[i].next_due.saturating_duration_since(now)))
	}

	/// Records that slot `i`'s pass finished at `now`.
	///
	/// The next deadline is measured from completion, not from the deadline just
	/// met. A pass that overruns its interval therefore still rests for a full
	/// interval instead of coming due the moment it returns, and its deadline
	/// cannot drift permanently into the past.
	fn finished(&mut self, i: usize, now: Instant) {
		self.slots[i].next_due = now + self.slots[i].interval;
	}

	/// Records that slot `i`'s pass stopped at `now` with work still queued.
	///
	/// Due again at once, but at `now` rather than in the past, so every peer
	/// that is already overdue runs first: a job with a backlog cannot starve
	/// the schedule it shares.
	fn backlogged(&mut self, i: usize, now: Instant) {
		self.slots[i].next_due = now;
	}

	/// Records slot `i`'s pass by what it reported: [`Self::backlogged`] for
	/// [`Progress::More`], [`Self::finished`] for [`Progress::Done`].
	fn record(&mut self, i: usize, progress: Progress, now: Instant) {
		match progress {
			Progress::More => self.backlogged(i, now),
			Progress::Done => self.finished(i, now),
		}
	}
}

/// The bounded-pass jobs, on their shared schedule.
fn maintenance_slots(opts: &EngineOptions) -> Schedule {
	Schedule::new(&Job::MAINTENANCE, opts, Instant::now())
}

/// The unbounded sweeps, on their shared schedule.
fn sweep_slots(opts: &EngineOptions) -> Schedule {
	Schedule::new(&Job::SWEEP, opts, Instant::now())
}

/// The jobs a datastore open read-only still runs, besides the cluster
/// heartbeat: only the expired RPC session purge. A read-only store still
/// accepts attached sessions, so without the purge their expired copies, which
/// an anonymous `attach` can create, would accumulate for as long as it stays
/// read-only.
fn read_only_slots(opts: &EngineOptions) -> Schedule {
	Schedule::new(&[Job::RpcSessionGc], opts, Instant::now())
}

/// Drives the schedule until cancelled, running one job per iteration.
///
/// A pass that reports [`Progress::More`] is due again at once, behind any
/// peer that is already overdue. Dispatch is injected so the scheduling
/// behaviour can be exercised without a datastore.
async fn maintenance_loop<F, Fut>(mut schedule: Schedule, canceller: CancellationToken, run: F)
where
	F: Fn(Job) -> Fut,
	Fut: Future<Output = Progress>,
{
	while let Some((i, delay)) = schedule.wait(Instant::now()) {
		tokio::select! {
			biased;
			_ = canceller.cancelled() => break,
			_ = sleep(delay) => {}
		}
		let progress = run(schedule.slots[i].job).await;
		schedule.record(i, progress, Instant::now());
	}
}

/// Spawns one task that runs the given group of jobs on a shared schedule.
///
/// `group` names the group in the task's log lines so an operator can tell the
/// two schedulers apart. `metrics_claim` is this task's holder of the process
/// metrics claim, given to the group that carries
/// [`Job::SystemMetricsRefresh`]; the task owns it, so the claim is released
/// when the task ends.
fn spawn_task_scheduler(
	group: &'static str,
	schedule: Schedule,
	runner: Runner,
	metrics_claim: Option<RefreshClaim>,
) -> Task {
	into_task(async move {
		trace!(
			"Running {} {group} jobs on a shared schedule: {}",
			schedule.slots.len(),
			schedule.jobs().map(Job::label).collect::<Vec<_>>().join(", ")
		);
		if !runner.migrated().await {
			trace!("Background task exited before its first pass: Running {group} jobs");
			return;
		}
		let canceller = runner.canceller.clone();
		maintenance_loop(schedule, canceller, move |job| {
			let runner = runner.clone();
			// A clone is the same holder; it lives for this pass, within the task.
			let metrics_claim = metrics_claim.clone();
			async move {
				// The datastore is gone, so there is nothing left to maintain.
				// Cancelling ends the schedule on its next iteration, and does the
				// same for every other task sharing this token.
				let Some(dbs) = runner.dbs.upgrade() else {
					runner.canceller.cancel();
					return Progress::Done;
				};
				runner.run(&dbs, job, metrics_claim.as_ref()).await
			}
		})
		.await;
		trace!("Background task exited: Running {group} jobs");
	})
}

// --------------------------------------------------
// Host-driven maintenance
// --------------------------------------------------

/// The outcome of one host-driven maintenance step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceSlice {
	/// The job that ran, if one was due.
	pub job: Option<Job>,
	/// Whether the job that ran left work behind. [`Progress::Done`] when no
	/// job was due.
	pub progress: Progress,
	/// How long until the next job is due; zero when one is due now, `None`
	/// when nothing is scheduled.
	pub next_due: Option<Duration>,
}

/// Maintenance state for a host that drives the jobs itself in place of the
/// datastore's tasks: every job on one schedule, stepped one pass at a time.
///
/// Owns the host's claim on the process metrics refresh, so dropping the
/// schedule releases the claim.
pub(crate) struct HostMaintenance {
	schedule: Schedule,
	runner: Runner,
	metrics_claim: RefreshClaim,
}

impl HostMaintenance {
	/// Every job this datastore would otherwise run on its own tasks, at the
	/// same cadences.
	pub(crate) fn new(
		dbs: &Arc<Datastore>,
		canceller: CancellationToken,
		opts: &EngineOptions,
	) -> Self {
		let mut jobs: Vec<Job> = Job::DEDICATED
			.into_iter()
			.filter(|job| {
				*job != Job::LiveQueryRouter || dbs.live_query_engine() == LiveQueryEngine::Router
			})
			.collect();
		jobs.extend(Job::MAINTENANCE);
		jobs.extend(Job::SWEEP);
		// A read-only datastore accepts this node's own membership rows and
		// attached sessions' durable copies, and refuses the state every other
		// job writes, so only the heartbeat and the session purge run.
		if dbs.is_read_only() {
			jobs.retain(|job| matches!(*job, Job::Heartbeat | Job::RpcSessionGc));
		}
		Self {
			schedule: Schedule::new(&jobs, opts, Instant::now()),
			runner: Runner::new(dbs, canceller, opts),
			metrics_claim: RefreshClaim::process(),
		}
	}

	/// Runs the job that is due, if any, and reports when to call again.
	///
	/// Never waits: a job that is not yet due is left for the call that comes
	/// after `next_due`. A pass that reports [`Progress::More`] is due again at
	/// once, behind any peer that is already overdue.
	pub(crate) async fn step(&mut self, dbs: &Arc<Datastore>) -> MaintenanceSlice {
		let now = Instant::now();
		let Some((i, wait)) = self.schedule.wait(now) else {
			return MaintenanceSlice {
				job: None,
				progress: Progress::Done,
				next_due: None,
			};
		};
		if !wait.is_zero() {
			return MaintenanceSlice {
				job: None,
				progress: Progress::Done,
				next_due: Some(wait),
			};
		}
		let job = self.schedule.slots[i].job;
		let progress = self.runner.run(dbs, job, Some(&self.metrics_claim)).await;
		let now = Instant::now();
		self.schedule.record(i, progress, now);
		MaintenanceSlice {
			job: Some(job),
			progress,
			next_due: self.schedule.wait(now).map(|(_, wait)| wait),
		}
	}
}

// --------------------------------------------------
// Heartbeat outcome handling
// --------------------------------------------------

enum NodeMembershipUpdateResult {
	Updated,
	Cancelled,
	TimedOut,
	Failed(anyhow::Error),
}

fn classify_node_membership_update(res: anyhow::Result<()>) -> NodeMembershipUpdateResult {
	match res {
		Ok(()) => NodeMembershipUpdateResult::Updated,
		Err(e) if is_query_cancelled(&e) => NodeMembershipUpdateResult::Cancelled,
		Err(e) if is_query_timedout(&e) => NodeMembershipUpdateResult::TimedOut,
		Err(e) => NodeMembershipUpdateResult::Failed(e),
	}
}

/// Whether the heartbeat task should keep running after `result`.
fn run_node_membership_update(
	timeout_duration: Duration,
	result: NodeMembershipUpdateResult,
) -> bool {
	match result {
		NodeMembershipUpdateResult::Updated => true,
		NodeMembershipUpdateResult::Cancelled => false,
		NodeMembershipUpdateResult::TimedOut => {
			warn!("Timed out updating node registration information after {timeout_duration:?}");
			true
		}
		NodeMembershipUpdateResult::Failed(e) => {
			error!("Error updating node registration information: {e}");
			true
		}
	}
}

async fn interval_ticker(interval: Duration) -> IntervalStream {
	// Create a new interval timer
	let mut interval = common::time::interval(interval);
	// Don't bombard the database if we miss some ticks
	interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
	interval.tick().await;
	IntervalStream::new(interval)
}

#[cfg(test)]
mod test {
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	use tokio::time::Instant;
	use tokio_util::sync::CancellationToken;

	use super::{
		Job, Progress, Schedule, Slot, failure_lease, maintenance_loop, maintenance_slots,
		read_only_slots, sweep_slots,
	};
	// Used only by the tests that build a datastore, so it is gated with them:
	// the lib tests build under `-D warnings` once per kv backend.
	#[cfg(feature = "kv-mem")]
	use super::{Pass, RefreshClaim, observe_pass};
	#[cfg(feature = "kv-mem")]
	use crate::kvs::Datastore;
	#[cfg(feature = "kv-mem")]
	use crate::kvs::tasks;
	use crate::options::EngineOptions;

	/// A slot due one interval from now, as the scheduler registers them.
	fn slot(job: Job, interval: Duration) -> Slot {
		Slot {
			job,
			interval,
			next_due: Instant::now() + interval,
		}
	}

	fn schedule(slots: Vec<Slot>) -> Schedule {
		Schedule {
			slots,
		}
	}

	/// Drives `maintenance_loop` with a recorder that stops it after `limit`
	/// passes, then returns the jobs in the order they ran.
	async fn record_passes(slots: Vec<Slot>, limit: usize) -> Vec<Job> {
		record_passes_with_delay(slots, limit, |_| Duration::ZERO)
			.await
			.into_iter()
			.map(|(job, _)| job)
			.collect()
	}

	/// As [`record_passes`], but each pass takes the duration `cost` reports for
	/// its job, and each pass is paired with the instant it started, so a slow
	/// job's effect on the schedule can be observed.
	async fn record_passes_with_delay(
		slots: Vec<Slot>,
		limit: usize,
		cost: impl Fn(Job) -> Duration,
	) -> Vec<(Job, Instant)> {
		let canceller = CancellationToken::new();
		let log = Arc::new(Mutex::new(Vec::new()));
		let stop = canceller.clone();
		let sink = Arc::clone(&log);
		maintenance_loop(schedule(slots), canceller, move |job| {
			let sink = Arc::clone(&sink);
			let stop = stop.clone();
			let delay = cost(job);
			async move {
				{
					let mut passes = sink.lock().unwrap();
					passes.push((job, Instant::now()));
					if passes.len() >= limit {
						stop.cancel();
					}
				}
				if !delay.is_zero() {
					tokio::time::sleep(delay).await;
				}
				Progress::Done
			}
		})
		.await;
		Arc::into_inner(log).unwrap().into_inner().unwrap()
	}

	/// A pass that reports a backlog is run again at once rather than resting
	/// out its interval, so a paged job covers its whole input as fast as its
	/// slices allow instead of one page per interval.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_reruns_a_backlogged_job_at_once() {
		let canceller = CancellationToken::new();
		let stop = canceller.clone();
		let passes = Arc::new(Mutex::new(Vec::new()));
		let sink = Arc::clone(&passes);
		let started = Instant::now();
		maintenance_loop(
			schedule(vec![slot(Job::ResumeIndexBuilds, Duration::from_secs(30))]),
			canceller,
			move |_| {
				let sink = Arc::clone(&sink);
				let stop = stop.clone();
				async move {
					let mut passes = sink.lock().unwrap();
					passes.push(Instant::now());
					// Three pages of work, then the input is exhausted.
					if passes.len() < 3 {
						Progress::More
					} else {
						stop.cancel();
						Progress::Done
					}
				}
			},
		)
		.await;
		let passes = Arc::into_inner(passes).unwrap().into_inner().unwrap();
		assert_eq!(passes.len(), 3);
		// The first pass waits out the interval; the backlogged ones do not.
		assert!(passes[0] >= started + Duration::from_secs(30));
		assert!(
			passes[2] - passes[0] < Duration::from_secs(1),
			"backlogged passes rested: {passes:?}"
		);
	}

	#[test]
	fn next_slot_is_none_when_nothing_is_registered() {
		assert!(schedule(vec![]).next().is_none());
	}

	#[test]
	fn next_slot_picks_the_earliest_deadline() {
		let slots = schedule(vec![
			slot(Job::NodeCleanup, Duration::from_secs(300)),
			slot(Job::NodeExpire, Duration::from_secs(15)),
			slot(Job::ChangefeedGc, Duration::from_secs(30)),
		]);
		assert_eq!(slots.next(), Some(1));
	}

	#[test]
	fn next_slot_breaks_deadline_ties_on_registration_order() {
		let due = Instant::now() + Duration::from_secs(60);
		let mut slots = schedule(vec![
			slot(Job::RpcSessionGc, Duration::from_secs(60)),
			slot(Job::ReclaimTombstones, Duration::from_secs(60)),
		]);
		for s in &mut slots.slots {
			s.next_due = due;
		}
		assert_eq!(slots.next(), Some(0));
	}

	#[test]
	fn next_slot_prefers_an_overdue_job_over_one_just_run() {
		let mut slots = schedule(vec![
			// Just ran, so its deadline sits in the future.
			slot(Job::ChangefeedGc, Duration::from_secs(30)),
			// Overdue.
			slot(Job::NodeExpire, Duration::from_secs(15)),
		]);
		slots.slots[1].next_due = Instant::now() - Duration::from_secs(5);
		assert_eq!(slots.next(), Some(1));
	}

	/// A pass that stops with a backlog is due again at once, but every peer
	/// that is already overdue runs before it.
	#[test]
	fn a_backlogged_job_yields_to_overdue_peers() {
		let now = Instant::now();
		let mut slots = schedule(vec![
			slot(Job::ChangefeedGc, Duration::from_secs(30)),
			slot(Job::NodeExpire, Duration::from_secs(15)),
		]);
		slots.slots[1].next_due = now - Duration::from_secs(5);
		slots.backlogged(0, now);
		assert_eq!(slots.wait(now), Some((1, Duration::ZERO)), "the overdue peer runs first");
		slots.finished(1, now);
		assert_eq!(slots.wait(now), Some((0, Duration::ZERO)), "then the backlogged job, now");
	}

	#[test]
	fn slots_skip_zero_intervals() {
		let opts = EngineOptions::default()
			.with_mvcc_gc_interval(Duration::ZERO)
			.with_rpc_session_gc_interval(Duration::ZERO);
		let maintenance: Vec<_> = maintenance_slots(&opts).jobs().collect();
		let sweeps: Vec<_> = sweep_slots(&opts).jobs().collect();
		assert!(!sweeps.contains(&Job::MvccGc));
		assert!(!sweeps.contains(&Job::RpcSessionGc));
		// The rest of each schedule is unaffected.
		assert!(maintenance.contains(&Job::NodeExpire));
		assert!(sweeps.contains(&Job::LockCleanup));
		assert!(sweeps.contains(&Job::ReclaimTombstones));
	}

	/// The live-query event collector keeps its own cadence: disabling
	/// changefeed collection leaves the live-event window enforced, and
	/// disabling the live-event collector leaves changefeed collection running.
	#[test]
	fn live_event_collection_is_scheduled_apart_from_changefeed_collection() {
		let opts = EngineOptions::default().with_changefeed_gc_interval(Duration::ZERO);
		let maintenance: Vec<_> = maintenance_slots(&opts).jobs().collect();
		assert!(!maintenance.contains(&Job::ChangefeedGc));
		assert!(maintenance.contains(&Job::LiveQueryGc));

		let opts = EngineOptions::default().with_live_query_gc_interval(Duration::ZERO);
		let maintenance: Vec<_> = maintenance_slots(&opts).jobs().collect();
		assert!(maintenance.contains(&Job::ChangefeedGc));
		assert!(!maintenance.contains(&Job::LiveQueryGc));
	}

	/// A read-only store runs only the session purge, which writes nothing but
	/// the session copies it still accepts.
	#[test]
	fn a_read_only_store_schedules_only_the_session_purge() {
		let jobs: Vec<_> = read_only_slots(&EngineOptions::default()).jobs().collect();
		assert_eq!(jobs, [Job::RpcSessionGc]);
		let opts = EngineOptions::default().with_rpc_session_gc_interval(Duration::ZERO);
		assert!(read_only_slots(&opts).is_empty());
	}

	/// The three groups must partition the job set: a job in none would never
	/// run, and a job in two would run twice per interval and contend with
	/// itself for its own lease.
	#[test]
	fn the_groups_partition_every_job() {
		let mut grouped: Vec<Job> = Job::DEDICATED.to_vec();
		grouped.extend(Job::MAINTENANCE);
		grouped.extend(Job::SWEEP);
		let total = grouped.len();
		grouped.sort_by_key(|job| job.name());
		grouped.dedup();
		assert_eq!(total, grouped.len(), "a job is registered in two groups");
		for job in Job::ALL {
			assert!(grouped.contains(&job), "{} is in no group", job.label());
		}
		assert_eq!(total, Job::ALL.len());
	}

	/// The unbounded sweeps and the remote backend calls must not share a task
	/// with the bounded jobs: a reclaim that destroys a whole database, a purge
	/// that pages the session keyspace, or a GC call waiting on the placement
	/// driver would otherwise stall every short job for its duration.
	#[test]
	fn unbounded_sweeps_are_not_on_the_maintenance_schedule() {
		let opts = EngineOptions::default();
		let maintenance: Vec<_> = maintenance_slots(&opts).jobs().collect();
		for job in Job::SWEEP {
			assert!(!maintenance.contains(&job), "{} is on the maintenance schedule", job.label());
		}
		// And the metrics refresh must stay off the sweep schedule, so a long
		// sweep cannot leave `INFO FOR ROOT` and the process gauges stale.
		let sweeps: Vec<_> = sweep_slots(&opts).jobs().collect();
		assert!(!sweeps.contains(&Job::SystemMetricsRefresh));
	}

	/// Every job's metric name is distinct, so two jobs can never share a
	/// series.
	#[test]
	fn job_names_are_distinct() {
		let mut names: Vec<_> = Job::ALL.iter().map(|job| job.name()).collect();
		names.sort_unstable();
		names.dedup();
		assert_eq!(names.len(), Job::ALL.len());
	}

	#[test]
	fn maintenance_slots_registers_the_metrics_refresh_immediately() {
		let opts = EngineOptions::default();
		let slots = maintenance_slots(&opts);
		let metrics = slots
			.slots
			.iter()
			.find(|s| s.job == Job::SystemMetricsRefresh)
			.expect("the metrics refresh is always registered");
		// Due now, so a cold start reports real utilisation rather than zeroes.
		assert!(metrics.next_due <= Instant::now());
		// Every other job waits out one full interval.
		let others = slots.slots.iter().filter(|s| s.job != Job::SystemMetricsRefresh);
		for s in others {
			assert!(s.next_due > Instant::now(), "{} should not be due yet", s.job.label());
		}
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_runs_every_registered_job() {
		let passes = record_passes(
			vec![
				slot(Job::NodeExpire, Duration::from_millis(5)),
				slot(Job::ChangefeedGc, Duration::from_millis(10)),
				slot(Job::NodeCleanup, Duration::from_millis(40)),
			],
			40,
		)
		.await;
		for job in [Job::NodeExpire, Job::ChangefeedGc, Job::NodeCleanup] {
			assert!(passes.contains(&job), "{} never ran: {passes:?}", job.label());
		}
		// Cadence is honoured, not just liveness: the 5ms job runs more often
		// than the 40ms one.
		let fast = passes.iter().filter(|j| **j == Job::NodeExpire).count();
		let slow = passes.iter().filter(|j| **j == Job::NodeCleanup).count();
		assert!(fast > slow, "5ms job ran {fast}x, 40ms job ran {slow}x");
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_does_not_let_one_job_monopolise_the_schedule() {
		// A pass that takes far longer than every interval leaves both jobs
		// permanently overdue, which is exactly when a scheduler starves one.
		let passes = record_passes_with_delay(
			vec![
				slot(Job::ReclaimTombstones, Duration::from_millis(5)),
				slot(Job::NodeExpire, Duration::from_millis(5)),
			],
			20,
			|job| {
				if matches!(job, Job::ReclaimTombstones) {
					Duration::from_millis(100)
				} else {
					Duration::ZERO
				}
			},
		)
		.await;
		for pair in passes.windows(2) {
			assert_ne!(
				pair[0].0, pair[1].0,
				"the same job ran twice in a row while the other was overdue: {passes:?}"
			);
		}
	}

	/// A job's next deadline is measured from when its pass finished, so a pass
	/// that overruns its interval still rests a full interval afterwards rather
	/// than being immediately due again.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_rests_a_full_interval_after_an_overrunning_pass() {
		let interval = Duration::from_millis(50);
		let cost = Duration::from_millis(100);
		let passes =
			record_passes_with_delay(vec![slot(Job::ReclaimTombstones, interval)], 4, |_| cost)
				.await;
		for pair in passes.windows(2) {
			let gap = pair[1].1.saturating_duration_since(pair[0].1);
			assert!(
				gap >= cost + interval,
				"passes started {gap:?} apart; the overrunning pass did not rest for {interval:?}"
			);
		}
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_stops_dispatching_once_cancelled() {
		// The recorder cancels on its third pass; the loop must break rather
		// than start a fourth.
		let passes = record_passes(
			vec![
				slot(Job::NodeExpire, Duration::from_millis(5)),
				slot(Job::ChangefeedGc, Duration::from_millis(5)),
			],
			3,
		)
		.await;
		assert_eq!(passes.len(), 3, "dispatched after cancellation: {passes:?}");
	}

	#[test]
	fn node_membership_update_exits_when_cancelled() {
		let should_continue = super::run_node_membership_update(
			Duration::from_secs(60),
			super::NodeMembershipUpdateResult::Cancelled,
		);
		assert!(!should_continue);
	}

	#[test]
	fn node_membership_update_continues_after_timeout() {
		let should_continue = super::run_node_membership_update(
			Duration::from_secs(60),
			super::NodeMembershipUpdateResult::TimedOut,
		);
		assert!(should_continue);
	}

	#[test]
	fn node_membership_update_continues_after_success() {
		let should_continue = super::run_node_membership_update(
			Duration::from_secs(60),
			super::NodeMembershipUpdateResult::Updated,
		);
		assert!(should_continue);
	}

	#[test]
	fn node_membership_update_continues_after_error() {
		let should_continue = super::run_node_membership_update(
			Duration::from_secs(60),
			super::NodeMembershipUpdateResult::Failed(anyhow::anyhow!("update failed")),
		);
		assert!(should_continue);
	}

	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn tasks_complete() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn tasks_complete_channel_closed() {
		let can = CancellationToken::new();
		// Tick fast enough that passes are genuinely in flight when the
		// cancellation lands, rather than cancelling before anything has run.
		let opt = EngineOptions::default()
			.with_node_membership_refresh_interval(Duration::from_millis(10))
			.with_node_membership_check_interval(Duration::from_millis(10))
			.with_index_compaction_interval(Duration::from_millis(10))
			.with_event_processing_interval(Duration::from_millis(10));
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		tokio::time::sleep(Duration::from_millis(200)).await;
		can.cancel();
		tokio::time::timeout(Duration::from_secs(10), tasks.resolve())
			.await
			.map_err(|e| format!("Timed out after {e}"))
			.unwrap()
			.map_err(|e| format!("Resolution failed: {e}"))
			.unwrap();
	}

	/// The heartbeat must keep its cadence while the rest of the engine's
	/// background work is running, because a stale heartbeat gets this node
	/// archived by another member's expiry scan.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn heartbeat_keeps_its_cadence_alongside_other_tasks() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default()
			.with_node_membership_refresh_interval(Duration::from_millis(50));
		let dbs = Datastore::new("memory").await.unwrap();
		dbs.insert_node().await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		tokio::time::sleep(Duration::from_millis(500)).await;
		let age = dbs.node_heartbeat_age().await.unwrap().expect("the node was registered");
		can.cancel();
		tasks.resolve().await.unwrap();
		assert!(age < Duration::from_millis(500), "heartbeat was {age:?} stale");
	}

	/// However many datastores a process builds, one of them refreshes the
	/// process metrics: the snapshot is a single per-process cache whose CPU
	/// percentage is a delta since its own previous refresh, so a second
	/// refresher would cut that window short by an amount that depends on
	/// nothing but scheduling.
	///
	/// The claim is process-wide, so every datastore this test binary builds
	/// contends for it: the holder that ends the wait below may be one of those
	/// rather than one of these two. What is asserted is the property itself —
	/// that a scheduler holds the claim and a further holder is refused.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn only_one_datastore_refreshes_the_process_metrics() {
		let opts = EngineOptions::default()
			.with_system_metrics_refresh_interval(Duration::from_millis(10));
		let _first =
			Datastore::builder().with_engine_options(opts).build_with_path("memory").await.unwrap();
		let _second =
			Datastore::builder().with_engine_options(opts).build_with_path("memory").await.unwrap();
		// A scheduler takes the claim on a pass of its own task, so wait for one
		// rather than assuming it has already run. A probe that wins the race
		// against a pass holds the claim only for the length of the check, and
		// the next pass takes it back.
		tokio::time::timeout(Duration::from_secs(30), async {
			while RefreshClaim::process().take() {
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.expect("no scheduler holds the metrics claim, so every datastore refreshes");
	}

	/// The claim on the process metrics refresh belongs to the scheduler that
	/// carries the refresh, not to the runner every task shares: once that
	/// scheduler ends, the claim is free for another datastore in the process
	/// however long a task still holding a runner — a drain inside a
	/// user-defined event that outlived the shutdown wait — goes on running.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn the_metrics_claim_ends_with_the_scheduler_that_holds_it() {
		use std::sync::atomic::AtomicBool;

		use super::{Runner, spawn_task_scheduler};

		static SLOT: AtomicBool = AtomicBool::new(false);
		let can = CancellationToken::new();
		let opts =
			EngineOptions::default().with_system_metrics_refresh_interval(Duration::from_millis(5));
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.with_engine_options(opts)
			.build_with_path("memory")
			.await
			.unwrap();
		dbs.check_version().await.unwrap();
		let runner = Runner::new(&dbs, can.clone(), &opts);
		// Stands in for every other task: a drain that outlives the scheduler
		// holds exactly this.
		let outliving = runner.clone();
		let scheduler = spawn_task_scheduler(
			"maintenance",
			Schedule::new(&[Job::SystemMetricsRefresh], &opts, Instant::now()),
			runner,
			Some(RefreshClaim::on(&SLOT)),
		);
		// A probe that wins the race against a pass holds the claim only for
		// the length of the check, and the next pass takes it back.
		tokio::time::timeout(Duration::from_secs(30), async {
			while RefreshClaim::on(&SLOT).take() {
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		})
		.await
		.expect("the scheduler never took the claim");

		can.cancel();
		scheduler.await.unwrap();
		assert!(
			RefreshClaim::on(&SLOT).take(),
			"the claim must end with the scheduler, not with the runner"
		);
		drop(outliving);
	}

	/// The live-query router is the only conditionally-spawned task: under the
	/// default inline engine it has nothing to deliver, so it is not spawned.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn live_query_router_is_not_spawned_under_the_inline_engine() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		// heartbeat, write barrier, event processing, index compaction, graph
		// fold, table migration, maintenance, sweeps.
		assert_eq!(tasks.0.len(), 8);
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	/// A dedicated job's interval of zero disables it, as a scheduled job's
	/// does: its task is not spawned, rather than spawned to tick at a period
	/// its timer refuses.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_dedicated_job_with_a_zero_interval_is_not_spawned() {
		let can = CancellationToken::new();
		// Every dedicated job off: the heartbeat, the four drains and the
		// router, under the engine that would spawn it.
		let opt = EngineOptions::default()
			.with_node_membership_refresh_interval(Duration::ZERO)
			.with_event_processing_interval(Duration::ZERO)
			.with_index_compaction_interval(Duration::ZERO)
			.with_graph_fold_interval(Duration::ZERO)
			.with_table_migration_interval(Duration::ZERO)
			.with_live_query_router_interval(Duration::ZERO);
		let dbs = Datastore::builder()
			.with_config(
				surrealdb_cnf::ConfigMap::empty().with_key_value("live_query_engine", "router"),
			)
			.build_with_path("memory")
			.await
			.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		let groups = [super::maintenance_slots(&opt), super::sweep_slots(&opt)]
			.iter()
			.filter(|schedule| !schedule.is_empty())
			.count();
		assert_eq!(tasks.0.len(), groups, "only the scheduled groups are spawned");
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	/// A group whose every job is disabled must not leave an idle task behind.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_fully_disabled_group_is_not_spawned() {
		let can = CancellationToken::new();
		// Every maintenance job off; the sweep group is untouched.
		let opt = EngineOptions::default()
			.with_system_metrics_refresh_interval(Duration::ZERO)
			.with_node_membership_check_interval(Duration::ZERO)
			.with_node_membership_cleanup_interval(Duration::ZERO)
			.with_changefeed_gc_interval(Duration::ZERO)
			.with_live_query_gc_interval(Duration::ZERO)
			.with_index_build_resume_interval(Duration::ZERO)
			.with_diskann_writer_lease_release_interval(Duration::ZERO);
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		assert_eq!(tasks.0.len(), 7, "the empty maintenance group should not be spawned");
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	/// A host that drives maintenance itself gets one pass per call, is told
	/// when to call again, and is refused while the datastore already runs its
	/// own tasks.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_host_drives_one_pass_per_slice() {
		use super::Progress;

		let running = Datastore::new("memory").await.unwrap();
		assert!(
			running.run_maintenance_slice().await.is_err(),
			"a datastore running its own tasks must refuse a host-driven slice"
		);

		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		dbs.check_version().await.unwrap();
		// The metrics refresh is the one job due at registration, so the first
		// slice runs it and nothing else.
		let first = dbs.run_maintenance_slice().await.unwrap();
		assert_eq!(first.job, Some(Job::SystemMetricsRefresh));
		assert_eq!(first.progress, Progress::Done);
		let next = first.next_due.expect("more jobs are scheduled");
		assert!(!next.is_zero(), "every other job waits out its interval");
		// Nothing else is due yet: the second slice runs nothing and reports the
		// same wait.
		let second = dbs.run_maintenance_slice().await.unwrap();
		assert_eq!(second.job, None);
		assert!(second.next_due.is_some_and(|wait| wait <= next));
	}

	/// The first driver to start owns the datastore's maintenance for good:
	/// once a host has driven a slice, starting the tasks is refused and the
	/// host keeps its schedule, so no job can run under both.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_host_driven_datastore_refuses_its_own_tasks() {
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		dbs.check_version().await.unwrap();
		dbs.run_maintenance_slice().await.unwrap();
		assert!(
			dbs.start_maintenance_tasks().is_err(),
			"a host-driven datastore must refuse to start its own tasks"
		);
		assert!(!dbs.maintenance_tasks_running());
		dbs.run_maintenance_slice().await.expect("the host remains the driver");
	}

	/// A host-driven datastore answers no write barriers: the write fence they
	/// arm lapses unless a heartbeat commits within its window, which a host's
	/// schedule does not promise. A checked start is refused before it records
	/// anything, and the host's heartbeat records neither an answer nor a
	/// mark, so builds elsewhere take the node for one that predates the
	/// barrier rather than waiting on it.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_host_driven_datastore_answers_no_write_barriers() {
		use crate::key::schema::{WriteBarrierAckKey, WriteBarrierHeartbeatKey};
		use crate::kvs::TransactionType;

		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		dbs.check_version().await.unwrap();
		dbs.run_maintenance_slice().await.unwrap();
		assert!(
			dbs.start_maintenance_tasks_checked().await.is_err(),
			"a host-driven datastore must refuse a checked start of its own tasks"
		);
		assert!(!dbs.advertises_write_barrier(), "the refused start records nothing");

		// The heartbeat the host's schedule runs.
		super::heartbeat(&dbs, Duration::from_secs(5), &CancellationToken::new()).await.unwrap();
		assert!(!dbs.advertises_write_barrier());
		let txn = dbs.transaction(TransactionType::Read).await.unwrap();
		let mark = txn
			.get_key(
				&WriteBarrierHeartbeatKey {
					nd: dbs.id(),
				},
				None,
			)
			.await
			.unwrap();
		let answer = txn
			.get_key(
				&WriteBarrierAckKey {
					nd: dbs.id(),
				},
				None,
			)
			.await
			.unwrap();
		txn.cancel().await.unwrap();
		assert_eq!(mark, None, "the heartbeat is not marked as one from a node that answers");
		assert_eq!(answer, None, "the node records no answer");
	}

	/// A host's first slice is refused once a checked start of the datastore's
	/// own tasks has begun answering write barriers, so a host cannot take over
	/// a datastore that has begun recording that it answers them and leave
	/// every build elsewhere waiting for it.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_host_slice_is_refused_once_the_tasks_begin_answering_barriers() {
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		dbs.check_version().await.unwrap();
		dbs.begin_answering_write_barriers();
		assert!(dbs.run_maintenance_slice().await.is_err());
	}

	/// A default build is driven by its own tasks only where the
	/// `maintenance-tasks` feature started some: without the feature the
	/// builder leaves the datastore undriven, starting the tasks records none,
	/// and a host may drive the maintenance without having opted the tasks out
	/// first. With the feature, the same calls find the tasks running and the
	/// host-driven slice is refused.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_default_build_is_driven_by_its_tasks_only_with_the_feature() {
		let dbs = Datastore::new("memory").await.unwrap();
		let with_tasks = cfg!(feature = "maintenance-tasks");
		assert_eq!(
			dbs.maintenance_tasks_running(),
			with_tasks,
			"the builder records tasks only where the feature started some"
		);
		dbs.check_version().await.unwrap();
		dbs.start_maintenance_tasks().unwrap();
		assert_eq!(
			dbs.maintenance_tasks_running(),
			with_tasks,
			"starting the tasks records none in a build without the feature"
		);
		assert_eq!(
			dbs.run_maintenance_slice().await.is_ok(),
			!with_tasks,
			"a host may drive the maintenance exactly where no tasks run it"
		);
	}

	/// Records the passes the datastore reported, in order.
	#[cfg(feature = "kv-mem")]
	#[derive(Debug, Default)]
	struct PassRecorder(Mutex<Vec<surrealdb_observe::TaskPassEventSafe>>);

	#[cfg(feature = "kv-mem")]
	impl surrealdb_observe::ExecutionObserver for PassRecorder {
		fn on_task_pass(&self, event: &surrealdb_observe::TaskPassEvent) {
			self.0.lock().unwrap().push(event.safe);
		}
	}

	#[cfg(feature = "kv-mem")]
	impl PassRecorder {
		/// The jobs whose passes were reported, in order.
		fn jobs(&self) -> Vec<&'static str> {
			self.0.lock().unwrap().iter().map(|pass| pass.job).collect()
		}

		/// The last pass reported.
		fn last(&self) -> Option<surrealdb_observe::TaskPassEventSafe> {
			self.0.lock().unwrap().last().copied()
		}

		/// Waits for a pass of the metrics refresh, which is due the moment a
		/// schedule is built and so is the first pass a scheduler runs.
		async fn metrics_refresh_ran(&self) -> bool {
			let deadline = Instant::now() + Duration::from_secs(10);
			while Instant::now() < deadline {
				if self.jobs().contains(&Job::SystemMetricsRefresh.name()) {
					return true;
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
			false
		}
	}

	/// Gives a datastore built without its tasks the look of one last written
	/// before the semantic version stamp existed: the major version and no
	/// stamp, which owes this build every migration it ships.
	#[cfg(feature = "kv-mem")]
	async fn stamp_nothing_but_data(dbs: &Datastore) {
		use surrealdb_kvs::TransactionType::Write;

		use crate::key::schema::VersionKey;
		use crate::kvs::version::MajorVersion;

		let txn = dbs.transaction(Write).await.unwrap();
		txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
		txn.commit().await.unwrap();
	}

	/// Every task but the heartbeat waits for the datastore's migrations: on
	/// a datastore that owes them, the tasks run no other pass until
	/// `check_version` has run the migrations, and run their first once it has.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn the_tasks_wait_for_the_migrations_to_run() {
		let recorder = Arc::new(PassRecorder::default());
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.with_observer(Arc::clone(&recorder) as Arc<dyn surrealdb_observe::ExecutionObserver>)
			.build_with_path("memory")
			.await
			.unwrap();
		stamp_nothing_but_data(&dbs).await;
		dbs.start_maintenance_tasks().unwrap();
		// The metrics refresh is due the moment its schedule is built, so a
		// scheduler that did not wait would have run it by now.
		tokio::time::sleep(Duration::from_millis(200)).await;
		let heartbeat = Job::Heartbeat.name();
		let ran: Vec<_> = recorder.jobs().into_iter().filter(|job| *job != heartbeat).collect();
		assert!(ran.is_empty(), "a pass ran before the migrations had: {ran:?}");

		dbs.check_version().await.unwrap();
		assert!(recorder.metrics_refresh_ran().await, "no pass ran once the migrations had");
		dbs.shutdown().await.unwrap();
	}

	/// A host drives its jobs synchronously, so a slice before the migrations
	/// have run is refused rather than parked, and runs once they have.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_host_slice_is_refused_until_the_migrations_have_run() {
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		stamp_nothing_but_data(&dbs).await;
		assert!(
			dbs.run_maintenance_slice().await.is_err(),
			"a slice must be refused until the migrations have run"
		);

		dbs.check_version().await.unwrap();
		let first = dbs.run_maintenance_slice().await.unwrap();
		assert_eq!(first.job, Some(Job::SystemMetricsRefresh));
	}

	/// A datastore this process created owes no migration, so its tasks run
	/// without a `check_version`: the builder releases the wait itself.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_datastore_created_here_runs_its_maintenance_without_check_version() {
		let recorder = Arc::new(PassRecorder::default());
		let dbs = Datastore::builder()
			.with_observer(Arc::clone(&recorder) as Arc<dyn surrealdb_observe::ExecutionObserver>)
			.build_with_path("memory")
			.await
			.unwrap();
		assert!(
			recorder.metrics_refresh_ran().await,
			"the tasks waited on a migration it owes none of"
		);
		dbs.shutdown().await.unwrap();
	}

	/// A failure of the lease check itself is reported under no node's
	/// standing: the pass never learned whether it held the lease. Every
	/// other failure of a leased job is the holder's, and an unleased job's
	/// is its own.
	#[test]
	fn a_failed_lease_check_is_reported_under_no_nodes_standing() {
		use surrealdb_observe::TaskLeaseState;

		use crate::kvs::tasklease::LeaseCheckFailed;

		let check = anyhow::anyhow!("storage unreachable").context(LeaseCheckFailed);
		let work = anyhow::anyhow!("storage unreachable");
		let adoption = Job::EventAdoption.lease_on_failure();
		assert_eq!(failure_lease(&check, adoption), TaskLeaseState::Unknown);
		assert_eq!(failure_lease(&work, adoption), TaskLeaseState::Held);
		assert_eq!(
			failure_lease(&work, Job::EventProcessing.lease_on_failure()),
			TaskLeaseState::NotLeased
		);
		// The mark holds through further context on the way up.
		let wrapped = check.context("adopting queues");
		assert_eq!(failure_lease(&wrapped, adoption), TaskLeaseState::Unknown);
	}

	/// A pass that failed is reported with what it attached to its error: the
	/// items it handled and the queue's age before the failure, and a
	/// backlog, since it stopped short of the end of its work; under its own
	/// lease standing, unless the lease check itself failed, which is nobody's
	/// standing however far the pass got. A failure with no pass attached
	/// reports no counts, under the job's standing on failure.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	async fn a_failed_pass_is_reported_with_what_it_did_before_failing() {
		use surrealdb_observe::{Outcome, TaskLeaseState};

		use crate::kvs::tasklease::LeaseCheckFailed;

		let recorder = Arc::new(PassRecorder::default());
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.with_observer(Arc::clone(&recorder) as Arc<dyn surrealdb_observe::ExecutionObserver>)
			.build_with_path("memory")
			.await
			.unwrap();
		let mut pass = Pass::done(TaskLeaseState::Held);
		pass.items = 4;
		pass.ran = 3;
		pass.observe_age(Duration::from_secs(7));

		let failed = pass.failed(anyhow::anyhow!("storage unreachable"));
		observe_pass(&dbs, Job::EventAdoption, Duration::ZERO, Err(&failed));
		let event = recorder.last().unwrap();
		assert_eq!(event.outcome, Outcome::Error);
		assert_eq!(event.items, 4, "the items handled before the failure are reported");
		assert_eq!(event.oldest_age, Some(Duration::from_secs(7)));
		assert!(event.backlog, "a pass that failed stopped short of its work's end");
		assert_eq!(event.lease, TaskLeaseState::Held, "under the standing the pass had");

		let check = pass.failed(anyhow::anyhow!("storage unreachable").context(LeaseCheckFailed));
		observe_pass(&dbs, Job::EventAdoption, Duration::ZERO, Err(&check));
		let event = recorder.last().unwrap();
		assert_eq!(event.items, 4);
		assert_eq!(event.lease, TaskLeaseState::Unknown, "a failed lease check is nobody's");

		let bare = anyhow::anyhow!("storage unreachable");
		observe_pass(&dbs, Job::EventAdoption, Duration::ZERO, Err(&bare));
		let event = recorder.last().unwrap();
		assert_eq!((event.items, event.oldest_age, event.backlog), (0, None, false));
		assert_eq!(event.lease, TaskLeaseState::Held, "the job's standing on failure");
	}
}
