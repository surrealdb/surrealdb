use core::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use common::time::{Instant, MissedTickBehavior, sleep};
use futures::StreamExt;
use surrealdb_datastore::triggers::CommitTriggers;
use surrealdb_types::Error;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tokio_util::sync::CancellationToken;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

use crate::err::{is_query_cancelled, is_query_timedout};
use crate::kvs::{Datastore, LiveQueryEngine};
use crate::observe::process::RefreshClaim;
use crate::options::EngineOptions;

mod interval;

use self::interval::IntervalStream;

#[cfg(not(target_family = "wasm"))]
type Task = Pin<Box<dyn Future<Output = Result<(), tokio::task::JoinError>> + Send + 'static>>;

#[cfg(target_family = "wasm")]
type Task = Pin<Box<()>>;

/// Spawns `fut` on the ambient runtime and returns a handle the caller can await
/// to observe the task finishing.
///
/// Awaiting the returned [`Task`] is only meaningful off wasm. `spawn_local`
/// yields no join handle, so on wasm the task is detached and the returned
/// `Task` completes immediately without waiting for `fut`.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn into_task<F>(fut: F) -> Task
where
	F: Future<Output = ()> + Send + 'static,
{
	Box::pin(spawn(fut))
}

#[cfg(target_family = "wasm")]
pub(crate) fn into_task<F>(fut: F) -> Task
where
	F: Future<Output = ()> + 'static,
{
	spawn(fut);
	Box::pin(())
}

/// The most attempts one heartbeat tick will make.
///
/// The tick's budget is the real bound — an attempt that times out consumes all
/// of it and ends the tick. This bounds the other case: a write that fails in
/// milliseconds returns nearly the whole budget, and without a cap a write
/// failing instantly and forever would spin.
const MAX_NODE_MEMBERSHIP_UPDATE_ATTEMPTS: u32 = 3;

/// The smallest budget a heartbeat tick will run on.
///
/// The budget is otherwise derived from the staleness window, and a short
/// configured window derives one no storage write could finish in — which would
/// fail every attempt by construction rather than bounding one.
const MIN_NODE_MEMBERSHIP_TICK_BUDGET: Duration = Duration::from_millis(100);

/// The longest one heartbeat tick may spend, however wide the staleness window.
///
/// A window of [`Duration::MAX`] is the documented degradation of an interval
/// too large to derive from, and means "never considered stale" — but a tick
/// still has to produce a deadline the clock can represent. Past this bound
/// waiting longer inside one tick buys nothing: a node-registration write that
/// has not completed by now is not going to, and the next tick retries anyway.
const MAX_NODE_MEMBERSHIP_TICK_BUDGET: Duration = Duration::from_secs(300);

/// How long one heartbeat tick may spend on its attempts.
///
/// The heartbeat value is stamped before the write that carries it, so a write
/// taking `L` leaves the row already `L` old the moment it lands, and the next
/// replacement cannot arrive sooner than that next write's own `L`. The age
/// therefore peaks at about `2L`, and a node stays ready only while
/// `2L <= window` — so a write slower than half the window cannot keep this node
/// ready however patient the tick is. Admitting one would spend the tick on a
/// write whose success no longer helps, so the budget stops at `window / 2`.
///
/// One refresh interval is also held back, so a tick that spends its whole
/// budget without landing still leaves the next tick room inside the window.
/// Whichever of the two binds first wins.
///
/// Floored at [`MIN_NODE_MEMBERSHIP_TICK_BUDGET`] so a window narrower than the
/// interval still gets one real attempt rather than none, and capped at
/// [`MAX_NODE_MEMBERSHIP_TICK_BUDGET`] so the deadline stays representable.
fn node_membership_tick_budget(interval: Duration, max_heartbeat_age: Duration) -> Duration {
	max_heartbeat_age
		.saturating_sub(interval)
		.min(max_heartbeat_age / 2)
		.max(MIN_NODE_MEMBERSHIP_TICK_BUDGET)
		.min(MAX_NODE_MEMBERSHIP_TICK_BUDGET)
}

/// How long a trigger-driven index-compaction pass waits before running.
///
/// The compaction interval is the floor on how *stale* the queue may get; this
/// is the floor on how *often* a write may start a pass. It bounds the cost of
/// the wake-up path — most importantly the lease check on nodes that do not
/// hold the compaction lease, which returns almost immediately and would
/// otherwise spin for as long as writes keep arriving — and batches the commits
/// that land during the wait into a single pass.
const INDEX_COMPACTION_TRIGGER_DEBOUNCE: Duration = Duration::from_millis(250);

enum NodeMembershipUpdateResult {
	Updated,
	Cancelled,
	TimedOut,
	Failed(anyhow::Error),
}

pub struct Tasks(#[cfg_attr(target_family = "wasm", expect(dead_code))] Vec<Task>);

impl Tasks {
	/// Adds a task to this set, so it is joined with the rest at shutdown.
	///
	/// Only compiled off wasm, where a [`Task`] carries no join handle and the
	/// set is never awaited.
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn push(&mut self, task: Task) {
		self.0.push(task);
	}

	#[cfg(target_family = "wasm")]
	pub async fn resolve(self) -> Result<(), Error> {
		Ok(())
	}
	#[cfg(not(target_family = "wasm"))]
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
/// - **Async event processing** and **index compaction** each keep their own task because both are
///   driven by the write path and drain their queue to empty, so under sustained load they do not
///   return between ticks.
/// - The remaining jobs run on two [`spawn_task_scheduler`] instances, split by whether one pass
///   has a bound. [`maintenance_slots`] carries the jobs whose cost is bounded by catalog size, so
///   none can delay a peer by more than one short pass. [`sweep_slots`] carries the two whose queue
///   is nearly always empty but whose individual entries are not bounded: reclaiming one tombstone
///   destroys an entire namespace or database prefix, and a session purge pages the whole session
///   keyspace opening a write transaction per expired entry. Keeping the groups apart means a long
///   sweep cannot stall the short jobs — in particular it cannot leave the cached process metrics
///   stale for its duration.
/// - The **live-query router** is spawned only under [`LiveQueryEngine::Router`]; under the default
///   inline engine it has nothing to deliver.
///
/// Every task holds a [`Weak`] reference to the datastore, never a strong one.
/// The datastore owns these handles, so a strong reference would close a cycle
/// through them and the datastore could never be dropped. Each pass upgrades
/// for the duration of that pass and exits the task once the upgrade fails,
/// which is what lets an embedder that simply drops its datastore — rather than
/// calling [`Datastore::shutdown`] — still wind the tasks down.
///
/// The datastore starts these itself, so the only reason to call this directly
/// is to start them against a datastore built with
/// [`Builder::without_maintenance_tasks`](crate::kvs::ds::builder::Builder::without_maintenance_tasks).
pub fn init(dbs: &Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	let weak = Arc::downgrade(dbs);
	// The triggers are shared state in their own right, so a task holding them
	// does not keep the datastore alive.
	let triggers = dbs.commit_triggers();
	let mut tasks = Vec::with_capacity(6);
	tasks.push(spawn_task_node_membership_refresh(Weak::clone(&weak), canceller.clone(), opts));
	tasks.push(spawn_task_event_processing(
		Weak::clone(&weak),
		Arc::clone(triggers),
		canceller.clone(),
		opts,
	));
	tasks.push(spawn_task_index_compaction(
		Weak::clone(&weak),
		Arc::clone(triggers),
		canceller.clone(),
		opts,
	));
	for (group, slots) in [("maintenance", maintenance_slots(opts)), ("sweep", sweep_slots(opts))] {
		// Every job in a group can be disabled by interval, so a group can end up
		// empty; spawning a task that immediately exits would serve no purpose.
		if !slots.is_empty() {
			tasks.push(spawn_task_scheduler(
				group,
				slots,
				Weak::clone(&weak),
				canceller.clone(),
				opts,
			));
		}
	}
	if dbs.live_query_engine() == LiveQueryEngine::Router {
		tasks.push(spawn_task_live_query_router(weak, canceller, opts));
	}
	Tasks(tasks)
}

/// Spawns the per-node live-query router task.
///
/// Tails the dedicated `lqe` keyspace and delivers notifications off the write
/// path. The cadence bounds steady-state delivery latency, so it ticks
/// frequently and keeps its own task rather than queueing behind maintenance
/// work. Only spawned under [`LiveQueryEngine::Router`].
fn spawn_task_live_query_router(
	dbs: Weak<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.live_query_router_interval;
	into_task(async move {
		trace!("Running the live-query router every {interval:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					let Some(dbs) = dbs.upgrade() else { break };
					if let Err(e) = dbs.live_query_router_process().await {
						error!("Error running the live-query router: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running the live-query router");
	})
}

fn spawn_task_node_membership_refresh(
	dbs: Weak<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.node_membership_refresh_interval;
	// How stale the heartbeat may get before this node is unhealthy, which is
	// what bounds how long one tick may spend trying to refresh it.
	let max_heartbeat_age = opts.resolved_readiness_heartbeat_max_age();
	// Spawn a future
	into_task(async move {
		// Log the interval frequency
		trace!("Updating node registration information every {interval:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(interval).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					let Some(dbs) = dbs.upgrade() else { break };
					if !run_node_membership_update(interval, max_heartbeat_age, |budget| {
						update_node_membership(&dbs, &canceller, budget)
					}).await {
						break;
					}
				}
			}
		}
		trace!("Background task exited: Updating node registration information");
	})
}

/// Spawns a background task for index compaction
///
/// This function creates a background task that periodically runs the index
/// compaction process. The compaction process optimizes indexes (particularly
/// full-text indexes) by consolidating changes and removing unnecessary data,
/// which helps maintain query performance over time.
///
/// The task runs at the interval specified by `opts.index_compaction_interval`.
/// It keeps its own task rather than joining the maintenance scheduler because
/// the queue is fed by the write path and each pass drains it to empty, so under
/// sustained indexed writes a pass does not return between ticks.
///
/// # Arguments
///
/// * `dbs` - The datastore instance
/// * `triggers` - The commit wake-ups, so a write can start a pass early
/// * `canceller` - Token used to cancel the task when the engine is shutting down
/// * `opts` - Engine options containing the compaction interval
///
/// # Returns
///
/// * A pinned task that can be awaited
fn spawn_task_index_compaction(
	dbs: Weak<Datastore>,
	triggers: Arc<CommitTriggers>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.index_compaction_interval;
	// Spawn a future
	into_task(async move {
		// Log the interval frequency
		trace!("Running index compaction every {interval:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(interval).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Wake early when a commit queues compaction work, so the queue
				// drains at the write rate rather than at this task's cadence.
				// Nothing otherwise relates the two, and the gap between them is
				// what lets a count index's delta log — which every read of that
				// index sums — grow without bound.
				_ = triggers.index_compaction.notified() => {
					// Debounce before running. A node that does not hold the
					// compaction lease returns from a pass almost immediately,
					// so honouring every notification would spin on the lease
					// check for as long as writes keep arriving. The wait also
					// batches the commits landing in the meantime into one pass.
					tokio::select! {
						biased;
						_ = canceller.cancelled() => break,
						_ = sleep(INDEX_COMPACTION_TRIGGER_DEBOUNCE) => {}
					}
					let Some(dbs) = dbs.upgrade() else { break };
					if let Err(e) =
						Datastore::index_compaction(dbs, interval, canceller.clone()).await
					{
						if canceller.is_cancelled() {
							break;
						}
						error!("Error running index compaction: {e}");
					}
				}
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					let Some(dbs) = dbs.upgrade() else { break };
					if let Err(e) =
						Datastore::index_compaction(dbs, interval, canceller.clone()).await
					{
						if canceller.is_cancelled() {
							break;
						}
						error!("Error running index compaction: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running index compaction");
	})
}

/// Spawns the async event processing task.
///
/// Keeps its own task rather than joining the maintenance scheduler for two
/// reasons: the queue is fed by the write path and each pass drains it to empty,
/// and the events themselves run user-defined SurrealQL of unbounded duration.
fn spawn_task_event_processing(
	dbs: Weak<Datastore>,
	triggers: Arc<CommitTriggers>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.event_processing_interval;
	// Spawn a future
	into_task(async move {
		// Log the interval frequency
		trace!("Running event processing every {interval:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(interval).await;
		// Reports whether the datastore is still alive; a `false` ends the task.
		let process_events = async || {
			let Some(dbs) = dbs.upgrade() else {
				return false;
			};
			// The pass stops at the next batch boundary once cancelled, so a
			// shutdown does not wait out a queue the write path keeps refilling.
			if let Err(e) = dbs.event_processing(interval, &canceller).await
				&& !canceller.is_cancelled()
			{
				error!("Error running event processing: {e}");
			}
			true
		};
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Wake early when new async events are committed.
				_ = triggers.async_event.notified() => if !process_events().await { break },
				// Receive a notification on the channel
				Some(_) = ticker.next() => if !process_events().await { break }
			}
		}
		trace!("Background task exited: Running event processing");
	})
}

// --------------------------------------------------
// Maintenance scheduler
// --------------------------------------------------

/// A periodic maintenance job multiplexed onto a shared scheduler task.
///
/// Every variant runs on a cadence of seconds to minutes. They are split across
/// two schedulers by whether a single pass is bounded — [`maintenance_slots`]
/// versus [`sweep_slots`] — so an unbounded sweep cannot stall the short jobs.
/// Work that cannot share a task at all — the cluster heartbeat, async event
/// processing, index compaction and the live-query router — keeps its own; see
/// [`init`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceJob {
	/// Archive cluster members whose heartbeat has gone stale.
	NodeExpire,
	/// Delete archived members and garbage-collect their live queries.
	NodeCleanup,
	/// Garbage-collect expired changefeed data.
	ChangefeedGc,
	/// Destroy data left behind by `REMOVE NAMESPACE/DATABASE/INDEX`.
	ReclaimTombstones,
	/// Adopt index builds stranded by a crashed or expired owner node.
	ResumeIndexBuilds,
	/// Purge expired durable RPC sessions.
	RpcSessionGc,
	/// Advance the TiKV MVCC garbage-collection safepoint.
	TikvGc,
	/// Resolve stale TiKV transactional locks.
	TikvLockCleanup,
	/// Refresh the cached process/system utilisation metrics.
	SystemMetricsRefresh,
}

impl MaintenanceJob {
	/// Every variant, used to assert the two schedules partition the job set.
	/// Must list all of them; a new variant belongs here and in exactly one of
	/// [`maintenance_slots`] / [`sweep_slots`].
	#[cfg(test)]
	const ALL: [Self; 9] = [
		Self::NodeExpire,
		Self::NodeCleanup,
		Self::ChangefeedGc,
		Self::ReclaimTombstones,
		Self::ResumeIndexBuilds,
		Self::RpcSessionGc,
		Self::TikvGc,
		Self::TikvLockCleanup,
		Self::SystemMetricsRefresh,
	];

	/// Description used in the scheduler's log lines.
	fn label(self) -> &'static str {
		match self {
			Self::NodeExpire => "inactive node expiry",
			Self::NodeCleanup => "archived node cleanup",
			Self::ChangefeedGc => "changefeed garbage collection",
			Self::ReclaimTombstones => "tombstone reclaim",
			Self::ResumeIndexBuilds => "stalled index build recovery",
			Self::RpcSessionGc => "expired RPC session purge",
			Self::TikvGc => "TiKV MVCC GC",
			Self::TikvLockCleanup => "TiKV lock cleanup",
			Self::SystemMetricsRefresh => "system metrics refresh",
		}
	}
}

/// One job's place in the schedule.
struct Slot {
	job: MaintenanceJob,
	interval: Duration,
	next_due: Instant,
}

/// The jobs whose cost per pass is bounded by catalog size.
///
/// Each of these reads a fixed number of catalog rows, or a queue that a single
/// pass empties in bounded work, so they can share one task without delaying a
/// peer by more than one short pass.
fn maintenance_slots(opts: &EngineOptions) -> Vec<Slot> {
	slots(&[
		(MaintenanceJob::SystemMetricsRefresh, opts.system_metrics_refresh_interval),
		(MaintenanceJob::NodeExpire, opts.node_membership_check_interval),
		(MaintenanceJob::NodeCleanup, opts.node_membership_cleanup_interval),
		(MaintenanceJob::ChangefeedGc, opts.changefeed_gc_interval),
		(MaintenanceJob::ResumeIndexBuilds, opts.index_build_resume_interval),
		(MaintenanceJob::TikvGc, opts.tikv_gc_interval),
		(MaintenanceJob::TikvLockCleanup, opts.tikv_lock_cleanup_interval),
	])
}

/// The jobs whose queue is nearly always empty but whose entries are unbounded.
///
/// `reclaim_tombstones` destroys a whole namespace or database prefix per queue
/// entry, and `purge_expired_rpc_sessions` pages the entire session keyspace,
/// opening a write transaction for every expired entry. Both are therefore a
/// no-op scan in the steady state and arbitrarily long right after a `REMOVE` or
/// on a busy durable-session deployment. They share a task with each other, not
/// with the bounded jobs.
fn sweep_slots(opts: &EngineOptions) -> Vec<Slot> {
	slots(&[
		(MaintenanceJob::ReclaimTombstones, opts.reclaim_interval),
		(MaintenanceJob::RpcSessionGc, opts.rpc_session_gc_interval),
	])
}

/// Builds a schedule from the given job/interval pairs.
///
/// A zero interval leaves the job unregistered, which is how the documented
/// "set to zero to disable" options are honoured. Zero is not a valid tick
/// period for any job, so treating it uniformly as "disabled" also avoids
/// turning a misconfigured interval into a busy loop.
fn slots(jobs: &[(MaintenanceJob, Duration)]) -> Vec<Slot> {
	// One `now` for the whole schedule, so jobs sharing an interval share a
	// deadline exactly and registration order — not nanosecond skew between
	// clock reads — decides which of them runs first.
	let now = Instant::now();
	jobs.iter()
		.copied()
		.filter(|(_, interval)| !interval.is_zero())
		.map(|(job, interval)| Slot {
			job,
			interval,
			// The metrics refresh is due immediately so a cold start reports real
			// utilisation rather than zeroes. Every other job waits out one full
			// interval, which keeps startup from firing several lease acquisitions
			// at the same moment.
			next_due: match job {
				MaintenanceJob::SystemMetricsRefresh => now,
				_ => now + interval,
			},
		})
		.collect()
}

/// Index of the slot to run next: the earliest deadline, and among equal
/// deadlines the one registered first (`min_by_key` yields the first minimum).
///
/// An overdue job sorts ahead of one whose deadline was just set past the
/// current instant, so a saturated schedule drains oldest-first and a job that
/// has just run cannot immediately run again while a peer is waiting.
fn next_slot(slots: &[Slot]) -> Option<usize> {
	slots.iter().enumerate().min_by_key(|(_, s)| s.next_due).map(|(i, _)| i)
}

/// Runs one pass of `job`, logging and swallowing its error so that a failing
/// job cannot stop the others. Cancellation is not an error.
async fn run_maintenance_job(
	job: MaintenanceJob,
	dbs: &Arc<Datastore>,
	canceller: &CancellationToken,
	opts: &EngineOptions,
	reclaim_grace: Duration,
	metrics_claim: &RefreshClaim,
) {
	let res = match job {
		MaintenanceJob::NodeExpire => dbs.expire_nodes().await,
		MaintenanceJob::NodeCleanup => dbs.remove_nodes().await,
		MaintenanceJob::ChangefeedGc => dbs.changefeed_process(&opts.changefeed_gc_interval).await,
		MaintenanceJob::ReclaimTombstones => Datastore::reclaim_tombstones(
			Arc::clone(dbs),
			opts.reclaim_interval,
			reclaim_grace,
			canceller.clone(),
		)
		.await
		.map(|_| ()),
		MaintenanceJob::ResumeIndexBuilds => dbs
			.resume_stalled_index_builds(opts.index_build_resume_interval, canceller.clone())
			.await
			.map(|_| ()),
		MaintenanceJob::RpcSessionGc => {
			dbs.purge_expired_rpc_sessions(&opts.rpc_session_gc_interval).await
		}
		MaintenanceJob::TikvGc => dbs.run_mvcc_gc(opts.tikv_gc_lifetime).await,
		MaintenanceJob::TikvLockCleanup => dbs.run_lock_cleanup(opts.tikv_gc_lifetime).await,
		MaintenanceJob::SystemMetricsRefresh => {
			// The snapshot is one cache per process, and its CPU percentage is a
			// delta since the previous refresh of it, so only the datastore
			// holding the process-wide claim refreshes; the others let their
			// pass go by. Retried every pass, so the refresh moves to a
			// surviving datastore when the holder's task ends.
			if metrics_claim.take() {
				crate::observe::refresh_process_snapshot().await;
			}
			Ok(())
		}
	};
	if let Err(e) = res
		&& !canceller.is_cancelled()
	{
		error!("Error running {}: {e}", job.label());
	}
}

/// Drives the schedule until cancelled, running one job per iteration.
///
/// Dispatch is injected so the scheduling behaviour can be exercised without a
/// datastore.
async fn maintenance_loop<F, Fut>(mut slots: Vec<Slot>, canceller: CancellationToken, run: F)
where
	F: Fn(MaintenanceJob) -> Fut,
	Fut: Future<Output = ()>,
{
	while let Some(i) = next_slot(&slots) {
		let delay = slots[i].next_due.saturating_duration_since(Instant::now());
		tokio::select! {
			biased;
			_ = canceller.cancelled() => break,
			_ = sleep(delay) => {}
		}
		run(slots[i].job).await;
		// Measure the next deadline from completion, not from the deadline just
		// met. A pass that overruns its interval therefore still rests for a
		// full interval instead of coming due the moment it returns, and its
		// deadline cannot drift permanently into the past.
		slots[i].next_due = Instant::now() + slots[i].interval;
	}
}

/// Spawns one task that runs the given group of jobs on a shared schedule.
///
/// `group` names the group in the task's log lines so an operator can tell the
/// two schedulers apart.
fn spawn_task_scheduler(
	group: &'static str,
	slots: Vec<Slot>,
	dbs: Weak<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let opts = *opts;
	// Clamp the grace up to at least the TiKV GC lifetime: on TiKV,
	// `unsafe_destroy_range` bypasses MVCC, so data must not be reclaimed while a
	// snapshot older than the GC safepoint (`now - tikv_gc_lifetime`) could still
	// read it. Deriving the effective grace here means a longer `--tikv-gc-lifetime`
	// can never be undercut by leaving `--reclaim-grace` at its default.
	let reclaim_grace = opts.reclaim_grace.max(opts.tikv_gc_lifetime);
	into_task(async move {
		trace!(
			"Running {} {group} jobs on a shared schedule: {}",
			slots.len(),
			slots.iter().map(|s| s.job.label()).collect::<Vec<_>>().join(", ")
		);
		let jobs_canceller = canceller.clone();
		// This task's holder of the process-wide metrics claim: taken by its
		// first refresh pass, and released when the task ends or is dropped so
		// another datastore can take over. Only the group carrying that job
		// ever takes it.
		let metrics_claim = RefreshClaim::process();
		maintenance_loop(slots, canceller, move |job| {
			let dbs = Weak::clone(&dbs);
			let canceller = jobs_canceller.clone();
			let metrics_claim = metrics_claim.clone();
			async move {
				// The datastore is gone, so there is nothing left to maintain.
				// Cancelling ends the schedule on its next iteration, and does the
				// same for every other task sharing this token.
				let Some(dbs) = dbs.upgrade() else {
					canceller.cancel();
					return;
				};
				run_maintenance_job(job, &dbs, &canceller, &opts, reclaim_grace, &metrics_claim)
					.await
			}
		})
		.await;
		trace!("Background task exited: Running {group} jobs");
	})
}

async fn update_node_membership(
	dbs: &Datastore,
	canceller: &CancellationToken,
	timeout_duration: Duration,
) -> NodeMembershipUpdateResult {
	match dbs.update_node_with_timeout(timeout_duration, canceller).await {
		Ok(()) => NodeMembershipUpdateResult::Updated,
		Err(e) if is_query_cancelled(&e) => NodeMembershipUpdateResult::Cancelled,
		Err(e) if is_query_timedout(&e) => NodeMembershipUpdateResult::TimedOut,
		Err(e) => NodeMembershipUpdateResult::Failed(e),
	}
}

/// Runs one heartbeat tick: attempts the node-registration write, retrying
/// until it lands or the tick has spent its budget.
///
/// Every attempt is handed all the budget the tick has left, so the write a
/// tick can service is as slow as its whole budget. Nothing is spent on a
/// shorter probe first: the heartbeat value is stamped before the write that
/// carries it, so a probe that fails does not just waste time, it adds its own
/// duration to how old the row is when the next one finally lands.
///
/// Retrying still costs nothing in the case that matters. An attempt that times
/// out has consumed the budget, so the tick ends; one that fails in
/// milliseconds — a write conflict, say — returns almost all of it, and the
/// next attempt starts immediately with nearly the full budget.
///
/// `attempt` is handed the budget for that attempt and must not outlive it.
/// Attempts are sequential — the next one starts only once the previous has
/// returned, so a tick never has two registration writes in flight.
///
/// Returns whether the heartbeat task should keep running: `false` only for
/// [`NodeMembershipUpdateResult::Cancelled`], which means shutdown and is
/// therefore never retried. Every other outcome is transient, so an exhausted
/// tick still returns `true` and the next tick tries again.
async fn run_node_membership_update<F, Fut>(
	interval: Duration,
	max_heartbeat_age: Duration,
	mut attempt: F,
) -> bool
where
	F: FnMut(Duration) -> Fut,
	Fut: Future<Output = NodeMembershipUpdateResult>,
{
	let budget = node_membership_tick_budget(interval, max_heartbeat_age);
	// `checked_add` because a caller may hand us any window; the budget is capped
	// so this resolves in practice, and a clock that cannot represent the deadline
	// falls back to spending the budget on a single attempt rather than panicking.
	let deadline = Instant::now().checked_add(budget);
	let mut last_failure = None;
	let mut attempts = 0;
	while attempts < MAX_NODE_MEMBERSHIP_UPDATE_ATTEMPTS {
		let remaining = match deadline {
			Some(deadline) => deadline.saturating_duration_since(Instant::now()),
			None => budget,
		};
		if remaining.is_zero() {
			break;
		}
		attempts += 1;
		match attempt(remaining).await {
			NodeMembershipUpdateResult::Updated => return true,
			NodeMembershipUpdateResult::Cancelled => return false,
			NodeMembershipUpdateResult::TimedOut => {
				last_failure = Some(NodeMembershipUpdateResult::TimedOut);
			}
			NodeMembershipUpdateResult::Failed(e) => {
				last_failure = Some(NodeMembershipUpdateResult::Failed(e));
			}
		}
		if deadline.is_none() {
			break;
		}
	}
	// One line per exhausted tick, reporting the outcome that ended it: a
	// per-attempt line would multiply the log by the attempt count while a stall
	// lasts.
	match last_failure {
		Some(NodeMembershipUpdateResult::TimedOut) => {
			warn!("Timed out updating node registration information after {attempts} attempts");
		}
		Some(NodeMembershipUpdateResult::Failed(e)) => {
			error!("Error updating node registration information after {attempts} attempts: {e}");
		}
		_ => {}
	}
	true
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

	// Used only by the tests that build a datastore, so it is gated with them:
	// the lib tests build under `-D warnings` once per kv backend.
	#[cfg(feature = "kv-mem")]
	use super::RefreshClaim;
	use super::{
		MAX_NODE_MEMBERSHIP_TICK_BUDGET, MAX_NODE_MEMBERSHIP_UPDATE_ATTEMPTS,
		MIN_NODE_MEMBERSHIP_TICK_BUDGET, MaintenanceJob, NodeMembershipUpdateResult, Slot,
		maintenance_loop, maintenance_slots, next_slot, node_membership_tick_budget,
		run_node_membership_update, sweep_slots,
	};
	#[cfg(feature = "kv-mem")]
	use crate::kvs::Datastore;
	#[cfg(feature = "kv-mem")]
	use crate::kvs::tasks;
	use crate::options::EngineOptions;

	/// A slot due one interval from now, as the scheduler registers them.
	fn slot(job: MaintenanceJob, interval: Duration) -> Slot {
		Slot {
			job,
			interval,
			next_due: Instant::now() + interval,
		}
	}

	/// Drives `maintenance_loop` with a recorder that stops it after `limit`
	/// passes, then returns the jobs in the order they ran.
	async fn record_passes(slots: Vec<Slot>, limit: usize) -> Vec<MaintenanceJob> {
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
		cost: impl Fn(MaintenanceJob) -> Duration,
	) -> Vec<(MaintenanceJob, Instant)> {
		let canceller = CancellationToken::new();
		let log = Arc::new(Mutex::new(Vec::new()));
		let stop = canceller.clone();
		let sink = Arc::clone(&log);
		maintenance_loop(slots, canceller, move |job| {
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
			}
		})
		.await;
		Arc::into_inner(log).unwrap().into_inner().unwrap()
	}

	/// The staleness window the derivation produces for `interval` when nothing
	/// is configured, so a test that does not care about the window uses the same
	/// one production does.
	fn derived_window(interval: Duration) -> Duration {
		EngineOptions::default()
			.with_node_membership_refresh_interval(interval)
			.resolved_readiness_heartbeat_max_age()
	}

	/// Drives one heartbeat tick over a scripted outcome sequence, recording the
	/// budget each attempt was given and the instant it started.
	///
	/// A scripted `TimedOut` consumes the whole budget it was handed, as a real
	/// one does; every other outcome returns at once. The sequence is consumed
	/// one entry per attempt, so a tick that attempts more times than the script
	/// allows is a test failure rather than a silent repeat of the last outcome.
	async fn record_heartbeat_tick(
		interval: Duration,
		window: Duration,
		outcomes: Vec<NodeMembershipUpdateResult>,
	) -> (bool, Vec<(Duration, Instant)>) {
		let script = Arc::new(Mutex::new(outcomes.into_iter()));
		let log = Arc::new(Mutex::new(Vec::new()));
		let sink = Arc::clone(&log);
		let keep_running = run_node_membership_update(interval, window, move |budget| {
			let script = Arc::clone(&script);
			let sink = Arc::clone(&sink);
			async move {
				sink.lock().unwrap().push((budget, Instant::now()));
				let outcome = {
					let mut script = script.lock().unwrap();
					script.next().expect("attempted more times than the script allows")
				};
				if matches!(outcome, NodeMembershipUpdateResult::TimedOut) {
					tokio::time::sleep(budget).await;
				}
				outcome
			}
		})
		.await;
		(keep_running, Arc::into_inner(log).unwrap().into_inner().unwrap())
	}

	/// Drives one heartbeat tick against a write that always takes `cost`,
	/// timing out whenever the attempt it was given is shorter than that.
	async fn record_heartbeat_tick_against_a_write_taking(
		interval: Duration,
		window: Duration,
		cost: Duration,
	) -> (bool, Vec<Duration>) {
		let log = Arc::new(Mutex::new(Vec::new()));
		let sink = Arc::clone(&log);
		let keep_running = run_node_membership_update(interval, window, move |budget| {
			let sink = Arc::clone(&sink);
			async move {
				sink.lock().unwrap().push(budget);
				if budget < cost {
					tokio::time::sleep(budget).await;
					NodeMembershipUpdateResult::TimedOut
				} else {
					tokio::time::sleep(cost).await;
					NodeMembershipUpdateResult::Updated
				}
			}
		})
		.await;
		(keep_running, Arc::into_inner(log).unwrap().into_inner().unwrap())
	}

	/// Runs `ticks` heartbeat ticks against a write of constant `latency`, and
	/// returns the greatest age any node row reached at the moment it was
	/// replaced.
	///
	/// Models what the datastore does: the heartbeat value is stamped when the
	/// attempt starts and the row is replaced when that attempt completes, so the
	/// age of the row being replaced is measured from the *previous* landed
	/// attempt's stamp to this one's completion. Ticks are paced like the real
	/// ticker, which delays rather than queues a tick its predecessor overran.
	async fn worst_modelled_heartbeat_age(
		interval: Duration,
		window: Duration,
		latency: Duration,
		ticks: usize,
	) -> Duration {
		// Seeded as though a write had just landed, stamped now.
		let stamped = Arc::new(Mutex::new(Instant::now()));
		let worst = Arc::new(Mutex::new(Duration::ZERO));
		for _ in 0..ticks {
			let tick_started = Instant::now();
			let stamped = Arc::clone(&stamped);
			let worst = Arc::clone(&worst);
			run_node_membership_update(interval, window, move |budget| {
				let stamped = Arc::clone(&stamped);
				let worst = Arc::clone(&worst);
				async move {
					let stamp = Instant::now();
					if budget < latency {
						tokio::time::sleep(budget).await;
						return NodeMembershipUpdateResult::TimedOut;
					}
					tokio::time::sleep(latency).await;
					let replaced = Instant::now();
					let mut stamped = stamped.lock().unwrap();
					let age = replaced.duration_since(*stamped);
					*stamped = stamp;
					let mut worst = worst.lock().unwrap();
					*worst = (*worst).max(age);
					NodeMembershipUpdateResult::Updated
				}
			})
			.await;
			// `MissedTickBehavior::Delay`: the next tick is one interval after this
			// one fired, or immediate if this one already overran that.
			let spent = Instant::now().duration_since(tick_started);
			if spent < interval {
				tokio::time::sleep(interval - spent).await;
			}
		}
		*worst.lock().unwrap()
	}

	/// `n` scripted timeouts, which the enum cannot express as `vec![_; n]`
	/// because one of its variants carries a non-`Clone` error.
	fn timed_out(n: usize) -> Vec<NodeMembershipUpdateResult> {
		(0..n).map(|_| NodeMembershipUpdateResult::TimedOut).collect()
	}

	/// A write that fails fast is retried inside the same tick, because it
	/// returned nearly the whole budget: the next attempt still has the patience
	/// to land.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn a_fast_failure_is_retried_within_the_tick() {
		let interval = Duration::from_secs(3);
		let window = derived_window(interval);
		let budget = node_membership_tick_budget(interval, window);
		let (keep_running, attempts) = record_heartbeat_tick(
			interval,
			window,
			vec![
				NodeMembershipUpdateResult::Failed(anyhow::anyhow!("conflict")),
				NodeMembershipUpdateResult::Updated,
			],
		)
		.await;

		assert!(keep_running);
		assert_eq!(attempts.len(), 2, "a fast failure should have been retried");
		assert!(
			attempts.iter().all(|(b, _)| *b == budget),
			"every attempt gets the whole remaining budget: {attempts:?}"
		);
	}

	/// A write that times out has consumed the tick's budget, so the tick ends
	/// rather than starting an attempt with nothing left to give it.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn a_timed_out_write_ends_the_tick() {
		let interval = Duration::from_secs(3);
		let window = derived_window(interval);
		let budget = node_membership_tick_budget(interval, window);
		let started = Instant::now();
		let (keep_running, attempts) = record_heartbeat_tick(interval, window, timed_out(3)).await;

		assert!(keep_running, "an exhausted tick is transient, not a reason to stop");
		assert_eq!(attempts.len(), 1, "a timeout spends the whole budget");
		assert_eq!(attempts[0].0, budget);
		assert_eq!(Instant::now().duration_since(started), budget);
	}

	/// An instantly-failing write must not spin: the budget cannot end the tick
	/// when nothing consumes it, so the attempt cap does.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn an_instantly_failing_write_stops_at_the_attempt_cap() {
		let interval = Duration::from_secs(3);
		let (keep_running, attempts) = record_heartbeat_tick(
			interval,
			derived_window(interval),
			(0..MAX_NODE_MEMBERSHIP_UPDATE_ATTEMPTS + 1)
				.map(|_| NodeMembershipUpdateResult::Failed(anyhow::anyhow!("nope")))
				.collect(),
		)
		.await;

		assert!(keep_running);
		assert_eq!(attempts.len(), MAX_NODE_MEMBERSHIP_UPDATE_ATTEMPTS as usize);
	}

	/// The tick's whole budget is available to a single write, so the slowest
	/// write it can service is the budget itself rather than a fraction of it.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn a_write_needing_the_whole_budget_still_lands() {
		let interval = Duration::from_secs(3);
		for window in [Duration::from_secs(9), Duration::from_secs(30)] {
			let budget = node_membership_tick_budget(interval, window);
			let (keep_running, budgets) =
				record_heartbeat_tick_against_a_write_taking(interval, window, budget).await;

			assert!(keep_running);
			assert_eq!(
				budgets,
				vec![budget],
				"a write as slow as the whole {budget:?} budget should land on the first attempt"
			);
		}
	}

	/// The budget is the lesser of the window less one refresh interval and half
	/// the window, floored and capped.
	#[test]
	fn the_heartbeat_tick_budget_is_bounded_by_half_the_window() {
		let interval = Duration::from_secs(3);
		// Default: min(9 - 3, 9 / 2) = 4.5s — half the window binds.
		assert_eq!(
			node_membership_tick_budget(interval, Duration::from_secs(9)),
			Duration::from_millis(4500)
		);
		// Widened: min(30 - 3, 30 / 2) = 15s.
		assert_eq!(
			node_membership_tick_budget(interval, Duration::from_secs(30)),
			Duration::from_secs(15)
		);
		// A window barely above the interval: the reserve binds instead.
		assert_eq!(
			node_membership_tick_budget(interval, Duration::from_secs(4)),
			Duration::from_secs(1)
		);
		// Never less than one real attempt, however narrow the window.
		assert_eq!(
			node_membership_tick_budget(interval, Duration::from_secs(1)),
			MIN_NODE_MEMBERSHIP_TICK_BUDGET
		);
	}

	/// The invariant the budget exists to keep. The heartbeat value is stamped
	/// before the write that carries it, so a write of `L` lands a row that is
	/// already `L` old and the next replacement is another `L` away: the age peaks
	/// near `2L`, and anything a tick wastes before the write that lands is added
	/// on top. A write at the edge of what the default config admits must still
	/// keep the age inside the window, tick after tick.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn a_slow_but_admitted_write_keeps_the_age_inside_the_window() {
		let interval = Duration::from_secs(3);
		let window = derived_window(interval);
		let latency = Duration::from_millis(4200);
		assert!(latency <= node_membership_tick_budget(interval, window));

		let worst = worst_modelled_heartbeat_age(interval, window, latency, 5).await;

		assert!(worst < window, "heartbeat reached {worst:?}, past the {window:?} window");
	}

	/// `Duration::MAX` is what the derivation degrades to for an interval too
	/// large to multiply out, and means "never considered stale". The tick has to
	/// turn that into a deadline the clock can represent: capped, not overflowed,
	/// and not a tick that returns instantly and spins.
	#[test_log::test(tokio::test(start_paused = true))]
	async fn a_saturated_window_is_capped_rather_than_overflowing() {
		let interval = Duration::from_secs(3);
		assert_eq!(
			node_membership_tick_budget(interval, Duration::MAX),
			MAX_NODE_MEMBERSHIP_TICK_BUDGET
		);

		let started = Instant::now();
		let (keep_running, attempts) =
			record_heartbeat_tick(interval, Duration::MAX, timed_out(2)).await;

		assert!(keep_running);
		assert_eq!(
			attempts.iter().map(|(b, _)| *b).collect::<Vec<_>>(),
			vec![MAX_NODE_MEMBERSHIP_TICK_BUDGET]
		);
		// Bounded by the cap, and it did real waiting rather than spinning.
		assert_eq!(Instant::now().duration_since(started), MAX_NODE_MEMBERSHIP_TICK_BUDGET);
	}

	#[test]
	fn next_slot_is_none_when_nothing_is_registered() {
		assert!(next_slot(&[]).is_none());
	}

	#[test]
	fn next_slot_picks_the_earliest_deadline() {
		let slots = vec![
			slot(MaintenanceJob::NodeCleanup, Duration::from_secs(300)),
			slot(MaintenanceJob::NodeExpire, Duration::from_secs(15)),
			slot(MaintenanceJob::ChangefeedGc, Duration::from_secs(30)),
		];
		assert_eq!(next_slot(&slots), Some(1));
	}

	#[test]
	fn next_slot_breaks_deadline_ties_on_registration_order() {
		let due = Instant::now() + Duration::from_secs(60);
		let mut slots = vec![
			slot(MaintenanceJob::RpcSessionGc, Duration::from_secs(60)),
			slot(MaintenanceJob::ReclaimTombstones, Duration::from_secs(60)),
		];
		for s in &mut slots {
			s.next_due = due;
		}
		assert_eq!(next_slot(&slots), Some(0));
	}

	#[test]
	fn next_slot_prefers_an_overdue_job_over_one_just_run() {
		let mut slots = vec![
			// Just ran, so its deadline sits in the future.
			slot(MaintenanceJob::ChangefeedGc, Duration::from_secs(30)),
			// Overdue.
			slot(MaintenanceJob::NodeExpire, Duration::from_secs(15)),
		];
		slots[1].next_due = Instant::now() - Duration::from_secs(5);
		assert_eq!(next_slot(&slots), Some(1));
	}

	#[test]
	fn slots_skip_zero_intervals() {
		let opts = EngineOptions::default()
			.with_tikv_gc_interval(Duration::ZERO)
			.with_rpc_session_gc_interval(Duration::ZERO);
		let maintenance: Vec<_> = maintenance_slots(&opts).into_iter().map(|s| s.job).collect();
		let sweeps: Vec<_> = sweep_slots(&opts).into_iter().map(|s| s.job).collect();
		assert!(!maintenance.contains(&MaintenanceJob::TikvGc));
		assert!(!sweeps.contains(&MaintenanceJob::RpcSessionGc));
		// The rest of each schedule is unaffected.
		assert!(maintenance.contains(&MaintenanceJob::NodeExpire));
		assert!(maintenance.contains(&MaintenanceJob::TikvLockCleanup));
		assert!(sweeps.contains(&MaintenanceJob::ReclaimTombstones));
	}

	/// The two groups must partition the job set: a job in neither would never
	/// run, and a job in both would run twice per interval and contend with
	/// itself for its own lease.
	#[test]
	fn the_two_groups_partition_every_job() {
		let opts = EngineOptions::default();
		let mut scheduled: Vec<_> =
			maintenance_slots(&opts).into_iter().chain(sweep_slots(&opts)).map(|s| s.job).collect();
		let total = scheduled.len();
		scheduled.dedup();
		assert_eq!(total, scheduled.len(), "a job is registered in both groups");
		for job in MaintenanceJob::ALL {
			assert!(scheduled.contains(&job), "{} is in neither group", job.label());
		}
	}

	/// The unbounded sweeps must not share a task with the bounded jobs: a
	/// reclaim that destroys a whole database, or a purge that pages the session
	/// keyspace, would otherwise stall every short job for its duration.
	#[test]
	fn unbounded_sweeps_are_not_on_the_maintenance_schedule() {
		let opts = EngineOptions::default();
		let maintenance: Vec<_> = maintenance_slots(&opts).into_iter().map(|s| s.job).collect();
		assert!(!maintenance.contains(&MaintenanceJob::ReclaimTombstones));
		assert!(!maintenance.contains(&MaintenanceJob::RpcSessionGc));
		// And the metrics refresh must stay off the sweep schedule, so a long
		// sweep cannot leave `INFO FOR ROOT` and the process gauges stale.
		let sweeps: Vec<_> = sweep_slots(&opts).into_iter().map(|s| s.job).collect();
		assert!(!sweeps.contains(&MaintenanceJob::SystemMetricsRefresh));
	}

	#[test]
	fn maintenance_slots_registers_the_metrics_refresh_immediately() {
		let opts = EngineOptions::default();
		let slots = maintenance_slots(&opts);
		let metrics = slots
			.iter()
			.find(|s| s.job == MaintenanceJob::SystemMetricsRefresh)
			.expect("the metrics refresh is always registered");
		// Due now, so a cold start reports real utilisation rather than zeroes.
		assert!(metrics.next_due <= Instant::now());
		// Every other job waits out one full interval.
		let others = slots.iter().filter(|s| s.job != MaintenanceJob::SystemMetricsRefresh);
		for s in others {
			assert!(s.next_due > Instant::now(), "{} should not be due yet", s.job.label());
		}
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_runs_every_registered_job() {
		let passes = record_passes(
			vec![
				slot(MaintenanceJob::NodeExpire, Duration::from_millis(5)),
				slot(MaintenanceJob::ChangefeedGc, Duration::from_millis(10)),
				slot(MaintenanceJob::NodeCleanup, Duration::from_millis(40)),
			],
			40,
		)
		.await;
		for job in
			[MaintenanceJob::NodeExpire, MaintenanceJob::ChangefeedGc, MaintenanceJob::NodeCleanup]
		{
			assert!(passes.contains(&job), "{} never ran: {passes:?}", job.label());
		}
		// Cadence is honoured, not just liveness: the 5ms job runs more often
		// than the 40ms one.
		let fast = passes.iter().filter(|j| **j == MaintenanceJob::NodeExpire).count();
		let slow = passes.iter().filter(|j| **j == MaintenanceJob::NodeCleanup).count();
		assert!(fast > slow, "5ms job ran {fast}x, 40ms job ran {slow}x");
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn maintenance_loop_does_not_let_one_job_monopolise_the_schedule() {
		// A pass that takes far longer than every interval leaves both jobs
		// permanently overdue, which is exactly when a scheduler starves one.
		let passes = record_passes_with_delay(
			vec![
				slot(MaintenanceJob::ReclaimTombstones, Duration::from_millis(5)),
				slot(MaintenanceJob::NodeExpire, Duration::from_millis(5)),
			],
			20,
			|job| match job {
				MaintenanceJob::ReclaimTombstones => Duration::from_millis(100),
				_ => Duration::ZERO,
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
		let passes = record_passes_with_delay(
			vec![slot(MaintenanceJob::ReclaimTombstones, interval)],
			4,
			|_| cost,
		)
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
				slot(MaintenanceJob::NodeExpire, Duration::from_millis(5)),
				slot(MaintenanceJob::ChangefeedGc, Duration::from_millis(5)),
			],
			3,
		)
		.await;
		assert_eq!(passes.len(), 3, "dispatched after cancellation: {passes:?}");
	}

	#[test_log::test(tokio::test(start_paused = true))]
	async fn node_membership_update_continues_after_success() {
		let (should_continue, attempts) = record_heartbeat_tick(
			Duration::from_secs(3),
			derived_window(Duration::from_secs(3)),
			vec![NodeMembershipUpdateResult::Updated],
		)
		.await;

		assert!(should_continue);
		assert_eq!(attempts.len(), 1, "a write that landed must not be repeated");
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
		let age = dbs.node_heartbeat_age().await.unwrap();
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

	/// The live-query router is the only conditionally-spawned task: under the
	/// default inline engine it has nothing to deliver, so it is not spawned.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn live_query_router_is_not_spawned_under_the_inline_engine() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		// heartbeat, event processing, index compaction, maintenance, sweeps.
		assert_eq!(tasks.0.len(), 5);
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	/// A group whose every job is disabled must not leave an idle task behind.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn a_fully_disabled_group_is_not_spawned() {
		let can = CancellationToken::new();
		// Both sweep jobs off; the maintenance group is untouched.
		let opt = EngineOptions::default()
			.with_reclaim_interval(Duration::ZERO)
			.with_rpc_session_gc_interval(Duration::ZERO);
		let dbs = Datastore::new("memory").await.unwrap();
		let tasks = tasks::init(&dbs, can.clone(), &opt);
		assert_eq!(tasks.0.len(), 4, "the empty sweep group should not be spawned");
		can.cancel();
		tasks.resolve().await.unwrap();
	}
}
