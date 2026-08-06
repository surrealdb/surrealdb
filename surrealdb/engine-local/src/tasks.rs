use core::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use surrealdb_core::err::{is_query_cancelled, is_query_timedout};
use surrealdb_core::kvs::{Datastore, LiveQueryEngine};
use surrealdb_core::options::EngineOptions;
use surrealdb_types::Error;
#[cfg(not(target_family = "wasm"))]
use tokio::{
	spawn, time,
	time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::{self as time, MissedTickBehavior};

use crate::interval::IntervalStream;

#[cfg(not(target_family = "wasm"))]
type Task = Pin<Box<dyn Future<Output = Result<(), tokio::task::JoinError>> + Send + 'static>>;

#[cfg(target_family = "wasm")]
type Task = Pin<Box<()>>;

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

enum NodeMembershipUpdateResult {
	Updated,
	Cancelled,
	TimedOut,
	Failed(anyhow::Error),
}

pub struct Tasks(#[cfg_attr(target_family = "wasm", expect(dead_code))] Vec<Task>);

impl Tasks {
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
/// Must be called after `dbs::init` and before `net::init`, which blocks until
/// the web server stops.
pub fn init(dbs: Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	let mut tasks = Vec::with_capacity(6);
	tasks.push(spawn_task_node_membership_refresh(Arc::clone(&dbs), canceller.clone(), opts));
	tasks.push(spawn_task_event_processing(Arc::clone(&dbs), canceller.clone(), opts));
	tasks.push(spawn_task_index_compaction(Arc::clone(&dbs), canceller.clone(), opts));
	for (group, slots) in [("maintenance", maintenance_slots(opts)), ("sweep", sweep_slots(opts))] {
		// Every job in a group can be disabled by interval, so a group can end up
		// empty; spawning a task that immediately exits would serve no purpose.
		if !slots.is_empty() {
			tasks.push(spawn_task_scheduler(
				group,
				slots,
				Arc::clone(&dbs),
				canceller.clone(),
				opts,
			));
		}
	}
	if dbs.live_query_engine() == LiveQueryEngine::Router {
		tasks.push(spawn_task_live_query_router(dbs, canceller, opts));
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
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.live_query_router_interval;
	Box::pin(spawn(async move {
		trace!("Running the live-query router every {interval:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.live_query_router_process().await {
						error!("Error running the live-query router: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running the live-query router");
	}))
}

fn spawn_task_node_membership_refresh(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.node_membership_refresh_interval;
	// Spawn a future
	Box::pin(spawn(async move {
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
					if !run_node_membership_update(
						NODE_MEMBERSHIP_UPDATE_TIMEOUT,
						update_node_membership(
							&dbs,
							&canceller,
							NODE_MEMBERSHIP_UPDATE_TIMEOUT,
						),
					).await {
						break;
					}
				}
			}
		}
		trace!("Background task exited: Updating node registration information");
	}))
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
/// * `canceller` - Token used to cancel the task when the engine is shutting down
/// * `opts` - Engine options containing the compaction interval
///
/// # Returns
///
/// * A pinned task that can be awaited
fn spawn_task_index_compaction(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.index_compaction_interval;
	let triggers = Arc::clone(dbs.commit_triggers());
	// Spawn a future
	Box::pin(spawn(async move {
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
						_ = time::sleep(INDEX_COMPACTION_TRIGGER_DEBOUNCE) => {}
					}
					if let Err(e) =
						Datastore::index_compaction(Arc::clone(&dbs), interval, canceller.clone()).await
					{
						if canceller.is_cancelled() {
							break;
						}
						error!("Error running index compaction: {e}");
					}
				}
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					if let Err(e) =
						Datastore::index_compaction(Arc::clone(&dbs), interval, canceller.clone()).await
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
	}))
}

/// Spawns the async event processing task.
///
/// Keeps its own task rather than joining the maintenance scheduler for two
/// reasons: the queue is fed by the write path and each pass drains it to empty,
/// and the events themselves run user-defined SurrealQL of unbounded duration.
fn spawn_task_event_processing(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let triggers = Arc::clone(dbs.commit_triggers());
	// Get the delay interval from the config
	let interval = opts.event_processing_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Running event processing every {interval:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(interval).await;
		//
		let process_events = async || {
			if let Err(e) = dbs.event_processing(interval).await {
				error!("Error running event processing: {e}");
			}
		};
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Wake early when new async events are committed.
				_ = triggers.async_event.notified() => process_events().await,
				// Receive a notification on the channel
				Some(_) = ticker.next() => process_events().await
			}
		}
		trace!("Background task exited: Running event processing");
	}))
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
			surrealdb_core::observe::refresh_process_snapshot().await;
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
			_ = time::sleep(delay) => {}
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
	dbs: Arc<Datastore>,
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
	Box::pin(spawn(async move {
		trace!(
			"Running {} {group} jobs on a shared schedule: {}",
			slots.len(),
			slots.iter().map(|s| s.job.label()).collect::<Vec<_>>().join(", ")
		);
		let jobs_canceller = canceller.clone();
		maintenance_loop(slots, canceller, move |job| {
			let dbs = Arc::clone(&dbs);
			let canceller = jobs_canceller.clone();
			async move { run_maintenance_job(job, &dbs, &canceller, &opts, reclaim_grace).await }
		})
		.await;
		trace!("Background task exited: Running {group} jobs");
	}))
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

async fn run_node_membership_update<Fut>(timeout_duration: Duration, update_node: Fut) -> bool
where
	Fut: Future<Output = NodeMembershipUpdateResult>,
{
	match update_node.await {
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
	let mut interval = time::interval(interval);
	// Don't bombard the database if we miss some ticks
	interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
	interval.tick().await;
	IntervalStream::new(interval)
}

#[cfg(test)]
mod test {
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	#[cfg(feature = "kv-mem")]
	use surrealdb_core::kvs::Datastore;
	#[cfg(feature = "kv-mem")]
	use surrealdb_core::options::EngineOptions;
	use tokio::time::Instant;
	use tokio_util::sync::CancellationToken;

	use super::{
		MaintenanceJob, Slot, maintenance_loop, maintenance_slots, next_slot, sweep_slots,
	};
	#[cfg(feature = "kv-mem")]
	use crate::tasks;

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

	#[test_log::test(tokio::test)]
	async fn node_membership_update_exits_when_cancelled() {
		let should_continue = super::run_node_membership_update(Duration::from_secs(60), async {
			super::NodeMembershipUpdateResult::Cancelled
		})
		.await;

		assert!(!should_continue);
	}

	#[test_log::test(tokio::test)]
	async fn node_membership_update_continues_after_timeout() {
		let should_continue = super::run_node_membership_update(Duration::from_secs(60), async {
			super::NodeMembershipUpdateResult::TimedOut
		})
		.await;

		assert!(should_continue);
	}

	#[test_log::test(tokio::test)]
	async fn node_membership_update_continues_after_success() {
		let should_continue = super::run_node_membership_update(Duration::from_secs(60), async {
			super::NodeMembershipUpdateResult::Updated
		})
		.await;

		assert!(should_continue);
	}

	#[test_log::test(tokio::test)]
	async fn node_membership_update_continues_after_error() {
		let should_continue = super::run_node_membership_update(Duration::from_secs(60), async {
			super::NodeMembershipUpdateResult::Failed(anyhow::anyhow!("update failed"))
		})
		.await;

		assert!(should_continue);
	}

	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn tasks_complete() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
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
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
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
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		dbs.insert_node().await.unwrap();
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
		tokio::time::sleep(Duration::from_millis(500)).await;
		let age = dbs.node_heartbeat_age().await.unwrap();
		can.cancel();
		tasks.resolve().await.unwrap();
		assert!(age < Duration::from_millis(500), "heartbeat was {age:?} stale");
	}

	/// The live-query router is the only conditionally-spawned task: under the
	/// default inline engine it has nothing to deliver, so it is not spawned.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	pub async fn live_query_router_is_not_spawned_under_the_inline_engine() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
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
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
		assert_eq!(tasks.0.len(), 4, "the empty sweep group should not be spawned");
		can.cancel();
		tasks.resolve().await.unwrap();
	}
}
