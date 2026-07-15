use core::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use surrealdb_core::err::{is_query_cancelled, is_query_timedout};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;
#[cfg(not(target_family = "wasm"))]
use tokio::{spawn, time, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::{self as time, MissedTickBehavior};

use crate::Error;
use crate::engine::IntervalStream;

#[cfg(not(target_family = "wasm"))]
type Task = Pin<Box<dyn Future<Output = Result<(), tokio::task::JoinError>> + Send + 'static>>;

#[cfg(target_family = "wasm")]
type Task = Pin<Box<()>>;

const NODE_MEMBERSHIP_UPDATE_TIMEOUT: Duration = Duration::from_secs(60);

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
			let _ = task.await;
		}
		Ok(())
	}
}

// The init starts a long-running thread for periodically calling
// Datastore.tick. Datastore.tick is responsible for running garbage collection
// and other background tasks.
//
// This function needs to be called before after the dbs::init and before the
// net::init functions. It needs to be before net::init because the net::init
// function blocks until the web server stops.
pub fn init(dbs: Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	let task1 = spawn_task_node_membership_refresh(Arc::clone(&dbs), canceller.clone(), opts);
	let task2 = spawn_task_node_membership_check(Arc::clone(&dbs), canceller.clone(), opts);
	let task3 = spawn_task_node_membership_cleanup(Arc::clone(&dbs), canceller.clone(), opts);
	let task4 = spawn_task_changefeed_cleanup(Arc::clone(&dbs), canceller.clone(), opts);
	let task5 = spawn_task_index_compaction(Arc::clone(&dbs), canceller.clone(), opts);
	let task6 = spawn_task_event_processing(Arc::clone(&dbs), canceller.clone(), opts);
	let task7 = spawn_task_tikv_gc(Arc::clone(&dbs), canceller.clone(), opts);
	let task8 = spawn_task_tikv_lock_cleanup(Arc::clone(&dbs), canceller.clone(), opts);
	let task9 = spawn_task_reclaim_tombstones(Arc::clone(&dbs), canceller.clone(), opts);
	let task10 = spawn_task_resume_index_builds(Arc::clone(&dbs), canceller.clone(), opts);
	let task11 = spawn_task_rpc_session_gc(Arc::clone(&dbs), canceller.clone(), opts);
	let task12 = spawn_task_live_query_router(dbs, canceller, opts);
	Tasks(vec![
		task1, task2, task3, task4, task5, task6, task7, task8, task9, task10, task11, task12,
	])
}

/// Spawns the per-node live-query router task.
///
/// Under the `Router` live-query engine this tails the dedicated `lqe` keyspace
/// and delivers notifications off the write path; under the default `Inline`
/// engine each tick is a cheap no-op (the datastore method returns immediately).
/// The cadence bounds steady-state delivery latency, so it ticks frequently.
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

fn spawn_task_node_membership_check(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.node_membership_check_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Processing and archiving inactive nodes every {interval:?}");
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
					if let Err(e) = dbs.expire_nodes().await {
						error!("Error processing and archiving inactive nodes: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Processing and archiving inactive nodes");
	}))
}

fn spawn_task_node_membership_cleanup(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.node_membership_cleanup_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Processing and cleaning archived nodes every {interval:?}");
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
					if let Err(e) = dbs.remove_nodes().await {
						error!("Error processing and cleaning archived nodes: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Processing and cleaning archived nodes");
	}))
}

fn spawn_task_changefeed_cleanup(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = opts.changefeed_gc_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Running changefeed garbage collection every {interval:?}");
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
					if let Err(e) = dbs.changefeed_process(&interval).await {
						error!("Error running changefeed garbage collection: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running changefeed garbage collection");
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

/// Spawns the periodic task that resumes stalled index builds.
///
/// A `CONCURRENTLY` index build is a detached task, so if its owning node dies
/// mid-build the durable build state is stranded in `Building`/`Closing` and the
/// index reports `status: indexing` with a frozen counter forever. This task
/// periodically adopts such builds (once the owner lease has expired) and drives
/// them to completion. An interval of `Duration::ZERO` disables it so operators
/// can recover stalled builds manually with `REBUILD INDEX`.
fn spawn_task_resume_index_builds(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.index_build_resume_interval;
	Box::pin(spawn(async move {
		if interval.is_zero() {
			trace!("Index build resume task disabled (interval=0)");
			return;
		}
		trace!("Resuming stalled index builds every {interval:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.resume_stalled_index_builds(interval, canceller.clone()).await {
						if canceller.is_cancelled() {
							break;
						}
						error!("Error resuming stalled index builds: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Resuming stalled index builds");
	}))
}

/// Spawns the periodic background reclaim of tombstoned data.
///
/// `REMOVE NAMESPACE/DATABASE/INDEX` delete only the catalog definition and
/// enqueue the data prefix for reclaim; this task periodically destroys the
/// orphaned data out-of-band (via `unsafe_destroy_range` on TiKV or a
/// transactional prefix delete on other backends), so the `REMOVE` statement
/// returns immediately. The task runs at `opts.reclaim_interval`.
fn spawn_task_reclaim_tombstones(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval and snapshot-safety grace from the config.
	let interval = opts.reclaim_interval;
	// Clamp the grace up to at least the TiKV GC lifetime: on TiKV,
	// `unsafe_destroy_range` bypasses MVCC, so data must not be reclaimed while a
	// snapshot older than the GC safepoint (`now - tikv_gc_lifetime`) could still
	// read it. Deriving the effective grace here means a longer `--tikv-gc-lifetime`
	// can never be undercut by leaving `--reclaim-grace` at its default.
	let grace = opts.reclaim_grace.max(opts.tikv_gc_lifetime);
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Running tombstone reclaim every {interval:?} (grace {grace:?})");
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
					if let Err(e) =
						Datastore::reclaim_tombstones(Arc::clone(&dbs), interval, grace, canceller.clone()).await
					{
						if canceller.is_cancelled() {
							break;
						}
						error!("Error running tombstone reclaim: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running tombstone reclaim");
	}))
}

fn spawn_task_event_processing(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let trigger = Arc::clone(dbs.async_event_trigger());
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
				_ = trigger.notified() => process_events().await,
				// Receive a notification on the channel
				Some(_) = ticker.next() => process_events().await
			}
		}
		trace!("Background task exited: Running event processing");
	}))
}

/// Spawns the periodic TiKV MVCC GC pass.
///
/// On non-TiKV backends `Datastore::run_mvcc_gc` is a no-op and the task
/// loops harmlessly. A configured interval of `Duration::ZERO` is treated
/// as "disabled" so operators can opt out without recompiling.
/// Spawns the periodic purge of expired durable RPC sessions.
///
/// When RPC session persistence is enabled, client-attached sessions are
/// mirrored to the KV store with an absolute expiry; this task sweeps out
/// entries whose expiry has passed (expired entries that are loaded again are
/// already dropped lazily). The sweep runs on every deployment — an empty
/// session keyspace costs one scan per interval — so leftovers are also
/// cleaned up after an operator disables session persistence. A distributed
/// task lease inside the datastore method makes it cluster-singleton.
fn spawn_task_rpc_session_gc(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.rpc_session_gc_interval;
	Box::pin(spawn(async move {
		if interval.is_zero() {
			trace!("RPC session GC task disabled (interval=0)");
			return;
		}
		trace!("Purging expired RPC sessions every {interval:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.purge_expired_rpc_sessions(&interval).await {
						error!("Error purging expired RPC sessions: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Purging expired RPC sessions");
	}))
}

fn spawn_task_tikv_gc(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.tikv_gc_interval;
	let lifetime = opts.tikv_gc_lifetime;
	Box::pin(spawn(async move {
		if interval.is_zero() {
			trace!("TiKV GC task disabled (interval=0)");
			return;
		}
		trace!("Running TiKV MVCC GC every {interval:?} with lifetime {lifetime:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.run_mvcc_gc(lifetime).await {
						error!("Error running TiKV MVCC GC: {e}");
					}
				}
			}
		}
		trace!("Background task exited: TiKV MVCC GC");
	}))
}

/// Spawns the periodic TiKV stale-lock cleanup pass.
fn spawn_task_tikv_lock_cleanup(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	let interval = opts.tikv_lock_cleanup_interval;
	let lifetime = opts.tikv_gc_lifetime;
	Box::pin(spawn(async move {
		if interval.is_zero() {
			trace!("TiKV lock-cleanup task disabled (interval=0)");
			return;
		}
		trace!("Running TiKV stale-lock cleanup every {interval:?} with lifetime {lifetime:?}");
		let mut ticker = interval_ticker(interval).await;
		loop {
			tokio::select! {
				biased;
				_ = canceller.cancelled() => break,
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.run_lock_cleanup(lifetime).await {
						error!("Error running TiKV lock cleanup: {e}");
					}
				}
			}
		}
		trace!("Background task exited: TiKV lock cleanup");
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
	#[cfg(feature = "kv-mem")]
	use std::sync::Arc;
	use std::time::Duration;

	#[cfg(feature = "kv-mem")]
	use surrealdb_core::kvs::Datastore;
	#[cfg(feature = "kv-mem")]
	use surrealdb_core::options::EngineOptions;
	#[cfg(feature = "kv-mem")]
	use tokio_util::sync::CancellationToken;

	#[cfg(feature = "kv-mem")]
	use crate::engine::tasks;

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
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(Arc::clone(&dbs), can.clone(), &opt);
		can.cancel();
		tokio::time::timeout(Duration::from_secs(10), tasks.resolve())
			.await
			.map_err(|e| format!("Timed out after {e}"))
			.unwrap()
			.map_err(|e| format!("Resolution failed: {e}"))
			.unwrap();
	}
}
