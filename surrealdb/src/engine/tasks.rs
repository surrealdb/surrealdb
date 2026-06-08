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
	let task8 = spawn_task_tikv_lock_cleanup(dbs, canceller, opts);
	Tasks(vec![task1, task2, task3, task4, task5, task6, task7, task8])
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
