use crate::engine::IntervalStream;
use crate::err::Error;
#[cfg(not(target_family = "wasm"))]
use core::future::Future;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use surrealdb_core::{kvs::Datastore, options::EngineOptions};
use tokio_util::sync::CancellationToken;

#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

#[cfg(not(target_family = "wasm"))]
type Task = Pin<Box<dyn Future<Output = Result<(), tokio::task::JoinError>> + Send + 'static>>;

#[cfg(target_family = "wasm")]
type Task = Pin<Box<()>>;

pub struct Tasks(#[allow(dead_code)] Vec<Task>);

impl Tasks {
	#[cfg(target_family = "wasm")]
	pub async fn resolve(self) -> Result<(), Error> {
		Ok(())
	}
	#[cfg(not(target_family = "wasm"))]
	pub async fn resolve(self) -> Result<(), Error> {
		for task in self.0.into_iter() {
			let _ = task.await;
		}
		Ok(())
	}
}

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
pub fn init(dbs: Arc<Datastore>, canceller: CancellationToken, opts: &EngineOptions) -> Tasks {
	let task1 = spawn_task_node_membership_refresh(dbs.clone(), canceller.clone(), opts);
	let task2 = spawn_task_node_membership_check(dbs.clone(), canceller.clone(), opts);
	let task3 = spawn_task_node_membership_cleanup(dbs.clone(), canceller.clone(), opts);
	let task4 = spawn_task_changefeed_cleanup(dbs.clone(), canceller.clone(), opts);
	let task5 = spawn_task_index_compaction(dbs.clone(), canceller.clone(), opts);
	let task6 = spawn_task_reclaim(dbs.clone(), canceller.clone(), opts);
	Tasks(vec![task1, task2, task3, task4, task5, task6])
}

fn spawn_task_node_membership_refresh(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let delay = opts.node_membership_refresh_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Updating node registration information every {delay:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(delay).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.node_membership_update().await {
						error!("Error updating node registration information: {e}");
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
	let delay = opts.node_membership_check_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Processing and archiving inactive nodes every {delay:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(delay).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.node_membership_expire().await {
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
	let delay = opts.node_membership_cleanup_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Processing and cleaning archived nodes every {delay:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(delay).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.node_membership_remove().await {
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
	let delay = opts.changefeed_gc_interval;
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Running changefeed garbage collection every {delay:?}");
		// Create a new time-based interval ticket
		let mut ticker = interval_ticker(delay).await;
		// Loop continuously until the task is cancelled
		loop {
			tokio::select! {
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					// A pass collects every range the catalog plans, each
					// bounded by its own key budget, so a shutdown drops it
					// where it stands rather than waiting for the plan to
					// finish. Every page commits on its own, and the tick
					// after resumes from the start of what is left.
					tokio::select! {
						biased;
						_ = canceller.cancelled() => break,
						res = dbs.changefeed_process() => {
							if let Err(e) = res {
								error!("Error running changefeed garbage collection: {e}");
							}
						}
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
					// The queue entries a pass reads are bounded, but each
					// index the pass meets folds a drain of its own, so a
					// shutdown drops the pass where it stands. Each page of a
					// fold commits before the queue entries that asked for it
					// are dropped, so nothing is lost by stopping between two.
					tokio::select! {
						biased;
						_ = canceller.cancelled() => break,
						res = dbs.index_compaction(interval) => {
							if let Err(e) = res {
								error!("Error running index compaction: {e}");
							}
						}
					}
				}
			}
		}
		trace!("Background task exited: Running index compaction");
	}))
}

/// Spawns a background task which reclaims the data of removed namespaces and databases
///
/// `REMOVE NAMESPACE` and `REMOVE DATABASE` delete only the catalog definition
/// and enqueue a job naming the orphaned data prefix. This task drains that
/// queue, destroying each prefix in bounded, resumable batches, so that the
/// cost of a removal is independent of how much data the removed object held.
///
/// The task runs at the interval specified by `opts.reclaim_interval`. A job is
/// stamped on the pass that first meets it and destroyed by a later one, so no
/// prefix is destroyed by the pass that first sees the removal that orphaned it.
///
/// Reclaim has no disabled state: it is what completes a removal, so a queue
/// nothing drains leaves the data of every removed object on disk and holds its
/// name against reuse. A zero interval, which would also panic the interval
/// timer, falls back to the default rate.
///
/// A cancellation abandons a pass in progress rather than waiting for it. Every
/// page of a pass commits on its own, together with the cursor that accounts for
/// it, so the next start resumes from the last durable key instead of repeating
/// the prefix.
///
/// # Arguments
///
/// * `dbs` - The datastore instance
/// * `canceller` - Token used to cancel the task when the engine is shutting down
/// * `opts` - Engine options containing the reclaim interval
fn spawn_task_reclaim(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	opts: &EngineOptions,
) -> Task {
	// Get the delay interval from the config
	let interval = match opts.reclaim_interval.is_zero() {
		true => EngineOptions::default().reclaim_interval,
		false => opts.reclaim_interval,
	};
	// Spawn a future
	Box::pin(spawn(async move {
		// Log the interval frequency
		trace!("Running data reclaim every {interval:?}");
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
					// A pass is bounded by a key budget rather than by the clock,
					// but a large backlog still makes it the longest-running of
					// these tasks, so a shutdown drops it where it stands
					tokio::select! {
						biased;
						_ = canceller.cancelled() => break,
						res = dbs.reclaim_tombstones(interval) => match res {
							Ok((visited, errors)) if errors > 0 => {
								error!("Data reclaim visited {visited} queue entries, {errors} of which failed");
							}
							Ok((visited, _)) if visited > 0 => {
								trace!("Data reclaim visited {visited} queue entries");
							}
							Ok(_) => {}
							Err(e) => error!("Error running data reclaim: {e}"),
						},
					}
				}
			}
		}
		trace!("Background task exited: Running data reclaim");
	}))
}

async fn interval_ticker(interval: Duration) -> IntervalStream {
	#[cfg(not(target_family = "wasm"))]
	use tokio::{time, time::MissedTickBehavior};
	#[cfg(target_family = "wasm")]
	use wasmtimer::{tokio as time, tokio::MissedTickBehavior};
	// Create a new interval timer
	let mut interval = time::interval(interval);
	// Don't bombard the database if we miss some ticks
	interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
	interval.tick().await;
	IntervalStream::new(interval)
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::engine::tasks;
	use std::sync::Arc;
	use std::time::Duration;
	use surrealdb_core::{kvs::Datastore, options::EngineOptions};
	use tokio_util::sync::CancellationToken;

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(dbs.clone(), can.clone(), &opt);
		can.cancel();
		tasks.resolve().await.unwrap();
	}

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete_channel_closed() {
		let can = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(dbs.clone(), can.clone(), &opt);
		can.cancel();
		tokio::time::timeout(Duration::from_secs(10), tasks.resolve())
			.await
			.map_err(|e| format!("Timed out after {e}"))
			.unwrap()
			.map_err(|e| format!("Resolution failed: {e}"))
			.unwrap();
	}

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete_with_a_zero_reclaim_interval() {
		// A zero reclaim interval falls back to the default rate, rather than
		// reaching the interval timer, which panics on a zero period
		let can = CancellationToken::new();
		let opt = EngineOptions::default().with_reclaim_interval(Duration::ZERO);
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let tasks = tasks::init(dbs.clone(), can.clone(), &opt);
		can.cancel();
		tasks.resolve().await.unwrap();
	}
}
