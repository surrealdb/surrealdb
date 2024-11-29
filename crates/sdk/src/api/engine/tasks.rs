use crate::engine::IntervalStream;
use crate::err::Error;
#[cfg(not(target_arch = "wasm32"))]
use core::future::Future;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use surrealdb_core::{kvs::Datastore, options::EngineOptions};
use tokio_util::sync::CancellationToken;

#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn;

#[cfg(not(target_arch = "wasm32"))]
type Task = Pin<Box<dyn Future<Output = Result<(), tokio::task::JoinError>> + Send + 'static>>;

#[cfg(target_arch = "wasm32")]
type Task = Pin<Box<()>>;

pub struct Tasks(#[allow(dead_code)] Vec<Task>);

impl Tasks {
	#[cfg(target_arch = "wasm32")]
	pub async fn resolve(self) -> Result<(), Error> {
		Ok(())
	}
	#[cfg(not(target_arch = "wasm32"))]
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
	Tasks(vec![task1, task2, task3, task4])
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
				// Yield back to other tasks
				_ = tokio::task::yield_now(), if tokio::runtime::Handle::try_current().is_ok() => (),
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
				// Yield back to other tasks
				_ = tokio::task::yield_now(), if tokio::runtime::Handle::try_current().is_ok() => (),
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
				// Yield back to other tasks
				_ = tokio::task::yield_now(), if tokio::runtime::Handle::try_current().is_ok() => (),
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
				// Yield back to other tasks
				_ = tokio::task::yield_now(), if tokio::runtime::Handle::try_current().is_ok() => (),
				// Receive a notification on the channel
				Some(_) = ticker.next() => {
					if let Err(e) = dbs.changefeed_process().await {
						error!("Error running changefeed garbage collection: {e}");
					}
				}
			}
		}
		trace!("Background task exited: Running changefeed garbage collection");
	}))
}

async fn interval_ticker(interval: Duration) -> IntervalStream {
	#[cfg(not(target_arch = "wasm32"))]
	use tokio::{time, time::MissedTickBehavior};
	#[cfg(target_arch = "wasm32")]
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
}
