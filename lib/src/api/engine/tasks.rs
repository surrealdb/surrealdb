use crate::engine::IntervalStream;
use crate::kvs::Datastore;
use crate::options::EngineOptions;
#[cfg(not(target_arch = "wasm32"))]
use crate::Error as RootError;
use futures::StreamExt;
#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn as spawn_future;
use tokio::sync::oneshot;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn_future;

/// This will be true if a task has completed
#[cfg(not(target_arch = "wasm32"))]
type FutureTask = JoinHandle<()>;

/// This will be true if a task has completed
#[cfg(target_arch = "wasm32")]
type FutureTask = Arc<AtomicBool>;

pub struct Tasks {
	pub nd: FutureTask,
}

impl Tasks {
	#[cfg(not(target_arch = "wasm32"))]
	pub async fn resolve(self) -> Result<(), RootError> {
		match self.nd.await {
			// cancelling this task is fine, and can happen when surrealdb exits.
			Ok(_) => {}
			Err(e) if e.is_cancelled() => {}
			Err(e) => {
				error!("Node agent task failed: {}", e);
				let inner_err =
					crate::err::Error::NodeAgent("node task failed and has been logged");
				return Err(RootError::Db(inner_err));
			}
		}
		Ok(())
	}
}

/// Starts tasks that are required for the correct running of the engine
pub fn start_tasks(opt: &EngineOptions, dbs: Arc<Datastore>) -> (Tasks, [oneshot::Sender<()>; 1]) {
	let nd = init(opt, dbs.clone());
	let cancellation_channels = [nd.1];
	(
		Tasks {
			nd: nd.0,
		},
		cancellation_channels,
	)
}

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
fn init(opt: &EngineOptions, dbs: Arc<Datastore>) -> (FutureTask, oneshot::Sender<()>) {
	let _init = crate::dbs::LoggingLifecycle::new("node agent initialisation".to_string());
	let tick_interval = opt.tick_interval;

	trace!("Ticker interval is {:?}", tick_interval);
	#[cfg(target_arch = "wasm32")]
	let completed_status = Arc::new(AtomicBool::new(false));
	#[cfg(target_arch = "wasm32")]
	let ret_status = completed_status.clone();

	// We create a channel that can be streamed that will indicate termination
	let (tx, mut rx) = oneshot::channel();

	let _fut = spawn_future(async move {
		let _lifecycle = crate::dbs::LoggingLifecycle::new("heartbeat task".to_string());
		let mut ticker = interval_ticker(tick_interval).await;

		loop {
			tokio::select! {
				v = ticker.next() => {
					// ticker will never return None;
					let i = v.unwrap();
					trace!("Node agent tick: {:?}", i);
					if let Err(e) = dbs.tick().await {
						error!("Error running node agent tick: {}", e);
						break;
					}
				}
				_ = &mut rx => {
					// termination requested
					break
				}
			}
		}

		#[cfg(target_arch = "wasm32")]
		completed_status.store(true, Ordering::Relaxed);
	});
	#[cfg(not(target_arch = "wasm32"))]
	return (_fut, tx);
	#[cfg(target_arch = "wasm32")]
	return (ret_status, tx);
}

async fn interval_ticker(interval: Duration) -> IntervalStream {
	#[cfg(not(target_arch = "wasm32"))]
	use tokio::{time, time::MissedTickBehavior};
	#[cfg(target_arch = "wasm32")]
	use wasmtimer::{tokio as time, tokio::MissedTickBehavior};

	let mut interval = time::interval(interval);
	// Don't bombard the database if we miss some ticks
	interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
	interval.tick().await;
	IntervalStream::new(interval)
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::engine::tasks::start_tasks;
	use crate::kvs::Datastore;
	use crate::options::EngineOptions;
	use std::sync::Arc;
	use std::time::Duration;

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete() {
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let (val, chans) = start_tasks(&opt, dbs.clone());
		for chan in chans {
			chan.send(()).unwrap();
		}
		val.resolve().await.unwrap();
	}

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete_channel_closed() {
		let opt = EngineOptions::default();
		let dbs = Arc::new(Datastore::new("memory").await.unwrap());
		let val = {
			let (val, _chans) = start_tasks(&opt, dbs.clone());
			val
		};
		tokio::time::timeout(Duration::from_secs(10), val.resolve())
			.await
			.map_err(|e| format!("Timed out after {e}"))
			.unwrap()
			.map_err(|e| format!("Resolution failed: {e}"))
			.unwrap();
	}
}
