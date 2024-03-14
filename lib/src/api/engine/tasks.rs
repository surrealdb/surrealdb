use flume::Sender;
use futures::StreamExt;
use futures_concurrency::stream::Merge;
#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

use surrealdb_core::dbs::Options;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;

use crate::engine::IntervalStream;
#[cfg(not(target_arch = "wasm32"))]
use crate::Error as RootError;
#[cfg(not(target_arch = "wasm32"))]
use surrealdb_core::err::Error;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn as spawn_future;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn_future;

const LOG: &str = "surrealdb::node";
const TASK_COUNT: usize = 2;

#[cfg(not(target_arch = "wasm32"))]
type FutureTask = JoinHandle<()>;
#[cfg(target_arch = "wasm32")]
/// This will be true if a task has completed
type FutureTask = Arc<AtomicBool>;

/// LoggingLifecycle is used to create log messages upon creation, and log messages when it is dropped
struct LoggingLifecycle {
	identifier: String,
}

impl LoggingLifecycle {
	fn new(identifier: String) -> Self {
		debug!("Started {}", identifier);
		Self {
			identifier,
		}
	}
}

impl Drop for LoggingLifecycle {
	fn drop(&mut self) {
		debug!("Stopped {}", self.identifier);
	}
}

pub struct Tasks {
	pub nd: FutureTask,
	pub lq: FutureTask,
}

impl Tasks {
	#[cfg(not(target_arch = "wasm32"))]
	pub async fn resolve(self) -> Result<(), RootError> {
		#[cfg(not(target_arch = "wasm32"))]
		{
			self.nd.await.map_err(|e| {
				error!("Node agent task failed: {}", e);
				RootError::Db(Error::NodeAgent("node task failed and has been logged"))
			})?;
			self.lq.await.map_err(|e| {
				error!("Live query task failed: {}", e);
				RootError::Db(Error::NodeAgent("live query task failed and has been logged"))
			})?;
		}
		Ok(())
	}
}

/// Starts tasks that are required for the correct running of the engine
pub fn start_tasks(opt: &EngineOptions, dbs: Arc<Datastore>) -> (Tasks, Vec<Sender<()>>) {
	let mut cancellation_channels = Vec::with_capacity(TASK_COUNT);
	let nd = init(opt, dbs.clone());
	let lq = live_query_change_feed(opt, dbs);
	cancellation_channels.push(nd.1);
	cancellation_channels.push(lq.1);
	(
		Tasks {
			nd: nd.0,
			lq: lq.0,
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
fn init(opt: &EngineOptions, dbs: Arc<Datastore>) -> (FutureTask, Sender<()>) {
	let _init = LoggingLifecycle::new("node agent initialisation".to_string());
	let tick_interval = opt.tick_interval;
	trace!("Ticker interval is {:?}", tick_interval);
	#[cfg(target_arch = "wasm32")]
	let completed_status = Arc::new(AtomicBool::new(false));
	#[cfg(target_arch = "wasm32")]
	let ret_status = completed_status.clone();

	// We create a channel that can be streamed that will indicate termination
	let (tx, rx) = flume::bounded(1);

	let _fut = spawn_future(async move {
		let _lifecycle = LoggingLifecycle::new("heartbeat task".to_string());
		let ticker = interval_ticker(tick_interval).await;
		let streams = (
			ticker.map(|i| {
				trace!("Node agent tick: {:?}", i);
				Some(i)
			}),
			rx.into_stream().map(|_| None),
		);
		let mut streams = streams.merge();

		while let Some(Some(_)) = streams.next().await {
			if let Err(e) = dbs.tick().await {
				error!("Error running node agent tick: {}", e);
				break;
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

// Start live query on change feeds notification processing
fn live_query_change_feed(opt: &EngineOptions, dbs: Arc<Datastore>) -> (FutureTask, Sender<()>) {
	let tick_interval = opt.tick_interval;
	#[cfg(target_arch = "wasm32")]
	let completed_status = Arc::new(AtomicBool::new(false));
	#[cfg(target_arch = "wasm32")]
	let ret_status = completed_status.clone();

	// We create a channel that can be streamed that will indicate termination
	let (tx, rx) = flume::bounded(1);

	let _fut = spawn_future(async move {
		let _lifecycle = LoggingLifecycle::new("live query agent task".to_string());
		if !FFLAGS.change_feed_live_queries.enabled() {
			// TODO verify test fails since return without completion
			#[cfg(target_arch = "wasm32")]
			completed_status.store(true, Ordering::Relaxed);
			return;
		}
		let ticker = interval_ticker(tick_interval).await;
		let streams = (
			ticker.map(|i| {
				trace!("Live query agent tick: {:?}", i);
				Some(i)
			}),
			rx.into_stream().map(|_| None),
		);
		let mut streams = streams.merge();

		let opt = Options::default();
		while let Some(Some(_)) = streams.next().await {
			if let Err(e) = dbs.process_lq_notifications(&opt).await {
				error!("Error running node agent tick: {}", e);
				break;
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
	use crate::tasks::start_tasks;
	use std::sync::Arc;
	use surrealdb_core::options::EngineOptions;

	#[test_log::test(tokio::test)]
	pub async fn tasks_complete() {
		let opt = EngineOptions::default();
		let dbs = Arc::new(surrealdb_core::kvs::Datastore::new("memory://").await.unwrap());
		let (val, mut chans) = start_tasks(&opt, dbs.clone());
		for chan in chans.drain(..) {
			chan.send(()).unwrap();
		}
		val.resolve().await.unwrap();
	}
}
