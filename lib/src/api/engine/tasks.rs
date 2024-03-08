use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

use surrealdb_core::dbs::Options;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;

use crate::Error as RootError;
use surrealdb_core::err::Error;
#[cfg(not(target_arch = "wasm32"))]
use tokio::spawn as spawn_future;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local as spawn_future;

const LOG: &str = "surrealdb::node";

#[derive(Clone)]
#[doc(hidden)]
/// CancellationToken is used as a shortcut for when we don't have access to tokio, such as in wasm
/// it's public because it is required to access from CLI, but otherwise it is an internal component
/// The intention is that it reflects tokio util cancellation token in behaviour
pub struct CancellationToken {
	#[cfg(not(target_arch = "wasm32"))]
	inner: tokio_util::sync::CancellationToken,
	#[cfg(target_arch = "wasm32")]
	inner: Arc<AtomicBool>,
}

impl CancellationToken {
	pub fn new() -> Self {
		Self {
			#[cfg(not(target_arch = "wasm32"))]
			inner: tokio_util::sync::CancellationToken::new(),
			#[cfg(target_arch = "wasm32")]
			inner: Arc::new(AtomicBool::new(false)),
		}
	}

	pub fn cancel(&self) {
		#[cfg(not(target_arch = "wasm32"))]
		self.cancel();
		#[cfg(target_arch = "wasm32")]
		self.inner.store(true, Ordering::Relaxed);
	}

	pub fn cancelled(&self) -> bool {
		#[cfg(not(target_arch = "wasm32"))]
		return self.inner.is_cancelled();
		#[cfg(target_arch = "wasm32")]
		return self.inner.load(Ordering::Relaxed);
	}
}

#[cfg(not(target_arch = "wasm32"))]
type FutureTask = JoinHandle<()>;
#[cfg(target_arch = "wasm32")]
/// This will be true if a task has completed
type FutureTask = AtomicBool;

pub struct Tasks {
	pub nd: FutureTask,
	pub lq: FutureTask,
}

impl Tasks {
	async fn resolve(self) -> Result<(), RootError> {
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
pub fn start_tasks(opt: &EngineOptions, ct: CancellationToken, dbs: Arc<Datastore>) -> Tasks {
	Tasks {
		nd: init(opt, ct.clone(), dbs.clone()),
		lq: live_query_change_feed(ct, dbs),
	}
}

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
fn init(opt: &EngineOptions, ct: CancellationToken, dbs: Arc<Datastore>) -> FutureTask {
	let tick_interval = opt.tick_interval;
	info!(target: LOG, "Started node agent");
	let completed_status = AtomicBool::new(false);

	let fut = spawn_future(async move {
		loop {
			if let Err(e) = dbs.tick().await {
				error!("Error running node agent tick: {}", e);
			}
			tokio::select! {
				_ = ct.cancelled() => {
					info!(target: LOG, "Gracefully stopping node agent");
					break;
				}
				_ = tokio::time::sleep(tick_interval) => {}
			}
		}

		info!(target: LOG, "Stopped node agent");
		completed_status.store(true, Ordering::Relaxed);
	});
	#[cfg(not(target_arch = "wasm32"))]
	return fut;
	#[cfg(target_arch = "wasm32")]
	return completed_status;
}

// Start live query on change feeds notification processing
fn live_query_change_feed(ct: CancellationToken, kvs: Arc<Datastore>) -> FutureTask {
	let completed_status = AtomicBool::new(false);
	let fut = spawn_future(async move {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		let tick_interval = Duration::from_secs(1);

		let opt = Options::default();
		loop {
			if let Err(e) = kvs.process_lq_notifications(&opt).await {
				error!("Error running node agent live query tick: {}", e);
			}
			tokio::select! {
				  _ = ct.cancelled() => {
					   info!(target: LOG, "Gracefully stopping live query node agent");
					   break;
				  }
				  _ = tokio::time::sleep(tick_interval) => {}
			}
		}
		info!("Stopped live query node agent");
		completed_status.store(true, Ordering::Relaxed);
	});
	#[cfg(not(target_arch = "wasm32"))]
	return fut;
	#[cfg(target_arch = "wasm32")]
	return completed_status;
}

#[cfg(test)]
mod test {
	use crate::engine::tasks::CancellationToken;
	use crate::tasks::start_tasks;
	use std::sync::Arc;
	use surrealdb_core::options::EngineOptions;

	#[test_log::test(tokio::test)]
	#[cfg(feature = "kv-mem")]
	pub async fn tasks_complete() {
		let ct = CancellationToken::new();
		let opt = EngineOptions::default();
		let dbs = Arc::new(surrealdb_core::kvs::Datastore::new("memory://").await.unwrap());
		let val = start_tasks(&opt, ct.clone(), dbs.clone());
		ct.cancel();
		val.into().unwrap();
	}
}
