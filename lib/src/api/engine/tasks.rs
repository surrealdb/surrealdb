#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

use surrealdb_core::dbs::Options;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;

#[cfg(not(target_arch = "wasm32"))]
use crate::Error as RootError;
#[cfg(not(target_arch = "wasm32"))]
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
		self.inner.cancel();
		#[cfg(target_arch = "wasm32")]
		self.inner.store(true, Ordering::Relaxed);
	}

	pub fn is_cancelled(&self) -> bool {
		#[cfg(not(target_arch = "wasm32"))]
		return self.inner.is_cancelled();
		#[cfg(target_arch = "wasm32")]
		return self.inner.load(Ordering::Relaxed);
	}

	pub async fn cancelled(&self) {
		#[cfg(not(target_arch = "wasm32"))]
		return self.inner.cancelled().await;
		#[cfg(target_arch = "wasm32")]
		{
			while !self.inner.load(Ordering::Relaxed) {
				tokio::task::yield_now().await;
			}
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub fn into_inner(self) -> tokio_util::sync::CancellationToken {
		self.inner
	}
}

#[cfg(not(target_arch = "wasm32"))]
type FutureTask = JoinHandle<()>;
#[cfg(target_arch = "wasm32")]
/// This will be true if a task has completed
type FutureTask = Arc<AtomicBool>;

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
	#[cfg(target_arch = "wasm32")]
	let completed_status = Arc::new(AtomicBool::new(false));
	#[cfg(target_arch = "wasm32")]
	let ret_status = completed_status.clone();
	let _fut = spawn_future(async move {
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
		#[cfg(target_arch = "wasm32")]
		completed_status.store(true, Ordering::Relaxed);
	});
	#[cfg(not(target_arch = "wasm32"))]
	return _fut;
	#[cfg(target_arch = "wasm32")]
	return ret_status;
}

// Start live query on change feeds notification processing
fn live_query_change_feed(ct: CancellationToken, kvs: Arc<Datastore>) -> FutureTask {
	#[cfg(target_arch = "wasm32")]
	let completed_status = Arc::new(AtomicBool::new(false));
	#[cfg(target_arch = "wasm32")]
	let ret_status = completed_status.clone();
	let _fut = spawn_future(async move {
		if !FFLAGS.change_feed_live_queries.enabled() {
			// TODO verify test fails since return without completion
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
		#[cfg(target_arch = "wasm32")]
		completed_status.store(true, Ordering::Relaxed);
	});
	#[cfg(not(target_arch = "wasm32"))]
	return _fut;
	#[cfg(target_arch = "wasm32")]
	return ret_status;
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
