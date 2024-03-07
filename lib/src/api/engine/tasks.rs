use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use surrealdb_core::dbs::Options;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::options::EngineOptions;

const LOG: &str = "surrealdb::node";

/// Starts tasks that are required for the correct running of the engine
pub fn start_tasks(opt: &EngineOptions, ct: CancellationToken, dbs: &Datastore) -> Tasks {
	Tasks {
		nd: init(opt, ct.clone(), dbs),
		lq: live_query_change_feed(ct, dbs),
	}
}

pub struct Tasks {
	pub nd: JoinHandle<()>,
	pub lq: JoinHandle<()>,
}

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
fn init(opt: &EngineOptions, ct: CancellationToken, dbs: &Datastore) -> JoinHandle<()> {
	let tick_interval = opt.tick_interval;
	info!(target: LOG, "Started node agent");

	tokio::spawn(async move {
		loop {
			if let Err(e) = dbs.tick().await {
				error!("Error running node agent tick: {}", e);
			}
			tokio::select! {
				_ = ct.cancelled() => {
					info!(target: LOG, "Gracefully stopping node agent");
					break;
				}
				_ = tokio::time::sleep(tick_interval.0) => {}
			}
		}

		info!(target: LOG, "Stopped node agent");
	})
}

// Start live query on change feeds notification processing
fn live_query_change_feed(ct: CancellationToken, kvs: &Datastore) -> JoinHandle<()> {
	tokio::spawn(async move {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		// Spawn the live query change feed consumer, which is used for catching up on relevant change feeds
		tokio::spawn(async move {
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
			info!("Stopped live query node agent")
		});
	})
}
