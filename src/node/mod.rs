use std::time::Duration;

use surrealdb::dbs::Options;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use surrealdb::fflags::FFLAGS;

use crate::cli::CF;

const LOG: &str = "surrealdb::node";

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
pub fn init(ct: CancellationToken) -> JoinHandle<()> {
	let opt = CF.get().unwrap();
	let tick_interval = opt.tick_interval;
	info!(target: LOG, "Started node agent");

	// This requires the nodes::init function to be called after the dbs::init function.
	let dbs = crate::dbs::DB.get().unwrap();

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
				_ = tokio::time::sleep(tick_interval) => {}
			}
		}

		info!(target: LOG, "Stopped node agent");
	})
}

// Start live query on change feeds notification processing
pub fn live_query_change_feed(ct: CancellationToken) -> JoinHandle<()> {
	tokio::spawn(async move {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		// Spawn the live query change feed consumer, which is used for catching up on relevant change feeds
		tokio::spawn(async move {
			let kvs = crate::dbs::DB.get().unwrap();
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
