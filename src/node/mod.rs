use futures_util::FutureExt;
use std::time::Duration;
use surrealdb::fflags::FFLAGS;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

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
		if FFLAGS.change_feed_live_queries.enabled() {
			warn!("\n\nFEATURE ENABLED SPAWNING\n\n");
			// Spawn the live query change feed consumer, which is used for catching up on relevant change feeds
			tokio::spawn(async move {
				let kvs = crate::dbs::DB.get().unwrap();
				let stop_signal = ct.cancelled();
				let tick_interval = Duration::from_secs(1);
				let mut interval = tokio::time::interval(tick_interval);
				// Don't bombard the database if we miss some ticks
				interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
				// Delay sending the first tick
				interval.tick().await;

				let ticker = IntervalStream::new(interval);

				let streams = (ticker.map(Some), futures::stream::once(stop_signal.map(|_| None)));

				let mut stream = streams.merge();

				while let Some(Some(_)) = stream.next().await {
					match kvs.process_lq_notifications().await {
						Ok(()) => trace!("Live Query poll ran successfully"),
						Err(error) => error!("Error running live query poll: {error}"),
					}
				}
			});
		} else {
			warn!("\n\nFEATURE DISABLED\n\n");
		}
	})
}
