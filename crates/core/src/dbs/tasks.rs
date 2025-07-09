use super::Datastore;
use super::interval::IntervalStream;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(in crate::dbs) async fn node_membership_refresh_task(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	interval: Duration,
) {
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
				if let Err(e) = dbs.node_membership_update().await {
					error!("Error updating node registration information: {e}");
				}
			}
		}
	}
	trace!("Background task exited: Updating node registration information");
}

pub(in crate::dbs) async fn node_membership_check_task(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	interval: Duration,
) {
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
				if let Err(e) = dbs.node_membership_expire().await {
					error!("Error processing and archiving inactive nodes: {e}");
				}
			}
		}
	}
	trace!("Background task exited: Processing and archiving inactive nodes");
}

pub(in crate::dbs) async fn node_membership_cleanup_task(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	interval: Duration,
) {
	// Get the delay interval from the config
	let delay = interval;
	// Spawn a future
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
}

pub(in crate::dbs) async fn changefeed_cleanup_task(
	dbs: Arc<Datastore>,
	canceller: CancellationToken,
	interval: Duration,
) {
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
				if let Err(e) = dbs.changefeed_process().await {
					error!("Error running changefeed garbage collection: {e}");
				}
			}
		}
	}
	trace!("Background task exited: Running changefeed garbage collection");
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
