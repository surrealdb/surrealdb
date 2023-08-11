use crate::cli::CF;
use crate::err::Error;

const LOG: &str = "surrealdb::node";

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
//
// This function needs to be called before after the dbs::init and before the net::init functions.
// It needs to be before net::init because the net::init function blocks until the web server stops.
pub async fn init() -> Result<(), Error> {
	let opt = CF.get().unwrap();
	let tick_interval = opt.tick_interval;
	info!(target: LOG, "Node agent starting.");

	// This requires the nodes::init function to be called after the dbs::init function.
	let dbs = crate::dbs::DB.get().unwrap();
	tokio::spawn(async move {
		loop {
			if let Err(e) = dbs.tick().await {
				error!("Error running node agent tick: {}", e);
			}
			tokio::time::sleep(tick_interval).await;
		}

		// TODO Do we need to add support for graceful stop?
	});

	info!(target: LOG, "Node agent started.");

	Ok(())
}
