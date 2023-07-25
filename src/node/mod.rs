use crate::cli::CF;
use crate::err::Error;

const LOG: &str = "surrealdb::node";

// The init starts a long-running thread for periodically calling Datastore.tick.
// Datastore.tick is responsible for running garbage collection and other
// background tasks.
// This can be a temporary solution until we have a proper background
// task system in place.
pub async fn init() -> Result<(), Error> {
	let opt = CF.get().unwrap();
	let tick_interval = opt.tick_interval;
	info!(target: LOG, "Node agent starting.");

	let gc = move || {
		let dbs = crate::dbs::DB.get().unwrap();
		tokio::spawn(async move {
			loop {
				tokio::time::sleep(tick_interval).await;
				if let Err(e) = dbs.tick().await {
					error!("Error running node agent tick: {}", e);
				}
			}

			// TODO Do we need to add support for graceful stop?
		});
	};
	std::thread::spawn(gc);

	info!(target: LOG, "Node agent started.");

	Ok(())
}
