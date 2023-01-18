use crate::cli::CF;
use crate::err::Error;
use once_cell::sync::OnceCell;
use surrealdb::kvs::Datastore;

pub static DB: OnceCell<Datastore> = OnceCell::new();

const LOG: &str = "surrealdb::dbs";

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.strict {
		true => info!(target: LOG, "Database strict mode is enabled"),
		false => info!(target: LOG, "Database strict mode is disabled"),
	};
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?;
	// Store database instance
	let _ = DB.set(dbs);
	// All ok
	Ok(())
}
