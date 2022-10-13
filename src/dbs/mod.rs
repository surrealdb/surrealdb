use crate::cli::{version, CF};
use crate::cnf::PKG_VERS;
use crate::err::Error;
use once_cell::sync::OnceCell;
use surrealdb::Datastore;

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
	// Log version
	info!(target: LOG, "Running v{} for {} on {}", *PKG_VERS, version::os(), version::arch());
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?;
	// Store database instance
	let _ = DB.set(dbs);
	// All ok
	Ok(())
}
