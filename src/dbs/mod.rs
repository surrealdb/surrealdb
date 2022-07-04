use crate::cli::CF;
use crate::err::Error;
use once_cell::sync::OnceCell;
use surrealdb::Datastore;

pub static DB: OnceCell<Datastore> = OnceCell::new();

pub async fn init() -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?;
	// Store database instance
	let _ = DB.set(dbs);
	// All ok
	Ok(())
}
