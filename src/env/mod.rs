use crate::cnf::PKG_VERSION;
use crate::err::Error;
use surrealdb::env;

const LOG: &str = "surrealdb::env";

pub async fn init() -> Result<(), Error> {
	// Log version
	info!(target: LOG, "Running {}", release());
	// All ok
	Ok(())
}

/// Get the current release identifier
pub fn release() -> String {
	format!("{} for {} on {}", *PKG_VERSION, env::os(), env::arch())
}
