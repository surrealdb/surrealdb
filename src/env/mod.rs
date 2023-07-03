use crate::cnf::PKG_VERSION;
use crate::err::Error;
use surrealdb::env::{arch, os};

pub async fn init() -> Result<(), Error> {
	// Log version
	info!("Running {}", release());
	// All ok
	Ok(())
}

/// Get the current release identifier
pub fn release() -> String {
	format!("{} for {} on {}", *PKG_VERSION, os(), arch())
}
