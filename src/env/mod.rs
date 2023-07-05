use crate::cnf::PKG_VERSION;
#[cfg(feature = "has-storage")]
use crate::err::Error;
use surrealdb::env::{arch, os};

#[cfg(feature = "has-storage")]
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
