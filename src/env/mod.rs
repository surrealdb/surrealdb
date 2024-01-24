use crate::cnf::PKG_VERSION;
use crate::err::Error;
use once_cell::sync::Lazy;
use surrealdb::env::{arch, os};

/// Stores the current release identifier
pub static RELEASE: Lazy<String> =
	Lazy::new(|| format!("{} for {} on {}", *PKG_VERSION, os(), arch()));

pub async fn init() -> Result<(), Error> {
	// Log version
	info!("Running {}", *RELEASE);
	// All ok
	Ok(())
}
