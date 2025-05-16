use crate::cnf::PKG_VERSION;
use anyhow::Result;
use std::sync::LazyLock;
use surrealdb::env::{arch, os};

/// Stores the current release identifier
pub static RELEASE: LazyLock<String> =
	LazyLock::new(|| format!("{} for {} on {}", *PKG_VERSION, os(), arch()));

pub fn init() -> Result<()> {
	// Log version
	info!("Running {}", *RELEASE);
	// All ok
	Ok(())
}
