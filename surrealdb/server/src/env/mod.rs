use std::sync::LazyLock;

use anyhow::Result;

use crate::cnf::PKG_VERSION;

/// Stores the current release identifier
pub static RELEASE: LazyLock<String> = LazyLock::new(|| {
	format!("{} for {} on {}", *PKG_VERSION, std::env::consts::OS, std::env::consts::ARCH)
});

pub fn init() -> Result<()> {
	// Log version
	info!("Running {}", *RELEASE);
	// All ok
	Ok(())
}
