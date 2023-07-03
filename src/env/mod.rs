use crate::cnf::PKG_VERSION;
#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
use crate::err::Error;
use surrealdb::env::{arch, os};

#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
const LOG: &str = "surrealdb::env";

#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
pub async fn init() -> Result<(), Error> {
	// Log version
	info!(target: LOG, "Running {}", release());
	// All ok
	Ok(())
}

/// Get the current release identifier
pub fn release() -> String {
	format!("{} for {} on {}", *PKG_VERSION, os(), arch())
}
