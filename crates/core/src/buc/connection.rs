use crate::{
	cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED},
	err::Error,
};
use dashmap::DashMap;
use std::sync::Arc;

use super::store::{prefixed::PrefixedStore, ObjectKey, ObjectStore};

// Helper type to represent how bucket connections are persisted
pub(crate) type BucketConnections = DashMap<(String, String, String), Arc<dyn ObjectStore>>;

/// Connect to a global bucket, if one is configured
/// If no global bucket is configured, the NoGlobalBucket error will be returned
/// The key in the global bucket will be: `{ns}/{db}/{bu}`
pub(crate) fn connect_global(ns: &str, db: &str, bu: &str) -> Result<Arc<dyn ObjectStore>, Error> {
	// Obtain the URL for the global bucket
	let Some(ref url) = *GLOBAL_BUCKET else {
		return Err(Error::NoGlobalBucket);
	};

	// Connect to the global bucket
	let global = connect(url, true, false)?;

	// Create a prefixstore for the specified bucket
	let key = ObjectKey::from(format!("/{ns}/{db}/{bu}"));
	Ok(Arc::new(PrefixedStore::new(global, key)))
}

/// Connects to a bucket by it's connection URL
/// The function:
/// - Checks if the global bucket is enforced
/// - Validates the URL
/// - Checks if the backend is supported
/// - Attempts to connect to the bucket
pub(crate) fn connect(
	url: &str,
	global: bool,
	readonly: bool,
) -> Result<Arc<dyn ObjectStore>, Error> {
	// Check if the global bucket is enforced
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::GlobalBucketEnforced);
	}

	// Connect to the backend
	super::backend::connect(url, global, readonly)
}
