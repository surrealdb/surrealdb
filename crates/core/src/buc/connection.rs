use crate::{
	cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED},
	err::Error,
};
use dashmap::DashMap;
use object_store::{parse_url, path::Path, prefix::PrefixStore, ObjectStore};
use std::sync::Arc;
use url::Url;

// Helper type to represent how bucket connections are persisted
pub type BucketConnections = DashMap<(String, String, String), Arc<dyn ObjectStore>>;

/// Connect to a global bucket, if one is configured
/// If no global bucket is configured, the NoGlobalBucket error will be returned
/// The key in the global bucket will be: `{ns}/{db}/{bu}`
pub fn connect_global(ns: &str, db: &str, bu: &str) -> Result<Arc<dyn ObjectStore>, Error> {
	// Obtain the URL for the global bucket
	let Some(ref url) = *GLOBAL_BUCKET else {
		return Err(Error::NoGlobalBucket);
	};

	// Connect to the global bucket
	let global = connect(url, true, false)?;

	// Create a prefixstore for the specified bucket
	let key = format!("{ns}/{db}/{bu}");
	let key = Path::parse(key.clone()).map_err(|_| Error::InvalidBucketKey(key))?;
	Ok(Arc::new(PrefixStore::new(global, key)))
}

/// Connects to a bucket by it's connection URL
/// The function:
/// - Checks if the global bucket is enfored
/// - Validates the URL
/// - Checks if the backend is supported
/// - Attempts to connect to the bucket
pub fn connect(url: &str, global: bool, readonly: bool) -> Result<Arc<dyn ObjectStore>, Error> {
	// Check if the global bucket is enforced
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::GlobalBucketEnforced);
	}

	// Attempt to parse the string into a `Url`
	let url = Url::parse(url).map_err(|_| Error::InvalidBucketUrl)?;

	// Check if the backend is supported
	let scheme = url.scheme();
	if !super::backend::supported(scheme, global, readonly) {
		Err(Error::UnsupportedBackend(scheme.into()))
	} else {
		// All good, connect to the store
		let (store, _) = parse_url(&url).map_err(|_| Error::InvalidBucketUrl)?;
		Ok(Arc::new(store) as Arc<dyn ObjectStore>)
	}
}
