use std::sync::Arc;

use dashmap::DashMap;

use super::store::prefixed::PrefixedStore;
use super::store::{ObjectKey, ObjectStore};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED};
use crate::err::Error;

// Helper type to represent how bucket connections are persisted
pub(crate) type BucketConnections = DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>;

#[derive(Hash, PartialEq, Eq)]
pub(crate) struct BucketConnectionKey {
	ns: NamespaceId,
	db: DatabaseId,
	bu: String,
}

impl BucketConnectionKey {
	pub fn new(ns: NamespaceId, db: DatabaseId, bu: &str) -> Self {
		Self {
			ns,
			db,
			bu: bu.into(),
		}
	}
}

/// Connect to a global bucket, if one is configured
/// If no global bucket is configured, the NoGlobalBucket error will be returned
/// The key in the global bucket will be: `{ns}/{db}/{bu}`
pub(crate) async fn connect_global(
	ns: NamespaceId,
	db: DatabaseId,
	bu: &str,
) -> Result<Arc<dyn ObjectStore>, Error> {
	// Obtain the URL for the global bucket
	let Some(ref url) = *GLOBAL_BUCKET else {
		return Err(Error::NoGlobalBucket);
	};

	// Connect to the global bucket
	let global = connect(url, true, false).await?;

	// Create a prefixstore for the specified bucket
	let key = ObjectKey::new(format!("/{ns}/{db}/{bu}"));
	Ok(Arc::new(PrefixedStore::new(global, key)))
}

/// Connects to a bucket by it's connection URL
/// The function:
/// - Checks if the global bucket is enforced
/// - Validates the URL
/// - Checks if the backend is supported
/// - Attempts to connect to the bucket
pub(crate) async fn connect(
	url: &str,
	global: bool,
	readonly: bool,
) -> Result<Arc<dyn ObjectStore>, Error> {
	// Check if the global bucket is enforced
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::GlobalBucketEnforced);
	}

	// Connect to the backend
	super::backend::connect(url, global, readonly).await
}
