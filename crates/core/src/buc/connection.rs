use crate::{
	cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED},
	err::Error,
};
use dashmap::DashMap;
use object_store::{parse_url, path::Path, prefix::PrefixStore, ObjectStore};
use std::sync::Arc;
use url::Url;

pub type BucketConnections = DashMap<(String, String, String), Arc<dyn ObjectStore>>;

pub fn connect_global(ns: &str, db: &str, bu: &str) -> Result<Arc<dyn ObjectStore>, Error> {
	// Obtain a global store
	let Some(ref url) = *GLOBAL_BUCKET else {
		return Err(Error::NoGlobalBucket);
	};

	let global = connect(url, true, false)?;

	// Create a prefixstore for the specified bucket
	let key = format!("{ns}/{db}/{bu}");
	let key = Path::parse(key.clone()).map_err(|_| Error::InvalidBucketKey(key))?;
	Ok(Arc::new(PrefixStore::new(global, key)))
}

pub fn connect(url: &str, global: bool, readonly: bool) -> Result<Arc<dyn ObjectStore>, Error> {
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::GlobalBucketEnforced);
	}

	let url = Url::parse(url).map_err(|_| Error::InvalidBucketUrl)?;

	let scheme = url.scheme();
	if !super::backend::supported(scheme, global, readonly) {
		Err(Error::UnsupportedBackend(scheme.into()))
	} else {
		let (store, _) = parse_url(&url).map_err(|_| Error::InvalidBucketUrl)?;
		Ok(Arc::new(store) as Arc<dyn ObjectStore>)
	}
}
