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
		return Err(Error::Unreachable("No global bucket configured".into()));
	};

	let global = connect(url, true, false)?;

	// Create a prefixstore for the specified bucket
	let key = Path::parse(format!("{ns}/{db}/{bu}"))
		.map_err(|_| Error::Unreachable("Failed to construct path segment".into()))?;
	Ok(Arc::new(PrefixStore::new(global, key)))
}

pub fn connect(url: &str, global: bool, readonly: bool) -> Result<Arc<dyn ObjectStore>, Error> {
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::Unreachable("Usage of the global bucket is enforced".into()));
	}

	let url = Url::parse(url)
		.map_err(|_| Error::Unreachable("Failed to parse bucket backend url".into()))?;

	if !super::backend::supported(url.scheme(), global, readonly) {
		Err(Error::Unreachable("Backend not supported".into()))
	} else {
		let (store, _) = parse_url(&url)
			.map_err(|_| Error::Unreachable("Failed to parse bucket backend url".into()))?;

		Ok(Arc::new(store) as Arc<dyn ObjectStore>)
	}
}
