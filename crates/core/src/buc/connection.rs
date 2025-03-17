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
	let global = match *GLOBAL_BUCKET {
		Some((ref url, None)) => connect(url, true, false, None)?,
		Some((ref url, Some(ref key))) => connect(url, true, false, Some(key))?,
		_ => {
			return Err(Error::Unreachable("No global bucket configured".into()));
		}
	};

	// Create a prefixstore for the specified bucket
	let key = Path::parse(format!("{ns}/{db}/{bu}"))
		.map_err(|_| Error::Unreachable("Failed to construct path segment".into()))?;
	Ok(Arc::new(PrefixStore::new(global, key)))
}

pub fn connect(
	url: &str,
	global: bool,
	readonly: bool,
	prefix: Option<&str>,
) -> Result<Arc<dyn ObjectStore>, Error> {
	if !global && *GLOBAL_BUCKET_ENFORCED {
		return Err(Error::Unreachable("Usage of the global bucket is enforced".into()));
	}

	let url = Url::parse(url)
		.map_err(|_| Error::Unreachable("Failed to parse bucket backend url".into()))?;

	if !super::backend::supported(url.scheme(), global, readonly) {
		Err(Error::Unreachable("Backend not supported".into()))
	} else {
		// TODO(kearfy): I believe the path here is not relevant, as the prefixstore will just extend the path of the parent store?
		let (store, _) = parse_url(&url)
			.map_err(|_| Error::Unreachable("Failed to parse bucket backend url".into()))?;

		let store = if let Some(prefix) = prefix {
			Arc::new(PrefixStore::new(store, prefix))
		} else {
			Arc::new(store) as Arc<dyn ObjectStore>
		};

		Ok(store)
	}
}
