mod entry;
mod key;
mod lookup;
mod weight;

pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;

pub type Cache = quick_cache::sync::Cache<key::Key, entry::Entry, weight::Weight>;

pub fn new() -> Cache {
	quick_cache::sync::Cache::with_weighter(
		*crate::cnf::DATASTORE_CACHE_SIZE,
		*crate::cnf::DATASTORE_CACHE_SIZE as u64,
		weight::Weight,
	)
}
