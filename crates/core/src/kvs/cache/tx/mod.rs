mod entry;
mod key;
mod lookup;
mod weight;

pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;

pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub(crate) fn new() -> Cache {
	Cache::with_weighter(
		*crate::cnf::TRANSACTION_CACHE_SIZE,
		*crate::cnf::TRANSACTION_CACHE_SIZE as u64,
		weight::Weight,
	)
}
