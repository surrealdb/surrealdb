mod entry;
mod key;
mod lookup;
mod weight;

pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;

pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub struct TransactionCache {
	/// Store the cache entries
	cache: Cache,
}

impl TransactionCache {
	/// Creates a new transaction cache
	pub(in crate::kvs) fn new() -> Self {
		let cache = Cache::with_weighter(
			*crate::cnf::TRANSACTION_CACHE_SIZE,
			*crate::cnf::TRANSACTION_CACHE_SIZE as u64,
			weight::Weight,
		);
		Self {
			cache,
		}
	}

	/// Fetch an item from the datastore cache
	pub(crate) fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	/// Insert an item into the datastore cache
	pub(crate) fn insert(&self, lookup: Lookup, entry: Entry) {
		self.cache.insert(lookup.into(), entry);
	}

	/// Remove an item from the datastore cache
	pub(crate) fn remove(&self, lookup: Lookup) {
		self.cache.remove(&lookup);
	}

	/// Clear all items from the datastore cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}
}
