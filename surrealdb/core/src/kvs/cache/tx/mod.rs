pub(crate) mod key;

use std::ops::Deref;

use priority_lfu::Cache;

pub struct TransactionCache {
	/// Store the cache entries
	cache: Cache,
}

impl TransactionCache {
	/// Creates a new transaction cache
	pub(in crate::kvs) fn new() -> Self {
		let cache = Cache::new(*crate::cnf::TRANSACTION_CACHE_SIZE);
		Self {
			cache,
		}
	}

	/// Clear all items from the transaction cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}
}

impl Deref for TransactionCache {
	type Target = Cache;

	fn deref(&self) -> &Self::Target {
		&self.cache
	}
}
