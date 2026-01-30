pub(crate) mod key;

use std::ops::Deref;

use anyhow::Result;
use priority_lfu::{Cache, CacheKeyLookup};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

pub struct DatastoreCache {
	/// Store the cache entries
	cache: Cache,
}

impl DatastoreCache {
	/// Creates a new datastore cache
	pub(in crate::kvs) fn new() -> Self {
		let cache = Cache::new(*crate::cnf::DATASTORE_CACHE_SIZE);
		Self {
			cache,
		}
	}

	/// Clear all items from the datastore cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}

	/// Clear the cache entries for a specific table by creating a new version
	pub(crate) fn clear_tb(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		// Generate a new live queries version to invalidate all related caches
		self.new_live_queries_version(ns, db, tb);
	}

	pub fn get_live_queries_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Uuid> {
		// Get the live-queries cache version
		let lookup = key::LiveQueriesVersionCacheKeyRef(ns, db, tb);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let version = Uuid::now_v7();
				self.cache.insert(lookup.to_owned_key(), version);
				Ok(version)
			}
		}
	}

	pub(crate) fn new_live_queries_version(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		let lookup = key::LiveQueriesVersionCacheKeyRef(ns, db, tb);
		self.cache.insert(lookup.to_owned_key(), Uuid::now_v7());
	}
}

impl Deref for DatastoreCache {
	type Target = Cache;

	fn deref(&self) -> &Self::Target {
		&self.cache
	}
}
