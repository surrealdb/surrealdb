mod entry;
mod key;
mod lookup;
mod weight;

use anyhow::Result;
pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub struct DatastoreCache {
	/// Store the cache entries
	cache: Cache,
}

impl DatastoreCache {
	/// Creates a new datastore cache
	pub(in crate::kvs) fn new(capacity: usize) -> Self {
		let cache = Cache::with_weighter(capacity, capacity as u64, weight::Weight);
		Self {
			cache,
		}
	}

	/// Fetches an item from the datastore cache
	pub(crate) fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	/// Inserts an item into the datastore cache
	pub(crate) fn insert(&self, lookup: Lookup, entry: Entry) {
		self.cache.insert(lookup.into(), entry);
	}

	/// Clear the cache entry for a table
	pub(crate) fn clear_tb(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		let key = Lookup::Tb(ns, db, tb);
		self.cache.remove(&key);
	}

	/// Clear all items from the datastore cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}

	pub fn get_live_queries_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Uuid> {
		// Get the live-queries cache version
		let key = Lookup::Lvv(ns, db, tb);
		let version = match self.get(&key) {
			Some(val) => val.try_info_lvv()?,
			None => {
				let version = Uuid::now_v7();
				let val = Entry::Lvv(version);
				self.insert(key, val);
				version
			}
		};
		Ok(version)
	}

	pub(crate) fn new_live_queries_version(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		let key = Lookup::Lvv(ns, db, tb);
		self.insert(key, Entry::Lvv(Uuid::now_v7()));
	}
}
