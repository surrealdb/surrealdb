mod entry;
mod key;
mod lookup;
mod weight;

use crate::err::Error;
pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use uuid::Uuid;

pub type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub struct DatastoreCache {
	/// Store the cache entries
	cache: Cache,
}

impl DatastoreCache {
	pub(in crate::kvs) fn new() -> Self {
		let cache = Cache::with_weighter(
			*crate::cnf::DATASTORE_CACHE_SIZE,
			*crate::cnf::DATASTORE_CACHE_SIZE as u64,
			weight::Weight,
		);
		Self {
			cache,
		}
	}

	pub(crate) fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	pub(crate) fn insert(&self, lookup: Lookup, entry: Entry) {
		self.cache.insert(lookup.into(), entry);
	}

	pub fn get_live_queries_version(&self, ns: &str, db: &str, tb: &str) -> Result<Uuid, Error> {
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

	pub(crate) fn new_live_queries_version(&self, ns: &str, db: &str, tb: &str) {
		let key = Lookup::Lvv(ns, db, tb);
		self.insert(key, Entry::Lvv(Uuid::now_v7()));
	}
}
