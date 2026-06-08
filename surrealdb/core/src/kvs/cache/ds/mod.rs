mod entry;
mod key;
mod lookup;
mod weight;

use anyhow::Result;
#[cfg(feature = "jwks")]
pub(crate) use entry::CachedJwks;
pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use quick_cache::sync::DefaultLifecycle;
use quick_cache::{DefaultHashBuilder, OptionsBuilder};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

/// Concurrent cache for values that should be shared across transactions on
/// this datastore (schema slices, live-query versions, JWKS payloads, etc.).
///
/// # Values
/// Cache values are cloned when fetched. Value types should be wrapped in an
/// `Arc<_>` to avoid expensive clone operations when retrieving values from the
/// cache. If interior mutability is required, `Arc<Mutex<_>>` or `Arc<RwLock<_>>`
/// can be used.
///
/// # Thread safety
/// The cache instance can be wrapped with an `Arc` and safely shared between
/// threads. All methods are accessible via non-mut references, so no further
/// synchronisation with mutexes is required for the cache itself.
pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

/// Datastore-wide cache; see [`Cache`] for behaviour notes.
pub struct DatastoreCache {
	/// Store the cache entries
	cache: Cache,
}

impl DatastoreCache {
	/// Creates a new empty cache for a datastore.
	pub(in crate::kvs) fn new(size: usize) -> Self {
		let options = OptionsBuilder::new()
			.estimated_items_capacity(size)
			.weight_capacity(size as u64)
			.build()
			.expect("valid datastore cache options");
		let cache = Cache::with_options(
			options,
			weight::Weight,
			DefaultHashBuilder::default(),
			DefaultLifecycle::default(),
		);
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

	/// Clear all items from the datastore cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}

	/// Set the latest libe query version for a table
	pub(crate) fn set_live_queries_version(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		let key = Lookup::Lvv(ns, db, tb);
		self.insert(key, Entry::Lvv(Uuid::now_v7()));
	}

	/// Get the latest live query version for a table
	pub fn get_live_queries_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Uuid> {
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
}
