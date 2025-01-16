mod entry;
mod key;
mod lookup;
mod weight;

use crate::channel;
use crate::err::Error;
use crate::key::table::vl::Vl;
use crate::kvs::{Key, Transaction};
use async_channel::{Receiver, Sender};
pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use std::ops::Range;
use uuid::Uuid;

pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub(crate) struct DatastoreCache {
	// Store the cache entries
	cache: Cache,
	// Manage the eviction of old cache versions
	sender: Sender<(CacheVersion, Uuid)>,
	_receiver: Receiver<(CacheVersion, Uuid)>,
}

#[derive(Clone, Copy)]
pub(crate) enum CacheVersion {
	Lq,
}

impl CacheVersion {
	fn get_key(&self, ns: &str, db: &str, tb: &str, version: Uuid) -> Key {
		match self {
			Self::Lq => crate::key::table::vl::new(ns, db, tb, version).into(),
		}
	}

	fn get_range(&self, ns: &str, db: &str, tb: &str) -> Range<Key> {
		match self {
			Self::Lq => {
				crate::key::table::vl::prefix(ns, db, tb)..crate::key::table::vl::suffix(ns, db, tb)
			}
		}
	}

	fn lookup_default<'a>(&self, ns: &'a str, db: &'a str, tb: &'a str) -> Lookup<'a> {
		match self {
			Self::Lq => Lookup::Lvs(ns, db, tb, Uuid::default()),
		}
	}
	fn lookup<'a>(&self, ns: &'a str, db: &'a str, tb: &'a str, key: &Key) -> Lookup<'a> {
		match self {
			Self::Lq => {
				let vl: Vl = key.into();
				Lookup::Lvs(ns, db, tb, vl.v)
			}
		}
	}
}

impl Default for DatastoreCache {
	fn default() -> Self {
		let cache = Cache::with_weighter(
			*crate::cnf::DATASTORE_CACHE_SIZE,
			*crate::cnf::DATASTORE_CACHE_SIZE as u64,
			weight::Weight,
		);
		let (sender, _receiver) = channel::unbounded();
		Self {
			cache,
			sender,
			_receiver,
		}
	}
}

impl DatastoreCache {
	pub(crate) fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	pub(crate) fn insert(&self, lookup: Lookup, entry: Entry) {
		self.cache.insert(lookup.into(), entry);
	}

	pub(crate) async fn set_new_version(
		&self,
		txn: &Transaction,
		ns: &str,
		db: &str,
		tb: &str,
		cache_version: CacheVersion,
	) -> Result<(), Error> {
		let new_version = Uuid::now_v7();
		// Set the new version
		let key = cache_version.get_key(ns, db, tb, new_version);
		txn.set(key, vec![], None).await?;
		// Request cleaning old versions
		self.sender.send((cache_version, new_version)).await?;
		Ok(())
	}

	pub(crate) async fn get_cache_lookup<'a>(
		&self,
		txn: &Transaction,
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		cache_version: CacheVersion,
	) -> Result<Lookup<'a>, Error> {
		let range = cache_version.get_range(ns, db, tb);
		let keys = txn.keys(range, 1, None).await?;
		let res = match keys.into_iter().next() {
			Some(key) => cache_version.lookup(ns, db, tb, &key),
			None => cache_version.lookup_default(ns, db, tb),
		};
		Ok(res)
	}
}
