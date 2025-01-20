mod entry;
mod key;
mod lookup;
mod weight;

use crate::channel;
use crate::err::Error;
use crate::key::table::vl;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{Key, Transaction};
use async_channel::{Receiver, Sender};
pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use uuid::Uuid;

pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

pub(crate) struct DatastoreCache {
	// Store the cache entries
	cache: Cache,
	// Manage the eviction of old cache versions
	sender: Sender<EvictionMessage>,
	// Receives eviction messages
	receiver: Receiver<EvictionMessage>,
	// The transaction factory
	tf: TransactionFactory,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CacheVersion {
	Lq,
}

impl CacheVersion {
	fn get_key(&self, ns: &str, db: &str, tb: &str, version: Uuid) -> Key {
		match self {
			Self::Lq => vl::new(ns, db, tb, version).into(),
		}
	}

	fn lookup_default<'a>(&self, ns: &'a str, db: &'a str, tb: &'a str) -> Lookup<'a> {
		match self {
			Self::Lq => Lookup::Lvs(ns, db, tb, Uuid::default()),
		}
	}
	fn lookup<'a>(&self, ns: &'a str, db: &'a str, tb: &'a str, v: Uuid) -> Lookup<'a> {
		match self {
			Self::Lq => Lookup::Lvs(ns, db, tb, v),
		}
	}
}

pub(crate) struct EvictionMessage {
	ns: String,
	db: String,
	tb: String,
	cache: CacheVersion,
	keep: Uuid,
}

impl DatastoreCache {
	pub(in crate::kvs) fn new(tf: TransactionFactory) -> Self {
		let cache = Cache::with_weighter(
			*crate::cnf::DATASTORE_CACHE_SIZE,
			*crate::cnf::DATASTORE_CACHE_SIZE as u64,
			weight::Weight,
		);
		let (sender, receiver) = channel::unbounded();
		Self {
			cache,
			sender,
			receiver,
			tf,
		}
	}

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
		cache: CacheVersion,
	) -> Result<(), Error> {
		let new_version = Uuid::now_v7();
		// Set the new version
		let key = cache.get_key(ns, db, tb, new_version);
		txn.set(key, vec![], None).await?;
		// Request cleaning old versions
		self.sender
			.send(EvictionMessage {
				ns: ns.to_owned(),
				db: db.to_owned(),
				tb: tb.to_owned(),
				cache,
				keep: new_version,
			})
			.await?;
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
		let version = txn.get_lq_version(ns, db, tb).await?;
		let res = match version {
			Some(version) => cache_version.lookup(ns, db, tb, version),
			None => cache_version.lookup_default(ns, db, tb),
		};
		Ok(res)
	}

	pub(crate) async fn apply_evictions(&self) {
		while let Ok(e) = self.receiver.try_recv() {
			if let Err(e) = self.evict(e).await {
				warn!("failed to cleanup cache version: {e}");
			}
		}
	}

	async fn evict(&self, e: EvictionMessage) -> Result<(), Error> {
		let range = match e.cache {
			CacheVersion::Lq => vl::range_below(&e.ns, &e.db, &e.tb, e.keep),
		};
		let tx = self
			.tf
			.transaction(crate::kvs::TransactionType::Write, crate::kvs::LockType::Optimistic)
			.await?;
		tx.delr(range).await?;
		tx.commit().await?;
		Ok(())
	}
}
