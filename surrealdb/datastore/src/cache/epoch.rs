//! Catalog definitions shared by the transactions of one catalog epoch.
//!
//! Where a datastore is its store's sole writer, every transaction that names
//! the same catalog epoch ([`crate::tx::Transaction::catalog_epoch`]) sees the
//! same namespaces, databases and tables, each in the same state: every commit
//! that writes one of their definitions moves the epoch. A definition one such
//! transaction read is therefore the definition every other one would read,
//! and is kept here under the epoch it was read in. An entry of an earlier
//! epoch is never answered; it stays until the cache's capacity evicts it.
//!
//! Only definitions that exist are kept: a lookup that finds nothing reads the
//! store each time.

use std::sync::Arc;

use quick_cache::sync::Cache;
use quick_cache::{Equivalent, UnitWeighter};
use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_strand::TableName;

/// How many definitions the cache keeps, across epochs.
const CAPACITY: usize = 4096;

#[derive(Hash, Eq, PartialEq)]
enum Key {
	Namespace(u64, String),
	Database(u64, String, String),
	Table(u64, String, String, TableName),
	Version(u64, NamespaceId, DatabaseId, TableName),
}

/// A borrowed [`Key`].
#[derive(Hash)]
pub(crate) enum Lookup<'a> {
	Namespace(u64, &'a str),
	Database(u64, &'a str, &'a str),
	Table(u64, &'a str, &'a str, &'a TableName),
	Version(u64, NamespaceId, DatabaseId, &'a TableName),
}

impl Equivalent<Key> for Lookup<'_> {
	fn equivalent(&self, key: &Key) -> bool {
		match (self, key) {
			(Self::Namespace(e, ns), Key::Namespace(ke, kns)) => e == ke && *ns == kns,
			(Self::Database(e, ns, db), Key::Database(ke, kns, kdb)) => {
				e == ke && *ns == kns && *db == kdb
			}
			(Self::Table(e, ns, db, tb), Key::Table(ke, kns, kdb, ktb)) => {
				e == ke && *ns == kns && *db == kdb && *tb == ktb
			}
			(Self::Version(e, ns, db, tb), Key::Version(ke, kns, kdb, ktb)) => {
				e == ke && ns == kns && db == kdb && *tb == ktb
			}
			_ => false,
		}
	}
}

impl From<Lookup<'_>> for Key {
	fn from(lookup: Lookup<'_>) -> Self {
		match lookup {
			Lookup::Namespace(e, ns) => Key::Namespace(e, ns.to_owned()),
			Lookup::Database(e, ns, db) => Key::Database(e, ns.to_owned(), db.to_owned()),
			Lookup::Table(e, ns, db, tb) => Key::Table(e, ns.to_owned(), db.to_owned(), tb.clone()),
			Lookup::Version(e, ns, db, tb) => Key::Version(e, ns, db, tb.clone()),
		}
	}
}

/// The catalog definitions read under each catalog epoch, by the epoch and
/// what names them.
pub(crate) struct EpochCatalog {
	entries: Cache<Key, Arc<dyn std::any::Any + Send + Sync>, UnitWeighter>,
}

impl Default for EpochCatalog {
	fn default() -> Self {
		Self {
			entries: Cache::with_weighter(CAPACITY, CAPACITY as u64, UnitWeighter),
		}
	}
}

impl EpochCatalog {
	/// The definition kept for `lookup`, as a `T`.
	pub(crate) fn get<T: Send + Sync + 'static>(&self, lookup: &Lookup<'_>) -> Option<Arc<T>> {
		self.entries.get(lookup).and_then(|held| held.downcast::<T>().ok())
	}

	/// Keep `value` for `lookup`.
	pub(crate) fn insert<T: Send + Sync + 'static>(&self, lookup: Lookup<'_>, value: Arc<T>) {
		self.entries.insert(lookup.into(), value);
	}
}
