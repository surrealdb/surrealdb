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
//! store each time. A listing is kept whole, empty or not, since an empty one
//! is what the store holds.
//!
//! A listing is a definition's neighbours, so it is kept under the same
//! contract: whatever adds, removes or rewrites a member moves the epoch.
//!
//! - The tables of a database: every write of a table definition (`DEFINE`, `ALTER` and `REMOVE
//!   TABLE`, and the rewrite of one that registers an event, a view or a live query) is a change of
//!   the `!tb` key, which marks the writing transaction as having changed the catalog
//!   (`Transaction::touches_catalog`), as does the deletion of a range that holds them.
//! - A table's fields, events and the views reading it: a write of a field (`!fd`), an event
//!   (`!ev`) or a view's registration (`!ft`) under a table, and the deletion of a range starting
//!   at the table's root or at one of those prefixes, marks the writing transaction the same way,
//!   whatever path the write takes and whether or not a catalog version is stamped with it.
//!
//! A transaction so marked names no epoch, so it neither reads what it wrote from the epoch nor
//! keeps it there.
//!
//! A table's definition is kept under its ids as well as its names.
//!
//! A table's layouts (its `!mi` ring and `!lg` generations) are kept too, empty
//! ones included. Every write of those keys moves the epoch: a mint
//! (`Transaction::mint_with_body`, reached from a stamped generation's
//! settlement and from the DDL that mints a migration) and a floor move
//! (`Transaction::move_floor`) mark the transaction as having changed the
//! catalog, and the DDL that clears a table's keys (`DEFINE TABLE ... OVERWRITE`,
//! `REMOVE TABLE`) stamps the table. Layouts are never answered to, or kept
//! from, a writeable transaction, whose reads must see its own writes.
//!
//! An index's build state is kept in one form only: that the index is settled online, under the
//! catalog epoch and the build epoch together ([`crate::tx::Transaction::build_epoch`]) and its
//! namespace, database, table and index id. The entry says that the index's durable build state
//! (`!bs`) is `Online` with no build owner, or absent while the catalog still names the index,
//! and nothing else is kept (index ids are never reused and a rename stamps the catalog, so the
//! id alone names the index): a building, closing, failed, replaced or retired index is read from
//! the store each time. Every commit that writes a `!bs` key or a value shape key of an index
//! (`!it`, `!ia`, `!ir`), or deletes a range that may hold one (a phase change, a generation or
//! owner change, a heartbeat, a removal of the index or of its table), moves the build epoch, and a
//! transaction that has itself written one names none. A transaction answers or keeps the entry
//! only while it names both epochs, so the build state it would read is the build state the entry
//! was read from.
//!
//! The value shapes a settled online B-tree index has stored (its `!it` marker, `!ia` and `!ir`
//! keys) are kept per index under the same pair of epochs, and only for an index the online entry
//! above names. Every writer of those keys moves the build epoch by writing them, and the
//! following also hold:
//!
//! - `Transaction::record_index_shapes` for a document write (`!ia`) stamps the table's catalog
//!   version in the same transaction, which moves the catalog epoch.
//! - `Transaction::track_index_shapes` (`!it` and `!ir`, at the beginning of a build) runs in the
//!   builder's transaction after `maintain_build_ownership` has rewritten the index's `!bs` in it,
//!   which moves the build epoch, and after `wipe_index_data` has deleted the index's range, which
//!   holds the same keys.
//! - `Transaction::record_index_shapes` for a build (`!ir`) runs between the `!bs` write that left
//!   `Online` and the one that returns to it, so no entry is kept or served for the index.
//! - A deletion of an index's range is a build's wipe or the cleanup of a retired build, each with
//!   a `!bs` write in the same transaction, or the cleanup of a removed index, which the catalog no
//!   longer names: the removal stamped it, and index ids are never reused.
//!
//! A transaction that has written a shape key of its own neither answers nor keeps shapes from the
//! epochs: a build's recording stamps nothing, so it still names both, and a rollback to a save
//! point could otherwise return its reads to the snapshot's shapes while an enclosing write stands.
//!
//! What a database's catalog state names (its table keys, and the catalog versions proven against
//! them) and which tables its `REFERENCE` fields can target are pure functions of its table
//! definitions (`!tb`), version stamps (`!cv`) and field definitions (`!fd`). They are kept whole
//! under the catalog epoch, on the same contract as the listings above: a write of a table
//! definition marks the writing transaction as having changed the catalog, a stamp writes `!cv`
//! and counts, and a write of a field marks it.

use std::sync::Arc;

use quick_cache::sync::Cache;
use quick_cache::{Equivalent, UnitWeighter};
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId};
use surrealdb_strand::TableName;

/// How many definitions the cache keeps, across epochs.
const CAPACITY: usize = 4096;

#[derive(Hash, Eq, PartialEq)]
enum Key {
	Namespace(u64, String),
	Database(u64, String, String),
	Table(u64, String, String, TableName),
	Version(u64, NamespaceId, DatabaseId, TableName),
	Layouts(u64, NamespaceId, DatabaseId, TableName),
	TableById(u64, NamespaceId, DatabaseId, TableName),
	Tables(u64, NamespaceId, DatabaseId),
	Events(u64, NamespaceId, DatabaseId, TableName),
	Fields(u64, NamespaceId, DatabaseId, TableName),
	Views(u64, NamespaceId, DatabaseId, TableName),
	OnlineIndex(u64, u64, NamespaceId, DatabaseId, TableName, IndexId),
	IndexShapes(u64, u64, NamespaceId, DatabaseId, TableName, IndexId),
	CatalogState(u64, NamespaceId, DatabaseId),
	ReferenceTargets(u64, NamespaceId, DatabaseId),
}

/// A borrowed [`Key`].
#[derive(Hash)]
pub(crate) enum Lookup<'a> {
	Namespace(u64, &'a str),
	Database(u64, &'a str, &'a str),
	Table(u64, &'a str, &'a str, &'a TableName),
	Version(u64, NamespaceId, DatabaseId, &'a TableName),
	Layouts(u64, NamespaceId, DatabaseId, &'a TableName),
	TableById(u64, NamespaceId, DatabaseId, &'a TableName),
	Tables(u64, NamespaceId, DatabaseId),
	Events(u64, NamespaceId, DatabaseId, &'a TableName),
	Fields(u64, NamespaceId, DatabaseId, &'a TableName),
	Views(u64, NamespaceId, DatabaseId, &'a TableName),
	/// An index read as settled online under a catalog epoch and a build epoch.
	OnlineIndex(u64, u64, NamespaceId, DatabaseId, &'a TableName, IndexId),
	/// The value shapes of an index settled online, under a catalog epoch and a build epoch.
	IndexShapes(u64, u64, NamespaceId, DatabaseId, &'a TableName, IndexId),
	/// What identifies the catalog of every table of a database.
	CatalogState(u64, NamespaceId, DatabaseId),
	/// Which tables the `REFERENCE` fields of a database can target.
	ReferenceTargets(u64, NamespaceId, DatabaseId),
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
			(Self::Version(e, ns, db, tb), Key::Version(ke, kns, kdb, ktb))
			| (Self::Layouts(e, ns, db, tb), Key::Layouts(ke, kns, kdb, ktb)) => {
				e == ke && ns == kns && db == kdb && *tb == ktb
			}
			(Self::TableById(e, ns, db, tb), Key::TableById(ke, kns, kdb, ktb))
			| (Self::Events(e, ns, db, tb), Key::Events(ke, kns, kdb, ktb))
			| (Self::Fields(e, ns, db, tb), Key::Fields(ke, kns, kdb, ktb))
			| (Self::Views(e, ns, db, tb), Key::Views(ke, kns, kdb, ktb)) => {
				e == ke && ns == kns && db == kdb && *tb == ktb
			}
			(Self::Tables(e, ns, db), Key::Tables(ke, kns, kdb))
			| (Self::CatalogState(e, ns, db), Key::CatalogState(ke, kns, kdb))
			| (Self::ReferenceTargets(e, ns, db), Key::ReferenceTargets(ke, kns, kdb)) => {
				e == ke && ns == kns && db == kdb
			}
			(
				Self::OnlineIndex(e, b, ns, db, tb, ix),
				Key::OnlineIndex(ke, kb, kns, kdb, ktb, kix),
			)
			| (
				Self::IndexShapes(e, b, ns, db, tb, ix),
				Key::IndexShapes(ke, kb, kns, kdb, ktb, kix),
			) => e == ke && b == kb && ns == kns && db == kdb && *tb == ktb && ix == kix,
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
			Lookup::Layouts(e, ns, db, tb) => Key::Layouts(e, ns, db, tb.clone()),
			Lookup::TableById(e, ns, db, tb) => Key::TableById(e, ns, db, tb.clone()),
			Lookup::Tables(e, ns, db) => Key::Tables(e, ns, db),
			Lookup::Events(e, ns, db, tb) => Key::Events(e, ns, db, tb.clone()),
			Lookup::Fields(e, ns, db, tb) => Key::Fields(e, ns, db, tb.clone()),
			Lookup::Views(e, ns, db, tb) => Key::Views(e, ns, db, tb.clone()),
			Lookup::OnlineIndex(e, b, ns, db, tb, ix) => {
				Key::OnlineIndex(e, b, ns, db, tb.clone(), ix)
			}
			Lookup::IndexShapes(e, b, ns, db, tb, ix) => {
				Key::IndexShapes(e, b, ns, db, tb.clone(), ix)
			}
			Lookup::CatalogState(e, ns, db) => Key::CatalogState(e, ns, db),
			Lookup::ReferenceTargets(e, ns, db) => Key::ReferenceTargets(e, ns, db),
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
