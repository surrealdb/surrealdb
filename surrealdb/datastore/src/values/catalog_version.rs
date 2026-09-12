//! The per-table catalog version stored under `!cv{tb}`.
//!
//! One value per table, rewritten by every catalog write that changes that
//! table's definition, its fields or its indexes. Readers use it as the
//! identity of the table's catalog entry: two transactions that read the same
//! value see the same definition, fields and indexes, so a snapshot built by
//! one can be served to the other. Writes to different tables touch different
//! keys, so they never conflict with each other over the version. What it is
//! *not* is a counter — see [`CatalogVersion::fresh`].

use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use uuid::Uuid;

/// The identity of one table's catalog entry at one point in its history.
///
/// A fresh random identifier rather than an incremented count, because a
/// rolled-back transaction must not be able to leave behind a version a later
/// commit reuses: with a counter, an aborted `DEFINE` that bumped `n` to `n+1`
/// and a committed one that also bumped `n` to `n+1` would be told apart by
/// nothing, and a snapshot built under the aborted transaction's view would be
/// served for the committed catalog. Two writers can never mint the same uuid,
/// so a version identifies exactly the catalog state the transaction that wrote
/// it could see.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CatalogVersion(pub Uuid);

impl_kv_value_revisioned!(CatalogVersion);

impl CatalogVersion {
	/// Mints the version a catalog write stamps on its database.
	pub fn fresh() -> Self {
		Self(Uuid::now_v7())
	}
}
