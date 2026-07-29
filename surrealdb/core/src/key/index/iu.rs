//! Index Count State Key Structure
//!
//! This module defines the `IndexCountKey` key used to track incremental
//! changes to the total number of indexed records for a given secondary
//! index. Instead of updating a single counter in place (which would cause
//! contention), SurrealDB appends small delta entries that are periodically
//! compacted into a single aggregate entry.
//!
//! Purpose
//! - Record point-in-time deltas (+N / -N) to the count of items referenced by an index on a
//!   specific table.
//! - Allow fast COUNT operations by summing deltas, and enable background compaction to collapse
//!   many deltas into one.
//!
//! Key pattern
//! - Prefix: `/*{ns}*{db}*{tb}+{ix}!iu`
//! - Suffix: `[{uid}] {pos} {count}`
//!   - `uid`: Optional pair of UUIDs uniquely identifying the origin of the delta. For normal
//!     update operations the pair is `(actor_id, event_id)`; for compacted keys it is `None`.
//!   - `pos`: Whether the delta is positive (`true`) or negative (`false`).
//!   - `count`: Magnitude of the delta (unsigned 64-bit integer).
//!
//! When compacted, snapshot-seen per-event delta entries are removed and a
//! single entry with `uid = None` is written carrying the net count with the
//! appropriate `pos` value.

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	/// A key representing a delta applied to the total item count of an index.
	///
	/// Fields
	/// - `ns`, `db`, `tb`, `ix`: Identify the namespace, database, table and index this count entry
	///   belongs to.
	/// - `uid`: Optional pair `(actor_id, event_id)` used to uniquely identify a delta written during a
	///   specific operation. `None` is reserved for compacted/aggregated entries.
	/// - `pos`: Direction of the delta: `true` for a positive increment, `false` for a decrement.
	/// - `count`: Magnitude of the delta.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct IndexCountKey<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'u',
		pub uid: Option<(Uuid, Uuid)>,
		pub pos: bool,
		pub count: u64,
	}
}

impl_kv_key_storekey!(IndexCountKey<'a> => ());

impl Categorise for IndexCountKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCountState
	}
}

key! {
	#[derive(Clone, Debug, PartialEq, PartialOrd)]
	pub struct IndexPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'u',
	}
}
impl_kv_range_storekey!(IndexPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = IndexCountKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			uid: Some((
				Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
				Uuid::from_bytes([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
			)),
			pos: true,
			count: 65535,
		};
		let enc = IndexCountKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\x03\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x0f\x0e\x0d\x0c\x0b\x0a\x09\x08\x07\x06\x05\x04\x03\x02\x01\0\x03\0\0\0\0\0\0\xff\xff", "key");
	}

	#[test]
	fn compacted_key() {
		let tb = TableName::from("testtb");
		let val = IndexCountKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			uid: None,
			pos: true,
			count: 65535,
		};
		let enc = IndexCountKey::encode_key(&val).unwrap();
		assert_eq!(
			&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\x02\x03\0\0\0\0\0\0\xff\xff",
			"compacted key"
		);
	}
}
