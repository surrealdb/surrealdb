//! Index Compaction Queue
//!
//! This module defines the key structure used for the index compaction queue.
//! The index compaction system periodically processes indexes that need
//! optimization, particularly full-text indexes that accumulate changes over
//! time.
//!
//! The `Ic` struct represents an entry in the compaction queue, identifying an
//! index that needs to be compacted. The compaction thread processes these
//! entries at regular intervals defined by the `index_compaction_interval`
//! configuration option.
use std::borrow::Cow;

use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
/// Represents an entry in the index compaction queue
///
/// When an index (particularly a full-text index) needs compaction, an `Ic` key
/// is created and stored in the database. The index compaction thread
/// periodically scans for these keys and processes the corresponding indexes.
///
/// Compaction helps optimize index performance by consolidating changes and
/// removing unnecessary data.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct IndexCompactionKey<'key> {
		b'/',
		b'!',
		b'i',
		b'c',
		pub ns: NamespaceId,
		pub db: DatabaseId,
		pub tb: Cow<'key, TableName>,
		pub ix: IndexId,
		pub nid: Uuid,
		pub uid: Uuid,
	}
}

impl_kv_key_storekey!(IndexCompactionKey<'a> => ());

impl Categorise for IndexCompactionKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCompaction
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct IndexCompactionPrefix {
		b'/',
		b'!',
		b'i',
		b'c',
	}
}
impl_kv_range_storekey!(IndexCompactionPrefix);

key! {
	/// Prefix of every compaction-queue entry belonging to one index.
	///
	/// Its encoded range spans exactly the index's queue entries, so the
	/// range end is the smallest key past them — the batched queue drain
	/// seeks there to continue with the next index's entries.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct IndexCompactionIndexPrefix<'key> {
		b'/',
		b'!',
		b'i',
		b'c',
		pub ns: NamespaceId,
		pub db: DatabaseId,
		pub tb: Cow<'key, TableName>,
		pub ix: IndexId,
	}
}
impl_kv_range_storekey!(IndexCompactionIndexPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::root::ic::IndexCompactionKey;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn range() {
		let range = IndexCompactionPrefix {}.encode_range().unwrap();
		assert_eq!(range.start.as_slice(), b"/!ic\0".to_vec());
		assert_eq!(range.end.as_slice(), b"/!id".to_vec());
	}

	#[test]
	fn key() {
		let val = IndexCompactionKey {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned(TableName::from("testtb")),
			ix: IndexId(3),
			nid: Uuid::from_u128(1),
			uid: Uuid::from_u128(2),
		};
		let enc = IndexCompactionKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!ic\x00\x00\x00\x01\x00\x00\x00\x02testtb\0\0\0\0\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}

	#[test]
	fn index_prefix_range_brackets_exactly_one_index() {
		let entry = |tb: &str, ix: u32| {
			IndexCompactionKey::encode_key(&IndexCompactionKey {
				ns: NamespaceId(1),
				db: DatabaseId(2),
				tb: Cow::Owned(TableName::from(tb)),
				ix: IndexId(ix),
				nid: Uuid::from_u128(1),
				uid: Uuid::from_u128(2),
			})
			.unwrap()
		};
		let range = IndexCompactionIndexPrefix {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned(TableName::from("testtb")),
			ix: IndexId(3),
		}
		.encode_range()
		.unwrap();
		// Entries of this index fall inside the range…
		assert!(range.start.as_slice() <= &*entry("testtb", 3));
		assert!(&*entry("testtb", 3) < range.end.as_slice());
		// …while the next index and the next table start at or past its end.
		assert!(&*entry("testtb", 4) >= range.end.as_slice());
		assert!(&*entry("testtc", 0) >= range.end.as_slice());
	}
}
