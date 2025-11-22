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
use std::ops::Range;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

/// Represents an entry in the index compaction queue
///
/// When an index (particularly a full-text index) needs compaction, an `Ic` key
/// is created and stored in the database. The index compaction thread
/// periodically scans for these keys and processes the corresponding indexes.
///
/// Compaction helps optimize index performance by consolidating changes and
/// removing unnecessary data.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct IndexCompactionKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: Cow<'key, str>,
	pub ix: IndexId,
	pub nid: Uuid,
	pub uid: Uuid,
}

impl_kv_key_storekey!(IndexCompactionKey<'_> => ());

impl Categorise for IndexCompactionKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCompaction
	}
}

impl<'key> IndexCompactionKey<'key> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'key, str>,
		ix: IndexId,
		nid: Uuid,
		uid: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'i',
			_c: b'c',
			ns,
			db,
			tb,
			ix,
			nid,
			uid,
		}
	}

	pub(crate) fn into_owned(self) -> IndexCompactionKey<'static> {
		IndexCompactionKey::new(
			self.ns,
			self.db,
			Cow::Owned(self.tb.into_owned()),
			self.ix,
			self.nid,
			self.uid,
		)
	}

	pub(crate) fn index_matches(&self, other: &IndexCompactionKey<'_>) -> bool {
		self.ns == other.ns && self.db == other.db && self.tb == other.tb && self.ix == other.ix
	}

	pub(crate) fn full_range() -> (Vec<u8>, Vec<u8>) {
		(b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec())
	}

	/// Returns a range that covers all compaction keys for a specific index
	pub(crate) fn range_for_index(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: IndexId,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let start = Self::new(
			ns,
			db,
			Cow::Owned(tb.to_string()),
			ix,
			Uuid::from_u128(0),
			Uuid::from_u128(0),
		);
		let end = Self::new(
			ns,
			db,
			Cow::Owned(tb.to_string()),
			ix,
			Uuid::from_u128(u128::MAX),
			Uuid::from_u128(u128::MAX),
		);
		let mut start_buf = Vec::new();
		let mut end_buf = Vec::new();
		storekey::encode(&mut start_buf, &start)?;
		storekey::encode(&mut end_buf, &end)?;
		Ok(start_buf..end_buf)
	}

	pub fn decode_key(k: &[u8]) -> anyhow::Result<IndexCompactionKey<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::root::ic::IndexCompactionKey;
	use crate::kvs::KVKey;

	#[test]
	fn range() {
		assert_eq!(IndexCompactionKey::full_range(), (b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec()));
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexCompactionKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed("testtb"), IndexId(3), Uuid::from_u128(1), Uuid::from_u128(2));
		let enc = IndexCompactionKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!ic\x00\x00\x00\x01\x00\x00\x00\x02testtb\0\0\0\0\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}

	#[test]
	fn range_for_index() {
		let range = IndexCompactionKey::range_for_index(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
		)
		.unwrap();

		// Verify the start key has minimum UUIDs (all zeros)
		let start_key = IndexCompactionKey::decode_key(&range.start).unwrap();
		assert_eq!(start_key.ns, NamespaceId(1));
		assert_eq!(start_key.db, DatabaseId(2));
		assert_eq!(start_key.tb, "testtb");
		assert_eq!(start_key.ix, IndexId(3));
		assert_eq!(start_key.nid, Uuid::from_u128(0));
		assert_eq!(start_key.uid, Uuid::from_u128(0));

		// Verify the end key has maximum UUIDs
		let end_key = IndexCompactionKey::decode_key(&range.end).unwrap();
		assert_eq!(end_key.ns, NamespaceId(1));
		assert_eq!(end_key.db, DatabaseId(2));
		assert_eq!(end_key.tb, "testtb");
		assert_eq!(end_key.ix, IndexId(3));
		assert_eq!(end_key.nid, Uuid::from_u128(u128::MAX));
		assert_eq!(end_key.uid, Uuid::from_u128(u128::MAX));

		// Verify that a key within the range is properly bounded
		let middle_key = IndexCompactionKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Borrowed("testtb"),
			IndexId(3),
			Uuid::from_u128(100),
			Uuid::from_u128(200),
		);
		let middle_enc = IndexCompactionKey::encode_key(&middle_key).unwrap();
		assert!(middle_enc >= range.start && middle_enc <= range.end);
	}
}
