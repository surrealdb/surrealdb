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

use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Represents an entry in the index compaction queue
///
/// When an index (particularly a full-text index) needs compaction, an `Ic` key
/// is created and stored in the database. The index compaction thread
/// periodically scans for these keys and processes the corresponding indexes.
///
/// Compaction helps optimize index performance by consolidating changes and
/// removing unnecessary data.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct IndexCompactionKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: Cow<'key, TableName>,
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
		tb: Cow<'key, TableName>,
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

	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec())
	}

	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<IndexCompactionKey<'_>> {
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
		assert_eq!(IndexCompactionKey::range(), (b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec()));
	}

	#[test]
	fn key() {
		let val = IndexCompactionKey::new(
			NamespaceId(1),
			DatabaseId(2),
			Cow::Owned(TableName::from("testtb")),
			IndexId(3),
			Uuid::from_u128(1),
			Uuid::from_u128(2),
		);
		let enc = IndexCompactionKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!ic\x00\x00\x00\x01\x00\x00\x00\x02testtb\0\0\0\0\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}
}
