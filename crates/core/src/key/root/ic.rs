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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::err::Error;
use crate::key::category::{Categorise, Category};
use crate::kvs::{impl_key, KeyDecode};

/// Represents an entry in the index compaction queue
///
/// When an index (particularly a full-text index) needs compaction, an `Ic` key
/// is created and stored in the database. The index compaction thread
/// periodically scans for these keys and processes the corresponding indexes.
///
/// Compaction helps optimize index performance by consolidating changes and
/// removing unnecessary data.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct IndexCompactionKey<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: &'a str,
	pub db: &'a str,
	pub tb: &'a str,
	pub ix: &'a str,
	pub nid: Uuid,
	pub uid: Uuid,
}

impl_key!(IndexCompactionKey<'a>);

impl Categorise for IndexCompactionKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCompaction
	}
}

impl<'a> IndexCompactionKey<'a> {
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
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
		IndexCompactionKey::new(self.ns, self.db, self.tb, self.ix, self.nid, self.uid)
	}

	pub(crate) fn index_matches(&self, other: &IndexCompactionKey<'_>) -> bool {
		self.ns == other.ns && self.db == other.db && self.tb == other.tb && self.ix == other.ix
	}

	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec())
	}

	pub fn decode_key(k: &[u8]) -> Result<IndexCompactionKey<'_>, Error> {
		Ok(IndexCompactionKey::decode(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::root::ic::IndexCompactionKey;
	use crate::kvs::KeyEncode;

	#[test]
	fn range() {
		assert_eq!(IndexCompactionKey::range(), (b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec()));
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexCompactionKey::new("testns", "testdb", "testtb", "testix", Uuid::from_u128(1), Uuid::from_u128(2));
		let enc = IndexCompactionKey::encode(&val).unwrap();
		assert_eq!(enc, b"/!ic\x00\x00\x00\x01\x00\x00\x00\x02testtb\0\0\0\0\x03\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}
}
