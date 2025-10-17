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
use std::borrow::Cow;
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
	pub ns: Cow<'a, str>,
	pub db: Cow<'a, str>,
	pub tb: Cow<'a, str>,
	pub ix: Cow<'a, str>,
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
		ns: Cow<'a, str>,
		db: Cow<'a, str>,
		tb: Cow<'a, str>,
		ix: Cow<'a, str>,
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
			Cow::Owned(self.ns.into_owned()),
			Cow::Owned(self.db.into_owned()),
			Cow::Owned(self.tb.into_owned()),
			Cow::Owned(self.ix.into_owned()),
			self.nid,
			self.uid,
		)
	}

	pub(crate) fn index_matches(&self, other: &IndexCompactionKey<'_>) -> bool {
		self.ns == other.ns && self.db == other.db && self.tb == other.tb && self.ix == other.ix
	}

	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!ic\0".to_vec(), b"/!ic\0xff".to_vec())
	}

	pub fn decode_key(k: &[u8]) -> Result<IndexCompactionKey<'_>, Error> {
		IndexCompactionKey::decode(k)
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
		let val = IndexCompactionKey::new(Cow::Borrowed("testns"), Cow::Borrowed("testdb"), Cow::Borrowed("testtb"), Cow::Borrowed("testix"), Uuid::from_u128(1), Uuid::from_u128(2));
		let enc = IndexCompactionKey::encode(&val).unwrap();
		assert_eq!(enc, b"/!ictestns\0testdb\0testtb\0testix\0\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}
}
