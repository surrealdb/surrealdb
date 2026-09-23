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
use std::ops::Range;
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

	/// Range covering every queued entry, whatever object it belongs to.
	///
	/// The end bound is the next category prefix rather than a suffix byte
	/// appended to this one: `ns` follows `/!ic` directly, so any real entry
	/// sorts above `/!ic` plus any single byte that is not a valid leading
	/// identifier byte, and an upper bound built that way would exclude the
	/// whole queue.
	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!ic\0".to_vec(), b"/!id".to_vec())
	}

	pub fn decode_key(k: &[u8]) -> Result<IndexCompactionKey<'_>, Error> {
		IndexCompactionKey::decode(k)
	}

	/// Range covering every queued entry belonging to one namespace.
	///
	/// `ns` is the first variable field and is terminated by a `0x00` byte, so
	/// the entries for one namespace are a contiguous prefix. `0xff` is never a
	/// valid UTF-8 byte, so it is an exclusive upper bound for any identifier
	/// that could follow the prefix.
	pub(crate) fn namespace_range(ns: &str) -> Range<Vec<u8>> {
		let mut beg = b"/!ic".to_vec();
		beg.extend_from_slice(ns.as_bytes());
		beg.push(0x00);
		let mut end = beg.clone();
		end.push(0xff);
		beg..end
	}

	/// Range covering every queued entry belonging to one database.
	///
	/// `ns` and `db` are the two leading variable fields and each is terminated
	/// by a `0x00` byte, so the entries for one database are a contiguous
	/// prefix bounded the same way as [`Self::namespace_range`].
	pub(crate) fn database_range(ns: &str, db: &str) -> Range<Vec<u8>> {
		let mut beg = b"/!ic".to_vec();
		beg.extend_from_slice(ns.as_bytes());
		beg.push(0x00);
		beg.extend_from_slice(db.as_bytes());
		beg.push(0x00);
		let mut end = beg.clone();
		end.push(0xff);
		beg..end
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::root::ic::IndexCompactionKey;
	use crate::kvs::KeyEncode;

	#[test]
	fn range() {
		let (beg, end) = IndexCompactionKey::range();
		assert_eq!((beg.clone(), end.clone()), (b"/!ic\0".to_vec(), b"/!id".to_vec()));
		// A real entry has to fall inside the range the compaction scan uses
		#[rustfmt::skip]
		let key = IndexCompactionKey::new(Cow::Borrowed("testns"), Cow::Borrowed("testdb"), Cow::Borrowed("testtb"), Cow::Borrowed("testix"), Uuid::from_u128(1), Uuid::from_u128(2)).encode().unwrap();
		assert!(key >= beg, "an entry must sort at or above the start bound");
		assert!(key < end, "an entry must sort below the end bound");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexCompactionKey::new(Cow::Borrowed("testns"), Cow::Borrowed("testdb"), Cow::Borrowed("testtb"), Cow::Borrowed("testix"), Uuid::from_u128(1), Uuid::from_u128(2));
		let enc = IndexCompactionKey::encode(&val).unwrap();
		assert_eq!(enc, b"/!ictestns\0testdb\0testtb\0testix\0\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}

	fn entry(ns: &str, db: &str) -> Vec<u8> {
		#[rustfmt::skip]
		let val = IndexCompactionKey::new(Cow::Borrowed(ns), Cow::Borrowed(db), Cow::Borrowed("testtb"), Cow::Borrowed("testix"), Uuid::from_u128(1), Uuid::from_u128(2));
		IndexCompactionKey::encode(&val).unwrap()
	}

	#[test]
	fn namespace_range() {
		let rng = IndexCompactionKey::namespace_range("testns");
		assert_eq!(rng.start, b"/!ictestns\0".to_vec());
		assert_eq!(rng.end, b"/!ictestns\0\xff".to_vec());
		assert!(rng.contains(&entry("testns", "testdb")));
		assert!(rng.contains(&entry("testns", "otherdb")));
		// A namespace whose name merely shares a prefix is a different range
		assert!(!rng.contains(&entry("testns2", "testdb")));
		assert!(!rng.contains(&entry("otherns", "testdb")));
	}

	#[test]
	fn database_range() {
		let rng = IndexCompactionKey::database_range("testns", "testdb");
		assert_eq!(rng.start, b"/!ictestns\0testdb\0".to_vec());
		assert_eq!(rng.end, b"/!ictestns\0testdb\0\xff".to_vec());
		assert!(rng.contains(&entry("testns", "testdb")));
		// A sibling database in the same namespace is a different range
		assert!(!rng.contains(&entry("testns", "testdb2")));
		assert!(!rng.contains(&entry("testns", "otherdb")));
		assert!(!rng.contains(&entry("otherns", "testdb")));
	}
}
