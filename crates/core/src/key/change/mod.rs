//! Stores change feeds
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::TableMutations;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Cf<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_d: u8,
	// ts is the timestamp of the change feed entry that is encoded in big-endian.
	pub ts: u64,
	_c: u8,
	pub tb: Cow<'a, str>,
}
impl_kv_key_storekey!(Cf<'_> => TableMutations);

impl Categorise for Cf<'_> {
	fn categorise(&self) -> Category {
		Category::ChangeFeed
	}
}

impl<'a> Cf<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: u64, tb: &'a str) -> Self {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'#',
			ts,
			_c: b'*',
			tb: Cow::Borrowed(tb),
		}
	}

	pub fn decode_key(k: &[u8]) -> Result<Cf<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

/// Create a complete changefeed key with timestamp
pub fn new(ns: NamespaceId, db: DatabaseId, ts: u64, tb: &str) -> Cf<'_> {
	Cf::new(ns, db, ts, tb)
}

/// A prefix or suffix for a database change feed
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub struct DatabaseChangeFeedRange {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_xx: u8,
}

impl DatabaseChangeFeedRange {
	pub fn new_prefix(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'#',
			_xx: 0x00,
		}
	}

	pub fn new_suffix(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'#',
			_xx: 0xff,
		}
	}
}

impl_kv_key_storekey!(DatabaseChangeFeedRange => Vec<u8>);

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub struct DatabaseChangeFeedTsRange {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ts: u64,
}

impl DatabaseChangeFeedTsRange {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: u64) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'#',
			ts,
		}
	}
}

impl_kv_key_storekey!(DatabaseChangeFeedTsRange => TableMutations);

/// Returns the prefix for the whole database change feeds since the
/// specified timestamp.
pub fn prefix_ts(ns: NamespaceId, db: DatabaseId, ts: u64) -> DatabaseChangeFeedTsRange {
	DatabaseChangeFeedTsRange::new(ns, db, ts)
}

/// Returns the prefix for the whole database change feeds
#[expect(unused)]
pub fn prefix(ns: NamespaceId, db: DatabaseId) -> DatabaseChangeFeedRange {
	DatabaseChangeFeedRange::new_prefix(ns, db)
}

/// Returns the suffix for the whole database change feeds
pub fn suffix(ns: NamespaceId, db: DatabaseId) -> DatabaseChangeFeedRange {
	DatabaseChangeFeedRange::new_suffix(ns, db)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn cf_key() {
		let val = Cf::new(NamespaceId(1), DatabaseId(2), 12345, "test");
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x30\x39*test\x00"
		);

		let val = Cf::new(NamespaceId(1), DatabaseId(2), 12346, "test");
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x30\x3a*test\x00"
		);
	}

	#[test]
	fn range_key() {
		let val = DatabaseChangeFeedRange::new_prefix(NamespaceId(1), DatabaseId(2));
		let enc = DatabaseChangeFeedRange::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00");

		let val = DatabaseChangeFeedRange::new_suffix(NamespaceId(1), DatabaseId(2));
		let enc = DatabaseChangeFeedRange::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\xff");
	}

	#[test]
	fn ts_prefix_key() {
		let val = DatabaseChangeFeedTsRange::new(NamespaceId(1), DatabaseId(2), 12345);
		let enc = DatabaseChangeFeedTsRange::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x30\x39");
	}
}
