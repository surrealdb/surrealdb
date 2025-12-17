//! Stores change feeds
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::TableMutations;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Cf<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_d: u8,
	// ts is the timestamp of the change feed entry that is encoded in big-endian.
	pub ts: Cow<'a, [u8]>,
	_c: u8,
	pub tb: Cow<'a, TableName>,
}
impl_kv_key_storekey!(Cf<'_> => TableMutations);

impl Categorise for Cf<'_> {
	fn categorise(&self) -> Category {
		Category::ChangeFeed
	}
}

impl<'a> Cf<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: &'a [u8], tb: &'a TableName) -> Self {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'#',
			ts: Cow::Borrowed(ts),
			_c: b'*',
			tb: Cow::Borrowed(tb),
		}
	}

	pub fn decode_key(k: &[u8]) -> Result<Cf<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

/// Create a complete changefeed key with timestamp
pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ts: &'a [u8], tb: &'a TableName) -> Cf<'a> {
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
pub struct DatabaseChangeFeedTsRange<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ts: Cow<'a, [u8]>,
}

impl<'a> DatabaseChangeFeedTsRange<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: &'a [u8]) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'#',
			ts: Cow::Borrowed(ts),
		}
	}
}

impl_kv_key_storekey!(DatabaseChangeFeedTsRange<'_> => TableMutations);

/// Returns the prefix for the whole database change feeds since the
/// specified timestamp.
pub fn prefix_ts(ns: NamespaceId, db: DatabaseId, ts: &[u8]) -> DatabaseChangeFeedTsRange<'_> {
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
	use crate::kvs::{KVKey, Timestamp};

	#[test]
	fn cf_key() {
		let ts1 = 12345u64.to_ts_bytes();
		let tb = TableName::from("test");
		let val = Cf::new(NamespaceId(1), DatabaseId(2), &ts1, &tb);
		let enc = Cf::encode_key(&val).unwrap();
		// Verify the encoded key - note that Cow<[u8]> is encoded with length prefix
		assert_eq!(
			enc,
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 57,
				0, 42, 116, 101, 115, 116, 0
			]
		);

		let ts2 = 12346u64.to_ts_bytes();
		let val = Cf::new(NamespaceId(1), DatabaseId(2), &ts2, &tb);
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 58,
				0, 42, 116, 101, 115, 116, 0
			]
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
		let ts = 12345u64.to_ts_bytes();
		let val = DatabaseChangeFeedTsRange::new(NamespaceId(1), DatabaseId(2), &ts);
		let enc = DatabaseChangeFeedTsRange::encode_key(&val).unwrap();
		// Verify the encoded key - note that Cow<[u8]> is encoded with length prefix
		assert_eq!(
			enc,
			&[
				47, 42, 0, 0, 0, 1, 42, 0, 0, 0, 2, 35, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 48, 57,
				0
			]
		);
	}
}
