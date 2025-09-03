//! Stores change feeds
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::TableMutations;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::vs::VersionStamp;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Cf<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_d: u8,
	// vs is the versionstamp of the change feed entry that is encoded in big-endian.
	pub vs: VersionStamp,
	_c: u8,
	pub tb: &'a str,
}

impl KVKey for Cf<'_> {
	type ValueType = TableMutations;
}

impl Categorise for Cf<'_> {
	fn categorise(&self) -> Category {
		Category::ChangeFeed
	}
}

impl<'a> Cf<'a> {
	#[cfg(test)]
	pub fn new(ns: NamespaceId, db: DatabaseId, vs: VersionStamp, tb: &'a str) -> Self {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'#',
			vs,
			_c: b'*',
			tb,
		}
	}

	pub fn decode_key(k: &[u8]) -> Result<Cf<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

pub fn versionstamped_key_prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = crate::key::database::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"#");
	Ok(k)
}

pub fn versionstamped_key_suffix(tb: &str) -> Vec<u8> {
	let mut k: Vec<u8> = vec![];
	k.extend_from_slice(b"*");
	k.extend_from_slice(tb.as_bytes());
	// Without this, decoding fails with UnexpectedEOF errors
	k.extend_from_slice(&[0x00]);
	k
}

/// A prefix or suffix for a database change feed
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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

impl KVKey for DatabaseChangeFeedRange {
	type ValueType = Vec<u8>;
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DatabaseChangeFeedTsRange {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ts: VersionStamp,
}

impl DatabaseChangeFeedTsRange {
	pub fn new(ns: NamespaceId, db: DatabaseId, vs: VersionStamp) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'#',
			ts: vs,
		}
	}
}

impl KVKey for DatabaseChangeFeedTsRange {
	type ValueType = TableMutations;
}

/// Returns the prefix for the whole database change feeds since the
/// specified versionstamp.
pub fn prefix_ts(ns: NamespaceId, db: DatabaseId, vs: VersionStamp) -> DatabaseChangeFeedTsRange {
	DatabaseChangeFeedTsRange::new(ns, db, vs)
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
	use crate::vs::*;

	#[test]
	fn cf_key() {
		let val = Cf::new(
			NamespaceId(1),
			DatabaseId(2),
			VersionStamp::try_from_u128(12345).unwrap(),
			"test",
		);
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x00\x00\x30\x39*test\x00");

		let val = Cf::new(
			NamespaceId(1),
			DatabaseId(2),
			VersionStamp::try_from_u128(12346).unwrap(),
			"test",
		);
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x00\x00\x30\x3a*test\x00");
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
		let val = DatabaseChangeFeedTsRange::new(
			NamespaceId(1),
			DatabaseId(2),
			VersionStamp::try_from_u128(12345).unwrap(),
		);
		let enc = DatabaseChangeFeedTsRange::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x00\x00\x30\x39"
		);
	}

	#[test]
	fn versionstamp_conversions() {
		let a = VersionStamp::from_u64(12345);
		let b = VersionStamp::try_into_u64(a).unwrap();
		assert_eq!(12345, b);

		let a = VersionStamp::try_from_u128(12345).unwrap();
		let b = a.into_u128();
		assert_eq!(12345, b);
	}
}
