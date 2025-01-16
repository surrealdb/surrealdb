//! Stores a LIVE SELECT query definition on the table
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Vl is used to track the versions of the live queries.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Vl<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	#[serde(with = "uuid::serde::compact")]
	pub v: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, v: Uuid) -> Vl<'a> {
	Vl::new(ns, db, tb, v)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!vl\x00");
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!vl\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00");
	k
}

impl Categorise for Vl<'_> {
	fn categorise(&self) -> Category {
		Category::TableVersionsLiveQueries
	}
}

impl<'a> Vl<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, v: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'v',
			_f: b'l',
			v,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::Key;
	use uuid::Timestamp;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let version = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = Vl::new("testns", "testdb", "testtb", version);
		let enc = Vl::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\x00*testdb\x00*testtb\x00!vl\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);

		let dec = Vl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!vl\x00")
	}

	#[test]
	fn suffix() {
		let val = super::suffix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!vl\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00")
	}

	#[test]
	fn ascendant_uuid() {
		// The more recent key should be first
		let uuid_new = uuid::Uuid::new_v7(Timestamp::from_gregorian(1000001, 0));
		// The older key should be last
		let uuid_old = uuid::Uuid::new_v7(Timestamp::from_gregorian(1000000, 0));
		let key_new: Key = super::new("testns", "testdb", "testtb", uuid_new).into();
		let key_old: Key = super::new("testns", "testdb", "testtb", uuid_old).into();
		// Check that the most recent key comes first
		assert!(key_new < key_old);
	}
}
