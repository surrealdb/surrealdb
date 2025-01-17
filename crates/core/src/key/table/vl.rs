//! Stores LIVE QUERIES cache versions

use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::ops::Range;
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
	/// The UUID must be a UUIDv7 so the serialized version preserved the order
	/// This specialized serializer ensure that the natural order is descendant.
	/// That way, the iterator will return the more recent UUID first.
	#[serde(with = "crate::sql::uuid::reverse")]
	pub v: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, v: Uuid) -> Vl<'a> {
	Vl::new(ns, db, tb, v)
}

fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!vl\x00");
	k
}

fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!vl\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00");
	k
}

pub(crate) fn full_range(ns: &str, db: &str, tb: &str) -> Range<Vec<u8>> {
	prefix(ns, db, tb)..suffix(ns, db, tb)
}

pub(crate) fn range_below(ns: &str, db: &str, tb: &str, uuid: Uuid) -> Range<Vec<u8>> {
	let mut k = Vl::new(ns, db, tb, uuid).encode().unwrap();
	k.extend_from_slice(b"\x00");
	k..suffix(ns, db, tb)
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
	use crate::key::table::vl::range_below;
	use crate::kvs::Key;
	use uuid::Uuid;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let version = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = Vl::new("testns", "testdb", "testtb", version);
		let enc = Vl::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\x00*testdb\x00*testtb\x00!vl\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6\xf5\xf4\xf3\xf2\xf1\xf0\xef"
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
		// Create UUIDs
		let (u1, u2, u3) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());

		// Convert to keys
		let k1: Key = super::new("testns", "testdb", "testtb", u1).into();
		let k2: Key = super::new("testns", "testdb", "testtb", u2).into();
		let k3: Key = super::new("testns", "testdb", "testtb", u3).into();

		// Check that the most recent key comes first
		assert!(k1 > k2, "{k1:?}\n{k2:?}");
		assert!(k2 > k3, "{k2:?}\n{k3:?}");
		assert!(k1 > k3, "{k1:?}\n{k3:?}");
	}

	#[test]
	fn test_range_bellow() {
		let u = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let r = range_below("testns", "testdb", "testtb", u);
		assert_eq!(r.start, b"/*testns\x00*testdb\x00*testtb\x00!vl\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6\xf5\xf4\xf3\xf2\xf1\xf0\xef\x00", "{:x?}", r.start);
	}
}
