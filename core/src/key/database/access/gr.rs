//! Stores a grant associated with an access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub ac: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub gr: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, db, ac, gr)
}

pub fn prefix(ns: &str, db: &str, ac: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, ac).encode().unwrap();
	k.extend_from_slice(&[b'!', b'g', b'r', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, ac: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, ac).encode().unwrap();
	k.extend_from_slice(&[b'!', b'g', b'r', 0xff]);
	k
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ns: &'a str, db: &'a str, ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'&',
			ac,
			_d: b'!',
			_e: b'g',
			_f: b'r',
			gr,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Gr::new(
			"testns",
			"testdb",
			"testac",
			"testgr",
		);
		let enc = Gr::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0!grtestgr\0");

		let dec = Gr::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb", "testac");
		assert_eq!(val, b"/*testns\0*testdb\0&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb", "testac");
		assert_eq!(val, b"/*testns\0*testdb\0&testac\0!gr\xff");
	}
}
