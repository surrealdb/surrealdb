/// Stores a DEFINE ACCESS ON DATABASE config definition
use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ac<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ac: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str) -> Ac<'a> {
	Ac::new(ns, db, ac)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'c', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'c', 0xff]);
	k
}

impl KeyRequirements for Ac<'_> {
	fn key_category(&self) -> Category {
		Category::DatabaseAccess
	}
}

impl<'a> Ac<'a> {
	pub fn new(ns: &'a str, db: &'a str, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'a',
			_e: b'c',
			ac,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ac::new(
			"testns",
			"testdb",
			"testac",
		);
		let enc = Ac::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!actestac\x00");

		let dec = Ac::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!ac\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!ac\xff");
	}
}
