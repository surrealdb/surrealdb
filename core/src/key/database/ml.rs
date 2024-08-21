//! Stores a DEFINE MODEL config definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ml<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ml: &'a str,
	pub vn: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, ml: &'a str, vn: &'a str) -> Ml<'a> {
	Ml::new(ns, db, ml, vn)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'm', b'l', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'm', b'l', 0xff]);
	k
}

impl Categorise for Ml<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseModel
	}
}

impl<'a> Ml<'a> {
	pub fn new(ns: &'a str, db: &'a str, ml: &'a str, vn: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'm',
			_e: b'l',
			ml,
			vn,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ml::new(
			"testns",
			"testdb",
			"testml",
			"1.0.0",
		);
		let enc = Ml::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!mltestml\x001.0.0\x00");
		let dec = Ml::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!ml\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!ml\xff");
	}
}
