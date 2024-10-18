//! Stores a DEFINE CONFIG definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Cg<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ty: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, ty: &'a str) -> Cg<'a> {
	Cg::new(ns, db, ty)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'g', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'g', 0xff]);
	k
}

impl Categorise for Cg<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseConfig
	}
}

impl<'a> Cg<'a> {
	pub fn new(ns: &'a str, db: &'a str, ty: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'c',
			_e: b'g',
			ty,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Cg::new(
			"testns",
			"testdb",
			"testty",
		);
		let enc = Cg::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!cgtestty\x00");
		let dec = Cg::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!cg\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!cg\xff");
	}
}
