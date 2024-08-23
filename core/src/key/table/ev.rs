//! Stores a DEFINE EVENT config definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ev<'a> {
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
	pub ev: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, ev: &'a str) -> Ev<'a> {
	Ev::new(ns, db, tb, ev)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!ev");
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(b"!ev\xff");
	k
}

impl Categorise for Ev<'_> {
	fn categorise(&self) -> Category {
		Category::TableEvent
	}
}

impl<'a> Ev<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ev: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'e',
			_f: b'v',
			ev,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ev::new(
			"testns",
			"testdb",
			"testtb",
			"testev",
		);
		let enc = Ev::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00*testtb\x00!evtestev\x00");

		let dec = Ev::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!ev\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!ev\xff");
	}
}
