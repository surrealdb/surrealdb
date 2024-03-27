use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
/// Stores a DEFINE TOKEN ON DATABASE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Tk<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tk: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tk: &'a str) -> Tk<'a> {
	Tk::new(ns, db, tk)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0xff]);
	k
}

impl KeyRequirements for Tk<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::DatabaseToken
	}
}

impl<'a> Tk<'a> {
	pub fn new(ns: &'a str, db: &'a str, tk: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'k',
			tk,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tk::new(
			"testns",
			"testdb",
			"testtk",
		);
		let enc = Tk::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!tktesttk\x00");

		let dec = Tk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!tk\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb");
		assert_eq!(val, b"/*testns\0*testdb\0!tk\xff");
	}
}
