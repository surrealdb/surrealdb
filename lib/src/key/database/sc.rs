//! Stores a DEFINE SCOPE config definition
use crate::key::error::KeyError;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Sc<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sc: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, sc: &'a str) -> Sc<'a> {
	Sc::new(ns, db, sc)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b's', b'c', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b's', b'c', 0xff]);
	k
}

impl KeyRequirements for Sc<'_> {
	fn key_category() -> KeyError {
		KeyError::DatabaseScope
	}
}

impl<'a> Sc<'a> {
	pub fn new(ns: &'a str, db: &'a str, sc: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b's',
			_e: b'c',
			sc,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Sc::new(
			"testns",
			"testdb",
			"testsc",
		);
		let enc = Sc::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!sctestsc\0");

		let dec = Sc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
