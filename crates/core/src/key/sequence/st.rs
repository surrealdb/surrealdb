//! Stores sequence states
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct St<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
	_f: u8,
	_g: u8,
	_h: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
}
impl_key!(St<'a>);

impl Categorise for St<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceState
	}
}

impl<'a> St<'a> {
	fn new(ns: &'a str, db: &'a str, sq: &'a str, nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b's',
			_e: b'q',
			sq,
			_f: b'!',
			_g: b's',
			_h: b't',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		let val = St::new(
			"testns",
			"testdb",
			"testsq",
			Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		);
		let enc = St::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!sqtestsq\0!st\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");

		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
