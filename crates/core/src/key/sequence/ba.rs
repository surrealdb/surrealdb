//! Stores sequence batches
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_key;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ba<'a> {
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
	pub start: i64,
}
impl_key!(Ba<'a>);

impl Categorise for Ba<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

impl<'a> Ba<'a> {
	pub(crate) fn new(ns: &'a str, db: &'a str, sq: &'a str, start: i64) -> Self {
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
			_g: b'b',
			_h: b'a',
			start,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		let val = Ba::new("testns", "testdb", "testsq", 100);
		let enc = Ba::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!sqtestsq\0!ba\x80\0\0\0\0\0\0\x64");

		let dec = Ba::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
