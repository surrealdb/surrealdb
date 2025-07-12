//! Stores index sequence states for FullTextSearch/DocIDS
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Is<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
}
impl_key!(Is<'a>);

impl Categorise for Is<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocIdsSequenceState
	}
}

impl<'a> Is<'a> {
	pub(crate) fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'b',
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
		let val = Is::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		);
		let enc = Is::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!st\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");

		let dec = Is::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
