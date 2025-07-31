//! Stores the key prefix for all keys under an index
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AllIndexRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
}

impl KVKey for AllIndexRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> AllIndexRoot<'a> {
	AllIndexRoot::new(ns, db, tb, ix)
}

impl Categorise for AllIndexRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexRoot
	}
}

impl<'a> AllIndexRoot<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
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
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn root() {
		let val = AllIndexRoot::new("testns", "testdb", "testtb", "testix");
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AllIndexRoot::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
		);
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0");
	}
}
