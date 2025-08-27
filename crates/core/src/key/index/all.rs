//! Stores the key prefix for all keys under an index
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AllIndexRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
}

impl KVKey for AllIndexRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str) -> AllIndexRoot<'a> {
	AllIndexRoot::new(ns, db, tb, ix)
}

impl Categorise for AllIndexRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexRoot
	}
}

impl<'a> AllIndexRoot<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str) -> Self {
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
		let val = AllIndexRoot::new(NamespaceId(1), DatabaseId(2), "testtb", "testix");
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AllIndexRoot::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
		);
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0");
	}
}
