//! Stores BTree nodes for doc lengths
use crate::idx::trees::btree::BState;
use crate::idx::trees::store::NodeId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct BlRoot<'a> {
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
}

impl KVKey for BlRoot<'_> {
	type ValueType = BState;
}

impl Categorise for BlRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodeDocLengths
	}
}

impl<'a> BlRoot<'a> {
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
			_e: b'!',
			_f: b'b',
			_g: b'l',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bl<'a> {
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
	pub node_id: NodeId,
}

impl KVKey for Bl<'_> {
	type ValueType = BState;
}

impl Categorise for Bl<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodeDocLengths
	}
}

impl<'a> Bl<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, node_id: NodeId) -> Self {
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
			_f: b'b',
			_g: b'l',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		let val = BlRoot::new("testns", "testdb", "testtb", "testix");
		let enc = BlRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bl");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bl::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bl::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bl\0\0\0\0\0\0\0\x07");
	}
}
