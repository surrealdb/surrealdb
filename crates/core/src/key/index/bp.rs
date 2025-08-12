//! Stores BTree nodes for postings
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::trees::btree::BState;
use crate::idx::trees::store::NodeId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct BpRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl Categorise for BpRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodePostings
	}
}

impl KVKey for BpRoot<'_> {
	type ValueType = BState;
}

impl<'a> BpRoot<'a> {
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
			_e: b'!',
			_f: b'b',
			_g: b'p',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bp<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub node_id: NodeId,
}

impl Categorise for Bp<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodePostings
	}
}

impl KVKey for Bp<'_> {
	type ValueType = BState;
}

impl<'a> Bp<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, node_id: NodeId) -> Self {
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
			_g: b'p',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		let val = BpRoot::new(NamespaceId(1), DatabaseId(2), "testtb", "testix");
		let enc = BpRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bp");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bp::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			7
		);
		let enc = Bp::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bp\0\0\0\0\0\0\0\x07"
		);
	}
}
