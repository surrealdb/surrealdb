//! Stores BTree nodes for doc ids
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::docids::btdocids::BTreeDocIdsState;
use crate::idx::trees::store::NodeId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct BdRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl_kv_key_storekey!(BdRoot<'_> => BTreeDocIdsState);

impl Categorise for BdRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNode
	}
}

impl<'a> BdRoot<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'b',
			_g: b'd',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Bd<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub node_id: NodeId,
}

impl_kv_key_storekey!(Bd<'_> => BTreeDocIdsState);

impl Categorise for Bd<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNode
	}
}

impl<'a> Bd<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, node_id: NodeId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'b',
			_g: b'd',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn root() {
		let val = BdRoot::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3));
		let enc = BdRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!bd");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bd::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			7
		);
		let enc = Bd::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!bd\0\0\0\0\0\0\0\x07"
		);
	}
}
