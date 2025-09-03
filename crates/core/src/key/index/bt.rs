//! Stores BTree nodes for terms
use std::borrow::Cow;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::ft::search::terms::SearchTermsState;
use crate::idx::trees::store::NodeId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct BtRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: Cow<'a, str>,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl KVKey for BtRoot<'_> {
	type ValueType = SearchTermsState;
}

impl Categorise for BtRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodeTerms
	}
}

impl<'a> BtRoot<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix: Cow::Borrowed(ix),
			_e: b'!',
			_f: b'b',
			_g: b't',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Bt<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: Cow<'a, str>,
	_e: u8,
	_f: u8,
	_g: u8,
	pub node_id: NodeId,
}

impl KVKey for Bt<'_> {
	type ValueType = Vec<u8>;
}

impl Categorise for Bt<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodeTerms
	}
}

impl<'a> Bt<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, node_id: NodeId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix: Cow::Borrowed(ix),
			_e: b'!',
			_f: b'b',
			_g: b't',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		#[rustfmt::skip]
		let val = BtRoot::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
		);
		let enc = BtRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bt");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bt::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			7
		);
		let enc = Bt::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bt\0\0\0\0\0\0\0\x07"
		);
	}
}
