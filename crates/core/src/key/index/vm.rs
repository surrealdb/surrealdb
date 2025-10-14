//! Stores MTree state and nodes
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::mtree::MState;
use crate::idx::trees::store::NodeId;
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode, Hash)]
pub(crate) struct MtreeStateKey<'a> {
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

impl_kv_key_storekey!(MtreeStateKey<'_> => MState);

impl<'a> MtreeStateKey<'a> {
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
			_f: b'v',
			_g: b'm',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct MtreeNodeStateKey<'a> {
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

impl_kv_key_storekey!(MtreeNodeStateKey<'_> => Vec<u8>);

impl<'a> MtreeNodeStateKey<'a> {
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
			_f: b'v',
			_g: b'm',
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
		let val = MtreeStateKey::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3));
		let enc = MtreeStateKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!vm");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = MtreeNodeStateKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			8
		);
		let enc = MtreeNodeStateKey::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!vm\0\0\0\0\0\0\0\x08"
		);
	}
}
