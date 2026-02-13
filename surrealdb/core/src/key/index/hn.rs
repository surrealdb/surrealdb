//! Store per-node edge data for layers of an HNSW index.
//!
//! Each node's edge list is stored as a separate KV entry keyed by `(layer, node_id)`,
//! replacing the previous chunk-based `Hl` storage. This avoids serializing the
//! entire graph on every insert and enables O(1) per-node persistence.
use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::ElementId;
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct HnswNode<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub layer: u16,
	pub node: ElementId,
}

impl_kv_key_storekey!(HnswNode<'_> => Vec<u8>);

impl<'a> HnswNode<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		layer: u16,
		node: ElementId,
	) -> Self {
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
			_f: b'h',
			_g: b'n',
			layer,
			node,
		}
	}

	/// Returns a key range covering all nodes in the given layer.
	/// Used to scan and reconstruct the full graph for a layer.
	pub(crate) fn new_layer_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		layer: u16,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, ix, layer, 0).encode_key()?;
		let end = Self::new(ns, db, tb, ix, layer, u64::MAX).encode_key()?;
		Ok(beg..end)
	}
	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<HnswNode<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = HnswNode::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7, 8);
		let enc = HnswNode::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hn\0\x07\0\0\0\0\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
