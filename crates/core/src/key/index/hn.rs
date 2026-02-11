//! Store per-node edge data for layers of an HNSW index.
//!
//! Each node's edge list is stored as a separate KV entry keyed by `(layer, node_id)`,
//! replacing the previous chunk-based `Hl` storage. This avoids serializing the
//! entire graph on every insert and enables O(1) per-node persistence.
use crate::err::Error;
use crate::idx::trees::hnsw::ElementId;
use crate::kvs::{impl_key, KeyDecode, KeyEncode};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct HnswNode<'a> {
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
	pub layer: u16,
	pub node: ElementId,
}

impl_key!(HnswNode<'a>);

impl<'a> HnswNode<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
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
			tb,
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
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		layer: u16,
	) -> Result<Range<Vec<u8>>, Error> {
		let beg = Self::new(ns, db, tb, ix, layer, 0).encode()?;
		let end = Self::new(ns, db, tb, ix, layer, u64::MAX).encode()?;
		Ok(beg..end)
	}
	pub(crate) fn decode_key(k: &[u8]) -> Result<HnswNode<'_>, Error> {
		HnswNode::decode(k)
	}
}

#[cfg(test)]
mod tests {
	use crate::key::index::hn::HnswNode;
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		let val = HnswNode::new("testns", "testdb", "testtb", "testix", 7, 8);
		let enc = HnswNode::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hn\0\x07\0\0\0\0\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);
		let dec = HnswNode::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
