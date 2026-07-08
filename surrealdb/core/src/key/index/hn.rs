//! Store per-node edge data for layers of an HNSW index.
//!
//! Each node's edge list is stored as a separate KV entry keyed by `(layer, node_id)`,
//! replacing the previous chunk-based `Hl` storage. This avoids serializing the
//! entire graph on every insert and enables O(1) per-node persistence.
use std::borrow::Cow;
use std::fmt::Debug;

use crate::catalog::IndexId;
use crate::idx::trees::hnsw::ElementId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct HnswNode<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'n',
		pub layer: u16,
		pub node: ElementId,
	}
}

impl_kv_key_storekey!(HnswNode<'a> => Vec<u8>);

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct HnswNodePrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'n',
		pub layer: u16,
	}
}
impl_kv_range_storekey!(HnswNodePrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = HnswNode {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			layer: 7,
			node: 8,
		};
		let enc = HnswNode::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hn\0\x07\0\0\0\0\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
