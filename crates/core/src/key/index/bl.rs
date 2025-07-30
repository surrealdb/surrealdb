//! Stores BTree nodes for doc lengths
use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::trees::store::NodeId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Bl<'a> {
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
	pub node_id: Option<NodeId>,
}
impl_key!(Bl<'a>);

impl Categorise for Bl<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNodeDocLengths
	}
}

impl<'a> Bl<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		node_id: Option<NodeId>,
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
			_f: b'b',
			_g: b'l',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bl::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			Some(7)
		);
		let enc = Bl::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0*testtb\0+testix\0!bl\x01\0\0\0\0\0\0\0\x07");

		let dec = Bl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
