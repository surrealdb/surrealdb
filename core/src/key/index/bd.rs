//! Stores BTree nodes for doc ids
use crate::idx::trees::store::NodeId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Bd<'a> {
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
	pub node_id: Option<NodeId>,
}

impl Categorise for Bd<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBTreeNode
	}
}

impl<'a> Bd<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
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
			_g: b'd',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bd::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			Some(7)
		);
		let enc = Bd::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bd\x01\0\0\0\0\0\0\0\x07");
		let dec = Bd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
