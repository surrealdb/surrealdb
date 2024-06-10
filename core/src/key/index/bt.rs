//! Stores BTree nodes for terms
use crate::idx::trees::store::NodeId;
use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Bt<'a> {
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

impl KeyRequirements for Bt<'_> {
	fn key_category(&self) -> Category {
		Category::IndexBTreeNodeTerms
	}
}

impl<'a> Bt<'a> {
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
			_g: b't',
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
		let val = Bt::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			Some(7)
		);
		let enc = Bt::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bt\x01\0\0\0\0\0\0\0\x07");
		let dec = Bt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
