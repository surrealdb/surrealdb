//! Stores cluster membership information
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::dbs::node::Node;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

// Represents cluster information.
// In the future, this could also include broadcast addresses and other
// information.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Nd {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub nd: Uuid,
}

impl_kv_key_storekey!(Nd => Node);

pub fn new(nd: Uuid) -> Nd {
	Nd::new(nd)
}

pub fn prefix() -> Vec<u8> {
	let mut k = crate::key::root::all::kv();
	k.extend_from_slice(b"!nd\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = crate::key::root::all::kv();
	k.extend_from_slice(b"!nd\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00");
	k
}

impl Categorise for Nd {
	fn categorise(&self) -> Category {
		Category::Node
	}
}

impl Nd {
	pub fn new(nd: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'd',
			nd,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Nd::new(Uuid::default());
		let enc = val.encode_key().unwrap();
		assert_eq!(&enc, b"/!nd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix();
		assert_eq!(val, b"/!nd\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix();
		assert_eq!(val, b"/!nd\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00")
	}
}
