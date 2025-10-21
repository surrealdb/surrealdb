//! Stores namespace ID generator state
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceIdGeneratorStateKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub nid: Uuid,
}

impl_kv_key_storekey!(NamespaceIdGeneratorStateKey=> SequenceState);

impl Categorise for NamespaceIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifierState
	}
}

impl NamespaceIdGeneratorStateKey {
	pub fn new(nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'i',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorStateKey::new(Uuid::from_u128(1));
		let enc = NamespaceIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!ni");
	}
}
