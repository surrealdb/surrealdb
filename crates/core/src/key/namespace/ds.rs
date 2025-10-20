//! Stores a database ID generator state
use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseIdGeneratorStateKey {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	nid: Uuid,
}

impl_kv_key_storekey!(DatabaseIdGeneratorStateKey => SequenceState);

pub fn new(ns: NamespaceId, nid: Uuid) -> DatabaseIdGeneratorStateKey {
	DatabaseIdGeneratorStateKey::new(ns, nid)
}

impl Categorise for DatabaseIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl DatabaseIdGeneratorStateKey {
	pub fn new(ns: NamespaceId, nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b's',
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
		#[rustfmt::skip]
		let val = DatabaseIdGeneratorStateKey::new(
			NamespaceId(123),Uuid::from_u128(15)
		);
		let enc = DatabaseIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}
}
