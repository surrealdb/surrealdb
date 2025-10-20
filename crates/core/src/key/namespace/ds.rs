//! Stores a database ID generator state
use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;
use storekey::{BorrowDecode, Encode};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseIdGeneratorBatchKey {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	start: i64,
}

impl_kv_key_storekey!(DatabaseIdGeneratorBatchKey => SequenceState);

pub fn new(ns: NamespaceId, start: i64) -> DatabaseIdGeneratorBatchKey {
	DatabaseIdGeneratorBatchKey::new(ns, start)
}

impl Categorise for DatabaseIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl DatabaseIdGeneratorBatchKey {
	pub fn new(ns: NamespaceId, start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b's',
			start,
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
		let val = DatabaseIdGeneratorBatchKey::new(
			NamespaceId(123),42
		);
		let enc = DatabaseIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}
}
