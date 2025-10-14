//! Stores a database ID generator state
use storekey::{BorrowDecode, Encode};

use crate::catalog::NamespaceId;
use crate::idg::u32::U32;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseIdGeneratorKey {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
}

impl_kv_key_storekey!(DatabaseIdGeneratorKey => U32);

pub fn new(ns: NamespaceId) -> DatabaseIdGeneratorKey {
	DatabaseIdGeneratorKey::new(ns)
}

impl Categorise for DatabaseIdGeneratorKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl DatabaseIdGeneratorKey {
	pub fn new(ns: NamespaceId) -> Self {
		Self {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'i',
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
		let val = DatabaseIdGeneratorKey::new(
			NamespaceId(123),
		);
		let enc = DatabaseIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}
}
