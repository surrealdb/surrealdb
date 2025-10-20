//! Stores namespace ID generator batch value
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::BatchValue;
use storekey::{BorrowDecode, Encode};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceIdGeneratorBatchKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub start: i64,
}

impl_kv_key_storekey!(NamespaceIdGeneratorBatchKey=> BatchValue);

impl Categorise for NamespaceIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifier
	}
}

impl NamespaceIdGeneratorBatchKey {
	pub fn new(start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'b',
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
		let val = NamespaceIdGeneratorBatchKey::new(15);
		let enc = NamespaceIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!nb");
	}
}
