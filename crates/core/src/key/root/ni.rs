//! Stores namespace ID generator state
use storekey::{BorrowDecode, Encode};

use crate::idg::u32::U32;
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceIdGeneratorKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
}

impl_kv_key_storekey!(NamespaceIdGeneratorKey=> U32);

impl Default for NamespaceIdGeneratorKey {
	fn default() -> Self {
		Self::new()
	}
}

impl NamespaceIdGeneratorKey {
	pub fn new() -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorKey::new();
		let enc = NamespaceIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!ni");
	}
}
