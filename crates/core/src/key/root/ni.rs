//! Stores namespace ID generator state
use serde::{Deserialize, Serialize};

use crate::idg::u32::U32;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct NamespaceIdGeneratorKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
}

impl KVKey for NamespaceIdGeneratorKey {
	type ValueType = U32;
}

impl Default for NamespaceIdGeneratorKey {
	fn default() -> Self {
		Self::new()
	}
}

impl Categorise for NamespaceIdGeneratorKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifier
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

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorKey::new();
		let enc = NamespaceIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!ni");
	}
}
