//! Stores a database ID generator state
use serde::{Deserialize, Serialize};

use crate::catalog::NamespaceId;
use crate::idg::u32::U32;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Di {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
}

impl KVKey for Di {
	type ValueType = U32;
}

pub fn new(ns: NamespaceId) -> Di {
	Di::new(ns)
}

impl Categorise for Di {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl Di {
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

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Di::new(
			NamespaceId(123),
		);
		let enc = Di::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}
}
