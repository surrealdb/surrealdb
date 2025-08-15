//! Stores the key prefix for all keys under a namespace
use serde::{Deserialize, Serialize};

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct NamespaceRoot {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
}

/// When querying all keys under a namespace, the output value could be any
/// value.
impl KVKey for NamespaceRoot {
	type ValueType = Vec<u8>;
}

pub fn new(ns: NamespaceId) -> NamespaceRoot {
	NamespaceRoot::new(ns)
}

impl Categorise for NamespaceRoot {
	fn categorise(&self) -> Category {
		Category::NamespaceRoot
	}
}

impl NamespaceRoot {
	#[inline]
	pub fn new(ns: NamespaceId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = NamespaceRoot::new(
			NamespaceId(1),
		);
		let enc = NamespaceRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01");
	}
}
