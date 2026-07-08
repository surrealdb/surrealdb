//! Stores the key prefix for all keys under a namespace

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NamespaceRoot {
		b'/',
		b'*',
		pub ns: NamespaceId,
	}
}

// When querying all keys under a namespace, the output value could be any
// value.
impl_kv_range_storekey!(NamespaceRoot);

impl Categorise for NamespaceRoot {
	fn categorise(&self) -> Category {
		Category::NamespaceRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = NamespaceRoot {
			ns: NamespaceId(1),
		};
		let enc = NamespaceRoot::encode_range(&val).unwrap();
		assert_eq!(enc.start.as_slice(), b"/*\x00\x00\x00\x01\0");
	}
}
