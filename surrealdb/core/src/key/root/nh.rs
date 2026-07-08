//! Stores namespace ID generator batch allocations

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;

key! {
	/// Key structure for storing namespace ID generator batch allocations.
	///
	/// This key is used to track batch allocations of namespace IDs at the root level.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating namespace identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NamespaceIdGeneratorBatchKey {
		b'/',
		b'!',
		b'n',
		b'h',
		pub start: i64,
	}
}

impl_kv_key_storekey!(NamespaceIdGeneratorBatchKey => BatchValue);

impl Categorise for NamespaceIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifierBatch
	}
}

key! {
	/// Key structure for storing namespace ID generator batch allocations.
	///
	/// This key is used to track batch allocations of namespace IDs at the root level.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating namespace identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NamespaceIdGeneratorBatchPrefix {
		b'/',
		b'!',
		b'n',
		b'h',
	}
}
impl_kv_range_storekey!(NamespaceIdGeneratorBatchPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorBatchKey {
			start: 123,
		};
		let enc = NamespaceIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!nh\x80\0\0\0\0\0\0\x7B");
	}
}
