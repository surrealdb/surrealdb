//! Stores database ID generator batch allocations

use anyhow::Result;

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;

key! {
	/// Key structure for storing database ID generator batch allocations.
	///
	/// This key is used to track batch allocations of database IDs within a namespace.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating database identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DatabaseIdGeneratorBatchKey {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'd',
		b'h',
		pub start: i64,
	}
}

impl_kv_key_storekey!(DatabaseIdGeneratorBatchKey => BatchValue);

impl Categorise for DatabaseIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifierBatch
	}
}

key! {
	/// Key structure for storing database ID generator batch allocations.
	///
	/// This key is used to track batch allocations of database IDs within a namespace.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating database identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DatabaseIdGeneratorBatchPrefix {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'd',
		b'h',
	}
}
impl_kv_range_storekey!(DatabaseIdGeneratorBatchPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = DatabaseIdGeneratorBatchKey {
			ns: NamespaceId(123),
			start: 42,
		};
		let enc = DatabaseIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\0\0\0\x7B!dh\x80\0\0\0\0\0\0\x2A");
	}
}
