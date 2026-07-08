//! Stores table ID generator batch allocations

use anyhow::Result;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;

key! {
	/// Key structure for storing table ID generator batch allocations.
	///
	/// This key is used to track batch allocations of table IDs within a database.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating table identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TableIdGeneratorBatchKey {
		pub prefix: DatabaseRoot,
		 b'!',
		 b't',
		 b'h',
		pub start: i64,
	}
}

impl_kv_key_storekey!(TableIdGeneratorBatchKey => BatchValue);
impl Categorise for TableIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifierBatch
	}
}

key! {
	/// Key structure for storing table ID generator batch allocations.
	///
	/// This key is used to track batch allocations of table IDs within a database.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating table identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TableIdGeneratorBatchPrefix {
		pub prefix: DatabaseRoot,
		 b'!',
		 b't',
		 b'h',
	}
}
impl_kv_range_storekey!(TableIdGeneratorBatchPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = TableIdGeneratorBatchKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(123),
				db: DatabaseId(234),
			},
			start: 42,
		};
		let enc = TableIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\0\0\0\x7B*\0\0\0\xEA!th\x80\0\0\0\0\0\0\x2A");
	}
}
