//! Stores index ID generator batch allocations

use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;

key! {
	/// Key structure for storing index ID generator batch allocations.
	///
	/// This key is used to track batch allocations of index IDs within a table.
	/// Each batch allocation represents a range of IDs that have been reserved
	/// by a particular node for generating index identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct IndexIdGeneratorBatchKey<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'i',
		b'h',
		pub start: i64,
	}
}
impl_kv_key_storekey!(IndexIdGeneratorBatchKey<'a> => BatchValue);

key! {
	pub(crate) struct IndexIdGeneratorBatchPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'i',
		b'h',
	}
}
impl_kv_range_storekey!(IndexIdGeneratorBatchPrefix<'_>);

impl<'a> Categorise for IndexIdGeneratorBatchKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifierBatch
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = IndexIdGeneratorBatchKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(123),
				db: DatabaseId(234),
			},
			tb: Cow::Borrowed(&tb),
			start: 15,
		};
		let enc = IndexIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ih\x80\0\0\0\0\0\0\x0F");
	}

	#[test]
	fn range() {
		let tb = TableName::from("testtb");
		let r = IndexIdGeneratorBatchPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(123),
				db: DatabaseId(234),
			},
			tb: Cow::Borrowed(&tb),
		}
		.encode_range()
		.unwrap();
		assert_eq!(r.start.as_slice(), b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ih\0");
		assert_eq!(r.end.as_slice(), b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ii");
	}
}
