//! Stores index ID generator batch allocations

use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::table::all::TableRoot;
use crate::kvs::sequences::BatchValue;
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

/// Key structure for storing index ID generator batch allocations.
///
/// This key is used to track batch allocations of index IDs within a table.
/// Each batch allocation represents a range of IDs that have been reserved
/// by a particular node for generating index identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct IndexIdGeneratorBatchKey<'a> {
	table_root: TableRoot<'a>,
	_c: u8,
	_d: u8,
	_e: u8,
	start: i64,
}

impl_kv_key_storekey!(IndexIdGeneratorBatchKey<'_> => BatchValue);

impl<'a> Categorise for IndexIdGeneratorBatchKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifierBatch
	}
}

impl<'a> IndexIdGeneratorBatchKey<'a> {
	/// Creates a new index ID generator batch key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `tb` - The table name
	/// * `start` - The starting value for this batch allocation
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, start: i64) -> Self {
		IndexIdGeneratorBatchKey {
			table_root: TableRoot::new(ns, db, tb),
			_c: b'!',
			_d: b'i',
			_e: b'h',
			start,
		}
	}

	/// Returns the key range for all index ID generator batches in a table.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `tb` - The table name
	///
	/// # Returns
	/// A range of encoded keys covering all possible batch allocations
	pub fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, i64::MIN).encode_key()?;
		let end = Self::new(ns, db, tb, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorBatchKey::new(
			NamespaceId(123),
			DatabaseId(234),
			"testtb",
			15
		);
		let enc = IndexIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ih\x80\0\0\0\0\0\0\x0F");
	}

	#[test]
	fn range() {
		let r =
			IndexIdGeneratorBatchKey::range(NamespaceId(123), DatabaseId(234), "testtb").unwrap();
		assert_eq!(r.start, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ih\0\0\0\0\0\0\0\0");
		assert_eq!(r.end, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ih\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
	}
}
