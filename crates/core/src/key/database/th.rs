//! Stores table ID generator batch allocations

use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::sequences::BatchValue;
use crate::kvs::{KVKey, impl_kv_key_storekey};

/// Key structure for storing table ID generator batch allocations.
///
/// This key is used to track batch allocations of table IDs within a database.
/// Each batch allocation represents a range of IDs that have been reserved
/// by a particular node for generating table identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct TableIdGeneratorBatchKey {
	database_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
	start: i64,
}

impl_kv_key_storekey!(TableIdGeneratorBatchKey => BatchValue);

impl Categorise for TableIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifierBatch
	}
}

impl TableIdGeneratorBatchKey {
	/// Creates a new table ID generator batch key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `start` - The starting value for this batch allocation
	pub fn new(ns: NamespaceId, db: DatabaseId, start: i64) -> Self {
		TableIdGeneratorBatchKey {
			database_root: DatabaseRoot::new(ns, db),
			_c: b'!',
			_d: b't',
			_e: b'h',
			start,
		}
	}

	/// Returns the key range for all table ID generator batches in a database.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	///
	/// # Returns
	/// A range of encoded keys covering all possible batch allocations
	pub fn range(ns: NamespaceId, db: DatabaseId) -> Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, i64::MIN).encode_key()?;
		let end = Self::new(ns, db, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = TableIdGeneratorBatchKey::new(NamespaceId(123), DatabaseId(234), 42);
		let enc = TableIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\0\0\0\x7B*\0\0\0\xEA!th\x80\0\0\0\0\0\0\x2A");
	}

	#[test]
	fn range() {
		let r = TableIdGeneratorBatchKey::range(NamespaceId(123), DatabaseId(234)).unwrap();
		assert_eq!(r.start, b"/*\0\0\0\x7B*\0\0\0\xEA!th\0\0\0\0\0\0\0\0");
		assert_eq!(r.end, b"/*\0\0\0\x7B*\0\0\0\xEA!th\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
	}
}
