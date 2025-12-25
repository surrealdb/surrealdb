//! Stores database ID generator batch allocations
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::sequences::BatchValue;
use crate::kvs::{KVKey, impl_kv_key_storekey};

/// Key structure for storing database ID generator batch allocations.
///
/// This key is used to track batch allocations of database IDs within a namespace.
/// Each batch allocation represents a range of IDs that have been reserved
/// by a particular node for generating database identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseIdGeneratorBatchKey {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	start: i64,
}

impl_kv_key_storekey!(DatabaseIdGeneratorBatchKey => BatchValue);

impl Categorise for DatabaseIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifierBatch
	}
}
impl DatabaseIdGeneratorBatchKey {
	/// Creates a new database ID generator batch key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `start` - The starting value for this batch allocation
	pub fn new(ns: NamespaceId, start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'h',
			start,
		}
	}

	/// Returns the key range for all database ID generator batches in a namespace.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	///
	/// # Returns
	/// A range of encoded keys covering all possible batch allocations
	pub fn range(ns: NamespaceId) -> Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, i64::MIN).encode_key()?;
		let end = Self::new(ns, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = DatabaseIdGeneratorBatchKey::new(NamespaceId(123), 42);
		let enc = DatabaseIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x7B!dh\x80\0\0\0\0\0\0\x2A");
	}

	#[test]
	fn range() {
		let r = DatabaseIdGeneratorBatchKey::range(NamespaceId(123)).unwrap();
		assert_eq!(r.start, b"/*\0\0\0\x7B!dh\0\0\0\0\0\0\0\0");
		assert_eq!(r.end, b"/*\0\0\0\x7B!dh\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
	}
}
