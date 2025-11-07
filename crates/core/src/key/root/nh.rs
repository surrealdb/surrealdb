//! Stores namespace ID generator batch allocations

use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::key::category::{Categorise, Category};
use crate::kvs::sequences::BatchValue;
use crate::kvs::{KVKey, impl_kv_key_storekey};

/// Key structure for storing namespace ID generator batch allocations.
///
/// This key is used to track batch allocations of namespace IDs at the root level.
/// Each batch allocation represents a range of IDs that have been reserved
/// by a particular node for generating namespace identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceIdGeneratorBatchKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub start: i64,
}

impl_kv_key_storekey!(NamespaceIdGeneratorBatchKey => BatchValue);

impl Categorise for NamespaceIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifierBatch
	}
}

impl NamespaceIdGeneratorBatchKey {
	/// Creates a new namespace ID generator batch key.
	///
	/// # Arguments
	/// * `start` - The starting value for this batch allocation
	pub fn new(start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'h',
			start,
		}
	}

	/// Returns the key range for all namespace ID generator batches.
	///
	/// # Returns
	/// A range of encoded keys covering all possible batch allocations
	pub fn range() -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(i64::MIN).encode_key()?;
		let end = Self::new(i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorBatchKey::new(123);
		let enc = NamespaceIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!nh\x80\0\0\0\0\0\0\x7B");
	}

	#[test]
	fn range() {
		let r = NamespaceIdGeneratorBatchKey::range().unwrap();
		assert_eq!(r.start, b"/!nh\0\0\0\0\0\0\0\0");
		assert_eq!(r.end, b"/!nh\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
	}
}
