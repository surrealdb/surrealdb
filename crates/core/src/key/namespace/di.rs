//! Stores a database ID generator batch value
use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::sequences::SequenceState;
use crate::kvs::{KVKey, impl_kv_key_storekey};
use anyhow::Result;
use std::ops::Range;
use storekey::{BorrowDecode, Encode};

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

impl_kv_key_storekey!(DatabaseIdGeneratorBatchKey => SequenceState);

impl Categorise for DatabaseIdGeneratorBatchKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl DatabaseIdGeneratorBatchKey {
	pub fn new(ns: NamespaceId, start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'i',
			start,
		}
	}

	pub fn range(ns: NamespaceId) -> Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, 0).encode_key()?;
		let end = Self::new(ns, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::root::nb::NamespaceIdGeneratorBatchKey;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = DatabaseIdGeneratorBatchKey::new(
			NamespaceId(123),42
		);
		let enc = DatabaseIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}

	#[test]
	fn range() {
		let r = NamespaceIdGeneratorBatchKey::range().unwrap();
		assert_eq!(r.start, b"/!di");
		assert_eq!(r.end, b"/!di");
	}
}
