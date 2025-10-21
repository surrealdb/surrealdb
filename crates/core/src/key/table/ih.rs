//! Stores the next and available freed IDs for Index batch value

use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::table::all::TableRoot;
use crate::kvs::sequences::SequenceState;
use crate::kvs::{KVKey, impl_kv_key_storekey};

// Index ID generator batch
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct IndexIdGeneratorBatchKey<'a> {
	table_root: TableRoot<'a>,
	_c: u8,
	_d: u8,
	_e: u8,
	start: i64,
}

impl_kv_key_storekey!(IndexIdGeneratorBatchKey<'_> => SequenceState);

impl<'a> Categorise for IndexIdGeneratorBatchKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifierBatch
	}
}

impl<'a> IndexIdGeneratorBatchKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, start: i64) -> Self {
		IndexIdGeneratorBatchKey {
			table_root: TableRoot::new(ns, db, tb),
			_c: b'!',
			_d: b'i',
			_e: b'h',
			start,
		}
	}

	pub fn range(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> anyhow::Result<Range<Vec<u8>>> {
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
