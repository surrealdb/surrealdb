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
		Category::TableIndexIdentifier
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
		let beg = Self::new(ns, db, tb, 0).encode_key()?;
		let end = Self::new(ns, db, tb, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn batch_key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorBatchKey::new(
			NamespaceId(123),
			DatabaseId(234),
			"testtb",
			42
		);
		let enc = IndexIdGeneratorBatchKey::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			vec![
				47, 42, 0, 0, 0, 123, 42, 0, 0, 0, 234, 42, 116, 101, 115, 116, 116, 98, 0, 33,
				105, 104, 128, 0, 0, 0, 0, 0, 0, 42
			]
		);
	}

	#[test]
	fn batch_range() {
		let r =
			IndexIdGeneratorBatchKey::range(NamespaceId(123), DatabaseId(234), "testtb").unwrap();
		assert_eq!(
			r.start,
			vec![
				47, 42, 0, 0, 0, 123, 42, 0, 0, 0, 234, 42, 116, 101, 115, 116, 116, 98, 0, 33,
				105, 104, 128, 0, 0, 0, 0, 0, 0, 0
			]
		);
		assert_eq!(
			r.end,
			vec![
				47, 42, 0, 0, 0, 123, 42, 0, 0, 0, 234, 42, 116, 101, 115, 116, 116, 98, 0, 33,
				105, 104, 255, 255, 255, 255, 255, 255, 255, 255
			]
		);
	}
}
