//! Store appended records for concurrent index building
use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::kvs::index::{Appending, AppendingId, BatchId};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct IndexAppending<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub appending_id: AppendingId,
	pub batch_id: BatchId,
}

impl_kv_key_storekey!(IndexAppending<'_> => Appending);

impl<'a> IndexAppending<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		appending_id: AppendingId,
		batch_id: BatchId,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'g',
			appending_id,
			batch_id,
		}
	}

	pub(crate) fn new_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, ix, AppendingId::MIN, BatchId::MIN).encode_key()?;
		let end = Self::new(ns, db, tb, ix, AppendingId::MAX, BatchId::MAX).encode_key()?;
		Ok(beg..end)
	}

	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<IndexAppending<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = IndexAppending::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 1, 2);
		let enc = IndexAppending::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ig\x00\x00\x00\x01\x00\x00\x00\x02",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}

	#[test]
	fn ib_range() {
		let tb = TableName::from("testtb");
		let range =
			IndexAppending::new_range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert_eq!(
			range.start,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ig\0\0\0\0\0\0\0\0"
		);
		assert_eq!(
			range.end,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ig\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
