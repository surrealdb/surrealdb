//! HNSW compaction generation key.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// HNSW pending compaction generation.
///
/// The generation lets compactors validate that the pending snapshot they
/// gathered is still current before deleting exact pending keys and mutating
/// the graph. Missing values are treated as generation `0`.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Hg<'a> {
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
}

impl_kv_key_storekey!(Hg<'_> => u64);

impl<'a> Hg<'a> {
	/// Creates the per-index generation guard for HNSW pending compaction.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
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
			_f: b'h',
			_g: b'g',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::index::hp::HnswPendingPrefix;
	use crate::kvs::KVKey;

	#[test]
	fn generation_key_is_outside_hp_range() {
		let tb = TableName::from("testtb");
		let key = Hg::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).encode_key().unwrap();
		let range =
			HnswPendingPrefix::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert!(!range.start.le(&key) || !key.lt(&range.end));
	}
}
