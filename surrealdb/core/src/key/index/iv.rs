use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Count-index compaction generation.
///
/// This key is intentionally outside the `!iu` count-entry range. It lets a
/// compactor validate that the count snapshot it read is still current before
/// deleting exact keys and writing the compacted aggregate. Missing values are
/// treated as generation `0`.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Iv<'a> {
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

impl_kv_key_storekey!(Iv<'_> => u64);

impl Categorise for Iv<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCountState
	}
}

impl<'a> Iv<'a> {
	/// Creates the per-index generation guard for `!iu` compaction.
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
			_f: b'i',
			_g: b'v',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::index::iu::IndexCountKey;
	use crate::kvs::KVKey;

	#[test]
	fn generation_key_is_outside_iu_range() {
		let tb = TableName::from("testtb");
		let key = Iv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).encode_key().unwrap();
		let range = IndexCountKey::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert!(!range.start.le(&key) || !key.lt(&range.end));
	}
}
