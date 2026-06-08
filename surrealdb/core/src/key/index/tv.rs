use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Full-text term-document compaction generation.
///
/// This key is intentionally outside the `!tt` delta range. It lets a
/// compactor validate that the term-doc snapshot it read is still current
/// before applying exact-key deletes. Missing values are treated as
/// generation `0`.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Tv<'a> {
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

impl_kv_key_storekey!(Tv<'_> => u64);

impl Categorise for Tv<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocuments
	}
}

impl<'a> Tv<'a> {
	/// Creates the per-index generation guard for `!tt` compaction.
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
			_f: b't',
			_g: b'v',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::index::tt::Tt;
	use crate::kvs::KVKey;

	#[test]
	fn generation_key_is_outside_tt_range() {
		let tb = TableName::from("testtb");
		let key = Tv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).encode_key().unwrap();
		let (beg, end) = Tt::terms_range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert!(!beg.le(&key) || !key.lt(&end));
	}
}
