//! Stores the key prefix for all keys under an index
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct AllIndexRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
}

impl_kv_key_storekey!(AllIndexRoot<'_> => Vec<u8>);

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	ix: IndexId,
) -> AllIndexRoot<'a> {
	AllIndexRoot::new(ns, db, tb, ix)
}

impl Categorise for AllIndexRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexRoot
	}
}

impl<'a> AllIndexRoot<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
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
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn root() {
		let val = AllIndexRoot::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3));
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AllIndexRoot::new(
			NamespaceId(1),
			DatabaseId(2),
			&TableName::new("testtb"),
			IndexId(3),
		);
		let enc = AllIndexRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03");
	}
}
