//! Stores the key prefix for all keys under a table
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct TableRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
}

impl_kv_key_storekey!(TableRoot<'_> => Vec<u8>);

pub fn new(ns: NamespaceId, db: DatabaseId, tb: &str) -> TableRoot<'_> {
	TableRoot::new(ns, db, tb)
}

impl Categorise for TableRoot<'_> {
	fn categorise(&self) -> Category {
		Category::TableRoot
	}
}

impl<'a> TableRoot<'a> {
	#[inline]
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = TableRoot::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
		);
		let enc = TableRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0");
	}
}
