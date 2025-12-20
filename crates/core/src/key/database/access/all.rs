//! Stores the key prefix for all keys under a database access method
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DbAccess<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ac: Cow<'a, str>,
}

impl_kv_key_storekey!(DbAccess<'_> => Vec<u8>);

pub fn new(ns: NamespaceId, db: DatabaseId, ac: &str) -> DbAccess<'_> {
	DbAccess::new(ns, db, ac)
}

impl Categorise for DbAccess<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessRoot
	}
}

impl<'a> DbAccess<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'&',
			ac: Cow::Borrowed(ac),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = DbAccess::new(NamespaceId(1), DatabaseId(2), "testac");
		let enc = DbAccess::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0");
	}
}
