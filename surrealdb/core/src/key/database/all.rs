//! Stores the key prefix for all keys under a database
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseRoot {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
}

impl_kv_key_storekey!(DatabaseRoot => Vec<u8>);

pub fn new(ns: NamespaceId, db: DatabaseId) -> DatabaseRoot {
	DatabaseRoot::new(ns, db)
}

impl Categorise for DatabaseRoot {
	fn categorise(&self) -> Category {
		Category::DatabaseRoot
	}
}

impl DatabaseRoot {
	#[inline]
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = DatabaseRoot::new(NamespaceId(1), DatabaseId(2));
		let enc = val.encode_key().unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02");
	}
}
