//! Stores the key prefix for all keys under a database
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::key::namespace::all::NamespaceRoot;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct DatabaseRoot {
	ns_root: NamespaceRoot,
	_a: u8,
	pub db: DatabaseId,
}

impl KVKey for DatabaseRoot {
	type ValueType = Vec<u8>;
}

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
			ns_root: NamespaceRoot::new(ns),
			_a: b'*',
			db,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = DatabaseRoot::new(
			NamespaceId(1),
			DatabaseId(2),
		);
		let enc = val.encode_key().unwrap();
		assert_eq!(enc, b"/*1\0*2\0");
	}
}
