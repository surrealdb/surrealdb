//! Stores the key prefix for all keys under a table
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::key::database::all::DatabaseRoot;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct TableRoot<'a> {
	pub db_root: DatabaseRoot,
	_c: u8,
	pub tb: &'a str,
}

impl KVKey for TableRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> TableRoot<'a> {
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
			db_root: DatabaseRoot::new(ns, db),
			_c: b'*',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = TableRoot::new(
			"testns",
			"testdb",
			"testtb",
		);
		let enc = TableRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0");
	}
}
