//! Stores the key prefix for all keys under a table
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Table<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
}
impl_key!(Table<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> Table<'a> {
	Table::new(ns, db, tb)
}

impl Categorise for Table<'_> {
	fn categorise(&self) -> Category {
		Category::TableRoot
	}
}

impl<'a> Table<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Table::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
		);
		let enc = Table::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0*testtb\0");

		let dec = Table::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
