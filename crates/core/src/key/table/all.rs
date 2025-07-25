//! Stores the key prefix for all keys under a table
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct TableRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
}
impl_key!(TableRoot<'a>);

impl KVKey for TableRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> TableRoot<'a> {
	TableRoot::new(ns, db, tb)
}

impl Categorise for TableRoot<'_> {
	fn categorise(&self) -> Category {
		Category::TableRoot
	}
}

impl<'a> TableRoot<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str) -> Self {
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
		let val = TableRoot::new(
			"testns",
			"testdb",
			"testtb",
		);
		let enc = TableRoot::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0");

		let dec = TableRoot::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
