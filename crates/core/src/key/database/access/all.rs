//! Stores the key prefix for all keys under a database access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use crate::kvs::KVKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct DbAccess<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub ac: &'a str,
}
impl_key!(DbAccess<'a>);

impl KVKey for DbAccess<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str) -> DbAccess<'a> {
	DbAccess::new(ns, db, ac)
}

impl Categorise for DbAccess<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessRoot
	}
}

impl<'a> DbAccess<'a> {
	pub fn new(ns: &'a str, db: &'a str, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'&',
			ac,
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
		let val = DbAccess::new(
			"testns",
			"testdb",
			"testac",
		);
		let enc = DbAccess::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0");

		let dec = DbAccess::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
