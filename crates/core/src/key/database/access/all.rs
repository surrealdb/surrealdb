//! Stores the key prefix for all keys under a database access method
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct DbAccess<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub ac: &'a str,
}

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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = DbAccess::new(
			"testns",
			"testdb",
			"testac",
		);
		let enc = DbAccess::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0");
	}
}
