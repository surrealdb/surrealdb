//! Stores the key prefix for all keys under a database
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AllDbRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
}

impl KVKey for AllDbRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: &'a str, db: &'a str) -> AllDbRoot<'a> {
	AllDbRoot::new(ns, db)
}

impl Categorise for AllDbRoot<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseRoot
	}
}

impl<'a> AllDbRoot<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
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
		let val = AllDbRoot::new(
			"testns",
			"testdb",
		);
		let enc = val.encode_key().unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0");
	}
}
