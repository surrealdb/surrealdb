//! Stores the key prefix for all keys under a database
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct AllDbRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
}
impl_key!(AllDbRoot<'a>);

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
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = AllDbRoot::new(
			"testns",
			"testdb",
		);
		let enc = AllDbRoot::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0");

		let dec = AllDbRoot::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
