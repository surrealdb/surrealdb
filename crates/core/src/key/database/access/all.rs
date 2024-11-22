//! Stores the key prefix for all keys under a database access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Access<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub ac: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str) -> Access<'a> {
	Access::new(ns, db, ac)
}

impl Categorise for Access<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessGrant
	}
}

impl<'a> Access<'a> {
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Access::new(
			"testns",
			"testdb",
			"testac",
		);
		let enc = Access::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0");

		let dec = Access::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
