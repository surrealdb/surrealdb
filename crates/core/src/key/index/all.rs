//! Stores the key prefix for all keys under an index
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct All<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
}
impl_key!(All<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> All<'a> {
	All::new(ns, db, tb, ix)
}

impl Categorise for All<'_> {
	fn categorise(&self) -> Category {
		Category::IndexRoot
	}
}

impl<'a> All<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
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
		let val = All::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
