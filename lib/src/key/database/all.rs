//! Stores the key prefix for all keys under a database
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct All<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str) -> All<'a> {
	All::new(ns, db)
}

impl<'a> All<'a> {
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = All::new(
			"testns",
			"testdb",
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
