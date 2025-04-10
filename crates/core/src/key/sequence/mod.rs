//! Stores sequence states
pub mod ba;
pub mod st;

use crate::err::Error;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
}
impl_key!(Prefix<'a>);

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, sq: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b's',
			_e: b'q',
			sq,
		}
	}
}

pub fn prefix(ns: &str, db: &str, sq: &str) -> Result<Vec<u8>, Error> {
	Prefix::new(ns, db, sq).encode()
}

#[cfg(test)]
mod tests {
	#[test]
	fn prefix() {
		use super::*;
		#[rustfmt::skip]
		let val = prefix(
			"testns",
			"testdb",
		"testsq"
		).unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!sqtestsq\0");
	}
}
