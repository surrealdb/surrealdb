//! Stores the key prefix for all keys under an index
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct All<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub ix: &'a str,
}

pub fn new(ns: u32, db: u32, tb: u32, ix: &str) -> All {
	All::new(ns, db, tb, ix)
}

impl<'a> All<'a> {
	pub fn new(ns: u32, db: u32, tb: u32, ix: &'a str) -> Self {
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = All::new(
			1,
			2,
			3,
			"testix",
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x01*\0\0\0\x02*\0\0\0\x03+testix\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
