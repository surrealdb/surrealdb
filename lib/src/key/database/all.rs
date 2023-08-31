//! Stores the key prefix for all keys under a database
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct All {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
}

pub fn new(ns: u32, db: u32) -> All {
	All::new(ns, db)
}

impl All {
	pub fn new(ns: u32, db: u32) -> Self {
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
			1,
			2,
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
