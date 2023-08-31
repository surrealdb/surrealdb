//! Stores the key prefix for all keys under a table
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Table {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
}

pub fn new(ns: u32, db: u32, tb: u32) -> Table {
	Table::new(ns, db, tb)
}

impl Table {
	pub fn new(ns: u32, db: u32, tb: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Table::new(
			1,
			2,
			3,
		);
		let enc = Table::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03");

		let dec = Table::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
