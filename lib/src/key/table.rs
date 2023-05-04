use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Table<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> Table<'a> {
	Table::new(ns, db, tb)
}

impl<'a> Table<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
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
			"test",
			"test",
			"test",
		);
		let enc = Table::encode(&val).unwrap();
		let dec = Table::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
