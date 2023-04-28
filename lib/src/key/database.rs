use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Database<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str) -> Database<'a> {
	Database::new(ns, db)
}

impl<'a> Database<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
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
		let val = Database::new(
			"test",
			"test",
		);
		let enc = Database::encode(&val).unwrap();
		let dec = Database::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
