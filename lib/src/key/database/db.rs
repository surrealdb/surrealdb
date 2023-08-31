/// Stores a DEFINE DATABASE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Db {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
}

pub fn new(ns: u32, db: u32) -> Db {
	Db::new(ns, db)
}

impl Db {
	pub fn new(ns: u32, db: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'd',
			_e: b'b',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Db::new(
			1,
			2,
		);
		let enc = Db::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!db");

		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
