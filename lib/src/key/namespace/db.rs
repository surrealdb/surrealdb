/// Stores a DEFINE DATABASE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Db<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: &'a str,
}

pub fn new(ns: u32, db: &str) -> Db {
	Db::new(ns, db)
}

pub fn prefix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'b', 0x00]);
	k
}

pub fn suffix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'b', 0xff]);
	k
}

impl<'a> Db<'a> {
	pub fn new(ns: u32, db: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'b',
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
		let val = Db::new(
			123,
			"testdb",
		);
		let enc = Db::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!dbtestdb\0");

		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(123);
		assert_eq!(val, b"/*\x00\x00\x00\x01!db\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(123);
		assert_eq!(val, b"/*testns\0!db\xff")
	}
}
