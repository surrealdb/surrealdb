/// Stores a DEFINE FUNCTION config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Fc<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub fc: &'a str,
}

pub fn new(ns: u32, db: u32, fc: &str) -> Fc {
	Fc::new(ns, db, fc)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'f', b'n', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'f', b'n', 0xff]);
	k
}

impl<'a> Fc<'a> {
	pub fn new(ns: u32, db: u32, fc: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'f',
			_e: b'n',
			fc,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Fc::new(
			1,
			2,
			"testfc",
		);
		let enc = Fc::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fntestfc\x00");
		let dec = Fc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fn\x00");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fn\xff");
	}
}
