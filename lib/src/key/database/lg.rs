//! Stores a DEFINE LOGIN ON DATABASE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lg<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub dl: &'a str,
}

pub fn new(ns: u32, db: u32, dl: &str) -> Lg {
	Lg::new(ns, db, dl)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'g', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'g', 0xff]);
	k
}

impl<'a> Lg<'a> {
	pub fn new(ns: u32, db: u32, dl: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'l',
			_e: b'g',
			dl,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lg::new(
			1,
			2,
			"testdl",
		);
		let enc = Lg::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!lgtestdl\0");

		let dec = Lg::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
