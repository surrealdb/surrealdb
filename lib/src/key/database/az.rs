//! Stores a DEFINE ANALYZER config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Az<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub az: &'a str,
}

pub fn new(ns: u32, db: u32, tb: &str) -> Az {
	Az::new(ns, db, tb)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'z', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'z', 0xff]);
	k
}

impl<'a> Az<'a> {
	pub fn new(ns: u32, db: u32, az: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'a', // a
			_e: b'z', // z
			az,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
            let val = Az::new(
            1,
            2,
            "test",
        );
		let enc = Az::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aztest\x00");
		let dec = Az::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!az\x00");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!az\xff");
	}
}
