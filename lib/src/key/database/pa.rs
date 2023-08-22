//! Stores a DEFINE PARAM config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Pa<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub pa: &'a str,
}

pub fn new(ns: u32, db: u32, pa: &str) -> Pa {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'p', b'a', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'p', b'a', 0xff]);
	k
}

impl<'a> Pa<'a> {
	pub fn new(ns: u32, db: u32, pa: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'p',
			_e: b'a',
			pa,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Pa::new(
			1,
			2,
			"testpa",
		);
		let enc = Pa::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!patestpa\0");

		let dec = Pa::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
