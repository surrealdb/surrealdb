use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Sc<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sc: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, sc: &'a str) -> Sc<'a> {
	Sc::new(ns, db, sc)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x73, 0x63, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x73, 0x63, 0xff]);
	k
}

impl<'a> Sc<'a> {
	pub fn new(ns: &'a str, db: &'a str, sc: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x73, // s
			_e: 0x63, // c
			sc,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Sc::new(
			"test",
			"test",
			"test",
		);
		let enc = Sc::encode(&val).unwrap();
		let dec = Sc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
