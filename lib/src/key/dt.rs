use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Dt<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tk: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> Dt<'a> {
	Dt::new(ns, db, tb)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x74, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x74, 0xff]);
	k
}

impl<'a> Dt<'a> {
	pub fn new(ns: &'a str, db: &'a str, tk: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x64, // d
			_e: 0x74, // t
			tk,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dt::new(
			"test",
			"test",
			"test",
		);
		let enc = Dt::encode(&val).unwrap();
		let dec = Dt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
