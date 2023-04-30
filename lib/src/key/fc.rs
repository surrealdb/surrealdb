use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Fc<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub fc: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, fc: &'a str) -> Fc<'a> {
	Fc::new(ns, db, fc)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x66, 0x6e, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x66, 0x6e, 0xff]);
	k
}

impl<'a> Fc<'a> {
	pub fn new(ns: &'a str, db: &'a str, fc: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x66, // f
			_e: 0x6e, // n
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
			"test",
			"test",
			"test",
		);
		let enc = Fc::encode(&val).unwrap();
		let dec = Fc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
