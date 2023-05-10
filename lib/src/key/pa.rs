use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Pa<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub pa: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, pa: &'a str) -> Pa<'a> {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x70, 0x61, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x70, 0x61, 0xff]);
	k
}

impl<'a> Pa<'a> {
	pub fn new(ns: &'a str, db: &'a str, pa: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x70, // p
			_e: 0x61, // a
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
			"test",
			"test",
			"test",
		);
		let enc = Pa::encode(&val).unwrap();
		let dec = Pa::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
