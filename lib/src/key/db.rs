use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Db<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str) -> Db<'a> {
	Db::new(ns, db)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x62, 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x62, 0xff]);
	k
}

impl<'a> Db<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x21, // !
			_c: 0x64, // d
			_d: 0x62, // b
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
			"test",
			"test",
		);
		let enc = Db::encode(&val).unwrap();
		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
