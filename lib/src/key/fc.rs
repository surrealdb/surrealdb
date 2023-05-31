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
	k.extend_from_slice(&[b'!', b'f', b'n', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'f', b'n', 0xff]);
	k
}

impl<'a> Fc<'a> {
	pub fn new(ns: &'a str, db: &'a str, fc: &'a str) -> Self {
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
			"test",
			"test",
			"test",
		);
		let enc = Fc::encode(&val).unwrap();
		let dec = Fc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
