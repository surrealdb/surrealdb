use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Du<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub user: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, user: &'a str) -> Du<'a> {
	Du::new(ns, db, user)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'u', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'u', 0xff]);
	k
}

impl<'a> Du<'a> {
	pub fn new(ns: &'a str, db: &'a str, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'd',
			_e: b'u',
			user,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Du::new(
			"test",
			"test",
			"test",
		);
		let enc = Du::encode(&val).unwrap();
		let dec = Du::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
