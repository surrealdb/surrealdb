use crate::key::CHAR_PATH;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct St<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub sc: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub tk: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, sc: &'a str, tk: &'a str) -> St<'a> {
	St::new(ns, db, sc, tk)
}

pub fn prefix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::scope::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[b'!', b's', b't', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::scope::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[b'!', b's', b't', 0xff]);
	k
}

impl<'a> St<'a> {
	pub fn new(ns: &'a str, db: &'a str, sc: &'a str, tk: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: CHAR_PATH,
			sc,
			_d: b'!',
			_e: b's',
			_f: b't',
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
		let val = St::new(
			"testns",
			"testdb",
			"testsc",
			"testtk",
		);
		let enc = St::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0\xb1testsc\0!sttesttk\0");

		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
