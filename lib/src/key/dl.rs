use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Dl<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub dl: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, dl: &'a str) -> Dl<'a> {
	Dl::new(ns, db, dl)
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'l', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'd', b'l', 0xff]);
	k
}

impl<'a> Dl<'a> {
	pub fn new(ns: &'a str, db: &'a str, dl: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'd',
			_e: b'l',
			dl,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dl::new(
			"testns",
			"testdb",
			"testdl",
		);
		let enc = Dl::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!dltestdl\0");

		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
