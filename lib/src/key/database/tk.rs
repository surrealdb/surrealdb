/// Stores a DEFINE TOKEN ON DATABASE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Tk<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tk: &'a str,
}

pub fn new(ns: u32, db: u32, tk: &str) -> Tk {
	Tk::new(ns, db, tk)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0xff]);
	k
}

impl<'a> Tk<'a> {
	pub fn new(ns: u32, db: u32, tk: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'k',
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
		let val = Tk::new(
			1,
			2,
			"testtk",
		);
		let enc = Tk::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tktesttk\x00");

		let dec = Tk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tk\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(1, 2);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tk\xff");
	}
}
