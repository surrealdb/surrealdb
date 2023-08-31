//! Stores a DEFINE TABLE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Tb<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tb: &'a str,
}

pub fn new(ns: u32, db: u32, tb: &str) -> Tb {
	Tb::new(ns, db, tb)
}

pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'b', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'b', 0xff]);
	k
}

impl<'a> Tb<'a> {
	pub fn new(ns: u32, db: u32, tb: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'b',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tb::new(
			1,
			2,
			"testtb",
		);
		let enc = Tb::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tbtesttb\0");

		let dec = Tb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
