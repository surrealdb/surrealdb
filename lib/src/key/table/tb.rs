//! Stores a DEFINE TABLE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Tb {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	_e: u8,
	_f: u8,
}

pub fn new(ns: u32, db: u32, tb: u32) -> Tb {
	Tb::new(ns, db, tb)
}

impl Tb {
	pub fn new(ns: u32, db: u32, tb: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b't',
			_f: b'b',
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
			3,
		);
		let enc = Tb::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tb\x00\x00\x00\x03");

		let dec = Tb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
