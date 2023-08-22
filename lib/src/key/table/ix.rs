/// Stores a DEFINE INDEX config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ix<'a> {
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
	pub ix: &'a str,
}

pub fn new(ns: u32, db: u32, tb: u32, ix: &str) -> Ix {
	Ix::new(ns, db, tb, ix)
}

pub fn prefix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'i', b'x', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'i', b'x', 0xff]);
	k
}

impl<'a> Ix<'a> {
	pub fn new(ns: u32, db: u32, tb: u32, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'i',
			_f: b'x',
			ix,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ix::new(
			1,
			2,
			3,
			"testix",
		);
		let enc = Ix::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0!ixtestix\0");

		let dec = Ix::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
