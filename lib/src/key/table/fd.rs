/// Stores a DEFINE FIELD config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Fd<'a> {
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
	pub fd: &'a str,
}

pub fn new(ns: u32, db: u32, tb: u32, fd: &str) -> Fd {
	Fd::new(ns, db, tb, fd)
}

pub fn prefix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'f', b'd', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'f', b'd', 0xff]);
	k
}

impl<'a> Fd<'a> {
	pub fn new(ns: u32, db: u32, tb: u32, fd: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'f',
			_f: b'd',
			fd,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Fd::new(
			1,
			2,
			3,
			"testfd",
		);
		let enc = Fd::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03!fdtestfd\x00");

		let dec = Fd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(1, 2, 3);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03!fd\x00");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(1, 2, 3);
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03!fd\xff");
	}
}
