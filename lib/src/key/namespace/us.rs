use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Us<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: &'a str,
}

pub fn new(ns: u32, user: &str) -> Us {
	Us::new(ns, user)
}

pub fn prefix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0x00]);
	k
}

pub fn suffix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0xff]);
	k
}

impl<'a> Us<'a> {
	pub fn new(ns: u32, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'u',
			_d: b's',
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
		let val = Us::new(
			123,
			"testuser",
		);
		let enc = Us::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00!ustestuser\x00");
		let dec = Us::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(123);
		assert_eq!(val, b"/*testns\0!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(123);
		assert_eq!(val, b"/*testns\0!us\xff");
	}
}
