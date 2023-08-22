//! Stores a DEFINE LOGIN ON NAMESPACE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lg<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
	pub us: &'a str,
}

pub fn new(ns: u32, us: &str) -> Lg {
	Lg::new(ns, us)
}

pub fn prefix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'g', 0x00]);
	k
}

pub fn suffix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'g', 0xff]);
	k
}

impl<'a> Lg<'a> {
	pub fn new(ns: u32, us: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'l',
			_d: b'g',
			us,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lg::new(
			123,
			"testus",
		);
		let enc = Lg::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!lgtestus\0");

		let dec = Lg::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
