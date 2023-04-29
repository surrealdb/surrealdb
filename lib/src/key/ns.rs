use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ns<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: &'a str,
}

pub fn new(ns: &str) -> Ns<'_> {
	Ns::new(ns)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6e, 0x73, 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6e, 0x73, 0xff]);
	k
}

impl<'a> Ns<'a> {
	pub fn new(ns: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x21, // !
			_b: 0x6e, // n
			_c: 0x73, // s
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ns::new(
			"test",
		);
		let enc = Ns::encode(&val).unwrap();
		let dec = Ns::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
