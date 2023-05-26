use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nu<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: &'a str,
}

pub fn new<'a>(ns: &'a str, user: &'a str) -> Nu<'a> {
	Nu::new(ns, user)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b'u', 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b'u', 0xff]);
	k
}

impl<'a> Nu<'a> {
	pub fn new(ns: &'a str, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'n',
			_d: b'u',
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
		let val = Nu::new(
			"test",
			"test",
		);
		let enc = Nu::encode(&val).unwrap();
		let dec = Nu::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
