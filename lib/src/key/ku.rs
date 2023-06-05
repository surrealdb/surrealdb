use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ku<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub user: &'a str,
}

pub fn new<'a>(user: &'a str) -> Ku<'a> {
	Ku::new(user)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'k', b'u', 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'k', b'u', 0xff]);
	k
}

impl<'a> Ku<'a> {
	pub fn new(user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'k',
			_c: b'u',
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
		let val = Ku::new(
			"test",
		);
		let enc = Ku::encode(&val).unwrap();
		let dec = Ku::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
