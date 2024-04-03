use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Us<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: &'a str,
}

pub fn new<'a>(ns: &'a str, user: &'a str) -> Us<'a> {
	Us::new(ns, user)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0xff]);
	k
}

impl KeyRequirements for Us<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::NamespaceUser
	}
}

impl<'a> Us<'a> {
	pub fn new(ns: &'a str, user: &'a str) -> Self {
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
			"testns",
			"testuser",
		);
		let enc = Us::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00!ustestuser\x00");
		let dec = Us::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns");
		assert_eq!(val, b"/*testns\0!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns");
		assert_eq!(val, b"/*testns\0!us\xff");
	}
}
