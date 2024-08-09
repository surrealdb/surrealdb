//! Stores a DEFINE USER ON ROOT config definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Us<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub user: &'a str,
}

pub fn new(user: &str) -> Us<'_> {
	Us::new(user)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'u', b's', 0xff]);
	k
}

impl Categorise for Us<'_> {
	fn categorise(&self) -> Category {
		Category::User
	}
}

impl<'a> Us<'a> {
	pub fn new(user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'u',
			_c: b's',
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
		let val = Us::new("testuser");
		let enc = Us::encode(&val).unwrap();
		assert_eq!(enc, b"/!ustestuser\x00");
		let dec = Us::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix();
		assert_eq!(val, b"/!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix();
		assert_eq!(val, b"/!us\xff");
	}
}
