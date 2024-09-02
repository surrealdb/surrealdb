//! Stores a DEFINE ACCESS ON ROOT configuration
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ac<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ac: &'a str,
}

pub fn new(ac: &str) -> Ac<'_> {
	Ac::new(ac)
}

pub fn prefix() -> Vec<u8> {
	let mut k = crate::key::root::all::new().encode().unwrap();
	k.extend_from_slice(b"!ac\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = crate::key::root::all::new().encode().unwrap();
	k.extend_from_slice(b"!ac\xff");
	k
}

impl Categorise for Ac<'_> {
	fn categorise(&self) -> Category {
		Category::Access
	}
}

impl<'a> Ac<'a> {
	pub fn new(ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'a',
			_c: b'c',
			ac,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ac::new("testac");
		let enc = Ac::encode(&val).unwrap();
		assert_eq!(enc, b"/!actestac\x00");
		let dec = Ac::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix();
		assert_eq!(val, b"/!ac\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix();
		assert_eq!(val, b"/!ac\xff");
	}
}
