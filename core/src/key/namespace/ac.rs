//! Stores a DEFINE ACCESS ON NAMESPACE config definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ac<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub ac: &'a str,
}

pub fn new<'a>(ns: &'a str, ac: &'a str) -> Ac<'a> {
	Ac::new(ns, ac)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'c', 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'a', b'c', 0xff]);
	k
}

impl Categorise for Ac<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccess
	}
}

impl<'a> Ac<'a> {
	pub fn new(ns: &'a str, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'a',
			_d: b'c',
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
		let val = Ac::new(
			"testns",
			"testac",
		);
		let enc = Ac::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!actestac\0");
		let dec = Ac::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
