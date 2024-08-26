//! Stores a grant associated with an access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ac: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub gr: &'a str,
}

pub fn new<'a>(ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ac, gr)
}

pub fn prefix(ac: &str) -> Vec<u8> {
	let mut k = super::all::new(ac).encode().unwrap();
	k.extend_from_slice(b"!gr\x00");
	k
}

pub fn suffix(ac: &str) -> Vec<u8> {
	let mut k = super::all::new(ac).encode().unwrap();
	k.extend_from_slice(b"!gr\xff");
	k
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::AccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'&',
			ac,
			_b: b'!',
			_c: b'g',
			_d: b'r',
			gr,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Gr::new(
			"testac",
			"testgr",
		);
		let enc = Gr::encode(&val).unwrap();
		assert_eq!(enc, b"/&testac\0!grtestgr\0");

		let dec = Gr::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testac");
		assert_eq!(val, b"/&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testac");
		assert_eq!(val, b"/&testac\0!gr\xff");
	}
}
