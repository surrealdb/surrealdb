//! Stores a DEFINE NAMESPACE config definition
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
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
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0xff]);
	k
}

impl Categorise for Ns<'_> {
	fn categorise(&self) -> Category {
		Category::Namespace
	}
}

impl<'a> Ns<'a> {
	pub fn new(ns: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b's',
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
            "testns",
        );
		let enc = Ns::encode(&val).unwrap();
		assert_eq!(enc, b"/!nstestns\0");

		let dec = Ns::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
