//! Stores a DEFINE TOKEN ON NAMESPACE config definition
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Tk<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub tk: &'a str,
}

pub fn new<'a>(ns: &'a str, tk: &'a str) -> Tk<'a> {
	Tk::new(ns, tk)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0xff]);
	k
}

impl KeyRequirements for Tk<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::NamespaceToken
	}
}

impl<'a> Tk<'a> {
	pub fn new(ns: &'a str, tk: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b't',
			_d: b'k',
			tk,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tk::new(
			"testns",
			"testtk",
		);
		let enc = Tk::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!tktesttk\0");
		let dec = Tk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
