//! Stores a DEFINE TOKEN ON NAMESPACE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Tk<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
	pub tk: &'a str,
}

pub fn new(ns: u32, tk: &str) -> Tk {
	Tk::new(ns, tk)
}

pub fn prefix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0x00]);
	k
}

pub fn suffix(ns: u32) -> Vec<u8> {
	let mut k = super::all::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b'k', 0xff]);
	k
}

impl<'a> Tk<'a> {
	pub fn new(ns: u32, tk: &'a str) -> Self {
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
			1,
			"testtk",
		);
		let enc = Tk::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!tktesttk\0");
		let dec = Tk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
