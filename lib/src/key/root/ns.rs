//! Stores a DEFINE NAMESPACE config definition
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use crate::kvs::KeyStack;
use derive::Key;
use serde::{Deserialize, Serialize};

const SIZE: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
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

pub fn prefix() -> KeyStack<SIZE> {
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0x00]);
	KeyStack::<SIZE>::from(k)
}

pub fn suffix() -> KeyStack<SIZE> {
	let mut k = super::all::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0xff]);
	KeyStack::<SIZE>::from(k)
}

impl KeyRequirements for Ns<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::Namespace
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
