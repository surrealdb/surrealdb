//! Stores a DEFINE ACCESS ON ROOT configuration
use serde::{Deserialize, Serialize};

use crate::catalog::AccessDefinition;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AccessKey<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ac: &'a str,
}

impl KVKey for AccessKey<'_> {
	type ValueType = AccessDefinition;
}

pub fn new(ac: &str) -> AccessKey<'_> {
	AccessKey::new(ac)
}

pub fn prefix() -> Vec<u8> {
	let mut k = crate::key::root::all::kv();
	k.extend_from_slice(b"!ac\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = crate::key::root::all::kv();
	k.extend_from_slice(b"!ac\xff");
	k
}

impl Categorise for AccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::Access
	}
}

impl<'a> AccessKey<'a> {
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AccessKey::new("testac");
		let enc = AccessKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!actestac\x00");
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
