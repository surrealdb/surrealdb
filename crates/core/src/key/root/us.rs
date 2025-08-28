//! Stores a DEFINE USER ON ROOT config definition
use serde::{Deserialize, Serialize};

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Us<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub user: &'a str,
}

impl KVKey for Us<'_> {
	type ValueType = catalog::UserDefinition;
}

pub fn new(user: &str) -> Us<'_> {
	Us::new(user)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::all::kv();
	k.extend_from_slice(b"!us\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::all::kv();
	k.extend_from_slice(b"!us\xff");
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Us::new("testuser");
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!ustestuser\x00");
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
