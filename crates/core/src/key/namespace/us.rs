//! Stores a DEFINE USER ON NAMESPACE config definition
use crate::expr::statements::define::DefineUserStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Us<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: &'a str,
}

impl KVKey for Us<'_> {
	type ValueType = DefineUserStatement;
}

pub fn new<'a>(ns: &'a str, user: &'a str) -> Us<'a> {
	Us::new(ns, user)
}

pub fn prefix(ns: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!us\x00");
	Ok(k)
}

pub fn suffix(ns: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!us\xff");
	Ok(k)
}

impl Categorise for Us<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceUser
	}
}

impl<'a> Us<'a> {
	pub fn new(ns: &'a str, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'u',
			_d: b's',
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
		let val = Us::new(
			"testns",
			"testuser",
		);
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00!ustestuser\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!us\xff");
	}
}
