//! Stores a DEFINE USER ON NAMESPACE config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{self, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Us<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: &'a str,
}

impl KVKey for Us<'_> {
	type ValueType = catalog::UserDefinition;
}

pub fn new(ns: NamespaceId, user: &str) -> Us<'_> {
	Us::new(ns, user)
}

pub fn prefix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!us\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId) -> Result<Vec<u8>> {
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
	pub fn new(ns: NamespaceId, user: &'a str) -> Self {
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
			NamespaceId(1),
			"testuser",
		);
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!ustestuser\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!us\xff");
	}
}
