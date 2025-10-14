//! Stores a DEFINE USER ON NAMESPACE config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{self, NamespaceId};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct UserDefinitionKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub user: Cow<'a, str>,
}

impl_kv_key_storekey!(UserDefinitionKey<'_> => catalog::UserDefinition);

pub fn new(ns: NamespaceId, user: &str) -> UserDefinitionKey<'_> {
	UserDefinitionKey::new(ns, user)
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

impl<'a> UserDefinitionKey<'a> {
	pub fn new(ns: NamespaceId, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'u',
			_d: b's',
			user: Cow::Borrowed(user),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = UserDefinitionKey::new(
			NamespaceId(1),
			"testuser",
		);
		let enc = UserDefinitionKey::encode_key(&val).unwrap();
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
