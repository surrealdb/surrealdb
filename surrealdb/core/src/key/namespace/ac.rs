//! Stores a DEFINE ACCESS ON NAMESPACE configuration
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{AccessDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceAccessKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub ac: Cow<'a, str>,
}

impl_kv_key_storekey!(NamespaceAccessKey<'_> => AccessDefinition);

pub fn new(ns: NamespaceId, ac: &str) -> NamespaceAccessKey<'_> {
	NamespaceAccessKey::new(ns, ac)
}

pub fn prefix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = crate::key::namespace::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!ac\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = crate::key::namespace::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!ac\xff");
	Ok(k)
}

impl Categorise for NamespaceAccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccess
	}
}

impl<'a> NamespaceAccessKey<'a> {
	pub fn new(ns: NamespaceId, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'a',
			_d: b'c',
			ac: Cow::Borrowed(ac),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = NamespaceAccessKey::new(NamespaceId(1), "testac");
		let enc = NamespaceAccessKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!actestac\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!ac\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!ac\xff");
	}
}
