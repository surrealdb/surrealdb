//! Stores a DEFINE ACCESS ON NAMESPACE configuration
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{AccessDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AccessKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub ac: &'a str,
}

impl KVKey for AccessKey<'_> {
	type ValueType = AccessDefinition;
}

pub fn new(ns: NamespaceId, ac: &str) -> AccessKey<'_> {
	AccessKey::new(ns, ac)
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

impl Categorise for AccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccess
	}
}

impl<'a> AccessKey<'a> {
	pub fn new(ns: NamespaceId, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'a',
			_d: b'c',
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
		let val = AccessKey::new(
			NamespaceId(1),
			"testac",
		);
		let enc = AccessKey::encode_key(&val).unwrap();
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
