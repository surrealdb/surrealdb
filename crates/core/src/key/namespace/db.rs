//! Stores a DEFINE DATABASE config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct DatabaseKey<'key> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: &'key str,
}

impl KVKey for DatabaseKey<'_> {
	type ValueType = DatabaseDefinition;
}

pub fn new(ns: NamespaceId, db: &str) -> DatabaseKey {
	DatabaseKey::new(ns, db)
}

pub fn prefix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!db\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!db\xff");
	Ok(k)
}

impl Categorise for DatabaseKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAlias
	}
}

impl<'key> DatabaseKey<'key> {
	pub fn new(ns: NamespaceId, db: &'key str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'b',
			db,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = DatabaseKey::new(
			NamespaceId(1),
			"test",
		);
		let enc = DatabaseKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!dbtest\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!db\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!db\xff")
	}
}
