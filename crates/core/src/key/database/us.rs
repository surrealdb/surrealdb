//! Stores a DEFINE USER ON DATABASE config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Us<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub user: &'a str,
}

impl KVKey for Us<'_> {
	type ValueType = catalog::UserDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId, user: &str) -> Us<'_> {
	Us::new(ns, db, user)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!us\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!us\xff");
	Ok(k)
}

impl Categorise for Us<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseUser
	}
}

impl<'a> Us<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, user: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'u',
			_e: b's',
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
			DatabaseId(2),
			"testuser",
		);
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ustestuser\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!us\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!us\xff");
	}
}
