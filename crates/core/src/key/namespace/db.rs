//! Stores a DEFINE DATABASE config definition
use crate::catalog::DatabaseId;
use crate::catalog::NamespaceId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Db {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: DatabaseId,
}
impl_key!(Db);

pub fn new(ns: NamespaceId, db: DatabaseId) -> Db {
	Db::new(ns, db)
}

pub fn prefix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode()?;
	k.extend_from_slice(b"!db\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode()?;
	k.extend_from_slice(b"!db\xff");
	Ok(k)
}

impl Categorise for Db {
	fn categorise(&self) -> Category {
		Category::DatabaseAlias
	}
}

impl Db {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
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
	
	use crate::kvs::KeyDecode;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Db::new(
			NamespaceId(1),
			DatabaseId(2),
		);
		let enc = Db::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!dbtestdb\0");

		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*1\0!db\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*1\0!db\xff")
	}
}
