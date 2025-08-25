//! Stores a DEFINE CONFIG definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{ConfigDefinition, DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Cg<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ty: &'a str,
}

impl KVKey for Cg<'_> {
	type ValueType = ConfigDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId, ty: &str) -> Cg<'_> {
	Cg::new(ns, db, ty)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0x00]);
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0xff]);
	Ok(k)
}

impl Categorise for Cg<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseConfig
	}
}

impl<'a> Cg<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ty: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'c',
			_e: b'g',
			ty,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Cg::new(
			NamespaceId(1),
			DatabaseId(2),
			"testty",
		);
		let enc = Cg::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cgtestty\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cg\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cg\xff");
	}
}
